package protocol

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	format "github.com/ipfs/go-ipld-format"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// FileBlockProcessor implements BlockProcessor for individual files
// It uses the StreamingBlockstore pattern for coordination and passes files directly to UnixFSNodeGenerator
type FileBlockProcessor struct {
	*BaseBlockProcessor

	// Archive blockstore for pipeline coordination (shared with ArchiveBlockProcessor)
	blockstore StreamingBlockstore

	// IPFS processing components (injected via constructor)
	dagService    format.DAGService
	nodeGenerator upload.UnixFSNodeGenerator

	// File management
	fileReader io.ReadSeekCloser
	filePath   string
}

// NewFileBlockProcessor creates a new FileBlockProcessor with the given dependencies
func NewFileBlockProcessor(ctx context.Context, blockstore StreamingBlockstore, fileReader io.ReadSeekCloser, dagService format.DAGService, nodeGenerator upload.UnixFSNodeGenerator, logger *core.Logger) (*FileBlockProcessor, error) {
	return NewFileBlockProcessorWithDefaults(ctx, blockstore, fileReader, dagService, nodeGenerator, logger, NewDoneTracker())
}

// NewFileBlockProcessorWithPath creates a new FileBlockProcessor with file path metadata
func NewFileBlockProcessorWithPath(ctx context.Context, blockstore StreamingBlockstore, fileReader io.ReadSeekCloser, filePath string, dagService format.DAGService, nodeGenerator upload.UnixFSNodeGenerator, logger *core.Logger) (*FileBlockProcessor, error) {
	if logger != nil {
		logger.Debug("Creating FileBlockProcessor with file path", zap.String("filePath", filePath))
	}

	processor, err := NewFileBlockProcessorWithDefaults(ctx, blockstore, fileReader, dagService, nodeGenerator, logger, NewDoneTracker())
	if err != nil {
		return nil, err
	}

	processor.filePath = filePath

	if logger != nil {
		logger.Debug("FileBlockProcessor created successfully", zap.String("filePath", filePath))
	}

	return processor, nil
}

// NewFileBlockProcessorWithDefaults creates a new FileBlockProcessor with the given dependencies and shared DoneTracker
func NewFileBlockProcessorWithDefaults(ctx context.Context, blockstore StreamingBlockstore, fileReader io.ReadSeekCloser, dagService format.DAGService, nodeGenerator upload.UnixFSNodeGenerator, logger *core.Logger, doneTracker DoneTracker) (*FileBlockProcessor, error) {
	if logger != nil {
		logger.Debug("Creating FileBlockProcessor with default settings")
	}

	if blockstore == nil {
		return nil, ds.ErrNotFound
	}
	if fileReader == nil {
		return nil, io.EOF
	}
	if dagService == nil {
		return nil, fmt.Errorf("dagService is required")
	}
	if nodeGenerator == nil {
		return nil, fmt.Errorf("nodeGenerator is required")
	}

	// Create base processor with shared DoneTracker
	base := NewBaseBlockProcessorWithDefaults(ctx, logger, doneTracker)

	processor := &FileBlockProcessor{
		BaseBlockProcessor: base,
		blockstore:         blockstore,
		dagService:         dagService,
		nodeGenerator:      nodeGenerator,
		fileReader:         fileReader,
		filePath:           "",
	}

	if logger != nil {
		logger.Debug("FileBlockProcessor with defaults created successfully")
	}

	return processor, nil
}

// Next implements BlockProcessor interface
func (fp *FileBlockProcessor) Next() (blocks.Block, error) {
	logger := fp.GetLogger()
	logger.Debug("Requesting next block from FileBlockProcessor")

	// Check if processor is closed
	if fp.isClosed() {
		logger.Debug("Cannot retrieve block: processor is closed")
		return nil, fmt.Errorf("processor is closed")
	}

	// Check if context is already cancelled before starting processing
	if err := fp.GetContext().Err(); err != nil {
		logger.Debug("Cannot retrieve block: context cancelled", zap.Error(err))
		return nil, err
	}

	// Start processing if not already started
	if !fp.isStarted() {
		logger.Debug("Starting file processing pipeline")
		fp.markStarted()
		fp.startFileProcessing()
	}

	logger.Debug("Waiting for next block from processing stream")
	// Read next block from the streaming datastore
	select {
	case entry, ok := <-fp.blockstore.GetBlockStream(fp.GetContext()):
		if !ok {
			logger.Debug("Block processing stream has ended")
			// Channel closed, check for errors
			select {
			case err := <-fp.errorChan:
				logger.Debug("Processing error detected", zap.Error(err))
				return nil, err
			default:
				logger.Debug("All blocks processed, returning end of stream")
				return nil, io.EOF
			}
		}

		logger.Debug("Retrieved block from stream", zap.String("cid", entry.Block.Cid().String()))
		// Mark block as processed in datastore
		fp.blockstore.MarkBlockProcessed(entry.Key.String())
		return entry.Block, nil

	case <-fp.GetContext().Done():
		logger.Debug("Processing cancelled due to context", zap.Error(fp.GetContext().Err()))
		return nil, fp.GetContext().Err()

	case err := <-fp.errorChan:
		logger.Debug("Processing error detected", zap.Error(err))
		return nil, err
	}
}

// Roots implements BlockProcessor interface
func (fp *FileBlockProcessor) Roots() []cid.Cid {
	roots := fp.getRootCIDs()
	if logger := fp.GetLogger(); logger != nil {
		logger.Debug("Retrieving root CIDs from FileBlockProcessor",
			zap.Int("rootCount", len(roots)),
			zap.Strings("roots", func() []string {
				rootStrings := make([]string, len(roots))
				for i, r := range roots {
					rootStrings[i] = r.String()
				}
				return rootStrings
			}()))
	}
	return roots
}

// startFileProcessing begins the background processing of the file
func (fp *FileBlockProcessor) startFileProcessing() {
	if logger := fp.GetLogger(); logger != nil {
		logger.Debug("Starting background file processing")
	}
	fp.startBackgroundGoroutine(func() error {
		err := fp.processFile(fp.GetContext())
		if logger := fp.GetLogger(); logger != nil {
			if err != nil {
				logger.Error("File processing failed in background", zap.Error(err))
			} else {
				logger.Debug("Background file processing completed successfully")
			}
		}
		return err
	})
}

// processFile processes the file by passing it directly to the UnixFS node generator
func (fp *FileBlockProcessor) processFile(ctx context.Context) error {
	ctx, span := core.TraceMethod(ctx, "FileBlockProcessor.processFile")
	defer span.End()

	logger := fp.GetLogger()
	logger.Debug("Processing file for IPFS storage",
		zap.String("filePath", fp.filePath))

	// Ensure the file is seekable
	seekableFile := upload.NewUniversalReader(fp.fileReader)

	// Create the UnixFS node from the file using injected dependencies
	node, err := fp.nodeGenerator.CreateNode(ctx, seekableFile)
	if err != nil {
		logger.Error("Failed to create UnixFS node from file", zap.Error(err))
		return err
	}

	logger.Debug("Successfully created UnixFS node",
		zap.String("cid", node.Cid().String()))

	// Store the node in the blockstore via injected dagService
	if err := fp.dagService.Add(ctx, node); err != nil {
		logger.Error("Failed to store UnixFS node in DAG service", zap.Error(err))
		return err
	}

	logger.Debug("UnixFS node stored successfully in DAG service")

	// Mark processing as completed with root CID
	fp.markCompleted([]cid.Cid{node.Cid()})

	// Signal that processing is complete
	fp.blockstore.ProcessingDone()

	// Mark root CID as done in datastore
	fp.blockstore.MarkDone(node.Cid())

	logger.Info("File processed successfully",
		zap.String("file_path", fp.filePath),
		zap.String("root_cid", node.Cid().String()))

	return nil
}

// Release implements BlockProcessor interface
func (fp *FileBlockProcessor) Release() {
	if fp.GetLogger() != nil {
		fp.GetLogger().Debug("Releasing FileBlockProcessor resources",
			zap.String("filePath", fp.filePath))
	}

	// Close the file reader if it's still open
	if fp.fileReader != nil {
		if err := fp.fileReader.Close(); err != nil {
			if logger := fp.GetLogger(); logger != nil {
				logger.Warn("Failed to close file reader",
					zap.String("file_path", fp.filePath),
					zap.Error(err))
			}
		}
		fp.fileReader = nil
	}

	// Close the base processor (handles cleanup, context cancellation, etc.)
	fp.Close()

	// Close the blockstore
	if fp.blockstore != nil {
		if err := fp.blockstore.Close(); err != nil {
			if logger := fp.GetLogger(); logger != nil {
				logger.Warn("Failed to close blockstore", zap.Error(err))
			}
		}
	}

	if fp.GetLogger() != nil {
		fp.GetLogger().Debug("FileBlockProcessor resources released successfully")
	}
}

// GetFilePath returns the file path if available
func (fp *FileBlockProcessor) GetFilePath() string {
	return fp.filePath
}
