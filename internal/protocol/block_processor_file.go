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
		logger.Debug("NewFileBlockProcessorWithPath() called", zap.String("filePath", filePath))
	}

	processor, err := NewFileBlockProcessorWithDefaults(ctx, blockstore, fileReader, dagService, nodeGenerator, logger, NewDoneTracker())
	if err != nil {
		return nil, err
	}

	processor.filePath = filePath

	if logger != nil {
		logger.Debug("NewFileBlockProcessorWithPath(): created successfully", zap.String("filePath", filePath))
	}

	return processor, nil
}

// NewFileBlockProcessorWithDefaults creates a new FileBlockProcessor with the given dependencies and shared DoneTracker
func NewFileBlockProcessorWithDefaults(ctx context.Context, blockstore StreamingBlockstore, fileReader io.ReadSeekCloser, dagService format.DAGService, nodeGenerator upload.UnixFSNodeGenerator, logger *core.Logger, doneTracker DoneTracker) (*FileBlockProcessor, error) {
	if logger != nil {
		logger.Debug("NewFileBlockProcessorWithDefaults() called")
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
		logger.Debug("NewFileBlockProcessorWithDefaults(): created successfully")
	}

	return processor, nil
}

// Next implements BlockProcessor interface
func (fp *FileBlockProcessor) Next() (blocks.Block, error) {
	fp.GetLogger().Debug("FileBlockProcessor.Next() called")

	// Check if processor is closed
	if fp.isClosed() {
		fp.GetLogger().Debug("FileBlockProcessor.Next(): processor is closed")
		return nil, fmt.Errorf("processor is closed")
	}

	// Start processing if not already started
	if !fp.isStarted() {
		fp.GetLogger().Debug("FileBlockProcessor.Next(): starting file processing")
		fp.markStarted()
		fp.startFileProcessing()
	}

	fp.GetLogger().Debug("FileBlockProcessor.Next(): reading from block stream")
	// Read next block from the streaming datastore
	select {
	case entry, ok := <-fp.blockstore.GetBlockStream(fp.GetContext()):
		if !ok {
			fp.GetLogger().Debug("FileBlockProcessor.Next(): block stream channel closed")
			// Channel closed, check for errors
			select {
			case err := <-fp.errorChan:
				fp.GetLogger().Debug("FileBlockProcessor.Next(): error from error channel", zap.Error(err))
				return nil, err
			default:
				fp.GetLogger().Debug("FileBlockProcessor.Next(): returning io.EOF")
				return nil, io.EOF
			}
		}

		fp.GetLogger().Debug("FileBlockProcessor.Next(): got block from stream", zap.String("cid", entry.Block.Cid().String()))
		// Mark block as processed in datastore
		fp.blockstore.MarkBlockProcessed(entry.Key.String())
		return entry.Block, nil

	case <-fp.GetContext().Done():
		fp.GetLogger().Debug("FileBlockProcessor.Next(): context done", zap.Error(fp.GetContext().Err()))
		return nil, fp.GetContext().Err()

	case err := <-fp.errorChan:
		fp.GetLogger().Debug("FileBlockProcessor.Next(): error from error channel", zap.Error(err))
		return nil, err
	}
}

// Roots implements BlockProcessor interface
func (fp *FileBlockProcessor) Roots() []cid.Cid {
	roots := fp.getRootCIDs()
	if fp.GetLogger() != nil {
		fp.GetLogger().Debug("FileBlockProcessor.Roots() called",
			zap.Int("rootsCount", len(roots)),
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
	if fp.GetLogger() != nil {
		fp.GetLogger().Debug("FileBlockProcessor.startFileProcessing() called")
	}
	fp.startBackgroundGoroutine(func() error {
		err := fp.processFile(fp.GetContext())
		if fp.GetLogger() != nil {
			if err != nil {
				fp.GetLogger().Error("FileBlockProcessor.startFileProcessing(): processing failed", zap.Error(err))
			} else {
				fp.GetLogger().Debug("FileBlockProcessor.startFileProcessing(): processing completed successfully")
			}
		}
		return err
	})
}

// processFile processes the file by passing it directly to the UnixFS node generator
func (fp *FileBlockProcessor) processFile(ctx context.Context) error {
	fp.GetLogger().Debug("FileBlockProcessor.processFile() starting",
		zap.String("file_path", fp.filePath))

	// Ensure the file is seekable
	seekableFile := upload.NewUniversalReader(fp.fileReader)

	// Create the UnixFS node from the file using injected dependencies
	node, err := fp.nodeGenerator.CreateNode(ctx, seekableFile)
	if err != nil {
		fp.GetLogger().Error("FileBlockProcessor.processFile(): failed to create node", zap.Error(err))
		return err
	}

	fp.GetLogger().Debug("FileBlockProcessor.processFile(): created UnixFS node",
		zap.String("cid", node.Cid().String()))

	// Store the node in the blockstore via injected dagService
	if err := fp.dagService.Add(ctx, node); err != nil {
		fp.GetLogger().Error("FileBlockProcessor.processFile(): failed to add node to DAG service", zap.Error(err))
		return err
	}

	fp.GetLogger().Debug("FileBlockProcessor.processFile(): added node to DAG service")

	// Mark processing as completed with root CID
	fp.markCompleted([]cid.Cid{node.Cid()})

	// Signal that processing is complete
	fp.blockstore.ProcessingDone()

	// Mark root CID as done in datastore
	fp.blockstore.MarkDone(node.Cid())

	fp.GetLogger().Info("File processed successfully",
		zap.String("file_path", fp.filePath),
		zap.String("root_cid", node.Cid().String()))

	return nil
}

// Release implements BlockProcessor interface
func (fp *FileBlockProcessor) Release() {
	if fp.GetLogger() != nil {
		fp.GetLogger().Debug("FileBlockProcessor.Release() called",
			zap.String("file_path", fp.filePath))
	}

	// Close the file reader if it's still open
	if fp.fileReader != nil {
		if err := fp.fileReader.Close(); err != nil && fp.GetLogger() != nil {
			fp.GetLogger().Warn("Failed to close file reader",
				zap.String("file_path", fp.filePath),
				zap.Error(err))
		}
		fp.fileReader = nil
	}

	// Close the base processor (handles cleanup, context cancellation, etc.)
	fp.Close()

	// Close the blockstore
	if fp.blockstore != nil {
		if err := fp.blockstore.Close(); err != nil && fp.GetLogger() != nil {
			fp.GetLogger().Warn("Failed to close blockstore", zap.Error(err))
		}
	}

	if fp.GetLogger() != nil {
		fp.GetLogger().Debug("FileBlockProcessor.Release() completed")
	}
}

// GetFilePath returns the file path if available
func (fp *FileBlockProcessor) GetFilePath() string {
	return fp.filePath
}
