package protocol

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	pluginUpload "go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
	contentArchive "go.lumeweb.com/ipfs-content/archive"
)

// StreamingBlockstore defines the interface for streaming archive datastores
// This includes the standard blockstore interface plus archive-specific methods
type StreamingBlockstore interface {
	blockstore.Blockstore
	GetBlockStream(ctx context.Context) <-chan *BlockEntry
	MarkBlockProcessed(blockKey string)
	MarkBlockPersisted(c cid.Cid)
	MarkDone(c cid.Cid)
	WaitDone(ctx context.Context, c cid.Cid) bool
	Done(c cid.Cid)
	GetPendingCount() int
	GetProcessedCount() int
	IsClosed() bool
	ProcessingDone() // Signal that all processing is complete, stop accepting new submissions
	Close() error
}

// ArchiveBlockProcessor implements BlockProcessor for archive files (tar, zip, etc.)
type ArchiveBlockProcessor struct {
	*BaseBlockProcessor

	// Archive blockstore for pipeline coordination
	blockstore StreamingBlockstore

	// Archive extraction components
	extractor contentArchive.ArchiveExtractor

	// Streaming processor for UnixFS node generation
	streamProcessor pluginUpload.StreamingArchiveProcessor
}

// NewArchiveBlockProcessor creates a new ArchiveBlockProcessor with dependency injection
func NewArchiveBlockProcessor(ctx context.Context, blockstore StreamingBlockstore, extractor contentArchive.ArchiveExtractor, streamProcessor pluginUpload.StreamingArchiveProcessor, logger *core.Logger, doneTracker DoneTracker) (*ArchiveBlockProcessor, error) {
	if blockstore == nil {
		return nil, fmt.Errorf("blockstore is required")
	}
	if extractor == nil {
		return nil, fmt.Errorf("extractor is required")
	}
	if streamProcessor == nil {
		return nil, fmt.Errorf("stream processor is required")
	}

	// Create base processor with done tracker
	base := NewBaseBlockProcessorWithDefaults(ctx, logger, doneTracker)

	return &ArchiveBlockProcessor{
		BaseBlockProcessor: base,
		blockstore:         blockstore,
		extractor:          extractor,
		streamProcessor:    streamProcessor,
	}, nil
}

// Next implements BlockProcessor interface
func (ap *ArchiveBlockProcessor) Next() (blocks.Block, error) {
	// Check if processor is closed
	if ap.isClosed() {
		return nil, fmt.Errorf("processor is closed")
	}

	// Start processing if not already started
	if !ap.isStarted() {
		ap.markStarted()
		ap.startProcessing()
	}

	// Read next block from the streaming blockstore
	select {
	case entry, ok := <-ap.blockstore.GetBlockStream(ap.GetContext()):
		if !ok {
			// Channel closed, check for errors
			select {
			case err := <-ap.errorChan:
				return nil, err
			default:
				return nil, io.EOF
			}
		}

		// Mark block as processed in blockstore
		ap.blockstore.MarkBlockProcessed(entry.Key.String())
		return entry.Block, nil

	case <-ap.GetContext().Done():
		return nil, ap.GetContext().Err()

	case err := <-ap.errorChan:
		return nil, err
	}
}

// GetStreamingBlockstore returns the underlying StreamingBlockstore for callback wiring
func (ap *ArchiveBlockProcessor) GetStreamingBlockstore() StreamingBlockstore {
	return ap.blockstore
}

// Roots implements BlockProcessor interface
func (ap *ArchiveBlockProcessor) Roots() []cid.Cid {
	rootCIDs := ap.getRootCIDs()
	if rootCIDs != nil {
		return rootCIDs
	}

	// If not completed yet, try to get root from stream processor
	if ap.streamProcessor != nil {
		if rootNode, err := ap.streamProcessor.GetRootNode(ap.GetContext()); err == nil && rootNode != nil {
			return []cid.Cid{rootNode.Cid()}
		}
	}

	return nil
}

// Release implements BlockProcessor interface
func (ap *ArchiveBlockProcessor) Release() {
	// Close extractor
	if ap.extractor != nil {
		if err := ap.extractor.Close(); err != nil {
			if logger := ap.GetLogger(); logger != nil {
				logger.Error("Failed to close archive extractor", zap.Error(err))
			}
		}
	}

	// Close streaming processor
	if ap.streamProcessor != nil {
		if err := ap.streamProcessor.Close(); err != nil {
			if logger := ap.GetLogger(); logger != nil {
				logger.Error("Failed to close streaming processor", zap.Error(err))
			}
		}
	}

	// Close blockstore
	if ap.blockstore != nil {
		if err := ap.blockstore.Close(); err != nil {
			if logger := ap.GetLogger(); logger != nil {
				logger.Error("Failed to close archive blockstore", zap.Error(err))
			}
		}
	}

	// Close base processor (handles cleanup, context cancellation, error channel, etc.)
	ap.Close()
}

// startProcessing begins the background processing of the archive
func (ap *ArchiveBlockProcessor) startProcessing() {
	ap.startBackgroundGoroutine(func() error {
		// Process the archive using the streaming processor
		err := ap.streamProcessor.ProcessArchive(ap.GetContext(), ap.extractor)
		if err != nil {
			return fmt.Errorf("failed to process archive: %w", err)
		}

		// Get root node and set roots
		rootNode, err := ap.streamProcessor.GetRootNode(ap.GetContext())
		if err != nil {
			return fmt.Errorf("failed to get root node: %w", err)
		}

		// Mark processing as completed with root CIDs
		ap.markCompleted([]cid.Cid{rootNode.Cid()})

		// Signal that processing is complete
		ap.blockstore.ProcessingDone()

		if logger := ap.GetLogger(); logger != nil {
			logger.Info("Archive processed successfully",
				zap.String("root_cid", rootNode.Cid().String()))
		}

		return nil
	})
}
