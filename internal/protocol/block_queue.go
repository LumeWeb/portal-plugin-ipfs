package protocol

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/gammazero/workerpool"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

const (
	maxRetries = 3 // Max retries for block processing

	// Bloom filter configuration
	blockQueueBloomFilterEstimateItems = 50000 // Realistic upper bound for most workloads
	blockQueueBloomFilterFalsePositive = 0.005 // 0.5% false positive rate
)

// blockJob represents a unit of work for processing a block.
type blockJob struct {
	Block blocks.Block
}

// reset clears all fields to prepare for reuse
func (j *blockJob) reset() {
	j.Block = nil
}

// blockJobPool is a pool of blockJob instances to reduce allocations.
var blockJobPool = sync.Pool{
	New: func() interface{} {
		return new(blockJob)
	},
}

// BlockQueue manages the processing of blocks from a CAR file.
type BlockQueue struct {
	proto      ProtoNode
	logger     *core.Logger
	wp         *workerpool.WorkerPool // Worker pool
	ctx        context.Context
	cancel     context.CancelFunc
	errorCount int32 // Atomic error counter

	processedFilter *bloom.BloomFilter // Bloom filter for quick existence checks
}

// NewBlockQueue creates a new BlockQueue instance.
func NewBlockQueue(coreCtx core.Context, proto ProtoNode, logger *core.Logger) *BlockQueue {
	ctx, cancel := context.WithTimeout(coreCtx.GetContext(), 30*time.Minute)

	// Initialize bloom filter
	processedFilter := bloom.NewWithEstimates(blockQueueBloomFilterEstimateItems, blockQueueBloomFilterFalsePositive)

	bp := &BlockQueue{
		proto:           proto,
		logger:          logger,
		wp:              workerpool.New(1), // Worker pool for concurrent block processing
		ctx:             ctx,
		cancel:          cancel,
		processedFilter: processedFilter,
	}

	return bp
}

// queueBlock queues a block for processing.
func (bp *BlockQueue) queueBlock(block blocks.Block) error {
	select {
	case <-bp.ctx.Done():
		return bp.ctx.Err()
	default:
		job := blockJobPool.Get().(*blockJob)
		job.Block = block

		bp.wp.SubmitWait(func() {
			err := bp.processBlock(bp.ctx, job)
			if err != nil {
				bp.logger.Error("Block processing failed", zap.Error(err), zap.String("CID", job.Block.Cid().String()))
				bp.handleError(err)
			}
			job.reset()
			blockJobPool.Put(job)
		})
		return nil
	}
}

// processBlock processes a single block.
func (bp *BlockQueue) processBlock(ctx context.Context, job *blockJob) error {
	ctx, span := core.TraceMethod(ctx, "BlockQueue.processBlock")
	defer span.End()

	// Check bloom filter to skip already processed blocks
	cidStr := job.Block.Cid().String()
	if bp.processedFilter.Test([]byte(cidStr)) {
		// Bloom filter hit - verify with authoritative blockstore to avoid false positives
		hasBlock, err := bp.proto.GetNode().HasBlock(ctx, job.Block.Cid())
		if err == nil && hasBlock {
			bp.logger.Debug("Block confirmed as processed, skipping", zap.String("CID", cidStr))
			return nil
		}
		bp.logger.Debug("Bloom filter false positive, proceeding with processing", zap.String("CID", cidStr))
	}

	// Retry logic
	var err error
	for i := 0; i < maxRetries; i++ {
		err = bp.processBlockInternal(ctx, job)
		if err == nil {
			return nil
		}
		bp.logger.Warn("Retrying block processing", zap.Error(err), zap.Int("attempt", i+1), zap.String("CID", job.Block.Cid().String()))
		time.Sleep(time.Second) // Backoff delay between retries
	}
	return err
}

func (bp *BlockQueue) processBlockInternal(ctx context.Context, job *blockJob) error {
	ctx, span := core.TraceMethod(ctx, "BlockQueue.processBlockInternal")
	defer span.End()

	bp.logger.Debug("Processing block", zap.String("CID", job.Block.Cid().String()))

	// Import block into IPFS node
	err := bp.proto.GetNode().AddBlock(ctx, job.Block)
	if err != nil {
		return fmt.Errorf("failed to add block: %w", err)
	}

	// Update bloom filter to mark block as processed
	bp.processedFilter.Add([]byte(job.Block.Cid().String()))

	return nil
}

// handleError logs the error and cancels the context.
func (bp *BlockQueue) handleError(err error) {
	bp.logger.Error("Critical error occurred", zap.Error(err))
	bp.cancel() // Cancel processing on critical errors
}

// ProcessBlocks processes blocks using a BlockProcessor to provide blocks and BlockQueue to process them.
func ProcessBlocks(ctx core.Context, processor BlockProcessor) ([]cid.Cid, []cid.Cid, error) {
	protoInterface := core.GetProtocol(internal.ProtocolName)
	if protoInterface == nil {
		return nil, nil, fmt.Errorf("protocol %s not found", internal.ProtocolName)
	}
	proto, ok := protoInterface.(ProtoNode)
	if !ok {
		return nil, nil, fmt.Errorf("protocol has unexpected type, expected ProtoNode")
	}
	logger := ctx.Logger()

	errChan := make(chan error, 100) // Buffered channel for error handling

	// Initialize BlockQueue for block processing
	bp := NewBlockQueue(ctx, proto, logger)
	if bp == nil {
		return nil, nil, fmt.Errorf("failed to create block queue")
	}
	defer bp.release()

	// Process blocks from processor concurrently
	var wg sync.WaitGroup

	for {
		block, err := processor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get next block: %w", err)
		}

		wg.Add(1)
		go func(b blocks.Block) {
			defer func() {
				if r := recover(); r != nil {
					errChan <- fmt.Errorf("panic in block processing: %v", r)
				}
				wg.Done()
			}()

			if err := bp.queueBlock(b); err != nil {
				if !isContextCanceled(err) {
					errChan <- fmt.Errorf("failed to queue block %s: %w", b.Cid().String(), err)
					return
				}
				return
			}

			// Notify processor of completion
			processor.Done(b.Cid())
		}(block)
	}

	// Wait for concurrent processing to complete
	wg.Wait()

	// Check for processing errors
	select {
	case err := <-errChan:
		return nil, nil, fmt.Errorf("block processing failed: %w", err)
	default:
		// No errors detected
	}

	if err := bp.ctx.Err(); err != nil {
		return nil, nil, fmt.Errorf("block processing failed: %w", err)
	}

	return processor.GetDoneCIDs(), processor.Roots(), nil
}

// release releases the resources used by the BlockQueue.
func (bp *BlockQueue) release() {
	bp.cancel()
	bp.wp.Stop()
}

// isContextCanceled checks if the context has been canceled.
func isContextCanceled(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}
