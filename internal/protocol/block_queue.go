package protocol

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/workerpool"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

const (
	maxRetries = 3 // Max retries for block processing
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
	New: func() any {
		return new(blockJob)
	},
}

// resolveWorkerCount determines the number of workers for block processing.
// If ProcessingWorkers is configured > 0, use that.
// Otherwise, default to 2 * NumCPU capped at 16 — this is a conservative starting
// point that works on small VPSes without saturating I/O. Block processing is I/O-bound
// (S3 upload + DB write per block), so the real ceiling is downstream I/O capacity.
// Operators should tune ProcessingWorkers based on their S3/DB throughput.
func resolveWorkerCount(ctx core.Context) int {
	cfg := core.GetProtocolConfig[*pluginConfig.ProtocolConfig](ctx, internal.ProtocolName)

	if cfg.BlockStore.ProcessingWorkers > 0 {
		return cfg.BlockStore.ProcessingWorkers
	}

	workers := 2 * runtime.NumCPU()
	workers = min(max(workers, 4), 16)
	return workers
}

// BlockQueue manages the processing of blocks from a CAR file.
type BlockQueue struct {
	proto       ProtoNode
	logger      *core.Logger
	wp          *workerpool.WorkerPool
	ctx         context.Context
	cancel      context.CancelFunc
	errChan     chan error
	errOnce     sync.Once
	failedCount atomic.Int32
}

// NewBlockQueue creates a new BlockQueue instance.
func NewBlockQueue(coreCtx core.Context, proto ProtoNode, logger *core.Logger) *BlockQueue {
	ctx, cancel := context.WithTimeout(coreCtx.GetContext(), 30*time.Minute)

	workers := resolveWorkerCount(coreCtx)

	bp := &BlockQueue{
		proto:   proto,
		logger:  logger,
		wp:      workerpool.New(workers),
		ctx:     ctx,
		cancel:  cancel,
		errChan: make(chan error, 1),
	}

	logger.Debug("BlockQueue created", zap.Int("workers", workers))
	return bp
}

// queueBlock submits a block for async processing by the worker pool.
func (bp *BlockQueue) queueBlock(block blocks.Block) error {
	select {
	case <-bp.ctx.Done():
		return bp.ctx.Err()
	default:
	}

	job := blockJobPool.Get().(*blockJob)
	job.Block = block

	bp.wp.Submit(func() {
		defer func() {
			job.reset()
			blockJobPool.Put(job)
		}()

		if bp.ctx.Err() != nil {
			return
		}

		err := bp.processBlock(bp.ctx, job)
		if err != nil && !isContextCanceled(err) {
			bp.logger.Error("Block processing failed",
				zap.Error(err),
				zap.String("CID", job.Block.Cid().String()))
			bp.recordError(err)
		}
	})

	return nil
}

// processBlock processes a single block.
// No bloom filter — CAR files contain unique blocks, so dedup checks
// are pure overhead and false positives can cause silent data loss
// (skipping a block that hasn't actually been stored).
func (bp *BlockQueue) processBlock(ctx context.Context, job *blockJob) error {
	ctx, span := core.TraceMethod(ctx, "BlockQueue.processBlock")
	defer span.End()

	var err error
	for i := range maxRetries {
		err = bp.processBlockInternal(ctx, job)
		if err == nil {
			return nil
		}
		bp.logger.Warn("Retrying block processing",
			zap.Error(err),
			zap.Int("attempt", i+1),
			zap.String("CID", job.Block.Cid().String()))
		time.Sleep(time.Second) // Backoff delay between retries
	}
	return err
}

func (bp *BlockQueue) processBlockInternal(ctx context.Context, job *blockJob) error {
	ctx, span := core.TraceMethod(ctx, "BlockQueue.processBlockInternal")
	defer span.End()

	if err := bp.proto.GetNode().AddBlock(ctx, job.Block); err != nil {
		return fmt.Errorf("failed to add block: %w", err)
	}
	return nil
}

// recordError records the first critical error and cancels processing.
func (bp *BlockQueue) recordError(err error) {
	bp.failedCount.Add(1)
	bp.errOnce.Do(func() {
		select {
		case bp.errChan <- err:
		default:
		}
		bp.cancel()
	})
}

// ProcessBlocks processes blocks using a BlockProcessor to provide blocks
// and BlockQueue to process them. After all blocks are processed, it flushes
// any buffered metadata through the provided Flusher.
func ProcessBlocks(ctx core.Context, processor BlockProcessor, flusher store.Flusher) ([]cid.Cid, []cid.Cid, error) {
	protoInterface := core.GetProtocol(internal.ProtocolName)
	if protoInterface == nil {
		return nil, nil, fmt.Errorf("protocol %s not found", internal.ProtocolName)
	}
	proto, ok := protoInterface.(ProtoNode)
	if !ok {
		return nil, nil, fmt.Errorf("protocol has unexpected type, expected ProtoNode")
	}
	logger := ctx.Logger()

	bp := NewBlockQueue(ctx, proto, logger)
	if bp == nil {
		return nil, nil, fmt.Errorf("failed to create block queue")
	}
	defer bp.release()

	// Read blocks from processor and submit to worker pool.
	// Submit() is non-blocking — the pool handles concurrency internally.
	for {
		block, err := processor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get next block: %w", err)
		}

		if err := bp.queueBlock(block); err != nil {
			if !isContextCanceled(err) {
				return nil, nil, fmt.Errorf("failed to queue block %s: %w", block.Cid().String(), err)
			}
			break
		}

		processor.Done(block.Cid())
	}

	// Wait for all queued work to complete
	bp.wp.StopWait()

	// Flush any remaining buffered metadata to the database.
	// Detach from bp.ctx so the flush succeeds even if it was canceled
	// by a block processing failure — the metadata for already-processed
	// blocks must be persisted regardless, or their S3 uploads become orphans.
	if flusher != nil {
		if err := flusher.Flush(core.DetachContext(bp.ctx)); err != nil {
			return nil, nil, fmt.Errorf("failed to flush metadata: %w", err)
		}
	}

	// Check for errors
	select {
	case err := <-bp.errChan:
		return nil, nil, fmt.Errorf("block processing failed: %w", err)
	default:
	}

	if err := bp.ctx.Err(); err != nil && !isContextCanceled(err) {
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
