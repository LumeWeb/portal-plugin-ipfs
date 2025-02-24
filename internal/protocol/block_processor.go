package protocol

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"sync"
	"time"

	"github.com/gammazero/workerpool"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/samber/lo"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

const (
	numShards  = 32 // Number of shards for the mutex
	maxRetries = 3  // Max retries for block processing
)

// blockJob represents a unit of work for processing a block.
type blockJob struct {
	Block     blocks.Block
	ParentCID cid.Cid
}

// reset clears all fields to prepare for reuse
func (j *blockJob) reset() {
	j.Block = nil
	j.ParentCID = cid.Undef
}

// blockJobPool is a pool of blockJob instances to reduce allocations.
var blockJobPool = sync.Pool{
	New: func() interface{} {
		return new(blockJob)
	},
}

// BlockProcessor manages the processing of blocks from a CAR file.
type BlockProcessor struct {
	rootCIDs       map[string]bool
	processedNodes map[string]*internal.NodeInfo
	proto          *Protocol
	logger         *core.Logger
	wp             *workerpool.WorkerPool // Worker pool
	numShards      int                    // Number of shards for the mutex
	mu             []sync.RWMutex         // Sharded mutex
	ctx            context.Context
	cancel         context.CancelFunc
	errorCount     int32 // Atomic error counter
}

// NewBlockProcessor creates a new BlockProcessor instance.
func NewBlockProcessor(coreCtx core.Context, proto *Protocol, logger *core.Logger, rootCIDs []cid.Cid) *BlockProcessor {
	ctx, cancel := context.WithTimeout(coreCtx.GetContext(), 30*time.Minute) // Add timeout
	bp := &BlockProcessor{
		rootCIDs:       lo.SliceToMap(rootCIDs, func(cid cid.Cid) (string, bool) { return string(cid.Bytes()), true }),
		processedNodes: make(map[string]*internal.NodeInfo),
		proto:          proto,
		logger:         logger,
		wp:             workerpool.New(10), // Initialize worker pool
		numShards:      numShards,
		mu:             make([]sync.RWMutex, numShards),
		ctx:            ctx,
		cancel:         cancel,
	}
	return bp
}

// getShard returns the shard number for a given CID.
func (bp *BlockProcessor) getShard(cidStr string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(cidStr))
	return int(hash.Sum32() % uint32(bp.numShards))
}

// queueBlock queues a block for processing.
func (bp *BlockProcessor) queueBlock(block blocks.Block, parentCid cid.Cid) error {
	select {
	case <-bp.ctx.Done():
		return bp.ctx.Err()
	default:
		job := blockJobPool.Get().(*blockJob)
		job.Block = block
		job.ParentCID = parentCid

		bp.wp.Submit(func() {
			err := bp.processBlock(bp.ctx, job)
			job.reset()
			blockJobPool.Put(job) // Release the blockJob back to the pool
			if err != nil {
				bp.logger.Error("Block processing failed", zap.Error(err), zap.String("CID", job.Block.Cid().String()))
				bp.handleError(err) // Just log the error, don't cancel context
			}
		})
		return nil
	}
}

// processBlock processes a single block.
func (bp *BlockProcessor) processBlock(ctx context.Context, job *blockJob) error {
	cidBytes := job.Block.Cid().Bytes()
	var cidKey [32]byte
	copy(cidKey[:], cidBytes)

	shard := bp.getShard(string(job.Block.Cid().Bytes()))

	// Retry logic
	var err error
	for i := 0; i < maxRetries; i++ {
		err = bp.processBlockInternal(ctx, job, shard)
		if err == nil {
			return nil
		}
		bp.logger.Warn("Retrying block processing", zap.Error(err), zap.Int("attempt", i+1), zap.String("CID", job.Block.Cid().String()))
		time.Sleep(time.Second) // Add a delay before retrying
	}
	return err // Return the last error
}

func (bp *BlockProcessor) processBlockInternal(ctx context.Context, job *blockJob, shard int) error {
	bp.logger.Debug("Processing block", zap.String("CID", job.Block.Cid().String()))

	// Import the block
	err := bp.proto.GetNode().AddBlock(ctx, job.Block)
	if err != nil {
		return fmt.Errorf("failed to add block: %w", err)
	}

	// Analyze the node
	nodeInfo, err := internal.AnalyzeNode(ctx, job.Block)
	if err != nil {
		return fmt.Errorf("failed to analyze node: %w", err)
	}

	bp.mu[shard].Lock()
	bp.processedNodes[string(job.Block.Cid().Bytes())] = nodeInfo
	bp.mu[shard].Unlock()

	return nil
}

// handleError logs the error and cancels the context.
func (bp *BlockProcessor) handleError(err error) {
	bp.logger.Error("Critical error occurred", zap.Error(err))
	bp.cancel() // Cancel the context on any error
}

// ProcessCar processes a CAR file.
func ProcessCar(ctx core.Context, r io.Reader) ([]cid.Cid, error) {
	protoInterface := core.GetProtocol(internal.ProtocolName)
	if protoInterface == nil {
		return nil, fmt.Errorf("protocol %s not found", internal.ProtocolName)
	}
	proto, ok := protoInterface.(*Protocol)
	if !ok {
		return nil, fmt.Errorf("protocol %s has unexpected type", internal.ProtocolName)
	}
	logger := ctx.Logger()

	processedCIDs := make([]cid.Cid, 0)

	cr, err := car.NewBlockReader(r)
	if err != nil {
		logger.Error("Failed to create block reader", zap.Error(err))
		return nil, fmt.Errorf("failed to create block reader: %w", err)
	}

	rootCIDs := cr.Roots
	if len(rootCIDs) == 0 {
		return nil, fmt.Errorf("CAR file contains no root CIDs")
	}

	bp := NewBlockProcessor(ctx, proto, logger, rootCIDs)
	if bp == nil {
		return nil, fmt.Errorf("failed to create block processor")
	}
	defer bp.release()

	// Queue all blocks
	for {
		block, err := cr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read block: %w", err)
		}

		if err := bp.queueBlock(block, cid.Undef); err != nil {
			if !isContextCanceled(err) {
				return nil, fmt.Errorf("failed to queue block: %w", err)
			}
			break
		}

		processedCIDs = append(processedCIDs, block.Cid())
	}

	// Wait for all blocks to be processed
	bp.wp.StopWait()
	if err := bp.ctx.Err(); err != nil {
		return nil, fmt.Errorf("block processing failed: %w", err)
	}

	return processedCIDs, nil
}

// release releases the resources used by the BlockProcessor.
func (bp *BlockProcessor) release() {
	bp.cancel()
	bp.wp.Stop()
}

// isContextCanceled checks if the context has been canceled.
func isContextCanceled(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}
