package protocol

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/gammazero/workerpool"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

const (
	bloomFilterEstimateItemsBlockstore     = 500000 // Reduced estimate based on typical usage
	bloomFilterFalsePositiveRateBlockstore = 0.005  // Lower false positive rate for better performance
)

// Compile-time interface compliance checks
var _ StreamingBlockstore = (*DefaultStreamingBlockstore)(nil)

// BlockEntry represents a block entry in the archive queue
type BlockEntry struct {
	Block   blocks.Block
	AddedAt time.Time
	Key     ds.Key
}

// DefaultStreamingBlockstore implements a blockstore that acts as a pipeline coordinator
// It streams blocks through a worker pool with bounded queue and supports passthrough blockstore
type DefaultStreamingBlockstore struct {
	// DoneTracker for tracking completed CIDs
	DoneTracker

	// Passthrough blockstore for fetching blocks that have already been processed
	passthrough blockstore.Blockstore

	// Logger for error reporting
	logger *core.Logger

	// Worker pool for block processing with bounded queue
	workerPool *workerpool.WorkerPool

	// Channel for delivering blocks to consumers (replaces old blockChan)
	blockDelivery chan *BlockEntry

	// Once to protect channel closing
	deliveryOnce sync.Once

	// Global mutex for synchronizing state
	stateMutex sync.RWMutex

	// Track if processing is complete
	processingDone bool

	// Optimized pending blocks storage
	pendingBlocks map[string]*BlockEntry // Map for pending blocks, key is binary CID representation
	pendingMutex  sync.RWMutex

	// Bloom filter for quick "ever seen" existence checks
	seenFilter      *bloom.BloomFilter
	seenFilterMutex sync.RWMutex // Protects seenFilter during concurrent access

	// Track blocks that have been processed and sent
	processedBlocks map[string]bool // Key is binary CID representation
	processedMutex  sync.RWMutex

	// Atomic counters for map sizes (optimized for logging without locks)
	pendingCountAtomic   atomic.Int64
	processedCountAtomic atomic.Int64

	// Control fields
	ctx    context.Context
	cancel context.CancelFunc
	closed bool
	closer sync.Once
}

// NewStreamingBlockstore creates a new archive blockstore with the given logger and passthrough blockstore
func NewStreamingBlockstore(logger *core.Logger, passthrough blockstore.Blockstore) *DefaultStreamingBlockstore {
	return NewStreamingBlockstoreWithDefaults(logger, passthrough, NewDoneTracker(), 0)
}

// NewStreamingBlockstoreWithBuffer creates a new archive blockstore with the given logger, passthrough blockstore, and buffer size
func NewStreamingBlockstoreWithBuffer(logger *core.Logger, passthrough blockstore.Blockstore, bufferSize int) *DefaultStreamingBlockstore {
	return NewStreamingBlockstoreWithDefaults(logger, passthrough, NewDoneTracker(), bufferSize)
}

// NewStreamingBlockstoreWithDefaults creates a new archive blockstore with the given logger, passthrough blockstore, done tracker, and queue size
func NewStreamingBlockstoreWithDefaults(logger *core.Logger, passthrough blockstore.Blockstore, doneTracker DoneTracker, queueSize int) *DefaultStreamingBlockstore {
	ctx, cancel := context.WithCancel(context.Background())

	// Ensure minimum queue size to prevent blocking
	if queueSize < 10 {
		queueSize = 10
	}

	// Use a buffer proportional to worker pool size for better throughput
	bufferSize := queueSize
	if bufferSize < 10 {
		bufferSize = 10
	}

	abs := &DefaultStreamingBlockstore{
		passthrough:     passthrough,
		logger:          logger,
		workerPool:      workerpool.New(queueSize),          // Bounded queue
		blockDelivery:   make(chan *BlockEntry, bufferSize), // Buffer sized for throughput
		pendingBlocks:   make(map[string]*BlockEntry),
		seenFilter:      bloom.NewWithEstimates(bloomFilterEstimateItemsBlockstore, bloomFilterFalsePositiveRateBlockstore),
		processedBlocks: make(map[string]bool),
		DoneTracker:     doneTracker,
		ctx:             ctx,
		cancel:          cancel,
	}

	if logger != nil {
		logger.Debug("DefaultStreamingBlockstore created",
			zap.Int("queueSize", queueSize),
			zap.Bool("hasPassthrough", passthrough != nil))
	}

	return abs
}

// isClosed returns whether the blockstore is closed (thread-safe)
func (s *DefaultStreamingBlockstore) isClosed() bool {
	s.stateMutex.RLock()
	defer s.stateMutex.RUnlock()
	return s.closed
}

// setClosed marks the blockstore as closed (thread-safe)
func (s *DefaultStreamingBlockstore) setClosed() {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()
	s.closed = true
}

// isProcessingDone returns whether processing is complete (thread-safe)
func (s *DefaultStreamingBlockstore) isProcessingDone() bool {
	s.stateMutex.RLock()
	defer s.stateMutex.RUnlock()
	return s.processingDone
}

// setProcessingDone marks processing as complete (thread-safe)
func (s *DefaultStreamingBlockstore) setProcessingDone() {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()
	s.processingDone = true
}

// Put implements blockstore.Blockstore.Put
// Stores a block and queues it for streaming
func (s *DefaultStreamingBlockstore) Put(ctx context.Context, block blocks.Block) error {
	ctx, span := core.TraceMethod(ctx, "DefaultStreamingBlockstore.Put")
	defer span.End()

	if s.isClosed() {
		return fmt.Errorf("blockstore is closed")
	}

	return s.putBlock(ctx, block)
}

// putBlock is a private method to directly put a block
func (s *DefaultStreamingBlockstore) putBlock(ctx context.Context, block blocks.Block) error {
	ctx, span := core.TraceMethod(ctx, "DefaultStreamingBlockstore.putBlock")
	defer span.End()

	if s.isClosed() {
		return fmt.Errorf("blockstore is closed")
	}

	if s.isProcessingDone() {
		return fmt.Errorf("processing is complete, cannot accept new blocks")
	}

	blockKey := KeyFromCID(block.Cid())
	cidKey := string(block.Cid().Bytes()) // Use binary representation as cache key for space efficiency
	blockKeyStr := blockKey.String()

	if s.logger != nil {
		s.logger.Debug("Processing block",
			zap.String("cid", block.Cid().String()),
			zap.Int("dataSize", len(block.RawData())),
			zap.Int("pendingCount", int(s.pendingCountAtomic.Load())))
	}

	entry := &BlockEntry{
		Block:   block,
		AddedAt: time.Now(),
		Key:     blockKey,
	}

	// Check if block is already pending to avoid duplicate submissions
	s.pendingMutex.Lock()
	if _, exists := s.pendingBlocks[cidKey]; exists {
		s.pendingMutex.Unlock()
		if s.logger != nil {
			s.logger.Debug("Skipping duplicate block",
				zap.String("cid", block.Cid().String()))
		}
		return nil
	}

	// Add to pending blocks map using binary CID as key
	s.pendingBlocks[cidKey] = entry
	s.pendingCountAtomic.Add(1)
	s.pendingMutex.Unlock()

	// Add to bloom filter for quick existence checks
	s.seenFilterMutex.Lock()
	s.seenFilter.Add([]byte(blockKeyStr))
	s.seenFilterMutex.Unlock()

	// Submit to worker pool
	s.workerPool.Submit(func() {
		// Check if context is cancelled or processing is done
		select {
		case <-s.ctx.Done():
			return
		case <-ctx.Done():
			return
		default:
		}

		// Check if processing is done or channel is closed before attempting to send
		if s.isProcessingDone() || s.isClosed() {
			return
		}

		// Try to deliver block to consumer with retry logic for full channel
		// Use exponential backoff for better resource efficiency under sustained high load
		backoff := 1 * time.Millisecond
		const maxBackoff = 100 * time.Millisecond

		for {
			// Check if processing is done or channel is closed before attempting to send
			if s.isProcessingDone() || s.isClosed() {
				return
			}

			select {
			case s.blockDelivery <- entry:
				if s.logger != nil {
					s.logger.Debug("Block delivered to queue",
						zap.String("cid", block.Cid().String()))
				}
				// Successfully sent, exit the retry loop
				return
			case <-s.ctx.Done():
				return
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				// Timeout on blocked send, check if we should continue
				if s.isProcessingDone() || s.isClosed() {
					return
				}
				// Exponential backoff with jitter
				if backoff < maxBackoff {
					backoff *= 2
				}
				// Add small random jitter to avoid thundering herd
				jitter := time.Duration(rand.Int63n(int64(backoff) / 4))
				backoff += jitter
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
	})

	if s.logger != nil {
		s.logger.Debug("Block queued for processing",
			zap.String("cid", block.Cid().String()))
	}

	return nil
}

// PutMany implements blockstore.Blockstore.PutMany
func (s *DefaultStreamingBlockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	ctx, span := core.TraceMethod(ctx, "DefaultStreamingBlockstore.PutMany")
	defer span.End()

	for _, block := range blocks {
		if err := s.Put(ctx, block); err != nil {
			return err
		}
	}
	return nil
}

// Get implements blockstore.Blockstore.Get
// First checks LRU cache, then falls back to passthrough blockstore
func (s *DefaultStreamingBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, span := core.TraceMethod(ctx, "DefaultStreamingBlockstore.Get")
	defer span.End()

	if s.isClosed() {
		return nil, fmt.Errorf("blockstore is closed")
	}

	// Convert CID to binary representation for cache lookup
	cidKey := string(c.Bytes())

	if s.logger != nil {
		s.logger.Debug("Retrieving block",
			zap.String("cid", c.String()),
			zap.Int("pendingCount", int(s.pendingCountAtomic.Load())),
			zap.Int("processedCount", int(s.processedCountAtomic.Load())))
	}

	// First check if block is pending in pending blocks map
	s.pendingMutex.RLock()
	entry, ok := s.pendingBlocks[cidKey]
	s.pendingMutex.RUnlock()

	if ok {
		if s.logger != nil {
			s.logger.Debug("Block found in pending",
				zap.String("cid", c.String()))
		}
		return entry.Block, nil
	}

	// Check if it was processed (sent out from queue)
	s.processedMutex.RLock()
	_, wasProcessed := s.processedBlocks[cidKey]
	s.processedMutex.RUnlock()

	// If we have a passthrough blockstore, use it
	if s.passthrough != nil {
		// If we know it was processed, check if the other side is done before trying passthrough
		if wasProcessed {
			if s.WaitDone(ctx, c) {
				if s.logger != nil {
					s.logger.Debug("Retrieving from passthrough store",
						zap.String("cid", c.String()))
				}
				// Add SkipQuotaCheckOption for passthrough operations
				passthroughCtx := store.SkipQuotaCheckOption(ctx, store.IsQuotaCheckSkipped(ctx))
				return s.passthrough.Get(passthroughCtx, c)
			}
		}
	}

	if s.logger != nil {
		s.logger.Debug("Block not found",
			zap.String("cid", c.String()))
	}
	return nil, fmt.Errorf("block not found")
}

// Has implements blockstore.Blockstore.Has
func (s *DefaultStreamingBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	ctx, span := core.TraceMethod(ctx, "DefaultStreamingBlockstore.Has")
	defer span.End()

	if s.isClosed() {
		return false, fmt.Errorf("blockstore is closed")
	}

	key := KeyFromCID(c)
	keyStr := key.String()

	// Quick negative check using bloom filter
	s.seenFilterMutex.RLock()
	exists := s.seenFilter.Test([]byte(keyStr))
	s.seenFilterMutex.RUnlock()

	if !exists {
		// Definitely never seen this CID
		return false, nil
	}

	// Convert CID to binary representation for cache lookup
	cidKey := string(c.Bytes())

	// Check pending blocks in pending blocks map
	s.pendingMutex.RLock()
	_, pendingExists := s.pendingBlocks[cidKey]
	s.pendingMutex.RUnlock()
	if pendingExists {
		return true, nil
	}

	// Check if it was processed (sent out from queue)
	s.processedMutex.RLock()
	_, wasProcessed := s.processedBlocks[cidKey]
	s.processedMutex.RUnlock()
	if wasProcessed {
		return true, nil
	}

	// Check if this CID is marked as done in DoneTracker
	// Use WaitDone to block until the CID is done
	if s.WaitDone(ctx, c) {
		return true, nil
	}

	// Check passthrough
	if s.passthrough != nil {
		// Add SkipQuotaCheckOption for passthrough operations
		passthroughCtx := store.SkipQuotaCheckOption(ctx, store.IsQuotaCheckSkipped(ctx))
		return s.passthrough.Has(passthroughCtx, c)
	}

	return false, nil
}

// GetSize implements blockstore.Blockstore.GetSize
func (s *DefaultStreamingBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	ctx, span := core.TraceMethod(ctx, "DefaultStreamingBlockstore.GetSize")
	defer span.End()

	if s.isClosed() {
		return -1, fmt.Errorf("blockstore is closed")
	}

	// Convert CID to binary representation for cache lookup
	cidKey := string(c.Bytes())

	// Check pending blocks in pending blocks map
	s.pendingMutex.RLock()
	if entry, ok := s.pendingBlocks[cidKey]; ok {
		s.pendingMutex.RUnlock()
		return len(entry.Block.RawData()), nil
	}
	s.pendingMutex.RUnlock()

	// Check passthrough
	if s.passthrough != nil {
		// Add SkipQuotaCheckOption for passthrough operations
		passthroughCtx := store.SkipQuotaCheckOption(ctx, store.IsQuotaCheckSkipped(ctx))
		return s.passthrough.GetSize(passthroughCtx, c)
	}

	return -1, fmt.Errorf("block not found")
}

// DeleteBlock implements blockstore.Blockstore.DeleteBlock
func (s *DefaultStreamingBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	ctx, span := core.TraceMethod(ctx, "DefaultStreamingBlockstore.DeleteBlock")
	defer span.End()

	if s.isClosed() {
		return fmt.Errorf("blockstore is closed")
	}

	// No-op: DeleteBlock is not used in this implementation
	// DefaultStreamingBlockstore is for block processing, not general block deletion
	return nil
}

// AllKeysChan implements blockstore.Blockstore.AllKeysChan
func (s *DefaultStreamingBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ctx, span := core.TraceMethod(ctx, "DefaultStreamingBlockstore.AllKeysChan")
	defer span.End()

	if s.isClosed() {
		return nil, fmt.Errorf("blockstore is closed")
	}

	// Combine results from pending blocks and passthrough
	ch := make(chan cid.Cid)

	go func() {
		defer close(ch)

		// Copy pending blocks entries while holding the lock
		var pendingCIDs []cid.Cid
		s.pendingMutex.RLock()
		for _, entry := range s.pendingBlocks {
			pendingCIDs = append(pendingCIDs, entry.Block.Cid())
		}
		s.pendingMutex.RUnlock()

		// Send pending block CIDs without holding the lock
		for _, blockCID := range pendingCIDs {
			select {
			case ch <- blockCID:
			case <-ctx.Done():
				return
			}
		}

		// Add results from passthrough if available
		if s.passthrough != nil {
			// Add SkipQuotaCheckOption for passthrough operations
			passthroughCtx := store.SkipQuotaCheckOption(ctx, store.IsQuotaCheckSkipped(ctx))
			passthroughChan, err := s.passthrough.AllKeysChan(passthroughCtx)
			if err == nil {
				for cid := range passthroughChan {
					select {
					case ch <- cid:
					case <-ctx.Done():
						return
					}
				}
			} else {
				if s.logger != nil {
					s.logger.Error("Failed to get keys from passthrough blockstore", zap.Error(err))
				}
			}
		}
	}()

	return ch, nil
}

// Close closes the blockstore
func (s *DefaultStreamingBlockstore) Close() error {
	var err error
	s.closer.Do(func() {
		if s.logger != nil {
			s.logger.Debug("Closing blockstore",
				zap.Int("pendingCount", int(s.pendingCountAtomic.Load())),
				zap.Int("processedCount", int(s.processedCountAtomic.Load())),
				zap.Int("deliveryChannelLength", len(s.blockDelivery)))
		}

		s.setClosed()
		s.cancel()

		// Stop worker pool
		if s.workerPool != nil {
			s.workerPool.Stop()
		}

		// Close the delivery channel
		s.closeDeliveryChannel()

		// Clear memory maps
		s.pendingMutex.Lock()
		s.pendingBlocks = make(map[string]*BlockEntry)
		s.pendingMutex.Unlock()

		s.processedMutex.Lock()
		s.processedBlocks = make(map[string]bool)
		s.processedMutex.Unlock()

		// Reset atomic counters
		s.pendingCountAtomic.Store(0)
		s.processedCountAtomic.Store(0)

		// Close passthrough if available
		if s.passthrough != nil {
			if s.logger != nil {
				s.logger.Debug("Closing passthrough store")
			}
			// Note: blockstore interface doesn't define Close(), so we can't close the passthrough
			// This is a limitation of the interface design
		}

		if s.logger != nil {
			s.logger.Debug("Blockstore closed")
		}
	})

	return err
}

// GetBlockStream returns a channel for streaming blocks
// This is the main consumption endpoint for the pipeline
func (s *DefaultStreamingBlockstore) GetBlockStream(_ context.Context) <-chan *BlockEntry {
	if s.logger != nil {
		s.logger.Debug("Opening block stream",
			zap.Int("deliveryChannelCapacity", cap(s.blockDelivery)),
			zap.Int("deliveryChannelLength", len(s.blockDelivery)),
			zap.Bool("isClosed", s.isClosed()))
	}

	return s.blockDelivery
}

// closeDeliveryChannel closes the delivery channel safely using sync.Once
func (s *DefaultStreamingBlockstore) closeDeliveryChannel() {
	s.deliveryOnce.Do(func() {
		if s.logger != nil {
			s.logger.Debug("Closing delivery channel")
		}
		close(s.blockDelivery)
	})
}

// ProcessingDone signals that all processing is complete and stops accepting new submissions
func (s *DefaultStreamingBlockstore) ProcessingDone() {
	if s.isProcessingDone() {
		return // Already done
	}

	// Stop accepting new submissions by stopping the worker pool
	if s.workerPool != nil {
		s.workerPool.StopWait() // Wait for current submissions to complete
	}

	s.setProcessingDone()

	if s.logger != nil {
		s.logger.Debug("Processing completed")
	}
	// Close delivery channel to signal consumers
	s.closeDeliveryChannel()
}

// MarkBlockProcessed marks a block as processed (sent to the next stage)
func (s *DefaultStreamingBlockstore) MarkBlockProcessed(blockKey string) {
	// Convert blockKey to binary CID for cache removal
	cidKey := KeyToCIDString(ds.NewKey(ds.NewKey(blockKey).Name()))
	
	// Get counts BEFORE any modifications for logging (atomic, no lock needed)
	pendingBefore := s.pendingCountAtomic.Load()
	processedBefore := s.processedCountAtomic.Load()

	if s.logger != nil {
		s.logger.Debug("Marking block processed",
			zap.String("blockKey", blockKey),
			zap.String("cidKey", cidKey),
			zap.Int("pendingBefore", int(pendingBefore)),
			zap.Int("processedBefore", int(processedBefore)))
	}

	// Remove from pending blocks map and decrement atomic counter
	s.pendingMutex.Lock()
	delete(s.pendingBlocks, cidKey)
	s.pendingCountAtomic.Add(-1)
	s.pendingMutex.Unlock()

	// Mark as processed using binary CID and increment atomic counter
	s.processedMutex.Lock()
	s.processedBlocks[cidKey] = true
	s.processedCountAtomic.Add(1)
	s.processedMutex.Unlock()

	// Get pending and processed counts AFTER modifications for logging (atomic, no lock needed)
	pendingAfter := s.pendingCountAtomic.Load()
	processedAfter := s.processedCountAtomic.Load()

	// Mark as done in DoneTracker
	if cidObj, err := cid.Decode(blockKey); err == nil {
		s.Done(cidObj)
	}

	if s.logger != nil {
		s.logger.Debug("Block marked processed",
			zap.String("blockKey", blockKey),
			zap.Int("pendingAfter", int(pendingAfter)),
			zap.Int("processedAfter", int(processedAfter)))
	}
}

// MarkDone marks a CID as done and updates bloom filter
func (s *DefaultStreamingBlockstore) MarkDone(c cid.Cid) {
	// Add to bloom filter for quick existence checks
	// Use the same format that blockstore keys use (with leading slash)
	key := KeyFromCID(c)

	if s.logger != nil {
		s.logger.Debug("Marking CID done",
			zap.String("cid", c.String()),
			zap.String("key", key.String()))
	}

	s.seenFilterMutex.Lock()
	s.seenFilter.Add([]byte(key.String()))
	s.seenFilterMutex.Unlock()

	// Mark as done in DoneTracker
	s.Done(c)
}

// GetPendingCount returns the number of blocks currently pending
func (s *DefaultStreamingBlockstore) GetPendingCount() int {
	return int(s.pendingCountAtomic.Load())
}

// GetProcessedCount returns the number of blocks that have been processed
func (s *DefaultStreamingBlockstore) GetProcessedCount() int {
	return int(s.processedCountAtomic.Load())
}

// WaitDone waits for a CID to be marked as done (implements DoneTracker interface)
func (s *DefaultStreamingBlockstore) WaitDone(ctx context.Context, c cid.Cid) bool {
	ctx, span := core.TraceMethod(ctx, "DefaultStreamingBlockstore.WaitDone")
	defer span.End()

	return s.DoneTracker.WaitDone(ctx, c)
}

// Done marks a CID as done (implements DoneTracker interface)
func (s *DefaultStreamingBlockstore) Done(c cid.Cid) {
	s.DoneTracker.Done(c)
}

// IsClosed returns whether the blockstore is closed
func (s *DefaultStreamingBlockstore) IsClosed() bool {
	return s.isClosed()
}
