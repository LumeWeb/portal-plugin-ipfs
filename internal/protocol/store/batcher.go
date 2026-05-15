package store

import (
	"context"
	"sync"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

const (
	// defaultBatchSize is the maximum number of blocks to accumulate before flushing.
	defaultBatchSize = 50
)

// metadataBatcher accumulates PinnedBlocks and flushes them to BatchPin
// in batches, reducing per-block database transaction overhead.
// Batches are flushed when full (batchSize reached) or on explicit Flush/Close calls.
// Callers must call Flush after all blocks have been submitted to ensure
// the final partial batch is written.
type metadataBatcher struct {
	store     pluginCore.MetadataStore
	logger    *core.Logger
	batchSize int

	mu     sync.Mutex
	batch  []pluginCore.PinnedBlock
	err    error
	closed bool
}

// newMetadataBatcher creates a new metadataBatcher.
func newMetadataBatcher(store pluginCore.MetadataStore, logger *core.Logger, batchSize int) *metadataBatcher {
	return &metadataBatcher{
		store:     store,
		logger:    logger,
		batchSize: batchSize,
	}
}

// Add enqueues a PinnedBlock for batched writing.
// If the batch is full, it flushes synchronously before adding.
// Returns an error if a previous flush failed or the batcher is closed.
func (b *metadataBatcher) Add(ctx context.Context, block pluginCore.PinnedBlock) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.err != nil {
		return b.err
	}
	if b.closed {
		return nil
	}

	b.batch = append(b.batch, block)

	if len(b.batch) >= b.batchSize {
		return b.flushLocked(ctx)
	}

	return nil
}

// Flush forces a flush of any accumulated blocks.
func (b *metadataBatcher) Flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return b.err
	}

	return b.flushLocked(ctx)
}

// flushLocked flushes the current batch. Caller must hold b.mu.
func (b *metadataBatcher) flushLocked(ctx context.Context) error {
	if len(b.batch) == 0 {
		return nil
	}

	toFlush := b.batch
	b.batch = make([]pluginCore.PinnedBlock, 0, b.batchSize)

	b.logger.Debug("flushing metadata batch", zap.Int("count", len(toFlush)))

	if err := b.store.BatchPin(ctx, toFlush); err != nil {
		b.err = err
		return err
	}

	return nil
}

// Close flushes any remaining blocks and marks the batcher as closed.
func (b *metadataBatcher) Close(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	err := b.flushLocked(ctx)
	b.closed = true
	return err
}
