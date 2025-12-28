package protocol

import (
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// BaseBlockProcessor provides common functionality for all block processors
// It handles lifecycle management, error handling, and coordination patterns
type BaseBlockProcessor struct {
	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Logger for error reporting
	logger *core.Logger

	// Synchronization
	wg sync.WaitGroup
	mu sync.RWMutex

	// State tracking
	started        bool
	completed      bool
	rootCIDs       []cid.Cid
	errorChan      chan error
	closed         bool
	closeOnce      sync.Once
	errorCloseOnce sync.Once

	// Done tracking
	DoneTracker
}

// NewBaseBlockProcessor creates a new BaseBlockProcessor with the given context and logger
func NewBaseBlockProcessor(ctx context.Context, logger *core.Logger) *BaseBlockProcessor {
	return NewBaseBlockProcessorWithDefaults(ctx, logger, NewDoneTracker())
}

// NewBaseBlockProcessorWithDefaults creates a new BaseBlockProcessor with the given context, logger, and done tracker
func NewBaseBlockProcessorWithDefaults(ctx context.Context, logger *core.Logger, doneTracker DoneTracker) *BaseBlockProcessor {
	// Derive a cancelable context from the provided context
	ctx, cancel := context.WithCancel(ctx)

	return &BaseBlockProcessor{
		ctx:         ctx,
		cancel:      cancel,
		logger:      logger,
		errorChan:   make(chan error, 10), // Buffer for errors
		DoneTracker: doneTracker,
	}
}

// markStarted marks the processor as started (thread-safe)
func (bp *BaseBlockProcessor) markStarted() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.started = true
}

// isStarted returns whether the processor has been started (thread-safe)
func (bp *BaseBlockProcessor) isStarted() bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.started
}

// markCompleted marks the processor as completed with the given root CIDs (thread-safe)
func (bp *BaseBlockProcessor) markCompleted(rootCIDs []cid.Cid) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.completed = true
	bp.rootCIDs = rootCIDs
}

// isCompleted returns whether the processor has been completed (thread-safe)
func (bp *BaseBlockProcessor) isCompleted() bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.completed
}

// getRootCIDs returns the root CIDs (thread-safe)
func (bp *BaseBlockProcessor) getRootCIDs() []cid.Cid {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	if bp.completed {
		return bp.rootCIDs
	}
	return nil
}

// isClosed returns whether the processor has been closed (thread-safe)
func (bp *BaseBlockProcessor) isClosed() bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.closed
}

// sendError sends an error to the error channel (non-blocking)
func (bp *BaseBlockProcessor) sendError(err error) {
	if err == nil {
		return
	}

	select {
	case bp.errorChan <- err:
	default:
		// Error channel is full, log warning
		if bp.logger != nil {
			bp.logger.Warn("Error channel full, dropping error", zap.String("error", err.Error()))
		}
	}
}

// startBackgroundGoroutine starts a background goroutine with proper error handling
func (bp *BaseBlockProcessor) startBackgroundGoroutine(fn func() error) {
	bp.wg.Add(1)
	go func() {
		defer bp.wg.Done()

		if err := fn(); err != nil {
			bp.sendError(fmt.Errorf("background processing failed: %w", err))
		}
	}()
}

// closeErrorChannel closes the error channel safely
func (bp *BaseBlockProcessor) closeErrorChannel() {
	bp.errorCloseOnce.Do(func() {
		close(bp.errorChan)
	})
}

// Close performs common cleanup for all processors
func (bp *BaseBlockProcessor) Close() {
	bp.closeOnce.Do(func() {
		bp.mu.Lock()
		bp.closed = true
		bp.mu.Unlock()

		// Cancel processing context
		bp.cancel()

		// Wait for any background processing to complete
		bp.wg.Wait()

		// Close error channel if it wasn't closed by background processing
		bp.closeErrorChannel()
	})
}

// GetContext returns the processor's context
func (bp *BaseBlockProcessor) GetContext() context.Context {
	return bp.ctx
}

// GetLogger returns the processor's logger
func (bp *BaseBlockProcessor) GetLogger() *core.Logger {
	return bp.logger
}
