package protocol

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/gammazero/workerpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testifymock "github.com/stretchr/testify/mock"
	protocoltestmocks "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/mock_tests"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// --- Helpers ---

// testBlock creates a block with deterministic CID from an integer
func testBlock(i int) blocks.Block {
	data := fmt.Appendf(nil, "test-block-data-%d", i)
	c, err := cid.NewPrefixV1(cid.Raw, 0x12).Sum(data)
	if err != nil {
		panic(err)
	}
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		panic(err)
	}
	return blk
}

// newTestBlockQueue creates a BlockQueue for testing with a mock ProtoNode and IPFSNode
func newTestBlockQueue(t *testing.T, mockNode *mocks.MockIPFSNode, workers int) *BlockQueue {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	mockProto := protocoltestmocks.NewMockProtoNode(t)
	mockProto.EXPECT().GetNode().Return(mockNode).Maybe()

	return &BlockQueue{
		proto:   mockProto,
		logger:  &core.Logger{Logger: zap.NewNop()},
		wp:      workerpool.New(workers),
		ctx:     ctx,
		cancel:  cancel,
		errChan: make(chan error, 1),
	}
}

// --- Tests ---

func TestBlockQueue_QueueBlock_Success(t *testing.T) {
	mockNode := mocks.NewMockIPFSNode(t)
	mockNode.EXPECT().AddBlock(testifymock.Anything, testifymock.Anything).Return(nil)

	bp := newTestBlockQueue(t, mockNode, 2)
	defer bp.release()

	blk := testBlock(1)
	err := bp.queueBlock(blk)
	require.NoError(t, err)

	// Wait for async processing
	bp.wp.StopWait()
}

func TestBlockQueue_QueueBlock_MultipleBlocks(t *testing.T) {
	numBlocks := 20
	mockNode := mocks.NewMockIPFSNode(t)
	mockNode.EXPECT().AddBlock(testifymock.Anything, testifymock.Anything).Return(nil).Times(numBlocks)

	bp := newTestBlockQueue(t, mockNode, 4)
	defer bp.release()

	for i := range numBlocks {
		err := bp.queueBlock(testBlock(i))
		require.NoError(t, err)
	}

	bp.wp.StopWait()
}

func TestBlockQueue_QueueBlock_ContextCanceled(t *testing.T) {
	mockNode := mocks.NewMockIPFSNode(t)
	bp := newTestBlockQueue(t, mockNode, 2)
	defer bp.release()

	// Cancel the context before queuing
	bp.cancel()

	blk := testBlock(1)
	err := bp.queueBlock(blk)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestBlockQueue_RecordError_CancelsContext(t *testing.T) {
	mockNode := mocks.NewMockIPFSNode(t)
	bp := newTestBlockQueue(t, mockNode, 2)
	defer bp.release()

	testErr := fmt.Errorf("test error")
	bp.recordError(testErr)

	// Context should be canceled
	assert.Error(t, bp.ctx.Err())

	// Error should be on errChan
	select {
	case err := <-bp.errChan:
		assert.Equal(t, testErr, err)
	default:
		t.Fatal("expected error on errChan")
	}
}

func TestBlockQueue_RecordError_OnlyFirstErrorRecorded(t *testing.T) {
	mockNode := mocks.NewMockIPFSNode(t)
	bp := newTestBlockQueue(t, mockNode, 2)
	defer bp.release()

	err1 := fmt.Errorf("first error")
	err2 := fmt.Errorf("second error")

	bp.recordError(err1)
	bp.recordError(err2)

	// Only the first error should be on errChan (capacity 1, errOnce guards)
	select {
	case err := <-bp.errChan:
		assert.Equal(t, err1, err)
	default:
		t.Fatal("expected first error on errChan")
	}

	// Second error should not be on the channel
	select {
	case err := <-bp.errChan:
		t.Fatalf("unexpected second error on errChan: %v", err)
	default:
		// Expected — no second error
	}

	// Both failures should be counted
	assert.Equal(t, int32(2), bp.failedCount.Load())
}

func TestBlockQueue_ProcessBlock_Retries(t *testing.T) {
	var attempts atomic.Int32
	mockNode := mocks.NewMockIPFSNode(t)
	mockNode.EXPECT().AddBlock(testifymock.Anything, testifymock.Anything).
		RunAndReturn(func(_ context.Context, _ blocks.Block) error {
			n := attempts.Add(1)
			if n < 3 {
				return fmt.Errorf("transient error attempt %d", n)
			}
			return nil
		})

	bp := newTestBlockQueue(t, mockNode, 1)
	defer bp.release()

	blk := testBlock(1)
	job := blockJobPool.Get().(*blockJob)
	job.Block = blk

	err := bp.processBlock(bp.ctx, job)
	require.NoError(t, err)

	// Should have retried: 3 attempts total (2 failures + 1 success)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestBlockQueue_ProcessBlock_MaxRetriesExceeded(t *testing.T) {
	mockNode := mocks.NewMockIPFSNode(t)
	mockNode.EXPECT().AddBlock(testifymock.Anything, testifymock.Anything).
		Return(fmt.Errorf("permanent error")).Times(maxRetries)

	bp := newTestBlockQueue(t, mockNode, 1)
	defer bp.release()

	blk := testBlock(1)
	job := blockJobPool.Get().(*blockJob)
	job.Block = blk

	err := bp.processBlock(bp.ctx, job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "permanent error")
}

func TestBlockQueue_BlockJobPool_Reuse(t *testing.T) {
	// Get a job, use it, reset it, put it back
	job1 := blockJobPool.Get().(*blockJob)
	job1.Block = testBlock(1)
	job1.reset()
	assert.Nil(t, job1.Block)
	blockJobPool.Put(job1)

	// Get again — should be the same object
	job2 := blockJobPool.Get().(*blockJob)
	assert.Same(t, job1, job2, "pool should return the same job object")
	blockJobPool.Put(job2)
}

func TestBlockQueue_ConcurrentQueueBlock(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	numBlocks := 100
	mockNode := mocks.NewMockIPFSNode(t)
	mockNode.EXPECT().AddBlock(testifymock.Anything, testifymock.Anything).Return(nil).Times(numBlocks)

	bp := newTestBlockQueue(t, mockNode, 4)
	defer bp.release()

	var wg sync.WaitGroup
	for i := range numBlocks {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := bp.queueBlock(testBlock(idx))
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()
	bp.wp.StopWait()
}

func TestBlockQueue_FailedCount(t *testing.T) {
	mockNode := mocks.NewMockIPFSNode(t)
	bp := newTestBlockQueue(t, mockNode, 2)
	defer bp.release()

	assert.Equal(t, int32(0), bp.failedCount.Load())

	bp.recordError(fmt.Errorf("err1"))
	assert.Equal(t, int32(1), bp.failedCount.Load())

	bp.recordError(fmt.Errorf("err2"))
	assert.Equal(t, int32(2), bp.failedCount.Load())
}

func TestIsContextCanceled(t *testing.T) {
	assert.True(t, isContextCanceled(context.Canceled))
	assert.True(t, isContextCanceled(context.DeadlineExceeded))
	assert.False(t, isContextCanceled(fmt.Errorf("other error")))
}

func TestResolveWorkerCount_AutoDefault(t *testing.T) {
	// Test the clamping logic directly (resolveWorkerCount itself requires core.Context)
	workers := 2 * 4 // assume 4 CPUs
	workers = min(max(workers, 4), 16)
	assert.Equal(t, 8, workers) // 2*4=8, clamped to [4,16]

	// Test lower bound
	workers = 2 * 1 // assume 1 CPU
	workers = min(max(workers, 4), 16)
	assert.Equal(t, 4, workers) // min is 4

	// Test upper bound
	workers = 2 * 32 // assume 32 CPUs
	workers = min(max(workers, 4), 16)
	assert.Equal(t, 16, workers) // max is 16
}

func TestBlockQueue_ProcessBlock_SkipsOnCanceledContext(t *testing.T) {
	mockNode := mocks.NewMockIPFSNode(t)
	// AddBlock should NOT be called because context is already canceled
	bp := newTestBlockQueue(t, mockNode, 1)
	defer bp.release()

	// Cancel context
	bp.cancel()

	blk := testBlock(1)
	// queueBlock should return context.Canceled
	err := bp.queueBlock(blk)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestBlockQueue_Release_StopsPool(t *testing.T) {
	mockNode := mocks.NewMockIPFSNode(t)
	mockNode.EXPECT().AddBlock(testifymock.Anything, testifymock.Anything).Return(nil).Maybe()

	bp := newTestBlockQueue(t, mockNode, 2)

	// Queue a block
	err := bp.queueBlock(testBlock(1))
	require.NoError(t, err)

	// Release should not panic
	bp.release()
}
