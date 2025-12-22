package protocol

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
)

func TestDoneTracker_Done(t *testing.T) {
	tests := []struct {
		name          string
		setupTracker  func() *DefaultDoneTracker
		cid           cid.Cid
		expectedState func(dt *DefaultDoneTracker) bool
	}{
		{
			name: "mark new CID as done",
			setupTracker: func() *DefaultDoneTracker {
				return NewDoneTracker()
			},
			cid: generateTestCIDFromInt(1),
			expectedState: func(dt *DefaultDoneTracker) bool {
				return dt.Count() == 1 // Should be kept for subsequent WaitDone calls
			},
		},
		{
			name: "mark existing waiter CID as done",
			setupTracker: func() *DefaultDoneTracker {
				dt := NewDoneTracker()
				testCid := generateTestCIDFromInt(1)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				go dt.WaitDone(ctx, testCid)     // Create a waiter
				time.Sleep(1 * time.Millisecond) // Give time for waiter to be registered
				return dt
			},
			cid: generateTestCIDFromInt(1),
			expectedState: func(dt *DefaultDoneTracker) bool {
				return dt.Count() == 0 // Should be removed after waiter is processed
			},
		},
		{
			name: "mark already done CID as done",
			setupTracker: func() *DefaultDoneTracker {
				dt := NewDoneTracker()
				testCid := generateTestCIDFromInt(1)
				dt.Done(testCid) // Mark as done first
				return dt
			},
			cid: generateTestCIDFromInt(1),
			expectedState: func(dt *DefaultDoneTracker) bool {
				return dt.Count() == 1 // Should remain since it was already done
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := tt.setupTracker()
			dt.Done(tt.cid)
			// Count should reflect done CIDs that are kept for subsequent WaitDone calls
			assert.True(t, tt.expectedState(dt), "Expected state mismatch")
		})
	}
}

func TestDoneTracker_WaitDone(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func(*DefaultDoneTracker) cid.Cid
		expectDone  bool
		expectError bool
	}{
		{
			name: "wait for already done CID",
			setupFunc: func(dt *DefaultDoneTracker) cid.Cid {
				testCid := generateTestCIDFromInt(1)
				dt.Done(testCid)
				return testCid
			},
			expectDone: true,
		},
		{
			name: "wait for CID that gets done later",
			setupFunc: func(dt *DefaultDoneTracker) cid.Cid {
				testCid := generateTestCIDFromInt(1)
				go func() {
					time.Sleep(10 * time.Millisecond)
					dt.Done(testCid)
				}()
				return testCid
			},
			expectDone: true,
		},
		{
			name: "context canceled while waiting",
			setupFunc: func(dt *DefaultDoneTracker) cid.Cid {
				testCid := generateTestCIDFromInt(1)
				return testCid
			},
			expectDone:  false,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := NewDoneTracker()
			testCid := tt.setupFunc(dt)

			var ctx context.Context
			if tt.expectError {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(context.Background(), 5*time.Millisecond)
				defer cancel()
			} else {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
			}

			done := dt.WaitDone(ctx, testCid)
			assert.Equal(t, tt.expectDone, done, "Done status mismatch")
		})
	}
}

func TestDoneTracker_ConcurrentWaiters(t *testing.T) {
	dt := NewDoneTracker()
	testCid := generateTestCIDFromInt(1)

	var wg sync.WaitGroup
	numWaiters := 10
	results := make([]bool, numWaiters)

	// Start multiple waiters
	for i := 0; i < numWaiters; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			results[index] = dt.WaitDone(ctx, testCid)
		}(i)
	}

	// Give waiters time to register
	time.Sleep(1 * time.Millisecond)

	// Mark CID as done
	dt.Done(testCid)

	// Wait for all waiters to complete
	wg.Wait()

	// All waiters should have received the done signal
	for i, result := range results {
		assert.True(t, result, "Waiter %d should have received done signal", i)
	}

	// Tracker should be clean
	assert.Equal(t, 0, dt.Count(), "Tracker should be empty after completion")
}

func TestDoneTracker_ContextCancellationCleanup(t *testing.T) {
	dt := NewDoneTracker()
	testCid := generateTestCIDFromInt(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	// Start a waiter that will be canceled
	go func() {
		time.Sleep(10 * time.Millisecond)
		dt.Done(testCid) // Try to mark done after cancellation
	}()

	done := dt.WaitDone(ctx, testCid)
	assert.False(t, done, "Should return false due to context cancellation")

	// Give some time for cleanup
	time.Sleep(1 * time.Millisecond)

	// CID should be cleaned up since there are no more waiters
	assert.Equal(t, 0, dt.Count(), "Tracker should be empty after cleanup")
}

func TestDoneTracker_GetDoneCIDs(t *testing.T) {
	dt := NewDoneTracker()
	cid1 := generateTestCIDFromInt(1)
	cid2 := generateTestCIDFromInt(2)

	// Initially no done CIDs
	doneCIDs := dt.GetDoneCIDs()
	assert.Empty(t, doneCIDs, "Should initially have no done CIDs")

	// Create waiters for both CIDs
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	go dt.WaitDone(ctx, cid1)
	go dt.WaitDone(ctx, cid2)

	// Give time for waiters to register
	time.Sleep(1 * time.Millisecond)

	// Mark one as done - it will be immediately cleaned up
	dt.Done(cid1)

	// Should see no done CIDs since they are cleaned up immediately
	doneCIDs = dt.GetDoneCIDs()
	assert.Empty(t, doneCIDs, "Should have no done CIDs since they are cleaned up immediately")

	// Mark the second as done
	dt.Done(cid2)

	// Should still be empty
	doneCIDs = dt.GetDoneCIDs()
	assert.Empty(t, doneCIDs, "Should have no done CIDs after cleanup")
}

func TestDoneTracker_Count(t *testing.T) {
	dt := NewDoneTracker()
	cid1 := generateTestCIDFromInt(1)
	cid2 := generateTestCIDFromInt(2)

	// Initially zero count
	assert.Equal(t, 0, dt.Count(), "Should initially have count of 0")

	// Create waiters
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	go dt.WaitDone(ctx, cid1)
	go dt.WaitDone(ctx, cid2)

	// Give time for waiters to register
	time.Sleep(1 * time.Millisecond)

	// Still zero count (waiters exist but not done)
	assert.Equal(t, 0, dt.Count(), "Should have count of 0 with only waiters")

	// Mark one as done
	dt.Done(cid1)

	// Count should still be 0 since done CIDs are immediately cleaned up
	assert.Equal(t, 0, dt.Count(), "Should have count of 0 after cleanup")

	// Mark the second as done
	dt.Done(cid2)

	// Count should still be 0
	assert.Equal(t, 0, dt.Count(), "Should have count of 0 after all cleanup")
}

func TestDoneTracker_Reset(t *testing.T) {
	dt := NewDoneTracker()
	cid1 := generateTestCIDFromInt(1)
	cid2 := generateTestCIDFromInt(2)

	// Create waiters
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	go dt.WaitDone(ctx, cid1)
	go dt.WaitDone(ctx, cid2)

	// Give time for waiters to register
	time.Sleep(1 * time.Millisecond)

	// Mark one as done
	dt.Done(cid1)

	// Reset should clear everything
	dt.Reset()

	assert.Equal(t, 0, dt.Count(), "Count should be 0 after reset")
	doneCIDs := dt.GetDoneCIDs()
	assert.Empty(t, doneCIDs, "Should have no done CIDs after reset")

	// Verify that the tracker is clean by trying to wait for the same CIDs
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()
	done1 := dt.WaitDone(ctx2, cid1)
	done2 := dt.WaitDone(ctx2, cid2)
	assert.False(t, done1, "Should not receive done signal after reset")
	assert.False(t, done2, "Should not receive done signal after reset")
}

func TestDoneTracker_MultipleCIDs(t *testing.T) {
	dt := NewDoneTracker()
	cids := []cid.Cid{
		generateTestCIDFromInt(1),
		generateTestCIDFromInt(2),
		generateTestCIDFromInt(3),
	}

	var wg sync.WaitGroup
	results := make([]bool, len(cids))

	// Start waiters for each CID
	for i, c := range cids {
		wg.Add(1)
		go func(index int, testCid cid.Cid) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			results[index] = dt.WaitDone(ctx, testCid)
		}(i, c)
	}

	// Give waiters time to register
	time.Sleep(1 * time.Millisecond)

	// Mark CIDs as done in reverse order
	for i := len(cids) - 1; i >= 0; i-- {
		dt.Done(cids[i])
		time.Sleep(1 * time.Millisecond) // Small delay between completions
	}

	// Wait for all waiters to complete
	wg.Wait()

	// All waiters should have received the done signal
	for i, result := range results {
		assert.True(t, result, "Waiter for CID %d should have received done signal", i)
	}

	// Tracker should be clean
	assert.Equal(t, 0, dt.Count(), "Tracker should be empty after all completions")
}

// generateTestCID creates a test CID from generated data
func generateTestCID(data []byte) cid.Cid {
	// Use sha2-256 (0x12) which is commonly available
	c, err := cid.NewPrefixV1(cid.Raw, 0x12).Sum(data)
	if err != nil {
		panic(err)
	}
	return c
}

// generateTestCIDFromInt creates a test CID from an integer (useful for unique CIDs)
func generateTestCIDFromInt(i int) cid.Cid {
	return generateTestCID([]byte(fmt.Sprintf("test-data-%d", i)))
}

// Test helper function to create a new DoneTracker
func TestNewDoneTracker(t *testing.T) {
	dt := NewDoneTracker()
	assert.NotNil(t, dt, "DoneTracker should not be nil")
	assert.Equal(t, 0, dt.Count(), "New DoneTracker should have count of 0")
	assert.Empty(t, dt.GetDoneCIDs(), "New DoneTracker should have no done CIDs")
}
