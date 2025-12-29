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

// TestDoneTracker_RaceConditionWaitDoneVsDone tests the specific race condition
// that was fixed: WaitDone checking completed with RLock, then acquiring Lock
// while Done could mark the CID as done in between.
func TestDoneTracker_RaceConditionWaitDoneVsDone(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	// Run this test multiple times to increase chance of catching race conditions
	for run := 0; run < 100; run++ {
		dt := NewDoneTracker()
		testCid := generateTestCIDFromInt(run)

		var wg sync.WaitGroup
		numGoroutines := 50

		// Start many goroutines that will call WaitDone
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				dt.WaitDone(ctx, testCid)
			}()
		}

		// Start many goroutines that will call Done
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				dt.Done(testCid)
			}()
		}

		wg.Wait()

		// Verify final state is consistent
		assert.True(t, dt.IsDone(testCid), "CID should be marked as done")
	}
}

// TestDoneTracker_RaceConditionMultipleCIDs tests concurrent operations on multiple CIDs
func TestDoneTracker_RaceConditionMultipleCIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	dt := NewDoneTracker()
	numCIDs := 20
	cids := make([]cid.Cid, numCIDs)
	for i := 0; i < numCIDs; i++ {
		cids[i] = generateTestCIDFromInt(i)
	}

	var wg sync.WaitGroup

	// Start many waiters for each CID
	for _, c := range cids {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(testCid cid.Cid) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cancel()
				dt.WaitDone(ctx, testCid)
			}(c)
		}
	}

	// Mark CIDs as done concurrently
	for _, c := range cids {
		wg.Add(1)
		go func(testCid cid.Cid) {
			defer wg.Done()
			time.Sleep(time.Duration(randInt(0, 10)) * time.Millisecond)
			dt.Done(testCid)
		}(c)
	}

	wg.Wait()

	// Verify all CIDs are marked as done
	for _, c := range cids {
		assert.True(t, dt.IsDone(c), "CID %v should be marked as done", c)
	}
}

// TestDoneTracker_StressTestHighConcurrency performs a stress test with high concurrency
func TestDoneTracker_StressTestHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	dt := NewDoneTracker()
	numOperations := 1000
	numCIDs := 50

	var wg sync.WaitGroup

	// Randomly perform WaitDone and Done operations
	for i := 0; i < numOperations; i++ {
		cidIndex := i % numCIDs
		testCid := generateTestCIDFromInt(cidIndex)

		if i%2 == 0 {
			// WaitDone operation
			wg.Add(1)
			go func(c cid.Cid) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
				dt.WaitDone(ctx, c)
			}(testCid)
		} else {
			// Done operation
			wg.Add(1)
			go func(c cid.Cid) {
				defer wg.Done()
				dt.Done(c)
			}(testCid)
		}
	}

	wg.Wait()

	// Verify tracker is still functional
	newCid := generateTestCIDFromInt(numCIDs + 1)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	go func() {
		time.Sleep(10 * time.Millisecond)
		dt.Done(newCid)
	}()

	done := dt.WaitDone(ctx, newCid)
	assert.True(t, done, "Should be able to wait for new CID after stress test")
}

// TestDoneTracker_PermanentCompletedRecord tests that completed CIDs are permanently recorded
func TestDoneTracker_PermanentCompletedRecord(t *testing.T) {
	dt := NewDoneTracker()
	testCid := generateTestCIDFromInt(1)

	// Mark CID as done without any waiters
	dt.Done(testCid)

	// Verify it's marked as done
	assert.True(t, dt.IsDone(testCid), "CID should be marked as done")

	// WaitDone should return immediately for completed CIDs
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	start := time.Now()
	done := dt.WaitDone(ctx, testCid)
	elapsed := time.Since(start)

	assert.True(t, done, "WaitDone should return true for completed CID")
	assert.Less(t, elapsed, 5*time.Millisecond, "WaitDone should return immediately for completed CID")

	// Multiple WaitDone calls should all return immediately
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		start := time.Now()
		done := dt.WaitDone(ctx, testCid)
		elapsed := time.Since(start)
		cancel()

		assert.True(t, done, "WaitDone call %d should return true", i)
		assert.Less(t, elapsed, 5*time.Millisecond, "WaitDone call %d should return immediately", i)
	}
}

// TestDoneTracker_PermanentCompletedRecordConcurrent tests permanent record with concurrent access
func TestDoneTracker_PermanentCompletedRecordConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	dt := NewDoneTracker()
	testCid := generateTestCIDFromInt(1)

	// Mark CID as done
	dt.Done(testCid)

	var wg sync.WaitGroup
	numGoroutines := 100

	// Many concurrent WaitDone calls should all return immediately
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			start := time.Now()
			done := dt.WaitDone(ctx, testCid)
			elapsed := time.Since(start)
			cancel()

			assert.True(t, done, "Goroutine %d: WaitDone should return true", index)
			assert.Less(t, elapsed, 5*time.Millisecond, "Goroutine %d: WaitDone should return immediately", index)
		}(i)
	}

	wg.Wait()
}

// TestDoneTracker_RaceConditionWaitDoneCancellation tests race condition between
// context cancellation and Done being called
func TestDoneTracker_RaceConditionWaitDoneCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	for run := 0; run < 50; run++ {
		dt := NewDoneTracker()
		testCid := generateTestCIDFromInt(run)

		var wg sync.WaitGroup
		numGoroutines := 20

		// Start many waiters that will be canceled
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
				defer cancel()
				dt.WaitDone(ctx, testCid)
			}()
		}

		// Also call Done concurrently
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(500 * time.Microsecond)
			dt.Done(testCid)
		}()

		wg.Wait()

		// Verify final state is consistent
		assert.True(t, dt.IsDone(testCid), "CID should be marked as done")
	}
}

// TestDoneTracker_RaceConditionIsDoneVsDone tests race condition between IsDone and Done
func TestDoneTracker_RaceConditionIsDoneVsDone(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	for run := 0; run < 50; run++ {
		dt := NewDoneTracker()
		testCid := generateTestCIDFromInt(run)

		var wg sync.WaitGroup
		numGoroutines := 50

		// Start many IsDone calls
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				dt.IsDone(testCid)
			}()
		}

		// Start many Done calls
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				dt.Done(testCid)
			}()
		}

		wg.Wait()

		// Verify final state
		assert.True(t, dt.IsDone(testCid), "CID should be marked as done")
	}
}

// TestDoneTracker_RaceConditionResetVsOperations tests race condition between Reset and other operations
func TestDoneTracker_RaceConditionResetVsOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	for run := 0; run < 20; run++ {
		dt := NewDoneTracker()
		numCIDs := 10
		cids := make([]cid.Cid, numCIDs)
		for i := 0; i < numCIDs; i++ {
			cids[i] = generateTestCIDFromInt(i)
		}

		var wg sync.WaitGroup

		// Start various operations
		for _, c := range cids {
			wg.Add(1)
			go func(testCid cid.Cid) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				dt.WaitDone(ctx, testCid)
			}(c)

			wg.Add(1)
			go func(testCid cid.Cid) {
				defer wg.Done()
				dt.Done(testCid)
			}(c)

			wg.Add(1)
			go func(testCid cid.Cid) {
				defer wg.Done()
				dt.IsDone(testCid)
			}(c)
		}

		// Reset concurrently
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(1 * time.Millisecond)
			dt.Reset()
		}()

		wg.Wait()

		// After reset, tracker should be clean
		assert.Equal(t, 0, dt.Count(), "Count should be 0 after reset")
	}
}

// randInt returns a random integer in [min, max)
func randInt(min, max int) int {
	return min + int(time.Now().UnixNano()%int64(max-min))
}
