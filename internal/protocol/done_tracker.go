package protocol

import (
	"context"
	"sync"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal/core"
)

// DoneTracker defines the interface for tracking completed CIDs
// This allows for dependency injection and testability
type DoneTracker interface {
	// Done marks a CID as completed and notifies all waiters
	Done(c cid.Cid)

	// WaitDone waits until the CID is marked as done or the context is canceled
	// Returns true if the CID is done, false if the context was canceled
	WaitDone(ctx context.Context, c cid.Cid) bool

	// IsDone checks if a CID is marked as done without blocking
	IsDone(c cid.Cid) bool

	// GetDoneCIDs returns a copy of all completed CIDs
	// This includes CIDs with active waiters and CIDs permanently marked as complete
	GetDoneCIDs() []cid.Cid

	// Count returns the number of done CIDs
	// This includes CIDs with active waiters and CIDs permanently marked as complete
	Count() int

	// Reset clears all waiters and done CIDs
	Reset()
}

// cidWaiter holds the state for tracking waiters for a specific CID
type cidWaiter struct {
	cid     cid.Cid
	done    bool            // Whether this CID is done
	waiters []chan struct{} // Channels waiting for this CID to be done
}

// DefaultDoneTracker provides common functionality for tracking completed CIDs
// This can be embedded in BlockProcessor implementations to stay DRY
type DefaultDoneTracker struct {
	mu        sync.RWMutex
	waiters   map[string]*cidWaiter // Map of CID binary representation to waiter state
	completed map[string]bool       // Permanent record of completed CIDs (binary CID key)
}

// NewDoneTracker creates a new DefaultDoneTracker instance
func NewDoneTracker() *DefaultDoneTracker {
	return &DefaultDoneTracker{
		waiters:   make(map[string]*cidWaiter),
		completed: make(map[string]bool),
	}
}

// Done marks a CID as completed and notifies all waiters
// This method is thread-safe
func (dt *DefaultDoneTracker) Done(c cid.Cid) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	cidKey := string(c.Bytes())
	waiter, exists := dt.waiters[cidKey]
	if !exists {
		// Add to permanent completed record
		dt.completed[cidKey] = true

		// Create a done marker entry to indicate this CID has been marked as done
		// This allows subsequent WaitDone calls to immediately return true
		waiter = &cidWaiter{
			cid:     c,
			done:    true,
			waiters: []chan struct{}{},
		}
		dt.waiters[cidKey] = waiter
		return
	}

	// If already done, nothing to do
	if waiter.done {
		return
	}

	// Mark as done and notify all waiters
	waiter.done = true
	waitersToClose := make([]chan struct{}, len(waiter.waiters))
	copy(waitersToClose, waiter.waiters)
	waiter.waiters = waiter.waiters[:0] // Clear the slice

	// Add to permanent completed record
	dt.completed[cidKey] = true

	// Delete the waiter from the map since it's fully processed
	// Note: The CID remains in the completed map for permanent tracking
	delete(dt.waiters, cidKey)

	// Release lock before closing channels to avoid deadlock
	dt.mu.Unlock()
	defer dt.mu.Lock()

	for _, ch := range waitersToClose {
		close(ch)
	}
}

// WaitDone waits until the CID is marked as done or the context is canceled
// Returns true if the CID is done, false if the context was canceled
// This method is thread-safe
func (dt *DefaultDoneTracker) WaitDone(ctx context.Context, c cid.Cid) bool {
	ctx, span := core.TraceMethod(ctx, "DefaultDoneTracker.WaitDone")
	defer span.End()

	cidKey := string(c.Bytes())

	// Use a single Lock for both completed and waiters checks to prevent race condition
	dt.mu.Lock()

	// Check permanent completed record for historical CIDs
	if dt.completed[cidKey] {
		dt.mu.Unlock()
		return true
	}

	waiter, exists := dt.waiters[cidKey]

	if !exists {
		waiter = &cidWaiter{
			cid:     c,
			done:    false,
			waiters: []chan struct{}{},
		}
		dt.waiters[cidKey] = waiter
	}

	if waiter.done {
		dt.mu.Unlock()
		return true
	}

	ch := make(chan struct{})
	waiter.waiters = append(waiter.waiters, ch)

	// Release lock before waiting to allow Done() to proceed
	dt.mu.Unlock()

	select {
	case <-ch:
		return true
	case <-ctx.Done():
		dt.mu.Lock()
		if newWaiter, exists := dt.waiters[cidKey]; exists {
			for i, wch := range newWaiter.waiters {
				if wch == ch {
					newWaiter.waiters = append(newWaiter.waiters[:i], newWaiter.waiters[i+1:]...)
					break
				}
			}
			if len(newWaiter.waiters) == 0 {
				delete(dt.waiters, cidKey)
			}
		}
		dt.mu.Unlock()
		return false
	}
}

// GetDoneCIDs returns a copy of all completed CIDs
// This includes CIDs with active waiters and CIDs permanently marked as complete
// This method is thread-safe
func (dt *DefaultDoneTracker) GetDoneCIDs() []cid.Cid {
	dt.mu.RLock()
	defer dt.mu.RUnlock()

	// Use map to avoid duplicates
	doneCIDMap := make(map[string]cid.Cid)

	// Add CIDs from waiters that are done
	for _, waiter := range dt.waiters {
		if waiter.done {
			cidKey := string(waiter.cid.Bytes())
			doneCIDMap[cidKey] = waiter.cid
		}
	}

	// Add CIDs from permanent completed record
	for cidKey := range dt.completed {
		// Parse the binary CID key back to a CID
		if c, err := cid.Cast([]byte(cidKey)); err == nil {
			doneCIDMap[cidKey] = c
		}
	}

	// Convert map to slice
	doneCIDs := make([]cid.Cid, 0, len(doneCIDMap))
	for _, c := range doneCIDMap {
		doneCIDs = append(doneCIDs, c)
	}

	return doneCIDs
}

// Reset clears all waiters and done CIDs
// This method is thread-safe
func (dt *DefaultDoneTracker) Reset() {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.waiters = make(map[string]*cidWaiter)
	dt.completed = make(map[string]bool)
}

// Count returns the number of done CIDs
// This includes CIDs with active waiters and CIDs permanently marked as complete
// This method is thread-safe
func (dt *DefaultDoneTracker) Count() int {
	dt.mu.RLock()
	defer dt.mu.RUnlock()

	count := 0

	// Count waiters that are done
	for _, waiter := range dt.waiters {
		if waiter.done {
			count++
		}
	}

	// Add count from permanent completed record (excluding duplicates with waiters)
	for cidKey := range dt.completed {
		if _, exists := dt.waiters[cidKey]; !exists {
			count++
		}
	}

	return count
}

// IsDone checks if a CID is marked as done without blocking
// This method checks both current state and permanent history
// This method is thread-safe
func (dt *DefaultDoneTracker) IsDone(c cid.Cid) bool {
	cidKey := string(c.Bytes())

	dt.mu.RLock()
	defer dt.mu.RUnlock()

	// Check permanent completed record first
	if dt.completed[cidKey] {
		return true
	}

	// Check current active waiters
	waiter, exists := dt.waiters[cidKey]
	return exists && waiter.done
}
