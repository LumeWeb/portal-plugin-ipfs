package protocol

import (
	"context"
	"sync"

	"github.com/ipfs/go-cid"
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
	GetDoneCIDs() []cid.Cid

	// Count returns the number of done CIDs
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
	completed map[string]bool        // Permanent record of completed CIDs (binary CID key)
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
	cidKey := string(c.Bytes())

	// Check permanent completed record for historical CIDs
	dt.mu.RLock()
	if dt.completed[cidKey] {
		dt.mu.RUnlock()
		return true
	}
	dt.mu.RUnlock()

	dt.mu.Lock()
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
// Note: Completed CIDs are removed from the map after processing,
// so this only returns CIDs that are currently being waited on but marked as done
// This method is thread-safe
func (dt *DefaultDoneTracker) GetDoneCIDs() []cid.Cid {
	dt.mu.RLock()
	defer dt.mu.RUnlock()

	var doneCIDs []cid.Cid
	for _, waiter := range dt.waiters {
		if waiter.done {
			doneCIDs = append(doneCIDs, waiter.cid)
		}
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
// Note: Completed CIDs are removed from the map after processing,
// so this only counts CIDs that are currently being waited on but marked as done
// This method is thread-safe
func (dt *DefaultDoneTracker) Count() int {
	dt.mu.RLock()
	defer dt.mu.RUnlock()

	count := 0
	for _, waiter := range dt.waiters {
		if waiter.done {
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
