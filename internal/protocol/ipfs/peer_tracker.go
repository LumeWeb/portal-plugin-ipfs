package ipfs

import (
	"crypto/rand"
	"math/big"
	"slices"
	"sync"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
)

// BlockRequestTracker tracks which peers have requested specific blocks.
// This enables probabilistic peer attribution when the actual client IP context
// is not available (e.g., in bitswap scenarios).
type BlockRequestTracker struct {
	mu       sync.RWMutex
	requests map[cid.Cid][]string
	peerCIDs map[string]map[cid.Cid]struct{}
}

const (
	// maxPeersPerCID bounds attribution memory for a single hot block.
	maxPeersPerCID = 64
	// maxTrackedCIDs bounds total tracker memory. Attribution is best-effort
	// once this limit is reached; block retrieval must not be blocked by it.
	maxTrackedCIDs = 100_000
)

// NewBlockRequestTracker creates a new empty BlockRequestTracker
func NewBlockRequestTracker() *BlockRequestTracker {
	return &BlockRequestTracker{
		requests: make(map[cid.Cid][]string),
		peerCIDs: make(map[string]map[cid.Cid]struct{}),
	}
}

// AddRequest records that a peer with the given IP has requested a block with the given CID
func (br *BlockRequestTracker) AddRequest(c cid.Cid, peerIP string) {
	br.mu.Lock()
	defer br.mu.Unlock()

	c = encoding.NormalizeCid(c)
	if peerIP == "" || !c.Defined() {
		return
	}

	peers := br.requests[c]
	if slices.Contains(peers, peerIP) {
		return
	}
	if len(peers) >= maxPeersPerCID {
		// Keep attribution current for hot blocks by rotating out the oldest
		// requester when the per-CID bound is reached.
		br.removePeerCIDLocked(peers[0], c)
		peers = peers[1:]
	}
	if peers == nil && len(br.requests) >= maxTrackedCIDs {
		br.evictOneCIDLocked()
	}

	br.requests[c] = append(peers, peerIP)
	br.addPeerCIDLocked(peerIP, c)
}

// GetAndRemoveRandomPeer selects a random peer IP for the given CID and removes it from the tracker
// Returns (peerIP, true) if a peer was found, or ("", false) otherwise
func (br *BlockRequestTracker) GetAndRemoveRandomPeer(c cid.Cid) (string, bool) {
	br.mu.Lock()
	defer br.mu.Unlock()

	c = encoding.NormalizeCid(c)
	peers, exists := br.requests[c]
	if !exists || len(peers) == 0 {
		return "", false
	}

	// Select random peer using crypto/rand
	index, err := randomIndex(len(peers))
	if err != nil {
		index = 0
	}

	selected := peers[index]
	br.removePeerCIDLocked(selected, c)

	// Remove selected peer from list
	if len(peers) == 1 {
		delete(br.requests, c)
	} else {
		peers[index] = peers[len(peers)-1]
		br.requests[c] = peers[:len(peers)-1]
	}

	return selected, true
}

// PopPeer removes and returns the first peer IP for the CID
// Returns (peerIP, true) if a peer was found, or ("", false) otherwise
func (br *BlockRequestTracker) PopPeer(c cid.Cid) (string, bool) {
	br.mu.Lock()
	defer br.mu.Unlock()

	c = encoding.NormalizeCid(c)
	peers, exists := br.requests[c]
	if !exists || len(peers) == 0 {
		return "", false
	}

	selected := peers[0]
	br.removePeerCIDLocked(selected, c)

	if len(peers) == 1 {
		delete(br.requests, c)
	} else {
		br.requests[c] = peers[1:]
	}

	return selected, true
}

// RemovePeer removes a specific peer from the CID's peer list
func (br *BlockRequestTracker) RemovePeer(c cid.Cid, peerIP string) {
	br.mu.Lock()
	defer br.mu.Unlock()

	c = encoding.NormalizeCid(c)
	peers, exists := br.requests[c]
	if !exists {
		return
	}

	for i, p := range peers {
		if p == peerIP {
			br.removePeerCIDLocked(peerIP, c)
			if len(peers) == 1 {
				delete(br.requests, c)
			} else {
				br.requests[c] = append(peers[:i], peers[i+1:]...)
			}
			return
		}
	}
}

// RemovePeerFromAll removes a specific peer from all CIDs in the tracker.
// This is useful for cleaning up tracking data when a peer disconnects.
func (br *BlockRequestTracker) RemovePeerFromAll(peerIP string) {
	br.mu.Lock()
	defer br.mu.Unlock()

	if peerIP == "" {
		return
	}

	cids := br.peerCIDs[peerIP]
	for c := range cids {
		peers := br.requests[c]
		for i, p := range peers {
			if p != peerIP {
				continue
			}
			if len(peers) == 1 {
				delete(br.requests, c)
			} else {
				br.requests[c] = append(peers[:i], peers[i+1:]...)
			}
			break
		}
	}
	delete(br.peerCIDs, peerIP)
}

func (br *BlockRequestTracker) addPeerCIDLocked(peerIP string, c cid.Cid) {
	cids := br.peerCIDs[peerIP]
	if cids == nil {
		cids = make(map[cid.Cid]struct{})
		br.peerCIDs[peerIP] = cids
	}
	cids[c] = struct{}{}
}

func (br *BlockRequestTracker) removePeerCIDLocked(peerIP string, c cid.Cid) {
	cids := br.peerCIDs[peerIP]
	if cids == nil {
		return
	}
	delete(cids, c)
	if len(cids) == 0 {
		delete(br.peerCIDs, peerIP)
	}
}

func (br *BlockRequestTracker) evictOneCIDLocked() {
	for c, peers := range br.requests {
		delete(br.requests, c)
		for _, peerIP := range peers {
			br.removePeerCIDLocked(peerIP, c)
		}
		return
	}
}

// randomIndex returns a cryptographically secure random index in [0, n)
func randomIndex(n int) (int, error) {
	if n <= 0 {
		return 0, nil
	}

	// Calculate the range size
	rangeSize := big.NewInt(int64(n))

	// Generate a random number in [0, n)
	index, err := rand.Int(rand.Reader, rangeSize)
	if err != nil {
		return 0, err
	}

	return int(index.Int64()), nil
}
