package ipfs

import (
	"testing"

	"github.com/ipfs/go-cid"
)

func TestBlockRequestTracker_AddRequest(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cid1, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	
	peers := []string{"192.168.1.1", "192.168.1.2", "192.168.1.3"}
	
	// Test adding single peer
	tracker.AddRequest(cid1, peers[0])
	peer, exists := tracker.GetAndRemoveRandomPeer(cid1)
	if !exists {
		t.Fatal("Expected peer to exist after adding")
	}
	if peer != peers[0] {
		t.Fatalf("Expected peer %s, got %s", peers[0], peer)
	}
	
	// Test that peer is removed after selection
	_, exists = tracker.GetAndRemoveRandomPeer(cid1)
	if exists {
		t.Fatal("Expected no peers after removal")
	}
	
	// Test adding multiple peers for same CID
	for _, peer := range peers {
		tracker.AddRequest(cid1, peer)
	}
	
	// We should be able to get all 3 peers
	count := 0
	for range peers {
		if peer, exists := tracker.GetAndRemoveRandomPeer(cid1); exists {
			count++
			t.Logf("Got peer: %s", peer)
		} else {
			break
		}
	}
	
	if count != len(peers) {
		t.Fatalf("Expected to get %d peers, got %d", len(peers), count)
	}
	
	// Verify no more peers available
	_, exists = tracker.GetAndRemoveRandomPeer(cid1)
	if exists {
		t.Fatal("Expected no peers after removing all")
	}
}

func TestBlockRequestTracker_AddRequest_DuplicatePrevention(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cid, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	
	// Add the same peer twice
	tracker.AddRequest(cid, "192.168.1.1")
	tracker.AddRequest(cid, "192.168.1.1")
	tracker.AddRequest(cid, "192.168.1.1")
	
	// Should only get one peer since duplicates should be ignored
	count := 0
	for i := 0; i < 10; i++ {
		if _, exists := tracker.GetAndRemoveRandomPeer(cid); exists {
			count++
		}
	}
	
	if count != 1 {
		t.Fatalf("Expected 1 peer, got %d (duplicates should be prevented)", count)
	}
}

func TestBlockRequestTracker_AddRequest_EmptyInputs(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cidUndefined := cid.Undef
	cid1, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	
	// Test with undefined CID
	tracker.AddRequest(cidUndefined, "192.168.1.1")
	peer, exists := tracker.GetAndRemoveRandomPeer(cidUndefined)
	if exists {
		t.Fatal("Should not add peer for undefined CID")
	}
	
	// Test with empty peer IP
	tracker.AddRequest(cid1, "")
	tracker.AddRequest(cid1, "192.168.1.1")
	
	peer, exists = tracker.GetAndRemoveRandomPeer(cid1)
	if !exists {
		t.Fatal("Expected peer to exist")
	}
	if peer != "192.168.1.1" {
		t.Fatalf("Expected peer 192.168.1.1, got %s", peer)
	}
	
	// Empty peer should not be counted
	_, exists = tracker.GetAndRemoveRandomPeer(cid1)
	if exists {
		t.Fatal("Should not have any peers left (empty peer should be ignored)")
	}
}

func TestBlockRequestTracker_GetAndRemoveRandomPeer_NonExistent(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cid, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	
	peer, exists := tracker.GetAndRemoveRandomPeer(cid)
	if exists {
		t.Fatalf("Expected no peer for non-existent CID, got %s", peer)
	}
}

func TestBlockRequestTracker_PopPeer(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cid, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	
	peers := []string{"192.168.1.1", "192.168.1.2", "192.168.1.3"}
	
	// Add peers in order
	for _, peer := range peers {
		tracker.AddRequest(cid, peer)
	}
	
	// PopPeer should return first peer (FIFO order)
	peer, exists := tracker.PopPeer(cid)
	if !exists {
		t.Fatal("Expected peer to exist")
	}
	if peer != peers[0] {
		t.Fatalf("Expected first peer %s, got %s", peers[0], peer)
	}
	
	// Second pop should return second peer
	peer, exists = tracker.PopPeer(cid)
	if !exists {
		t.Fatal("Expected peer to exist")
	}
	if peer != peers[1] {
		t.Fatalf("Expected second peer %s, got %s", peers[1], peer)
	}
	
	// Last pop should return third peer
	peer, exists = tracker.PopPeer(cid)
	if !exists {
		t.Fatal("Expected peer to exist")
	}
	if peer != peers[2] {
		t.Fatalf("Expected third peer %s, got %s", peers[2], peer)
	}
	
	// No more peers available
	_, exists = tracker.PopPeer(cid)
	if exists {
		t.Fatal("Expected no more peers")
	}
	
	t.Log("PopPeer works correctly")
}

func TestBlockRequestTracker_ConcurrentAccess(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cid1, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	cid2, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfW")
	
	done := make(chan bool)
	
	// Goroutine adding requests
	go func() {
		for i := 0; i < 100; i++ {
			tracker.AddRequest(cid1, "192.168.1.1")
			tracker.AddRequest(cid1, "192.168.1.2")
			tracker.AddRequest(cid2, "192.168.2.1")
		}
		done <- true
	}()
	
	// Goroutine removing requests
	go func() {
		for i := 0; i < 100; i++ {
			tracker.GetAndRemoveRandomPeer(cid1)
			tracker.GetAndRemoveRandomPeer(cid2)
		}
		done <- true
	}()
	
	// Wait for both goroutines
	<-done
	<-done
	
	// If we got here without panic or deadlock, test passes
	t.Log("Concurrent access test passed")
}

func TestBlockRequestTracker_MultipleCIDs(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cid1, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	cid2, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfW")
	cid3, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfY")
	
	// Add different peers for different CIDs
	tracker.AddRequest(cid1, "192.168.1.1")
	tracker.AddRequest(cid1, "192.168.1.2")
	tracker.AddRequest(cid2, "192.168.2.1")
	tracker.AddRequest(cid3, "192.168.3.1")
	tracker.AddRequest(cid3, "192.168.3.2")
	tracker.AddRequest(cid3, "192.168.3.3")
	
	// Verify CID1 has 2 peers
	peer, exists := tracker.GetAndRemoveRandomPeer(cid1)
	if !exists {
		t.Fatal("Expected CID1 to have peers")
	}
	t.Logf("CID1 got peer: %s", peer)
	peer, exists = tracker.GetAndRemoveRandomPeer(cid1)
	if !exists {
		t.Fatal("Expected CID1 to have another peer")
	}
	t.Logf("CID1 got peer: %s", peer)
	
	// No more peers for CID1
	_, exists = tracker.GetAndRemoveRandomPeer(cid1)
	if exists {
		t.Fatal("CID1 should have no more peers")
	}
	
	// Verify CID2 has 1 peer
	peer, exists = tracker.GetAndRemoveRandomPeer(cid2)
	if !exists {
		t.Fatal("Expected CID2 to have peers")
	}
	if peer != "192.168.2.1" {
		t.Fatalf("Expected peer 192.168.2.1, got %s", peer)
	}
	
	// Verify CID3 has 3 peers
	count := 0
	expectedPeers := map[string]bool{
		"192.168.3.1": true,
		"192.168.3.2": true,
		"192.168.3.3": true,
	}
	
	for i := 0; i < 3; i++ {
		if peer, exists := tracker.GetAndRemoveRandomPeer(cid3); exists {
			count++
			delete(expectedPeers, peer)
			t.Logf("CID3 got peer: %s", peer)
		}
	}
	
	if count != 3 {
		t.Fatalf("Expected 3 peers for CID3, got %d", count)
	}
	
	if len(expectedPeers) > 0 {
		t.Fatalf("Didn't get all expected peers: %v", expectedPeers)
	}
}

func TestBlockRequestTracker_RandomnessDistribution(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cid, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	
	peers := []string{"peer1", "peer2", "peer3", "peer4", "peer5"}
	
	// Add all peers
	for _, peer := range peers {
		tracker.AddRequest(cid, peer)
	}
	
	// Collect all selections to verify randomness
	selections := make(map[string]int)
	for i := 0; i < len(peers); i++ {
		if peer, exists := tracker.GetAndRemoveRandomPeer(cid); exists {
			selections[peer]++
		}
	}
	
	// Verify each peer was selected exactly once
	if len(selections) != len(peers) {
		t.Fatalf("Expected %d unique peer selections, got %d", len(peers), len(selections))
	}
	
	for _, peer := range peers {
		if selections[peer] != 1 {
			t.Fatalf("Peer %s was selected %d times, expected 1", peer, selections[peer])
		}
	}
}

func TestBlockRequestTracker_EmptyTracker(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cid, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	
	// Should not panic on empty tracker
	peer, exists := tracker.GetAndRemoveRandomPeer(cid)
	if exists {
		t.Fatalf("Should not return peer for empty tracker, got %s", peer)
	}
}

func TestBlockRequestTracker_RemovePeerFromAll_SinglePeer(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cid1, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	cid2, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfW")
	
	// Add the same peer to multiple CIDs
	tracker.AddRequest(cid1, "192.168.1.1")
	tracker.AddRequest(cid2, "192.168.1.1")
	
	// Remove peer from all CIDs
	tracker.RemovePeerFromAll("192.168.1.1")
	
	// Verify peer is removed from all CIDs
	_, exists1 := tracker.GetAndRemoveRandomPeer(cid1)
	_, exists2 := tracker.GetAndRemoveRandomPeer(cid2)
	
	if exists1 || exists2 {
		t.Fatal("Peer should be removed from all CIDs")
	}
}

func TestBlockRequestTracker_RemovePeerFromAll_MultiplePeers(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cid1, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	cid2, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfW")
	
	// Add multiple peers to multiple CIDs
	tracker.AddRequest(cid1, "192.168.1.1")
	tracker.AddRequest(cid1, "192.168.1.2")
	tracker.AddRequest(cid1, "192.168.1.3")
	
	tracker.AddRequest(cid2, "192.168.1.1") // Same peer in multiple CIDs
	tracker.AddRequest(cid2, "192.168.2.1")
	
	// Remove one peer from all CIDs
	tracker.RemovePeerFromAll("192.168.1.1")
	
	// CID1 should still have 2 peers (192.168.1.2 and 192.168.1.3)
	count1 := 0
	for i := 0; i < 2; i++ {
		if peer, exists := tracker.GetAndRemoveRandomPeer(cid1); exists {
			count1++
			t.Logf("CID1 got peer: %s", peer)
		}
	}
	
	if count1 != 2 {
		t.Fatalf("Expected CID1 to have 2 peers left, got %d", count1)
	}
	
	// CID2 should still have 1 peer (192.168.2.1)
	peer2, exists2 := tracker.GetAndRemoveRandomPeer(cid2)
	if !exists2 {
		t.Fatal("Expected CID2 to have 1 peer left")
	}
	if peer2 != "192.168.2.1" {
		t.Fatalf("Expected peer 192.168.2.1, got %s", peer2)
	}
	
	// Verify 192.168.1.1 is completely gone
	_, exists1 := tracker.GetAndRemoveRandomPeer(cid1)
	_, exists2Again := tracker.GetAndRemoveRandomPeer(cid2)
	
	if exists1 || exists2Again {
		t.Fatal("All peers should have been removed")
	}
}

func TestBlockRequestTracker_RemovePeerFromAll_EmptyIP(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cid, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	
	tracker.AddRequest(cid, "192.168.1.1")
	
	// Remove with empty IP should be safe no-op
	tracker.RemovePeerFromAll("")
	
	// Verify original peer still exists
	peer, exists := tracker.GetAndRemoveRandomPeer(cid)
	if !exists {
		t.Fatal("Original peer should still exist after empty IP removal")
	}
	if peer != "192.168.1.1" {
		t.Fatalf("Expected peer 192.168.1.1, got %s", peer)
	}
}

func TestBlockRequestTracker_RemovePeerFromAll_NonExistentPeer(t *testing.T) {
	tracker := NewBlockRequestTracker()
	
	cid1, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	cid2, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfW")
	
	tracker.AddRequest(cid1, "192.168.1.1")
	tracker.AddRequest(cid2, "192.168.2.1")
	
	// Remove peer that doesn't exist should be safe
	tracker.RemovePeerFromAll("10.0.0.1")
	
	// Verify existing peers are still there
	peer1, exists1 := tracker.GetAndRemoveRandomPeer(cid1)
	peer2, exists2 := tracker.GetAndRemoveRandomPeer(cid2)
	
	if !exists1 || !exists2 {
		t.Fatal("Existing peers should not be affected")
	}
	
	if peer1 != "192.168.1.1" || peer2 != "192.168.2.1" {
		t.Fatalf("Expected peers 192.168.1.1 and 192.168.2.1, got %s and %s", peer1, peer2)
	}
}

func TestBlockRequestTracker_RemovePeerFromAll_DisconnectScenario(t *testing.T) {
	// Simulate a realistic peer disconnect scenario where a peer has
	// requested multiple blocks and then disconnects
	tracker := NewBlockRequestTracker()
	
	peerA := "192.168.1.10"
	peerB := "192.168.2.10"
	peerC := "192.168.3.10"
	
	// Use valid CIDs from other tests
	cid0, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	cid1, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfW")
	cid2, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfY")
	cid3, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfZ")
	cid4, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfA")
	cids := []cid.Cid{cid0, cid1, cid2, cid3, cid4}
	
	// Peer A and B requested block 0
	tracker.AddRequest(cids[0], peerA)
	tracker.AddRequest(cids[0], peerB)
	
	// Peer A requested blocks 1-3
	tracker.AddRequest(cids[1], peerA)
	tracker.AddRequest(cids[2], peerA)
	tracker.AddRequest(cids[3], peerA)
	
	// Peer B requested block 4
	tracker.AddRequest(cids[4], peerB)
	
	// Peer C requested block 2
	tracker.AddRequest(cids[2], peerC)
	
	// Simulate peer A disconnecting
	tracker.RemovePeerFromAll(peerA)
	
	// Verify peer A requests are cleaned up across all CIDs
	// cid[0]: should have only peerB
	peer0, exists0 := tracker.GetAndRemoveRandomPeer(cids[0])
	if !exists0 {
		t.Fatal("cid[0] should still have peerB")
	}
	if peer0 != peerB {
		t.Fatalf("Expected peerB for cid[0], got %s", peer0)
	}
	_, exists0Again := tracker.GetAndRemoveRandomPeer(cids[0])
	if exists0Again {
		t.Fatal("cid[0] should have only peerB remaining")
	}
	
	// cid[1]: should have no peers (peerA disconnected)
	_, exists1 := tracker.GetAndRemoveRandomPeer(cids[1])
	if exists1 {
		t.Fatal("cid[1] should have no peers after peerA disconnect")
	}
	
	// cid[2]: should only have peerC (peerA was removed)
	peer2, exists2 := tracker.GetAndRemoveRandomPeer(cids[2])
	if !exists2 {
		t.Fatal("cid[2] should still have peerC")
	}
	if peer2 != peerC {
		t.Fatalf("Expected peerC for cid[2], got %s", peer2)
	}
	_, exists2Again := tracker.GetAndRemoveRandomPeer(cids[2])
	if exists2Again {
		t.Fatal("cid[2] should have only peerC remaining")
	}
	
	// cid[3]: should have no peers (peerA disconnected)
	_, exists3 := tracker.GetAndRemoveRandomPeer(cids[3])
	if exists3 {
		t.Fatal("cid[3] should have no peers after peerA disconnect")
	}
	
	// cid[4]: should still have peerB (was never removed)
	peer4, exists4 := tracker.GetAndRemoveRandomPeer(cids[4])
	if !exists4 {
		t.Fatal("cid[4] should still have peerB")
	}
	if peer4 != peerB {
		t.Fatalf("Expected peerB for cid[4], got %s", peer4)
	}
	_, exists4Again := tracker.GetAndRemoveRandomPeer(cids[4])
	if exists4Again {
		t.Fatal("cid[4] should have only peerB remaining")
	}
	
	t.Log("Disconnect scenario test passed - ghost wants cleaned up correctly")
}
