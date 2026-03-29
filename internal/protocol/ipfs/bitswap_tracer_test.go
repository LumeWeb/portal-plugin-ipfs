package ipfs

import (
	"testing"

	pb "github.com/ipfs/boxo/bitswap/message/pb"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	"github.com/ipfs/go-cid"
	ma "github.com/multiformats/go-multiaddr"
)

func TestExtractPeerIP_IPv4(t *testing.T) {
	tests := []struct {
		name  string
		maddr ma.Multiaddr
		want  string
	}{
		{
			name:  "valid IPv4",
			maddr: ma.StringCast("/ip4/192.168.1.1/tcp/4001"),
			want:  "192.168.1.1",
		},
		{
			name:  "valid IPv4 with different port",
			maddr: ma.StringCast("/ip4/10.0.0.5/tcp/8000"),
			want:  "10.0.0.5",
		},
		{
			name:  "IPv4 with multiple protocols",
			maddr: ma.StringCast("/ip4/172.16.0.1/quic-v1"),
			want:  "172.16.0.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractPeerIP(tt.maddr)
			if got != tt.want {
				t.Errorf("extractPeerIP() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractPeerIP_IPv6(t *testing.T) {
	tests := []struct {
		name  string
		maddr ma.Multiaddr
		want  string
	}{
		{
			name:  "valid IPv6",
			maddr: ma.StringCast("/ip6/2001:db8::1/tcp/4001"),
			want:  "2001:db8::1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractPeerIP(tt.maddr)
			if got != tt.want {
				t.Errorf("extractPeerIP() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractPeerIP_DNS(t *testing.T) {
	tests := []struct {
		name  string
		maddr ma.Multiaddr
		want  string
	}{
		{
			name:  "dns4",
			maddr: ma.StringCast("/dns4/example.com/tcp/4001"),
			want:  "example.com",
		},
		{
			name:  "dns6",
			maddr: ma.StringCast("/dns6/example.com/udp/4001/quic-v1"),
			want:  "example.com",
		},
		{
			name:  "dns",
			maddr: ma.StringCast("/dns/example.org/tcp/443"),
			want:  "example.org",
		},
		{
			name:  "dnsaddr",
			maddr: ma.StringCast("/dnsaddr/something.here"),
			want:  "something.here",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractPeerIP(tt.maddr)
			if got != tt.want {
				t.Errorf("extractPeerIP() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractPeerIP_InvalidOrSpecial(t *testing.T) {
	tests := []struct {
		name  string
		maddr ma.Multiaddr
		want  string
	}{
		{
			name:  "unspecified IPv4",
			maddr: ma.StringCast("/ip4/0.0.0.0/tcp/4001"),
			want:  "",
		},
		{
			name:  "loopback IPv4",
			maddr: ma.StringCast("/ip4/127.0.0.1/tcp/4001"),
			want:  "",
		},
		{
			name:  "loopback IPv6",
			maddr: ma.StringCast("/ip6/::1/tcp/4001"),
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractPeerIP(tt.maddr)
			if got != tt.want {
				t.Errorf("extractPeerIP() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTrackWantsFromBitSwapMessage(t *testing.T) {
	tracker := NewBlockRequestTracker()

	msg := bsmsg.New(false)
	cid1, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	cid2, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfW")
	msg.AddEntry(cid1, 1, pb.Message_Wantlist_Block, false)
	msg.AddEntry(cid2, 1, pb.Message_Wantlist_Block, false)

	peerIP := "192.168.1.1"

	// Call the pure helper function
	trackWantsFromBitSwapMessage(msg, peerIP, tracker)

	// Verify both CIDs were tracked
	peer1, exists1 := tracker.GetAndRemoveRandomPeer(cid1)
	if !exists1 {
		t.Fatal("Expected CID1 to be tracked")
	}
	if peer1 != peerIP {
		t.Errorf("Expected peer IP %s, got %s", peerIP, peer1)
	}

	peer2, exists2 := tracker.GetAndRemoveRandomPeer(cid2)
	if !exists2 {
		t.Fatal("Expected CID2 to be tracked")
	}
	if peer2 != peerIP {
		t.Errorf("Expected peer IP %s, got %s", peerIP, peer2)
	}
}

func TestTrackWantsFromBitSwapMessage_NilTracker(t *testing.T) {
	msg := bsmsg.New(false)
	cid, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	msg.AddEntry(cid, 1, pb.Message_Wantlist_Block, false)

	// Should not panic with nil tracker
	trackWantsFromBitSwapMessage(msg, "192.168.1.1", nil)
}

func TestTrackWantsFromBitSwapMessage_EmptyPeerIP(t *testing.T) {
	tracker := NewBlockRequestTracker()

	msg := bsmsg.New(false)
	cid, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	msg.AddEntry(cid, 1, pb.Message_Wantlist_Block, false)

	trackWantsFromBitSwapMessage(msg, "", tracker)

	// Should not track anything
	_, exists := tracker.GetAndRemoveRandomPeer(cid)
	if exists {
		t.Fatal("Should not track peer when IP is empty")
	}
}

func TestTrackWantsFromBitSwapMessage_NilWantlist(t *testing.T) {
	tracker := NewBlockRequestTracker()

	// Empty message
	msg := bsmsg.New(false)

	trackWantsFromBitSwapMessage(msg, "192.168.1.1", tracker)

	// Should not panic
}

func TestTrackWantsFromBitSwapMessage_UndefinedCID(t *testing.T) {
	tracker := NewBlockRequestTracker()

	msg := bsmsg.New(false)
	// Add undefined CID (should be skipped)
	msg.AddEntry(cid.Undef, 1, pb.Message_Wantlist_Block, false)
	// Add valid CID
	validCID, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	msg.AddEntry(validCID, 1, pb.Message_Wantlist_Block, false)

	trackWantsFromBitSwapMessage(msg, "192.168.1.1", tracker)

	// Should only track the valid CID
	peer, exists := tracker.GetAndRemoveRandomPeer(validCID)
	if !exists {
		t.Fatal("Expected valid CID to be tracked")
	}
	if peer != "192.168.1.1" {
		t.Errorf("Expected peer IP 192.168.1.1, got %s", peer)
	}

	// Undefined CID should not be tracked
	_, exists = tracker.GetAndRemoveRandomPeer(cid.Undef)
	if exists {
		t.Fatal("Undefined CID should not be tracked")
	}
}

func TestTrackWantsFromBitSwapMessage_MultipleWants(t *testing.T) {
	tracker := NewBlockRequestTracker()

	msg := bsmsg.New(false)

	cids := []string{
		"QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX",
		"QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfW",
		"QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfY",
	}

	for _, cidStr := range cids {
		cid, _ := cid.Decode(cidStr)
		msg.AddEntry(cid, 1, pb.Message_Wantlist_Block, false)
	}

	peerIP := "10.0.0.5"
	trackWantsFromBitSwapMessage(msg, peerIP, tracker)

	// Verify all CIDs were tracked
	for _, cidStr := range cids {
		cid, _ := cid.Decode(cidStr)
		peer, exists := tracker.GetAndRemoveRandomPeer(cid)
		if !exists {
			t.Fatalf("CID %s should have been tracked", cidStr)
		}
		if peer != peerIP {
			t.Errorf("Expected peer IP %s, got %s", peerIP, peer)
		}
	}
}

func TestTrackWantsFromBitSwapMessage_DuplicatePeers(t *testing.T) {
	tracker := NewBlockRequestTracker()

	msg := bsmsg.New(false)
	cid, _ := cid.Decode("QmYwAPJzv5CZsnA625qs3FTJ2xDkg7WjNnCm129r48gVfX")
	msg.AddEntry(cid, 1, pb.Message_Wantlist_Block, false)

	peerIP := "192.168.1.1"

	// Same peer requests same CID multiple times
	trackWantsFromBitSwapMessage(msg, peerIP, tracker)
	trackWantsFromBitSwapMessage(msg, peerIP, tracker)
	trackWantsFromBitSwapMessage(msg, peerIP, tracker)

	// Should only have one peer entry
	count := 0
	for i := 0; i < 5; i++ {
		if _, exists := tracker.GetAndRemoveRandomPeer(cid); exists {
			count++
		}
	}

	if count != 1 {
		t.Fatalf("Expected 1 peer (no duplicates), got %d", count)
	}
}
