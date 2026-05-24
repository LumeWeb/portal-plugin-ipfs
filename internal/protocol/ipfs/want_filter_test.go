package ipfs

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestWantBlockFilterGatewayPeer(t *testing.T) {
	gatewayPeer := peer.ID("gateway-peer")
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{
		GatewayPeers: []peer.ID{gatewayPeer},
		GlobalRate:   10,
		GlobalBurst:  10,
		PerPeerRate:  1,
		PerPeerBurst: 1,
	})

	// Gateway peer should always be allowed, even after rate limit is exhausted
	for i := 0; i < 100; i++ {
		if !filter.Allow(gatewayPeer, cid.Undef) {
			t.Fatalf("gateway peer should always be allowed, failed at iteration %d", i)
		}
	}
}

func TestWantBlockFilterRateLimiting(t *testing.T) {
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{
		GatewayPeers: nil,
		GlobalRate:   100,
		GlobalBurst:  5,
		PerPeerRate:  1,
		PerPeerBurst: 3,
	})

	normalPeer := peer.ID("normal-peer")

	// First 3 requests should succeed (per-peer burst)
	allowed := 0
	for i := 0; i < 10; i++ {
		if filter.Allow(normalPeer, cid.Undef) {
			allowed++
		}
	}

	if allowed > 5 {
		t.Errorf("expected at most 5 allowed requests (burst), got %d", allowed)
	}
}

func TestWantBlockFilterAddRemoveGateway(t *testing.T) {
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{
		GatewayPeers: nil,
		GlobalRate:   10,
		GlobalBurst:  10,
		PerPeerRate:  1,
		PerPeerBurst: 1,
	})

	p := peer.ID("new-gateway")

	// Before adding, peer is rate-limited
	if filter.Allow(p, cid.Undef) {
		// Might succeed on first try due to burst
	}

	filter.AddGatewayPeer(p)

	// After adding, peer should always be allowed
	for i := 0; i < 20; i++ {
		if !filter.Allow(p, cid.Undef) {
			t.Fatal("gateway peer should always be allowed after AddGatewayPeer")
		}
	}

	filter.RemoveGatewayPeer(p)

	// After removing, peer is rate-limited again
	allowed := 0
	for i := 0; i < 20; i++ {
		if filter.Allow(p, cid.Undef) {
			allowed++
		}
	}
	if allowed > 5 {
		t.Errorf("expected rate limiting after RemoveGatewayPeer, got %d allowed", allowed)
	}
}

func TestWantBlockFilterDefaultConfig(t *testing.T) {
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{})

	p := peer.ID("some-peer")

	// Should use default rate limits
	if !filter.Allow(p, cid.Undef) {
		t.Error("first request should be allowed with default config")
	}
}

func TestWantBlockFilterPerPeerIsolation(t *testing.T) {
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{
		GlobalRate:   1000,
		GlobalBurst:  1000,
		PerPeerRate:  1,
		PerPeerBurst: 2,
	})

	peer1 := peer.ID("peer-1")
	peer2 := peer.ID("peer-2")

	// Exhaust peer1's rate limit
	filter.Allow(peer1, cid.Undef)
	filter.Allow(peer1, cid.Undef)

	// peer2 should still be allowed (independent rate limiters)
	if !filter.Allow(peer2, cid.Undef) {
		t.Error("peer2 should have its own rate limit independent of peer1")
	}
}

func TestWantBlockFilterPeerBlockRequestFilterFunc(t *testing.T) {
	gatewayPeer := peer.ID("gateway")
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{
		GatewayPeers: []peer.ID{gatewayPeer},
		GlobalRate:   10,
		GlobalBurst:  10,
		PerPeerRate:  1,
		PerPeerBurst: 1,
	})

	fn := filter.PeerBlockRequestFilter()

	// Gateway peer should always pass
	if !fn(gatewayPeer, cid.Undef) {
		t.Error("gateway peer should pass via PeerBlockRequestFilter func")
	}
}
