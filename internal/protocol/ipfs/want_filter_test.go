package ipfs

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func newTestCollector() *topNDeniedPeersCollector {
	return newTopNDeniedPeersCollector(10)
}

func TestWantBlockFilterGatewayPeer(t *testing.T) {
	gatewayPeer := peer.ID("gateway-peer")
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{
		GatewayPeers:         []peer.ID{gatewayPeer},
		GlobalRate:           10,
		GlobalBurst:          10,
		PerPeerRate:          1,
		PerPeerBurst:         1,
		DeniedPeersCollector: newTestCollector(),
	})

	// Gateway peer should always be allowed, even after rate limit is exhausted
	for i := 0; i < 100; i++ {
		if !filter.Allow(gatewayPeer, cid.Undef) {
			t.Fatalf("gateway peer should always be allowed, failed at iteration %d", i)
		}
	}
}

func TestWantBlockFilterSelfPeer(t *testing.T) {
	selfPeer := peer.ID("self-peer")
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{
		SelfPeer:             selfPeer,
		GlobalRate:           1,
		GlobalBurst:          1,
		PerPeerRate:          1,
		PerPeerBurst:         1,
		DeniedPeersCollector: newTestCollector(),
	})

	// Self peer should always be allowed, even after rate limits are exhausted
	for i := 0; i < 100; i++ {
		if !filter.Allow(selfPeer, cid.Undef) {
			t.Fatalf("self peer should always be allowed, failed at iteration %d", i)
		}
	}
}

func TestWantBlockFilterRateLimiting(t *testing.T) {
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{
		GatewayPeers:         nil,
		GlobalRate:           100,
		GlobalBurst:          5,
		PerPeerRate:          1,
		PerPeerBurst:         3,
		DeniedPeersCollector: newTestCollector(),
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
		GatewayPeers:         nil,
		GlobalRate:           10,
		GlobalBurst:          10,
		PerPeerRate:          1,
		PerPeerBurst:         1,
		DeniedPeersCollector: newTestCollector(),
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
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{
		DeniedPeersCollector: newTestCollector(),
	})

	p := peer.ID("some-peer")

	// Should use default rate limits
	if !filter.Allow(p, cid.Undef) {
		t.Error("first request should be allowed with default config")
	}
}

func TestWantBlockFilterPerPeerIsolation(t *testing.T) {
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{
		GlobalRate:           1000,
		GlobalBurst:          1000,
		PerPeerRate:          1,
		PerPeerBurst:         2,
		DeniedPeersCollector: newTestCollector(),
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
		GatewayPeers:         []peer.ID{gatewayPeer},
		GlobalRate:           10,
		GlobalBurst:          10,
		PerPeerRate:          1,
		PerPeerBurst:         1,
		DeniedPeersCollector: newTestCollector(),
	})

	fn := filter.PeerBlockRequestFilter()

	// Gateway peer should always pass
	if !fn(gatewayPeer, cid.Undef) {
		t.Error("gateway peer should pass via PeerBlockRequestFilter func")
	}
}

func TestWantBlockFilterNilLogger(t *testing.T) {
	// Should not panic when no logger is provided
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{
		GlobalRate:           10,
		GlobalBurst:          1,
		PerPeerRate:          1,
		PerPeerBurst:         1,
		DeniedPeersCollector: newTestCollector(),
	})

	p := peer.ID("test-peer")

	// Exhaust rate limit to trigger denied log path
	filter.Allow(p, cid.Undef)
	// This should not panic even with nil logger
	filter.Allow(p, cid.Undef)
}

func TestWantBlockFilterTopDeniedPeers(t *testing.T) {
	collector := newTopNDeniedPeersCollector(3)
	filter := NewWantBlockFilter(nil, WantBlockFilterConfig{
		GlobalRate:           100,
		GlobalBurst:          100,
		PerPeerRate:          1,
		PerPeerBurst:         1,
		DeniedPeersCollector: collector,
	})

	// Peer A — denied 10 times
	peerA := peer.ID("peer-A")
	for i := 0; i < 11; i++ {
		filter.Allow(peerA, cid.Undef) // 1 allowed, 10 denied
	}

	// Peer B — denied 5 times
	peerB := peer.ID("peer-B")
	for i := 0; i < 6; i++ {
		filter.Allow(peerB, cid.Undef) // 1 allowed, 5 denied
	}

	// Peer C — denied 3 times
	peerC := peer.ID("peer-C")
	for i := 0; i < 4; i++ {
		filter.Allow(peerC, cid.Undef) // 1 allowed, 3 denied
	}

	// Peer D — denied 1 time (should NOT appear in top 3)
	peerD := peer.ID("peer-D")
	for i := 0; i < 2; i++ {
		filter.Allow(peerD, cid.Undef) // 1 allowed, 1 denied
	}

	// Collect metrics
	ch := make(chan prometheus.Metric, 10)
	collector.Collect(ch)
	close(ch)

	count := 0
	for m := range ch {
		dto := &dto.Metric{}
		if err := m.Write(dto); err != nil {
			t.Fatal(err)
		}
		count++
	}

	// Verify we have at most 3 metrics (top N)
	if count > 3 {
		t.Errorf("expected at most 3 top denied peers, got %d", count)
	}
}
