package ipfs

import (
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"golang.org/x/time/rate"
)

// WantBlockFilter implements bitswap.PeerBlockRequestFilter with gateway
// peer whitelisting and per-peer rate limiting. Gateway peers (identified
// by peer ID extracted from multiaddrs) get unlimited access. All other
// peers are rate-limited to prevent want-block overload from aggressive
// DHT peers.
type WantBlockFilter struct {
	mu           sync.RWMutex
	host         host.Host
	gatewayPeers map[peer.ID]struct{}
	limiter      *rate.Limiter
	peerLimiters map[peer.ID]*rate.Limiter
	perPeerRate  rate.Limit
	perPeerBurst int
}

// WantBlockFilterConfig holds the configuration for creating a WantBlockFilter.
type WantBlockFilterConfig struct {
	GatewayPeers []peer.ID
	GlobalRate   rate.Limit
	GlobalBurst  int
	PerPeerRate  rate.Limit
	PerPeerBurst int
}

// NewWantBlockFilter creates a new WantBlockFilter with the given configuration.
func NewWantBlockFilter(h host.Host, cfg WantBlockFilterConfig) *WantBlockFilter {
	gatewayPeers := make(map[peer.ID]struct{}, len(cfg.GatewayPeers))
	for _, p := range cfg.GatewayPeers {
		gatewayPeers[p] = struct{}{}
	}

	globalRate := cfg.GlobalRate
	globalBurst := cfg.GlobalBurst
	if globalRate <= 0 {
		globalRate = rate.Limit(config.DefaultBitswapGlobalWantRateLimit)
	}
	if globalBurst <= 0 {
		globalBurst = config.DefaultBitswapGlobalWantBurst
	}

	perPeerRate := cfg.PerPeerRate
	perPeerBurst := cfg.PerPeerBurst
	if perPeerRate <= 0 {
		perPeerRate = rate.Limit(config.DefaultBitswapPerPeerWantRateLimit)
	}
	if perPeerBurst <= 0 {
		perPeerBurst = config.DefaultBitswapPerPeerWantBurst
	}

	f := &WantBlockFilter{
		host:         h,
		gatewayPeers: gatewayPeers,
		limiter:      rate.NewLimiter(globalRate, globalBurst),
		peerLimiters: make(map[peer.ID]*rate.Limiter),
		perPeerRate:  perPeerRate,
		perPeerBurst: perPeerBurst,
	}
	WantBlockGatewayPeers.Set(float64(len(gatewayPeers)))
	WantBlockPeerLimiters.Set(0)
	return f
}

// Allow is called by bitswap for each wantlist entry from a peer.
// It returns true if the request should be fulfilled.
// Gateway peers always pass. Other peers are subject to global and per-peer rate limits.
func (f *WantBlockFilter) Allow(p peer.ID, c cid.Cid) bool {
	f.mu.RLock()
	_, isGateway := f.gatewayPeers[p]
	f.mu.RUnlock()

	if isGateway {
		WantBlockRequestsTotal.WithLabelValues(LabelWantAllowedGateway).Inc()
		return true
	}

	if !f.limiter.Allow() {
		WantBlockRequestsTotal.WithLabelValues(LabelWantDeniedGlobalRate).Inc()
		return false
	}

	peerLimiter := f.getPeerLimiter(p)
	if !peerLimiter.Allow() {
		WantBlockRequestsTotal.WithLabelValues(LabelWantDeniedPerPeerRate).Inc()
		return false
	}

	WantBlockRequestsTotal.WithLabelValues(LabelWantAllowed).Inc()
	return true
}

func (f *WantBlockFilter) getPeerLimiter(p peer.ID) *rate.Limiter {
	f.mu.RLock()
	limiter, ok := f.peerLimiters[p]
	f.mu.RUnlock()

	if ok {
		return limiter
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	limiter, ok = f.peerLimiters[p]
	if ok {
		return limiter
	}

	limiter = rate.NewLimiter(f.perPeerRate, f.perPeerBurst)
	f.peerLimiters[p] = limiter
	WantBlockPeerLimiters.Set(float64(len(f.peerLimiters)))
	return limiter
}

func (f *WantBlockFilter) AddGatewayPeer(p peer.ID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gatewayPeers[p] = struct{}{}
	WantBlockGatewayPeers.Set(float64(len(f.gatewayPeers)))
}

func (f *WantBlockFilter) RemoveGatewayPeer(p peer.ID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.gatewayPeers, p)
	WantBlockGatewayPeers.Set(float64(len(f.gatewayPeers)))
}

func (f *WantBlockFilter) RemovePeerLimiter(p peer.ID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.peerLimiters, p)
	WantBlockPeerLimiters.Set(float64(len(f.peerLimiters)))
}

// PeerBlockRequestFilter returns a function compatible with
// bitswap.WithPeerBlockRequestFilter.
func (f *WantBlockFilter) PeerBlockRequestFilter() func(peer.ID, cid.Cid) bool {
	return f.Allow
}
