package ipfs

import (
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// WantBlockFilter implements bitswap.PeerBlockRequestFilter with gateway
// peer whitelisting and per-peer rate limiting. Gateway peers (identified
// by peer ID extracted from multiaddrs) and the local node's own peer ID
// get unlimited access. All other peers are rate-limited to prevent
// want-block overload from aggressive DHT peers.
type WantBlockFilter struct {
	mu           sync.RWMutex
	host         host.Host
	selfPeer     peer.ID
	log          *zap.Logger
	gatewayPeers map[peer.ID]struct{}
	limiter      *rate.Limiter
	peerLimiters map[peer.ID]*rate.Limiter
	perPeerRate  rate.Limit
	perPeerBurst int
	deniedPeers  *topNDeniedPeersCollector
}

// WantBlockFilterConfig holds the configuration for creating a WantBlockFilter.
type WantBlockFilterConfig struct {
	SelfPeer             peer.ID
	Logger               *zap.Logger
	GatewayPeers         []peer.ID
	GlobalRate           rate.Limit
	GlobalBurst          int
	PerPeerRate          rate.Limit
	PerPeerBurst         int
	DeniedPeersCollector *topNDeniedPeersCollector
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

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	deniedPeers := cfg.DeniedPeersCollector
	if deniedPeers == nil {
		deniedPeers = newTopNDeniedPeersCollector(10)
	}

	f := &WantBlockFilter{
		host:         h,
		selfPeer:     cfg.SelfPeer,
		log:          logger.Named("wantblock_filter"),
		gatewayPeers: gatewayPeers,
		limiter:      rate.NewLimiter(globalRate, globalBurst),
		peerLimiters: make(map[peer.ID]*rate.Limiter),
		perPeerRate:  perPeerRate,
		perPeerBurst: perPeerBurst,
		deniedPeers:  deniedPeers,
	}
	WantBlockGatewayPeers.Set(float64(len(gatewayPeers)))
	WantBlockPeerLimiters.Set(0)
	return f
}

// Allow is called by bitswap for each wantlist entry from a peer.
// It returns true if the request should be fulfilled.
// Gateway peers and the local node always pass. Other peers are subject
// to global and per-peer rate limits.
func (f *WantBlockFilter) Allow(p peer.ID, c cid.Cid) bool {
	// Self peer — always allow without question.
	if p == f.selfPeer {
		WantBlockRequestsTotal.WithLabelValues(LabelWantAllowedSelf).Inc()
		return true
	}

	f.mu.RLock()
	_, isGateway := f.gatewayPeers[p]
	f.mu.RUnlock()

	if isGateway {
		WantBlockRequestsTotal.WithLabelValues(LabelWantAllowedGateway).Inc()
		return true
	}

	if !f.limiter.Allow() {
		WantBlockRequestsTotal.WithLabelValues(LabelWantDeniedGlobalRate).Inc()
		f.deniedPeers.increment(p.String())
		f.log.Debug("want-block denied by global rate limit",
			zap.Stringer("peer", p),
			zap.Stringer("cid", c),
		)
		return false
	}

	peerLimiter := f.getPeerLimiter(p)
	if !peerLimiter.Allow() {
		WantBlockRequestsTotal.WithLabelValues(LabelWantDeniedPerPeerRate).Inc()
		f.deniedPeers.increment(p.String())
		f.log.Debug("want-block denied by per-peer rate limit",
			zap.Stringer("peer", p),
			zap.Stringer("cid", c),
		)
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
	f.deniedPeers.RemovePeer(p.String())
}

// PeerBlockRequestFilter returns a function compatible with
// bitswap.WithPeerBlockRequestFilter.
func (f *WantBlockFilter) PeerBlockRequestFilter() func(peer.ID, cid.Cid) bool {
	return f.Allow
}
