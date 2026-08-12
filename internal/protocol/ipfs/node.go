package ipfs

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	pluginBuild "go.lumeweb.com/portal-plugin-ipfs/build"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/dag"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/time/rate"

	"github.com/avast/retry-go/v5"
	"github.com/ipfs/boxo/bitswap"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	tracerpkg "github.com/ipfs/boxo/bitswap/tracer"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/keystore"
	"github.com/ipfs/boxo/namesys"
	blocks "github.com/ipfs/go-block-format"
	format "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/crawler"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubrouter "github.com/libp2p/go-libp2p-pubsub-router"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	tcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
	webrtc "github.com/libp2p/go-libp2p/p2p/transport/webrtc"
	ws "github.com/libp2p/go-libp2p/p2p/transport/websocket"
	webtransport "github.com/libp2p/go-libp2p/p2p/transport/webtransport"
	libp2pRate "github.com/libp2p/go-libp2p/x/rate"
)

const (
	libp2pProtocolPrefix = "/p2p/"
	webSubdomainPrefix   = "web."
)

// DHTRouting combines the interfaces needed from a DHT routing implementation
type DHTRouting interface {
	Close() error
	Host() host.Host
	routing.ValueStore
	routing.ContentRouting
	routing.PeerRouting
}

// IPFSNode defines the interface for IPFS node operations
type IPFSNode interface {
	Close() error
	GetBlock(ctx context.Context, c cid.Cid) (format.Node, error)
	HasBlock(ctx context.Context, c cid.Cid) (bool, error)
	AddBlock(ctx context.Context, block blocks.Block) error
	DagService() format.DAGService
	GetBlockstore() blockstore.Blockstore
	PeerID() peer.ID
	ConnectionAddresses() ([]multiaddr.Multiaddr, error)
	DelegateAddresses() ([]multiaddr.Multiaddr, error)
	Peers() []peer.ID
	AddPeer(addr peer.AddrInfo)
	Pin(ctx context.Context, root cid.Cid, recursive bool) error
	TriggerReprovider()
	ProvideCID(ctx context.Context, c cid.Cid) error
	GetPublisher() pluginCore.IPNSPublisher
	GetKeystore() keystore.Keystore
	GetDatastore() datastore.Datastore
	GetPrivateKey() crypto.PrivKey
	AnnounceWeb() bool
	AnnounceDomain() string
	HostAddrs() []multiaddr.Multiaddr
	Port() int
}

// BlockReadyChecker checks if a block is ready to be announced to the network.
// This gates bitswap NotifyNewBlocks on the Ready column in the metadata store.
type BlockReadyChecker interface {
	BlockExists(ctx context.Context, c cid.Cid) error
}

// ReadyAwareExchange wraps an exchange.Interface and gates NotifyNewBlocks
// on the block's Ready status. Only blocks that have been confirmed as ready
// (via the metadata store's Ready column) are forwarded to the underlying
// exchange. This prevents announcing blocks that are still being uploaded
// while allowing immediate bitswap announcements for confirmed blocks.
type ReadyAwareExchange struct {
	exchange.Interface
	readyChecker BlockReadyChecker
}

func (r *ReadyAwareExchange) NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error {
	var readyBlocks []blocks.Block
	for _, b := range blks {
		if r.readyChecker.BlockExists(ctx, b.Cid()) == nil {
			readyBlocks = append(readyBlocks, b)
		}
	}
	if len(readyBlocks) > 0 {
		return r.Interface.NotifyNewBlocks(ctx, readyBlocks...)
	}
	return nil
}

// NodeFactory manages the creation and recreation of IPFS nodes
// It stores shared components that persist across node restarts
type NodeFactory struct {
	ctx            core.Context
	cfg            *config.ProtocolConfig
	reprovideStore pluginCore.ReprovideStore
	datastore      datastore.Batching
	blockstore     blockstore.Blockstore
	peerTracker    *BlockRequestTracker
	readyChecker   BlockReadyChecker
	bootstrapPeers []peer.AddrInfo
	bootstrapMutex sync.RWMutex

	deniedPeersCollector *topNDeniedPeersCollector
}

// NewNodeFactory creates a new node factory with the given shared components
func NewNodeFactory(ctx core.Context, cfg *config.ProtocolConfig, rs pluginCore.ReprovideStore, ds datastore.Batching, bs blockstore.Blockstore, peerTracker *BlockRequestTracker, readyChecker BlockReadyChecker) *NodeFactory {
	factory := &NodeFactory{
		ctx:                  ctx,
		cfg:                  cfg,
		reprovideStore:       rs,
		datastore:            ds,
		blockstore:           bs,
		peerTracker:          peerTracker,
		readyChecker:         readyChecker,
		bootstrapPeers:       make([]peer.AddrInfo, 0),
		bootstrapMutex:       sync.RWMutex{},
		deniedPeersCollector: newTopNDeniedPeersCollector(10),
	}

	core.PluginMetricsRegistry(internal.ProtocolName).MustRegister(factory.deniedPeersCollector)

	// Add initial bootstrap peers from config
	for _, bp := range cfg.BootstrapPeers {
		factory.AddBootstrapPeer(lo.FromPtr(bp.ToAddrInfo()))
	}

	return factory
}

// AddBootstrapPeer adds a bootstrap peer to the factory
func (f *NodeFactory) AddBootstrapPeer(addr peer.AddrInfo) {
	f.bootstrapMutex.Lock()
	defer f.bootstrapMutex.Unlock()
	f.bootstrapPeers = append(f.bootstrapPeers, addr)
}

// ClearBootstrapPeers removes all bootstrap peers from the factory
func (f *NodeFactory) ClearBootstrapPeers() {
	f.bootstrapMutex.Lock()
	defer f.bootstrapMutex.Unlock()
	f.bootstrapPeers = make([]peer.AddrInfo, 0)
}

// GetBootstrapPeers returns a copy of the bootstrap peers
func (f *NodeFactory) GetBootstrapPeers() []peer.AddrInfo {
	f.bootstrapMutex.RLock()
	defer f.bootstrapMutex.RUnlock()

	peers := make([]peer.AddrInfo, len(f.bootstrapPeers))
	copy(peers, f.bootstrapPeers)
	return peers
}

// CreateNode creates a new IPFS node instance using the factory's configuration
func (f *NodeFactory) CreateNode() (*Node, error) {
	return NewNode(f.ctx, f.cfg, f.reprovideStore, f.datastore, f.blockstore, f.peerTracker, f.readyChecker, f)
}

// A Node is a minimal IPFS node
type Node struct {
	log                 *core.Logger
	ctx                 core.Context
	host                host.Host
	routing             DHTRouting
	companionDHT        *dht.IpfsDHT
	companionDHTHealthy atomic.Bool
	fullRT              *fullrt.FullRT
	reprovider          *Reprovider
	blockService        blockservice.BlockService
	dagService          format.DAGService
	bitswap             *bitswap.Bitswap
	reproviderCancel    context.CancelFunc
	datastore           datastore.Datastore
	keystore            keystore.Keystore
	publisher           *namesys.IPNSPublisher
	pubsub              *pubsub.PubSub
	pubsubValueStore    *pubsubrouter.PubsubValueStore
	announceWeb         bool
	port                int
}

// Close closes the node
func (n *Node) Close() error {
	if n == nil {
		return nil
	}
	if n.reproviderCancel != nil {
		n.reproviderCancel()
	}
	var errs []error
	if n.companionDHT != nil {
		if err := n.companionDHT.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close companion DHT: %w", err))
		}
	}
	if err := n.routing.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close routing: %w", err))
	}
	if err := n.bitswap.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close bitswap: %w", err))
	}
	if err := n.host.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close host: %w", err))
	}
	if err := n.blockService.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close block service: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors during node close: %v", errs)
	}
	return nil
}

// GetBlock fetches a block from the IPFS network
func (n *Node) GetBlock(ctx context.Context, c cid.Cid) (format.Node, error) {
	ctx, span := core.TraceMethod(ctx, "Node.GetBlock")
	defer span.End()

	return n.dagService.Get(ctx, c)
}

// HasBlock checks if a block is locally pinned
func (n *Node) HasBlock(ctx context.Context, c cid.Cid) (bool, error) {
	ctx, span := core.TraceMethod(ctx, "Node.HasBlock")
	defer span.End()

	return n.blockService.Blockstore().Has(ctx, c)
}

// AddBlock adds a generic block to the IPFS node
func (n *Node) AddBlock(ctx context.Context, block blocks.Block) error {
	ctx, span := core.TraceMethod(ctx, "Node.AddBlock")
	defer span.End()

	if err := n.blockService.AddBlock(ctx, block); err != nil {
		return fmt.Errorf("failed to add block: %w", err)
	}
	return nil
}

func (n *Node) DagService() format.DAGService {
	return n.dagService
}

func (n *Node) GetBlockstore() blockstore.Blockstore {
	return n.blockService.Blockstore()
}

// PeerID returns the peer ID of the node
func (n *Node) PeerID() peer.ID {
	return n.routing.Host().ID()
}

func (n *Node) ConnectionAddresses() ([]multiaddr.Multiaddr, error) {
	annAddrs, err := AnnouncementAddresses(n.announceWeb, n.announceDomain(), n.host.Addrs(), n.port)
	if err != nil {
		return nil, err
	}

	connAddrs := lo.Map(annAddrs, func(addr multiaddr.Multiaddr, _ int) multiaddr.Multiaddr {
		return addr.Encapsulate(multiaddr.StringCast(libp2pProtocolPrefix + n.PeerID().String()))
	})

	return connAddrs, nil
}

func (n *Node) AnnounceWeb() bool {
	return n.announceWeb
}

func (n *Node) AnnounceDomain() string {
	return n.announceDomain()
}

func (n *Node) Port() int {
	return n.port
}

func (n *Node) announceDomain() string {
	if n.ctx == nil {
		return ""
	}
	httpSvc := core.GetService[core.HTTPService](n.ctx, core.HTTP_SERVICE)
	if httpSvc == nil {
		return ""
	}
	return httpSvc.APISubdomain(internal.ProtocolName, false)
}

func (n *Node) HostAddrs() []multiaddr.Multiaddr {
	if n.host == nil {
		return nil
	}
	return n.host.Addrs()
}

// DelegateAddresses returns the multiaddr addresses that can be used as delegates
func (n *Node) DelegateAddresses() ([]multiaddr.Multiaddr, error) {
	return n.ConnectionAddresses()
}

// Peers returns the list of peers in the routing table
func (n *Node) Peers() []peer.ID {
	return n.host.Peerstore().Peers()
}

// AddPeer adds a peer to the peerstore
func (n *Node) AddPeer(addr peer.AddrInfo) {
	n.host.Peerstore().AddAddrs(addr.ID, addr.Addrs, peerstore.AddressTTL)
}

// Pin pins a CID
func (n *Node) Pin(ctx context.Context, root cid.Cid, recursive bool) error {
	ctx, span := core.TraceMethod(ctx, "Node.Pin")
	defer span.End()

	log := n.log.Named("Pin").With(zap.Stringer("rootCID", root), zap.Bool("recursive", recursive))
	if !recursive {
		block, err := n.dagService.Get(ctx, root)
		if err != nil {
			return fmt.Errorf("failed to get block: %w", err)
		} else if err := n.blockService.AddBlock(ctx, block); err != nil {
			return fmt.Errorf("failed to add block: %w", err)
		}
		return nil
	}

	opts := &dag.WalkDAGOptions{
		NormalizeCID: true,
		Concurrent:   true,
		IgnoreErrors: true,
		Logger:       n.log,
	}

	err := dag.WalkDAG(ctx, n.dagService, root, func(nodeCtx context.Context, c cid.Cid, node *merkledag.ProtoNode) error {
		log := log.With(zap.Stringer("childCID", c))
		log.Debug("pinning child")
		// TODO: queue and handle these correctly
		nodeCtx, cancel := context.WithTimeout(nodeCtx, time.Minute)
		defer cancel()

		if err := n.blockService.AddBlock(nodeCtx, node); err != nil {
			log.Error("failed to add block", zap.Error(err))
			return err
		}
		log.Debug("pinned block")
		return nil
	}, opts)
	if err != nil {
		return fmt.Errorf("failed to walk DAG: %w", err)
	}
	n.reprovider.Trigger()
	return nil
}

// NewNode creates a new IPFS node
func NewNode(ctx core.Context, cfg *config.ProtocolConfig, rs pluginCore.ReprovideStore, ds datastore.Batching, bs blockstore.Blockstore, peerTracker *BlockRequestTracker, readyChecker BlockReadyChecker, factory *NodeFactory) (*Node, error) {
	hasher := hkdf.New(sha256.New, ctx.Config().Config().Core.Identity.PrivateKey(), ctx.Config().Config().Core.NodeID.Bytes(), []byte(internal.ProtocolName))
	derivedSeed := make([]byte, 32)

	if _, err := io.ReadFull(hasher, derivedSeed); err != nil {
		return nil, fmt.Errorf("failed to generate child key seed: %w", err)
	}

	edkey := ed25519.NewKeyFromSeed(derivedSeed)

	privateKey, err := crypto.UnmarshalEd25519PrivateKey(edkey)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	scalingLimits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&scalingLimits)

	scaled := scalingLimits.AutoScale()
	limits := scaled

	if cfg.DisableResourceLimits {
		limits = rcmgr.InfiniteLimits
	} else {
		// This node runs as a public server (ForceReachabilityPublic +
		// companion ModeServer), so inbound is a fundamental traffic class,
		// not a secondary direction. libp2p's default autoscale policy gives
		// the system inbound cap roughly HALF the total connection ceiling
		// (64 inbound vs 128 total), which would make inbound the de-facto
		// global cap and let the soft connmgr never prune before the hard
		// fence rejects new public peers. Raise the system inbound ceiling to
		// the total connection limit while keeping the autoscaled memory, FD,
		// stream, peer, and protocol limits intact.
		scaledLimiter := rcmgr.NewFixedLimiter(scaled)
		baseTotal := scaledLimiter.GetSystemLimits().GetConnTotalLimit()

		limits = rcmgr.PartialLimitConfig{
			System: rcmgr.ResourceLimits{
				ConnsInbound: rcmgr.LimitVal(baseTotal),
			},
		}.Build(scaled)
	}
	limiter := rcmgr.NewFixedLimiter(limits)

	// Build the sets of IP ranges that bypass rate limiting and the per-source
	// connection limiter.
	//
	// rateBypassNets (private/loopback/Docker) are always exempt so nginx and
	// internal services can connect regardless of public traffic shaping.
	// trustedGatewayNets come from explicitly configured gateways
	// (cfg.Gateways) and additionally receive reserved rcmgr admission and
	// connmgr peer protection below. Private-range peers are NOT automatically
	// granted gateway reserved capacity — that is an explicit, configured
	// decision.
	rateBypassNets := privateAndLoopbackNets()
	gatewayIPs, gatewayPeerIDs, gatewayAllowlisted, err := parseGatewayMultiaddrs(cfg.Gateways)
	if err != nil {
		return nil, err
	}
	trustedGatewayNets := gatewayIPs

	prefixLimits := make([]libp2pRate.PrefixLimit, 0, len(rateBypassNets)+len(trustedGatewayNets))
	connLimits4 := make([]rcmgr.NetworkPrefixLimit, 0)
	connLimits6 := make([]rcmgr.NetworkPrefixLimit, 0)

	for _, cidr := range append(rateBypassNets, trustedGatewayNets...) {
		prefix := netIPNetToPrefix(cidr)
		prefixLimits = append(prefixLimits, libp2pRate.PrefixLimit{Prefix: prefix, Limit: libp2pRate.Limit{}})
		if cidr.IP.To4() != nil {
			connLimits4 = append(connLimits4, rcmgr.NetworkPrefixLimit{Network: prefix, ConnCount: 1024})
		} else {
			connLimits6 = append(connLimits6, rcmgr.NetworkPrefixLimit{Network: prefix, ConnCount: 1024})
		}
	}

	// Trusted gateways are also allowlisted in the resource manager. libp2p
	// draws allowlisted connections against a separate reserved system pool
	// that activates once the normal system pool is full, so a trusted gateway
	// can still be admitted when public capacity is exhausted. The peer-
	// constrained form (/ip4/x/p2p/...) is preferred where a peer ID is known:
	// libp2p first admits by IP, then re-checks the authenticated peer ID after
	// the handshake and moves non-matching peers back to the normal pool.
	//
	// NOTE: the WithNetworkPrefixLimit 1024 above only relaxes the per-source/IP
	// limiter — it does NOT exempt these ranges from the global system ceiling.
	// The allowlist is what provides the reserved admission path.
	rm, err := rcmgr.NewResourceManager(limiter,
		rcmgr.WithConnRateLimiters(&libp2pRate.Limiter{
			NetworkPrefixLimits: prefixLimits,
			GlobalLimit:         libp2pRate.Limit{},
		}),
		rcmgr.WithNetworkPrefixLimit(connLimits4, connLimits6),
		rcmgr.WithAllowlistedMultiaddrs(gatewayAllowlisted),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource manager: %w", err)
	}

	lowWater, highWater := deriveConnLimits(limiter)
	cmgr, err := connmgr.NewConnManager(
		lowWater,
		highWater,
		connmgr.WithGracePeriod(30*time.Second),
		connmgr.WithSilencePeriod(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	// Keep configured gateway peers out of the soft-pruning set, matching the
	// whitelisted treatment they already get at the hard rcmgr layer
	// (NetworkPrefixLimit / rate-limiter bypass). With finite watermarks the
	// connection manager actually prunes now, so without this a gateway peer
	// could be disconnected once the pool crosses high-water even though the
	// hard layer explicitly allows those whitelisted endpoints.
	const gatewayProtectionTag = "portal-gateway"
	for _, gatewayID := range gatewayPeerIDs {
		cmgr.Protect(gatewayID, gatewayProtectionTag)
	}

	trustedProxies, err := parseTrustedProxies(cfg.TrustedProxies)
	if err != nil {
		return nil, fmt.Errorf("failed to parse trusted proxies: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.ListenAddrs()...),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(privateKey),
		libp2p.EnableRelay(),
		libp2p.ForceReachabilityPublic(),
		libp2p.ResourceManager(rm),
		libp2p.DefaultPeerstore,
		libp2p.UserAgent("lumeweb-ipfs/" + pluginBuild.GetInfo().Short()),
	}

	// The TCP transport is wired one of two ways. When the node sits behind a
	// proxy that appends PROXY protocol headers (e.g. nginx stream or HAProxy
	// with send-proxy), it must accept and parse those headers; otherwise use a
	// plain TCP transport so direct libp2p peers can connect without sending a
	// PROXY header first.
	if cfg.ProxyProtocol {
		opts = append(opts, libp2p.Transport(newProxyTCPTransport(trustedProxies)))
	} else {
		opts = append(opts, libp2p.Transport(tcp.NewTCPTransport))
	}

	opts = append(opts,
		libp2p.Transport(quic.NewTransport),
		libp2p.Transport(webtransport.New),
		libp2p.Transport(webrtc.New),
		libp2p.Transport(ws.New),
		libp2p.PrometheusRegisterer(prometheus.WrapRegistererWithPrefix("libp2p_", core.PluginMetricsRegistry(internal.ProtocolName))),
	)

	opts = append(opts, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
		var domain string
		if cfg.AnnounceWeb {
			httpSvc := core.GetService[core.HTTPService](ctx, core.HTTP_SERVICE)
			if httpSvc != nil {
				domain = httpSvc.APISubdomain(internal.ProtocolName, false)
			}
		}
		announceAddresses, err := AnnouncementAddresses(cfg.AnnounceWeb, domain, addrs, cfg.Port)
		if err != nil {
			ctx.Logger().Error("failed to get announcement addresses", zap.Error(err))
			return lo.Filter(addrs, func(addr multiaddr.Multiaddr, _ int) bool {
				return addr != nil
			})
		}

		if len(announceAddresses) == 0 {
			return lo.Filter(addrs, func(addr multiaddr.Multiaddr, _ int) bool {
				return addr != nil
			})
		}

		return lo.Filter(announceAddresses, func(addr multiaddr.Multiaddr, _ int) bool {
			return addr != nil
		})
	}))

	node, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	var routingImpl DHTRouting
	var hasProvider bool

	// Detect if we should use LAN DHT mode
	// Use LAN DHT when all bootstrap peers are local (for e2e testing without public swarm)
	useLANMode := allPeersLocal(factory.GetBootstrapPeers())

	// When cfg.Port is 0 (OS-assigned), extract the real port from the host's listen addresses.
	port := cfg.Port
	if port == 0 {
		port = resolvePortFromAddrs(node.Addrs())
	}

	ipfsNode := &Node{
		host:        node,
		log:         ctx.Logger(),
		ctx:         ctx,
		announceWeb: cfg.AnnounceWeb,
		port:        port,
	}

	// Build common DHT options using factory's GetBootstrapPeers()
	dhtOpts := []dht.Option{
		dht.Mode(dht.ModeServer),
		dht.BootstrapPeersFunc(func() []peer.AddrInfo {
			return factory.GetBootstrapPeers()
		}),
		dht.Datastore(ds),
	}

	// Add LAN protocol extension for local-only deployments if needed
	if useLANMode {
		dhtOpts = append(dhtOpts, dht.ProtocolExtension("/lan"))
	}

	switch cfg.DHTMode {
	case config.DHTModeBasic:
		// Use basic DHT
		basicDHT, dhtErr := dht.New(node, dhtOpts...)
		if dhtErr != nil {
			return nil, fmt.Errorf("failed to create basic dht: %w", dhtErr)
		}
		routingImpl = basicDHT
		hasProvider = true
	case config.DHTModeFullRT, "":
		// FullRT is an accelerated DHT client that crawls the full network
		// periodically and caches all DHT peers locally. GetClosestPeers is a
		// local trie lookup (microseconds) instead of an iterative network
		// query (~10s). ProvideMany groups keys by keyspace region and sends
		// bulk ADD_PROVIDER messages over shared connections.
		//
		// FullRT does not register protocol handlers — it cannot respond to
		// inbound DHT queries (FIND_NODE, GET_PROVIDER, PUT_VALUE). A
		// companion IpfsDHT in server mode runs alongside it to handle
		// inbound queries and keep this node's multiaddrs discoverable.
		companion, dhtErr := dht.New(node, dhtOpts...)
		if dhtErr != nil {
			return nil, fmt.Errorf("failed to create companion DHT: %w", dhtErr)
		}
		err := retry.New(
			retry.Attempts(5),
			retry.Delay(5*time.Second),
			retry.DelayType(retry.BackOffDelay),
			retry.OnRetry(func(n uint, err error) {
				CompanionDHTBootstrapAttemptsTotal.Inc()
				ctx.Logger().Warn("failed to bootstrap companion DHT, retrying",
					zap.Error(err),
					zap.Uint("attempt", n+1))
			}),
		).Do(func() error {
			CompanionDHTBootstrapAttemptsTotal.Inc()
			return companion.Bootstrap(ctx)
		})
		if err != nil {
			CompanionDHTBootstrapFailuresTotal.Inc()
			ctx.Logger().Error("failed to bootstrap companion DHT after retries, marking unhealthy",
				zap.Error(err))
			// Do NOT close the companion -- it may recover if bootstrap peers come back.
			// The reprovider's Ready() check will gate on companionDHTHealthy.
			// Recovery goroutine is started after fullrt.NewFullRT succeeds to avoid
			// use-after-close if NewFullRT fails and calls companion.Close().
		} else {
			ipfsNode.companionDHTHealthy.Store(true)
			CompanionDHTHealthy.Set(1)
		}
		ipfsNode.companionDHT = companion

		// FullRT's default crawler runs 200 parallel workers, which is the
		// largest connection-burst generator in the process and can exceed the
		// connmgr high-water during a crawl. Make its parallelism configurable
		// (default 96) so operators can bound the burst headroom against the
		// rcmgr connection ceiling.
		frtCrawler, crawlerErr := crawler.NewDefaultCrawler(
			node,
			crawler.WithParallelism(cfg.FullRTCrawlerParallelism),
		)
		if crawlerErr != nil {
			companion.Close()
			return nil, fmt.Errorf("failed to create fullrt crawler: %w", crawlerErr)
		}

		fullRTOpts := []fullrt.Option{
			fullrt.WithCrawler(frtCrawler),
			fullrt.DHTOption(
				append(dhtOpts,
					dht.BucketSize(20), // this cannot be changed
					dht.Concurrency(30),
				)...,
			),
		}

		frt, dhtErr := fullrt.NewFullRT(node, dht.DefaultPrefix, fullRTOpts...)
		if dhtErr != nil {
			companion.Close()
			return nil, fmt.Errorf("failed to create fullrt: %w", dhtErr)
		}
		routingImpl = frt
		ipfsNode.fullRT = frt
		hasProvider = true

		// Start recovery goroutine now that fullrt.NewFullRT succeeded.
		// The companion DHT is alive for the lifetime of the node.
		if !ipfsNode.companionDHTHealthy.Load() {
			go func() {
				recoverTicker := time.NewTicker(2 * time.Minute)
				defer recoverTicker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-recoverTicker.C:
						CompanionDHTBootstrapAttemptsTotal.Inc()
						if err := companion.Bootstrap(ctx); err == nil {
							ipfsNode.companionDHTHealthy.Store(true)
							CompanionDHTHealthy.Set(1)
							ctx.Logger().Info("companion DHT recovered via background bootstrap")
							return
						} else {
							CompanionDHTBootstrapFailuresTotal.Inc()
						}
					}
				}
			}()
		}
	}

	// Log configured bootstrap servers for debugging
	bootstrapPeers := factory.GetBootstrapPeers()
	bootstrapPeerStrings := lo.Map(bootstrapPeers, func(p peer.AddrInfo, _ int) string {
		return p.String()
	})
	ctx.Logger().Debug("IPFS node configured with bootstrap servers",
		zap.Strings("bootstrap_peers", bootstrapPeerStrings),
		zap.Int("count", len(bootstrapPeerStrings)),
		zap.String("dht_mode", cfg.DHTMode))

	bitswapOpts := []bitswap.Option{
		bitswap.EngineBlockstoreWorkerCount(cfg.BlockStore.MaxConcurrentRequests),
		bitswap.TaskWorkerCount(cfg.BlockStore.MaxConcurrentRequests),
		bitswap.MaxOutstandingBytesPerPeer(1 << 20),
	}

	if cfg.Bitswap.MaxQueuedWantlistEntriesPerPeer > 0 {
		bitswapOpts = append(bitswapOpts, bitswap.MaxQueuedWantlistEntriesPerPeer(cfg.Bitswap.MaxQueuedWantlistEntriesPerPeer))
	}

	bs = &blockstore.ValidatingBlockstore{Blockstore: bs}

	bitswapTracer := NewBitswapTracer(peerTracker, node)
	bitswapOpts = append(bitswapOpts, bitswap.WithTracer(tracerpkg.Tracer(bitswapTracer)))

	bitswapTracer.SetupDisconnectListeners()

	if cfg.Bitswap.GlobalWantRateLimit > 0 || len(gatewayPeerIDs) > 0 {
		wantFilter := NewWantBlockFilter(node, WantBlockFilterConfig{
			SelfPeer:             node.ID(),
			Logger:               ctx.Logger().Named("wantblock_filter"),
			GatewayPeers:         gatewayPeerIDs,
			GlobalRate:           rate.Limit(cfg.Bitswap.GlobalWantRateLimit),
			GlobalBurst:          cfg.Bitswap.GlobalWantBurst,
			PerPeerRate:          rate.Limit(cfg.Bitswap.PerPeerWantRateLimit),
			PerPeerBurst:         cfg.Bitswap.PerPeerWantBurst,
			DeniedPeersCollector: factory.deniedPeersCollector,
		})
		bitswapOpts = append(bitswapOpts, bitswap.WithPeerBlockRequestFilter(wantFilter.PeerBlockRequestFilter()))

		node.Network().Notify(&network.NotifyBundle{
			DisconnectedF: func(_ network.Network, conn network.Conn) {
				wantFilter.RemovePeerLimiter(conn.RemotePeer())
			},
		})
	}

	bitswapNet := bsnet.NewFromIpfsHost(node)
	_bitswap := bitswap.New(ctx, bitswapNet, routingImpl, bs, bitswapOpts...)

	readyExchange := &ReadyAwareExchange{Interface: _bitswap, readyChecker: readyChecker}

	blockServ := blockservice.New(bs, readyExchange)
	dagService := merkledag.NewDAGService(blockServ)

	for _, p := range cfg.Peers {
		addrs, err := peer.AddrInfoToP2pAddrs(p.ToAddrInfo())
		if err != nil {
			return nil, err
		}

		node.Peerstore().AddAddrs(p.ToAddrInfo().ID, addrs, peerstore.PermanentAddrTTL)
	}

	var rp *Reprovider
	var reproviderCancel context.CancelFunc
	if hasProvider {
		// NewDHTProvider abstracts the fullrt vs basic DHT choice:
		// - FullRT mode: delegates to FullRT.ProvideMany (keyspace-region batching)
		// - Basic mode: iterates with per-CID timeout over the primary DHT
		// - FullRT mode without fullRT ready: falls back to companion DHT
		// The ready function gates on FullRT readiness or companion health.
		var dhtForProvide routing.ContentRouting
		var readyFn func() bool

		if ipfsNode.fullRT != nil {
			readyFn = func() bool { return ipfsNode.fullRT.Ready() }
		} else if ipfsNode.companionDHT != nil {
			dhtForProvide = ipfsNode.companionDHT
			readyFn = func() bool {
				if !ipfsNode.companionDHTHealthy.Load() {
					return false
				}
				return ipfsNode.companionDHT.RoutingTable().Size() > 0
			}
		} else {
			// Basic mode: use the primary DHT directly, no health gating
			dhtForProvide = routingImpl
			readyFn = nil
		}

		reproviderProvider := NewDHTProvider(ipfsNode.fullRT, dhtForProvide, readyFn, cfg.Provider)
		rp = NewReprovider(reproviderProvider, rs, ctx.Logger().Named("reprovider"), cfg.Provider)
		reproviderCtx, cancel := context.WithCancel(ctx)
		reproviderCancel = cancel
		go rp.Run(reproviderCtx)

		// Periodic DHT metrics goroutine -- updates gauges every 30 seconds
		// to surface DHT health degradation even when the reprovider is idle.
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if ipfsNode.companionDHT != nil {
						CompanionDHTRoutingTableSize.Set(float64(ipfsNode.companionDHT.RoutingTable().Size()))

						peerCount := 0
						for range ipfsNode.host.Network().Conns() {
							peerCount++
						}
						CompanionDHTConnectedPeers.Set(float64(peerCount))

						if ipfsNode.companionDHTHealthy.Load() {
							CompanionDHTHealthy.Set(1)
						} else {
							CompanionDHTHealthy.Set(0)
						}
					}
					if ipfsNode.fullRT != nil {
						if ipfsNode.fullRT.Ready() {
							FullRTReady.Set(1)
						} else {
							FullRTReady.Set(0)
						}
						FullRTRoutingTableSize.Set(float64(len(ipfsNode.fullRT.Stat())))
					}
				}
			}
		}()
	}

	// Create boxo keystore for IPNS key management
	boxoKeystore := keystore.NewMemKeystore()
	// Wrap with safe keystore to prevent nil keys from being stored
	safeKeystore := NewSafeKeystore(boxoKeystore, ctx.Logger())

	// Initialize GossipSub for IPNS-over-PubSub routing
	gossipsub, err := pubsub.NewGossipSub(ctx, node,
		pubsub.WithMessageSigning(true),
		pubsub.WithStrictSignatureVerification(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gossipsub: %w", err)
	}

	// Create IPNS PubSub value store for near-instant propagation to subscribers
	pubsubValueStore, err := pubsubrouter.NewPubsubValueStore(ctx, node, gossipsub, ipns.Validator{KeyBook: node.Peerstore()},
		pubsubrouter.WithRebroadcastInterval(cfg.IPNS.PubSubRebroadcastInterval),
		pubsubrouter.WithRebroadcastInitialDelay(cfg.IPNS.PubSubRebroadcastInitialDelay),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub value store: %w", err)
	}
	ipfsNode.pubsub = gossipsub
	ipfsNode.pubsubValueStore = pubsubValueStore

	// Create boxo IPNS publisher with composite routing (DHT + PubSub)
	// When a companion (server-mode) DHT exists, writes go there since FullRT
	// is client-only and does not reliably put records into the DHT network.
	var compositeRouting *compositeValueStore
	if ipfsNode.companionDHT != nil {
		compositeRouting = newCompositeValueStoreWithCompanion(routingImpl, ipfsNode.companionDHT, pubsubValueStore, ctx.Logger())
	} else {
		compositeRouting = newCompositeValueStore(routingImpl, pubsubValueStore, ctx.Logger())
	}
	boxoPublisher := namesys.NewIPNSPublisher(compositeRouting, ds)

	// Update the IPFS node with remaining fields
	ipfsNode.routing = routingImpl
	ipfsNode.bitswap = _bitswap
	ipfsNode.blockService = blockServ
	ipfsNode.dagService = dagService
	ipfsNode.reprovider = rp
	ipfsNode.reproviderCancel = reproviderCancel
	ipfsNode.datastore = ds
	ipfsNode.keystore = safeKeystore
	ipfsNode.publisher = boxoPublisher

	return ipfsNode, nil
}
func (n *Node) TriggerReprovider() {
	n.reprovider.Trigger()
}

func (n *Node) ProvideCID(ctx context.Context, c cid.Cid) error {
	if n.fullRT != nil {
		return n.fullRT.Provide(ctx, c, true)
	}
	if n.companionDHT != nil {
		return n.companionDHT.Provide(ctx, c, true)
	}
	return n.routing.Provide(ctx, c, true)
}

func AnnouncementAddresses(announceWeb bool, domain string, hostAddrs []multiaddr.Multiaddr, configPort int) ([]multiaddr.Multiaddr, error) {
	if announceWeb && domain != "" {
		return announceFromDomainAndHostAddrs(domain, hostAddrs, configPort)
	}

	return filterPublicAddrs(hostAddrs), nil
}

func announceFromDomainAndHostAddrs(domain string, hostAddrs []multiaddr.Multiaddr, configPort int) ([]multiaddr.Multiaddr, error) {
	configPortStr := strconv.Itoa(configPort)
	var tcpAddrs []multiaddr.Multiaddr
	var wssAddrs []multiaddr.Multiaddr
	var udpAddrs []multiaddr.Multiaddr
	seenTCP := make(map[string]bool)
	seenWSS := make(map[string]bool)
	seenUDP := make(map[string]bool)

	for _, addr := range hostAddrs {
		var hasWS bool
		var hasQUIC bool
		var hasTCP bool
		var port string
		var udpProtos []string
		var certhashes []string
		multiaddr.ForEach(addr, func(c multiaddr.Component) bool {
			switch c.Protocol().Code {
			case multiaddr.P_TCP:
				port = c.Value()
				hasTCP = true
			case multiaddr.P_WS, multiaddr.P_WSS:
				hasWS = true
			case multiaddr.P_UDP:
				port = c.Value()
			case multiaddr.P_QUIC_V1:
				hasQUIC = true
				udpProtos = append(udpProtos, "quic-v1")
			case multiaddr.P_WEBTRANSPORT:
				udpProtos = append(udpProtos, "webtransport")
			case multiaddr.P_WEBRTC_DIRECT:
				hasQUIC = true
				udpProtos = append(udpProtos, "webrtc-direct")
			case multiaddr.P_CERTHASH:
				certhashes = append(certhashes, c.Value())
			}
			return true
		})

		if !hasWS && configPort != 0 && port != configPortStr {
			continue
		}

		if hasWS {
			ma, err := multiaddr.NewMultiaddr(fmt.Sprintf("/dns/%s%s/tcp/443/wss", webSubdomainPrefix, domain))
			if err != nil {
				continue
			}
			if !seenWSS[ma.String()] {
				seenWSS[ma.String()] = true
				wssAddrs = append(wssAddrs, ma)
			}
		} else if hasQUIC {
			var parts []string
			parts = append(parts, fmt.Sprintf("/dns/%s/udp/%s", domain, port))
			for _, p := range udpProtos {
				parts = append(parts, "/"+p)
			}
			for _, ch := range certhashes {
				parts = append(parts, fmt.Sprintf("/certhash/%s", ch))
			}
			ma, err := multiaddr.NewMultiaddr(strings.Join(parts, ""))
			if err != nil {
				continue
			}
			if !seenUDP[ma.String()] {
				seenUDP[ma.String()] = true
				udpAddrs = append(udpAddrs, ma)
			}
		} else if hasTCP {
			ma, err := multiaddr.NewMultiaddr(fmt.Sprintf("/dns/%s/tcp/%s", domain, port))
			if err != nil {
				continue
			}
			if !seenTCP[ma.String()] {
				seenTCP[ma.String()] = true
				tcpAddrs = append(tcpAddrs, ma)
			}
		}
	}

	result := append(tcpAddrs, append(wssAddrs, udpAddrs...)...)
	if len(result) > 0 {
		return result, nil
	}

	return filterPublicAddrs(hostAddrs), nil
}

func filterPublicAddrs(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	return lo.Filter(addrs, func(addr multiaddr.Multiaddr, _ int) bool {
		return !manet.IsIPLoopback(addr) && !manet.IsIPUnspecified(addr) && !manet.IsPrivateAddr(addr)
	})
}

// GetKeystore returns the node's keystore
func (n *Node) GetKeystore() keystore.Keystore {
	return n.keystore
}

// GetPublisher returns the node's IPNS publisher
func (n *Node) GetPublisher() pluginCore.IPNSPublisher {
	return n.publisher
}

// GetDatastore returns the node's datastore
func (n *Node) GetDatastore() datastore.Datastore {
	return n.datastore
}

// GetPrivateKey returns the node's private key
func (n *Node) GetPrivateKey() crypto.PrivKey {
	return n.host.Peerstore().PrivKey(n.host.ID())
}

// allPeersLocal checks if all bootstrap peers are local/private addresses
// Returns true if the node should use LAN DHT mode instead of WAN DHT
func allPeersLocal(peers []peer.AddrInfo) bool {
	if len(peers) == 0 {
		// No bootstrap peers configured, default to WAN mode
		return false
	}

	// Check each bootstrap peer
	for _, peerInfo := range peers {
		// Check if any address is public
		for _, addr := range peerInfo.Addrs {
			// Treat DNS addresses as public (non-local) since they typically resolve to public IPs
			if _, err := addr.ValueForProtocol(multiaddr.P_DNSADDR); err == nil {
				return false
			}
			if _, err := addr.ValueForProtocol(multiaddr.P_DNS); err == nil {
				return false
			}
			if _, err := addr.ValueForProtocol(multiaddr.P_DNS4); err == nil {
				return false
			}
			if _, err := addr.ValueForProtocol(multiaddr.P_DNS6); err == nil {
				return false
			}
			if isPublic := manet.IsPublicAddr(addr); isPublic {
				// Found a public address, need WAN DHT
				return false
			}
		}
	}

	// All addresses are local/private, use LAN DHT
	return true
}

func mustParsePrefix(s string) netip.Prefix {
	return netip.MustParsePrefix(s)
}

func mustParseCIDR(s string) *net.IPNet {
	_, ipNet, err := net.ParseCIDR(s)
	if err != nil {
		panic(fmt.Sprintf("invalid CIDR %q: %v", s, err))
	}
	return ipNet
}

// parseGatewayMultiaddrs parses configured gateway multiaddrs into a single
// source of truth consumed by every trust layer:
//
//   - ipNets:      IP networks for the rate-limiter exemption and the
//     per-source/IP connection limiter.
//   - peerIDs:     peer IDs for connmgr protection (never pruned) and the
//     Bitswap want-rate bypass.
//   - allowlisted: the full original multiaddrs (including any /p2p and
//     /ipcidr components, verbatim) for the rcmgr allowlist, which reserves
//     connection admission when the normal system pool is full. Only
//     multiaddrs that carry an IP component are returned.
//
// An IP with an explicit /ipcidr/N prefix yields that subnet (previously the
// /ipcidr component was ignored and the address collapsed to a /32 or /128
// host route). A malformed or invalid gateway component is a configuration
// error and fails node startup rather than being silently skipped.
func parseGatewayMultiaddrs(addrs []string) (
	ipNets []*net.IPNet,
	peerIDs []peer.ID,
	allowlisted []multiaddr.Multiaddr,
	err error,
) {
	for _, raw := range addrs {
		ma, maErr := multiaddr.NewMultiaddr(raw)
		if maErr != nil {
			return nil, nil, nil, fmt.Errorf("invalid gateway multiaddr %q: %w", raw, maErr)
		}

		ipStr := ""
		prefixBits := -1
		hasIP := false
		var parseErr error

		// Emit the currently-pending IP (with its /ipcidr prefix, if any) as a
		// network. Called when a new IP component is seen and after the loop, so
		// every IP component in the multiaddr yields a network — including
		// p2p-circuit relays, where the relay IP that actually originates the
		// connection must remain rate/conn-exempt and allowlisted.
		emitPending := func() {
			if ipStr == "" || prefixBits <= 0 {
				return
			}
			_, network, netErr := net.ParseCIDR(fmt.Sprintf("%s/%d", ipStr, prefixBits))
			if netErr != nil {
				parseErr = netErr
				return
			}
			ipNets = append(ipNets, network)
			ipStr = ""
			prefixBits = -1
		}

		multiaddr.ForEach(ma, func(c multiaddr.Component) bool {
			switch c.Protocol().Code {
			case multiaddr.P_IP4:
				emitPending()
				ipStr = c.Value()
				prefixBits = 32
				hasIP = true
			case multiaddr.P_IP6:
				emitPending()
				ipStr = c.Value()
				prefixBits = 128
				hasIP = true
			case multiaddr.P_IPCIDR:
				bits, atoiErr := strconv.Atoi(c.Value())
				if atoiErr != nil {
					parseErr = fmt.Errorf("invalid /ipcidr value %q: %w", c.Value(), atoiErr)
					return false
				}
				// /ipcidr binds to the pending IP (the one it follows).
				if ipStr != "" {
					maxBits := 32
					if ip := net.ParseIP(ipStr); ip != nil && ip.To4() == nil {
						maxBits = 128
					}
					if bits > maxBits {
						parseErr = fmt.Errorf(
							"invalid /ipcidr prefix /%d for %s (max /%d)", bits, ipStr, maxBits)
						return false
					}
				}
				prefixBits = bits
			case multiaddr.P_P2P:
				p, decodeErr := peer.Decode(c.Value())
				if decodeErr != nil {
					parseErr = fmt.Errorf("invalid /p2p peer ID %q: %w", c.Value(), decodeErr)
					return false
				}
				peerIDs = append(peerIDs, p)
			}
			return true
		})
		emitPending()
		if parseErr != nil {
			return nil, nil, nil, fmt.Errorf("invalid gateway multiaddr %q: %w", raw, parseErr)
		}

		// The allowlist receives the full original multiaddr (including any
		// /p2p and /ipcidr components) verbatim, as a single source of truth
		// shared by all four gateway trust consumers. Keeping /p2p is what
		// enables the post-handshake peer re-check (rcmgr.go:799
		// AllowedPeerAndMultiaddr) after the IP-based admission. Only
		// multiaddrs carrying an IP component are allowlisted: an IP-less,
		// peer-only multiaddr would make the rcmgr decline the entry with
		// "missing ip address" and fail node startup; the peer is still
		// enforced via cmgr.Protect and the Bitswap want-rate bypass.
		if hasIP {
			allowlisted = append(allowlisted, ma)
		}
	}

	return ipNets, peerIDs, allowlisted, nil
}

// privateAndLoopbackNets returns the fixed set of private / loopback / Docker
// networks that bypass IP-rate and per-subnet connection abuse controls. These
// are always exempt so nginx and internal services can connect regardless of
// public traffic shaping. They are intentionally distinct from configured
// trusted gateway networks: a private-range peer is not automatically treated
// as critical infrastructure that reserves capacity or is never pruned.
func privateAndLoopbackNets() []*net.IPNet {
	return []*net.IPNet{
		mustParseCIDR("127.0.0.0/8"),
		mustParseCIDR("::1/128"),
		mustParseCIDR("172.16.0.0/12"),
		mustParseCIDR("10.0.0.0/8"),
		mustParseCIDR("192.168.0.0/16"),
		mustParseCIDR("fc00::/7"),
	}
}

// deriveConnLimits converts the rcmgr hard limits into the connection
// manager's soft watermarks.
//
// The connection manager is the SOFT working-set manager: it prunes the pool
// back toward low-water once the count exceeds high-water. The rcmgr is the
// HARD emergency fence: it rejects new connections once a hard limit is
// reached. For graceful operation the invariant must be
//
//	lowWater < highWater << rcmgr hard limit (for the direction that bites)
//
// libp2p's GetConnLimit() on a limiter returns the TOTAL connection hard
// limit. For a public server the inbound ceiling can be the binding one (a
// rejected inbound dial is external downtime, whereas an outbound dial is
// just silently re-tried). So derive the watermarks from min(total, inbound)
// to act on the direction that first fills up, and leave high-water meaningfully
// below that hard limit to preserve burst headroom.
func deriveConnLimits(limiter rcmgr.Limiter) (low, high int) {
	sys := limiter.GetSystemLimits()
	hardTotal := sys.GetConnTotalLimit()
	hardInbound := sys.GetConnLimit(network.DirInbound)

	hardConnLimit := hardTotal
	if hardInbound < hardTotal {
		hardConnLimit = hardInbound
	}

	// Degenerate / unbounded hard limit. InfiniteLimits (used by
	// disable_resource_limits for a debug/unlimited mode) resolves its Conns
	// limits to math.MaxInt rather than a real ceiling, and a BlockAll ("0")
	// or -1 value would carry no meaningful bound either. There is nothing to
	// derive proportions from in those cases, so return effectively-never-
	// prune watermarks (math.MaxInt scale). This keeps the connection manager
	// out of the way in unlimited mode instead of collapsing the pool down to
	// an aggressive tiny floor, which would contradict the mode's intent.
	if hardConnLimit <= 0 || hardConnLimit >= math.MaxInt/2 {
		// Preserve the low < high ordering BasicConnMgr requires while keeping
		// both at a scale that never triggers ordinary pruning on a real host.
		return math.MaxInt / 2, math.MaxInt
	}

	high = int(float64(hardConnLimit) * 0.70)
	low = int(float64(high) * 0.75)
	if low < 1 {
		low = 1
	}
	if high < low {
		high = low
	}
	// Keep the pair strictly ordered (low < high) for tiny ceilings and clamp
	// high-water to the hard limit so the prune threshold can never exceed the
	// rcmgr fence. Without this, hard limits of 1 or 2 collapse low==high==1,
	// which violates the ordering BasicConnMgr requires.
	if high > hardConnLimit {
		high = hardConnLimit
	}
	if low >= high {
		low = high - 1
		if low < 1 {
			low, high = 1, 2
		}
	}
	return low, high
}

func netIPNetToPrefix(n *net.IPNet) netip.Prefix {
	ones, _ := n.Mask.Size()
	return netip.PrefixFrom(netIPToAddr(n.IP), ones)
}

func netIPToAddr(ip net.IP) netip.Addr {
	if v4 := ip.To4(); v4 != nil {
		return netip.AddrFrom4([4]byte(v4))
	}
	return netip.AddrFrom16([16]byte(ip.To16()))
}

func parseTrustedProxies(entries []string) ([]string, error) {
	var resolved []string
	for _, s := range entries {
		ip := net.ParseIP(s)
		if ip != nil {
			resolved = append(resolved, ip.String())
			continue
		}

		ips, err := net.LookupIP(s)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve trusted proxy hostname %q: %w", s, err)
		}
		for _, ip := range ips {
			if v4 := ip.To4(); v4 != nil {
				resolved = append(resolved, v4.String())
			} else {
				resolved = append(resolved, ip.String())
			}
		}
	}
	return resolved, nil
}

func resolvePortFromAddrs(addrs []multiaddr.Multiaddr) int {
	for _, addr := range addrs {
		multiaddr.ForEach(addr, func(c multiaddr.Component) bool {
			return true
		})
		port, err := addr.ValueForProtocol(multiaddr.P_TCP)
		if err != nil {
			port, err = addr.ValueForProtocol(multiaddr.P_UDP)
		}
		if err == nil {
			if p, err := strconv.Atoi(port); err == nil && p > 0 {
				return p
			}
		}
	}
	return 0
}
