package ipfs

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
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

	"github.com/ipfs/boxo/bitswap"
	tracerpkg "github.com/ipfs/boxo/bitswap/tracer"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/keystore"
	"github.com/ipfs/boxo/namesys"
	blocks "github.com/ipfs/go-block-format"
	format "github.com/ipfs/go-ipld-format"
	"github.com/avast/retry-go/v5"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	libp2pCoreConnmgr "github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubrouter "github.com/libp2p/go-libp2p-pubsub-router"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	webrtc "github.com/libp2p/go-libp2p/p2p/transport/webrtc"
	ws "github.com/libp2p/go-libp2p/p2p/transport/websocket"
	webtransport "github.com/libp2p/go-libp2p/p2p/transport/webtransport"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
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
	ctx             core.Context
	cfg             *config.ProtocolConfig
	reprovideStore  pluginCore.ReprovideStore
	datastore       datastore.Batching
	blockstore      blockstore.Blockstore
	peerTracker     *BlockRequestTracker
	readyChecker    BlockReadyChecker
	bootstrapPeers  []peer.AddrInfo
	bootstrapMutex  sync.RWMutex
}

// NewNodeFactory creates a new node factory with the given shared components
func NewNodeFactory(ctx core.Context, cfg *config.ProtocolConfig, rs pluginCore.ReprovideStore, ds datastore.Batching, bs blockstore.Blockstore, peerTracker *BlockRequestTracker, readyChecker BlockReadyChecker) *NodeFactory {
	factory := &NodeFactory{
		ctx:            ctx,
		cfg:            cfg,
		reprovideStore: rs,
		datastore:      ds,
		blockstore:     bs,
		peerTracker:    peerTracker,
		readyChecker:   readyChecker,
		bootstrapPeers: make([]peer.AddrInfo, 0),
		bootstrapMutex: sync.RWMutex{},
	}

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
	log              *core.Logger
	ctx              core.Context
	host             host.Host
	routing          DHTRouting
	companionDHT     *dht.IpfsDHT
	reprovider       *Reprovider
	blockService     blockservice.BlockService
	dagService       format.DAGService
	bitswap          *bitswap.Bitswap
	reproviderCancel context.CancelFunc
	datastore        datastore.Datastore
	keystore         keystore.Keystore
	publisher        *namesys.IPNSPublisher
	pubsub           *pubsub.PubSub
	pubsubValueStore *pubsubrouter.PubsubValueStore
	announceWeb      bool
	port             int
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

	limits := rcmgr.InfiniteLimits

	if cfg.AutoScaleResourceLimits {
		limits = scalingLimits.AutoScale()
	}
	limiter := rcmgr.NewFixedLimiter(limits)

	// Build list of IP ranges that bypass rate limiting and connection limits.
	// Private/Docker networks are always whitelisted (nginx and internal services
	// connect through these). Loopback is always unlimited.
	// Gateways (parsed from multiaddrs) add additional edge gateway ranges.
	gatewayNets := []*net.IPNet{
		mustParseCIDR("127.0.0.0/8"),
		mustParseCIDR("::1/128"),
		mustParseCIDR("172.16.0.0/12"),
		mustParseCIDR("10.0.0.0/8"),
		mustParseCIDR("192.168.0.0/16"),
		mustParseCIDR("fc00::/7"),
	}
	gatewayIPs, gatewayPeerIDs := parseGatewayMultiaddrs(cfg.Gateways)
	gatewayNets = append(gatewayNets, gatewayIPs...)

	prefixLimits := make([]libp2pRate.PrefixLimit, 0, len(gatewayNets))
	connLimits4 := make([]rcmgr.NetworkPrefixLimit, 0)
	connLimits6 := make([]rcmgr.NetworkPrefixLimit, 0)

	for _, cidr := range gatewayNets {
		prefix := netIPNetToPrefix(cidr)
		prefixLimits = append(prefixLimits, libp2pRate.PrefixLimit{Prefix: prefix, Limit: libp2pRate.Limit{}})
		if cidr.IP.To4() != nil {
			connLimits4 = append(connLimits4, rcmgr.NetworkPrefixLimit{Network: prefix, ConnCount: 1024})
		} else {
			connLimits6 = append(connLimits6, rcmgr.NetworkPrefixLimit{Network: prefix, ConnCount: 1024})
		}
	}

	rm, err := rcmgr.NewResourceManager(limiter,
		rcmgr.WithConnRateLimiters(&libp2pRate.Limiter{
			NetworkPrefixLimits: prefixLimits,
			GlobalLimit:         libp2pRate.Limit{},
		}),
		rcmgr.WithNetworkPrefixLimit(connLimits4, connLimits6),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource manager: %w", err)
	}

	cmgr, err := connmgr.NewConnManager(900, rm.(libp2pCoreConnmgr.GetConnLimiter).GetConnLimit())
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	trustedProxies := parseTrustedProxies(cfg.TrustedProxies)

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.ListenAddrs()...),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(privateKey),
		libp2p.EnableRelay(),
		libp2p.ForceReachabilityPublic(),
		libp2p.ResourceManager(rm),
		libp2p.DefaultPeerstore,
		libp2p.UserAgent("lumeweb-ipfs/" + pluginBuild.GetInfo().Short()),
		libp2p.Transport(newProxyTCPTransport(trustedProxies)),
		libp2p.Transport(quic.NewTransport),
		libp2p.Transport(webtransport.New),
		libp2p.Transport(webrtc.New),
		libp2p.Transport(ws.New),
		libp2p.PrometheusRegisterer(prometheus.WrapRegistererWithPrefix("libp2p_", core.PluginMetricsRegistry(internal.ProtocolName))),
	}

	opts = append(opts, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			ctx.Logger().Debug("ipfs AddrsFactory invoked",
				zap.Int("host_addr_count", len(addrs)),
				zap.Int("config_port", cfg.Port),
				zap.Strings("host_addrs", lo.Map(lo.Filter(addrs, func(a multiaddr.Multiaddr, _ int) bool { return a != nil }), func(a multiaddr.Multiaddr, _ int) string {
					return a.String()
				})),
			)
			var domain string
			if cfg.AnnounceWeb {
				httpSvc := core.GetService[core.HTTPService](ctx, core.HTTP_SERVICE)
				if httpSvc != nil {
					domain = httpSvc.APISubdomain(internal.ProtocolName, false)
				}
			}
			announceAddresses, err := AnnouncementAddresses(cfg.AnnounceWeb, domain, addrs, cfg.Port)
			ctx.Logger().Debug("ipfs announcement addresses resolved",
				zap.Bool("announce_web", cfg.AnnounceWeb),
				zap.String("domain", domain),
				zap.Int("config_port", cfg.Port),
				zap.Int("announce_addr_count", len(announceAddresses)),
				zap.Strings("announce_addrs", lo.Map(announceAddresses, func(a multiaddr.Multiaddr, _ int) string {
					return a.String()
				})),
				zap.Error(err),
			)
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
	var dhtProvider pluginCore.Provider
	var hasProvider bool

	// Detect if we should use LAN DHT mode
	// Use LAN DHT when all bootstrap peers are local (for e2e testing without public swarm)
	useLANMode := allPeersLocal(factory.GetBootstrapPeers())

	// Create node with minimal fields
	ipfsNode := &Node{
		host:        node,
		log:         ctx.Logger(),
		ctx:         ctx,
		announceWeb: cfg.AnnounceWeb,
		port:        cfg.Port,
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
		basicDHT, dhtErr := dht.New(ctx, node, dhtOpts...)
		if dhtErr != nil {
			return nil, fmt.Errorf("failed to create basic dht: %w", dhtErr)
		}
		routingImpl = basicDHT
		// Wrap basic DHT to implement pluginCore.Provider
		dhtProvider = newBasicDHTProvider(basicDHT)
		hasProvider = true
	case config.DHTModeFullRT, "":
		// FullRT is a client-only DHT — it does not register protocol handlers
		// and cannot respond to inbound DHT queries. Per its own docs, a
		// companion IpfsDHT in server mode must be run alongside it so that
		// other peers (including the website gateway) can query this node
		// for IPNS records and content routing.
		companion, dhtErr := dht.New(ctx, node, dhtOpts...)
		if dhtErr != nil {
			return nil, fmt.Errorf("failed to create companion DHT: %w", dhtErr)
		}
		err := retry.New(
			retry.Attempts(5),
			retry.Delay(5*time.Second),
			retry.DelayType(retry.BackOffDelay),
			retry.OnRetry(func(n uint, err error) {
				ctx.Logger().Warn("failed to bootstrap companion DHT, retrying",
					zap.Error(err),
					zap.Uint("attempt", n+1))
			}),
		).Do(func() error {
			return companion.Bootstrap(ctx)
		})
		if err != nil {
			ctx.Logger().Error("failed to bootstrap companion DHT after retries",
				zap.Error(err))
		}
		ipfsNode.companionDHT = companion

		fullRTOpts := []fullrt.Option{
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
		dhtProvider = frt
		hasProvider = true
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
			GatewayPeers: gatewayPeerIDs,
			GlobalRate:   rate.Limit(cfg.Bitswap.GlobalWantRateLimit),
			GlobalBurst:  cfg.Bitswap.GlobalWantBurst,
			PerPeerRate:  rate.Limit(cfg.Bitswap.PerPeerWantRateLimit),
			PerPeerBurst: cfg.Bitswap.PerPeerWantBurst,
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
		rp = NewReprovider(dhtProvider, rs, ctx.Logger().Named("reprovider"))
		reproviderCtx, cancel := context.WithCancel(ctx)
		reproviderCancel = cancel
		go rp.Run(reproviderCtx, cfg.Provider.Interval, cfg.Provider.Timeout, cfg.Provider.BatchSize)
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

func parseGatewayMultiaddrs(addrs []string) (ipNets []*net.IPNet, peerIDs []peer.ID) {
	for _, s := range addrs {
		ma, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			continue
		}

		for _, c := range ma {
			switch c.Protocol().Code {
			case multiaddr.P_IP4, multiaddr.P_IP6:
				ip := net.ParseIP(c.Value())
				if ip != nil {
					bits := 128
					if ip.To4() != nil {
						bits = 32
					}
					_, ipNet, _ := net.ParseCIDR(fmt.Sprintf("%s/%d", ip.String(), bits))
					if ipNet != nil {
						ipNets = append(ipNets, ipNet)
					}
				}
			case multiaddr.P_P2P:
				p, err := peer.Decode(c.Value())
				if err == nil {
					peerIDs = append(peerIDs, p)
				}
			}
		}
	}
	return ipNets, peerIDs
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

func parseTrustedProxies(entries []string) []string {
	var resolved []string
	for _, s := range entries {
		ip := net.ParseIP(s)
		if ip != nil {
			resolved = append(resolved, ip.String())
			continue
		}

		ips, err := net.LookupIP(s)
		if err != nil {
			continue
		}
		for _, ip := range ips {
			if v4 := ip.To4(); v4 != nil {
				resolved = append(resolved, v4.String())
			} else {
				resolved = append(resolved, ip.String())
			}
		}
	}
	return resolved
}

