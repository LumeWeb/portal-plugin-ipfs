package ipfs

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"io"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
	"golang.org/x/crypto/hkdf"

	"github.com/ipfs/boxo/bitswap"
	tracerpkg "github.com/ipfs/boxo/bitswap/tracer"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/keystore"
	"github.com/ipfs/boxo/namesys"
	blocks "github.com/ipfs/go-block-format"
	format "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	libp2pCoreConnmgr "github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
)

const (
	// libp2pProtocolPrefix is the protocol prefix for libp2p multiaddresses
	libp2pProtocolPrefix = "/p2p/"
)

var cachedAnnouncementAddresses []multiaddr.Multiaddr

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
	GetPublisher() pluginCore.IPNSPublisher
	GetKeystore() keystore.Keystore
	GetDatastore() datastore.Datastore
	GetPrivateKey() crypto.PrivKey
}

// NopExchange wraps an exchange.Interface and disables NotifyNewBlocks.
// This prevents the node from announcing new blocks to the network,
// because we want to selectively control when blocks are announced,
// thus we make NotifyNewBlocks a no-op.
type NopExchange struct {
	exchange.Interface
}

func (n *NopExchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	ctx, span := core.TraceMethod(ctx, "NopExchange.NotifyNewBlocks")
	defer span.End()

	return nil
}

// A Node is a minimal IPFS node
type Node struct {
	log              *core.Logger
	host             host.Host
	routing          DHTRouting
	reprovider       *Reprovider
	blockService     blockservice.BlockService
	dagService       format.DAGService
	bitswap          *bitswap.Bitswap
	reproviderCancel context.CancelFunc
	datastore        datastore.Batching
	keystore         keystore.Keystore
	publisher        *namesys.IPNSPublisher
}

// Close closes the node
func (n *Node) Close() error {
	if n == nil {
		return nil
	}
	if n.reproviderCancel != nil {
		n.reproviderCancel()
	}
	err := n.routing.Close()
	if err != nil {
		return err
	}
	err = n.bitswap.Close()
	if err != nil {
		return err
	}
	err = n.host.Close()
	if err != nil {
		return err
	}
	err = n.blockService.Close()
	if err != nil {
		return err
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
	return ConnectionAddresses(n)
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

	sess := merkledag.NewSession(ctx, n.dagService)
	seen := make(map[string]bool)
	err := merkledag.Walk(ctx, merkledag.GetLinksWithDAG(sess), root, func(c cid.Cid) bool {
		var key string
		switch c.Version() {
		case 0:
			key = cid.NewCidV1(c.Type(), c.Hash()).String()
		case 1:
			key = c.String()
		}
		if seen[key] {
			return false
		}
		log := log.With(zap.Stringer("childCID", c))
		log.Debug("pinning child")
		// TODO: queue and handle these correctly
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		node, err := sess.Get(ctx, c)
		if err != nil {
			log.Error("failed to get node", zap.Error(err))
			return false
		} else if err := n.blockService.AddBlock(ctx, node); err != nil {
			log.Error("failed to add block", zap.Error(err))
			return false
		}
		seen[key] = true
		log.Debug("pinned block")
		return true
	}, merkledag.Concurrent(), merkledag.IgnoreErrors())
	if err != nil {
		return fmt.Errorf("failed to walk DAG: %w", err)
	}
	n.reprovider.Trigger()
	return nil
}

// NewNode creates a new IPFS node
func NewNode(ctx core.Context, cfg *config.ProtocolConfig, rs pluginCore.ReprovideStore, ds datastore.Batching, bs blockstore.Blockstore, peerTracker *BlockRequestTracker) (*Node, error) {
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
	rm, err := rcmgr.NewResourceManager(limiter, rcmgr.WithMetricsDisabled())
	if err != nil {
		return nil, fmt.Errorf("failed to create resource manager: %w", err)
	}

	cmgr, err := connmgr.NewConnManager(900, rm.(libp2pCoreConnmgr.GetConnLimiter).GetConnLimit())
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.ListenAddresses...),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(privateKey),
		libp2p.EnableRelay(),
		libp2p.ResourceManager(rm),
		libp2p.DefaultPeerstore,
		libp2p.DefaultTransports,
		libp2p.PrometheusRegisterer(prometheus.WrapRegistererWithPrefix("libp2p_", core.PluginMetricsRegistry(internal.ProtocolName))),
		libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			announceAddresses, err := AnnouncementAddresses()
			if err != nil {
				ctx.Logger().Error("failed to get announcement addresses", zap.Error(err))
				return lo.Filter(addrs, func(addr multiaddr.Multiaddr, _ int) bool {
					return addr != nil
				})
			}

			return lo.Filter(announceAddresses, func(addr multiaddr.Multiaddr, _ int) bool {
				return addr != nil
			})
		}),
	}

	node, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}
	var routingImpl DHTRouting

	var dhtProvider pluginCore.Provider
	var hasProvider bool

	// Detect if we should use LAN DHT mode
	// Use LAN DHT when all bootstrap peers are local (for e2e testing without public swarm)
	useLANMode := allPeersLocal(cfg)

	// Build common DHT options
	dhtOpts := []dht.Option{
		dht.Mode(dht.ModeServer),
		dht.BootstrapPeers(lo.Map(cfg.BootstrapPeers, func(p config.IPFSPeer, _ int) peer.AddrInfo {
			return lo.FromPtr(p.ToAddrInfo())
		})...),
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
		// Use FullRT (default)

		// FullRT requires specific BucketSize and Concurrency
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
			return nil, fmt.Errorf("failed to create fullrt: %w", dhtErr)
		}
		routingImpl = frt
		// FullRT already implements pluginCore.Provider
		dhtProvider = frt
		hasProvider = true
	}

	// Log configured bootstrap servers for debugging
	bootstrapPeers := lo.Map(cfg.BootstrapPeers, func(p config.IPFSPeer, _ int) string {
		return p.ToAddrInfo().String()
	})
	ctx.Logger().Debug("IPFS node configured with bootstrap servers",
		zap.Strings("bootstrap_peers", bootstrapPeers),
		zap.Int("count", len(bootstrapPeers)),
		zap.String("dht_mode", cfg.DHTMode))

	bitswapOpts := []bitswap.Option{
		bitswap.EngineBlockstoreWorkerCount(cfg.BlockStore.MaxConcurrentRequests),
		bitswap.TaskWorkerCount(cfg.BlockStore.MaxConcurrentRequests),
		bitswap.MaxOutstandingBytesPerPeer(1 << 20),
	}

	bs = &blockstore.ValidatingBlockstore{bs}

	// Create tracer to track peer-to-peer block requests for probabilistic attribution
	bitswapTracer := NewBitswapTracer(peerTracker, node)
	bitswapOpts = append(bitswapOpts, bitswap.WithTracer(tracerpkg.Tracer(bitswapTracer)))

	// Setup disconnect listeners to clean up peer requests when peers disconnect
	bitswapTracer.SetupDisconnectListeners()

	bitswapNet := bsnet.NewFromIpfsHost(node)
	_bitswap := bitswap.New(ctx, bitswapNet, routingImpl, bs, bitswapOpts...)

	// Wrap the bitswap exchange with NopExchange to disable automatic block announcements
	nopExchange := &NopExchange{_bitswap}

	blockServ := blockservice.New(bs, nopExchange)
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

	// Create boxo IPNS publisher
	boxoPublisher := namesys.NewIPNSPublisher(routingImpl, ds)

	return &Node{
		log:              ctx.Logger(),
		routing:          routingImpl,
		host:             node,
		bitswap:          _bitswap,
		blockService:     blockServ,
		dagService:       dagService,
		reprovider:       rp,
		reproviderCancel: reproviderCancel,
		datastore:        ds,
		keystore:         safeKeystore,
		publisher:        boxoPublisher,
	}, nil
}
func (n *Node) TriggerReprovider() {
	n.reprovider.Trigger()
}

func AnnouncementAddresses() ([]multiaddr.Multiaddr, error) {
	if len(cachedAnnouncementAddresses) > 0 {
		return cachedAnnouncementAddresses, nil
	}

	unspecAddrs := []multiaddr.Multiaddr{
		// TCP
		multiaddr.StringCast("/ip4/0.0.0.0/tcp/4001"),
		multiaddr.StringCast("/ip6/::/tcp/4001"),
		// QUIC v1
		multiaddr.StringCast("/ip4/0.0.0.0/udp/4001/quic-v1"),
		multiaddr.StringCast("/ip6/::/udp/4001/quic-v1"),
		// WebSocket
		multiaddr.StringCast("/ip4/0.0.0.0/tcp/4002/ws"),
		multiaddr.StringCast("/ip6/::/tcp/4002/ws"),
	}

	announcementAddrs, err := manet.ResolveUnspecifiedAddresses(unspecAddrs, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve announcement addresses: %w", err)
	}

	announcementAddrs = lo.Filter(announcementAddrs, func(addr multiaddr.Multiaddr, i int) bool {
		return !manet.IsIPLoopback(addr) && !manet.IsIPUnspecified(addr) && !manet.IsPrivateAddr(addr)
	})

	cachedAnnouncementAddresses = announcementAddrs

	return announcementAddrs, nil
}

func ConnectionAddresses(node IPFSNode) ([]multiaddr.Multiaddr, error) {
	annAddrs, err := AnnouncementAddresses()
	if err != nil {
		return nil, err
	}

	connAddrs := lo.Map(annAddrs, func(addr multiaddr.Multiaddr, _ int) multiaddr.Multiaddr {
		return addr.Encapsulate(multiaddr.StringCast(libp2pProtocolPrefix + node.PeerID().String()))
	})

	return connAddrs, nil
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

// allPeersLocal checks if all configured bootstrap peers are local/private addresses
// Returns true if the node should use LAN DHT mode instead of WAN DHT
func allPeersLocal(cfg *config.ProtocolConfig) bool {
	if len(cfg.BootstrapPeers) == 0 {
		// No bootstrap peers configured, default to WAN mode
		return false
	}

	// Check each bootstrap peer
	for _, bp := range cfg.BootstrapPeers {
		peerInfo := bp.ToAddrInfo()

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


