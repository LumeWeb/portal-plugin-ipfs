package ipfs

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/samber/lo"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
	"golang.org/x/crypto/hkdf"
	"io"
	"net"
	"time"

	"github.com/ipfs/boxo/bitswap"
	bnetwork "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
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

var cachedAnnouncementAddresses []multiaddr.Multiaddr

// A Node is a minimal IPFS node
type Node struct {
	log          *core.Logger
	host         host.Host
	frt          *fullrt.FullRT
	reprovider   *Reprovider
	blockService blockservice.BlockService
	dagService   format.DAGService
	bitswap      *bitswap.Bitswap
}

// Close closes the node
func (n *Node) Close() error {
	err := n.frt.Close()
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
	return n.dagService.Get(ctx, c)
}

// HasBlock checks if a block is locally pinned
func (n *Node) HasBlock(ctx context.Context, c cid.Cid) (bool, error) {
	return n.blockService.Blockstore().Has(ctx, c)
}

// AddBlock adds a generic block to the IPFS node
func (n *Node) AddBlock(ctx context.Context, block blocks.Block) error {
	if err := n.blockService.AddBlock(ctx, block); err != nil {
		return fmt.Errorf("failed to add block: %w", err)
	}
	return nil
}

// PeerID returns the peer ID of the node
func (n *Node) PeerID() peer.ID {
	return n.frt.Host().ID()
}

func (n *Node) ConnectionAddresses() ([]multiaddr.Multiaddr, error) {
	annAddrs, err := AnnouncementAddresses()
	if err != nil {
		return nil, err
	}

	connAddrs := make([]multiaddr.Multiaddr, 0, len(annAddrs))

	for _, addr := range annAddrs {
		fullAddr := addr.Encapsulate(multiaddr.StringCast("/p2p/" + n.PeerID().String()))
		connAddrs = append(connAddrs, fullAddr)
	}

	return connAddrs, nil
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
func NewNode(ctx core.Context, cfg *config.Config, rs ReprovideStore, ds datastore.Batching, bs blockstore.Blockstore) (*Node, error) {
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

	scaledLimits := scalingLimits.AutoScale()
	limiter := rcmgr.NewFixedLimiter(scaledLimits)
	rm, err := rcmgr.NewResourceManager(limiter, rcmgr.WithMetricsDisabled())
	if err != nil {
		return nil, fmt.Errorf("failed to create resource manager: %w", err)
	}

	cmgr, err := connmgr.NewConnManager(900, rm.(libp2pCoreConnmgr.GetConnLimiter).GetConnLimit())
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	announceAddresses, err := AnnouncementAddresses()
	if err != nil {
		return nil, err
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.ListenAddresses...),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(privateKey),
		libp2p.EnableRelay(),
		libp2p.ResourceManager(rm),
		libp2p.DefaultPeerstore,
		libp2p.DefaultTransports,
		libp2p.AddrsFactory(func([]multiaddr.Multiaddr) []multiaddr.Multiaddr {
			return announceAddresses
		}),
	}

	node, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}
	fullRTOpts := []fullrt.Option{
		fullrt.DHTOption([]dht.Option{
			dht.Mode(dht.ModeServer),
			dht.BootstrapPeers(lo.Map(cfg.BootstrapPeers, func(p config.IPFSPeer, _ int) peer.AddrInfo {
				return lo.FromPtr(p.ToAddrInfo())
			})...),
			dht.BucketSize(20), // this cannot be changed
			dht.Concurrency(30),
			dht.Datastore(ds),
		}...),
	}

	frt, err := fullrt.NewFullRT(node, dht.DefaultPrefix, fullRTOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create fullrt: %w", err)
	}

	bitswapOpts := []bitswap.Option{
		bitswap.EngineBlockstoreWorkerCount(cfg.BlockStore.MaxConcurrentRequests),
		bitswap.TaskWorkerCount(cfg.BlockStore.MaxConcurrentRequests),
		bitswap.MaxOutstandingBytesPerPeer(1 << 20),
		bitswap.ProvideEnabled(true),
	}

	bitswapNet := bnetwork.NewFromIpfsHost(node, frt)
	_bitswap := bitswap.New(ctx, bitswapNet, bs, bitswapOpts...)

	blockServ := blockservice.New(bs, _bitswap)
	dagService := merkledag.NewDAGService(blockServ)

	for _, p := range cfg.Peers {
		addrs, err := peer.AddrInfoToP2pAddrs(p.ToAddrInfo())
		if err != nil {
			return nil, err
		}

		node.Peerstore().AddAddrs(p.ToAddrInfo().ID, addrs, peerstore.PermanentAddrTTL)
	}

	rp := NewReprovider(frt, rs, ctx.Logger().Named("reprovider"))
	go rp.Run(ctx, cfg.Provider.Interval, cfg.Provider.Timeout, cfg.Provider.BatchSize)

	return &Node{
		log:          ctx.Logger(),
		frt:          frt,
		host:         node,
		bitswap:      _bitswap,
		blockService: blockServ,
		dagService:   dagService,
		reprovider:   rp,
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
		multiaddr.StringCast("/ip4/0.0.0.0/tcp/4001"),
		multiaddr.StringCast("/ip6/::/tcp/4001"),
	}

	announcementAddrs, err := manet.ResolveUnspecifiedAddresses(unspecAddrs, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve announcement addresses: %w", err)
	}

	announcementAddrs = lo.Filter(announcementAddrs, func(addr multiaddr.Multiaddr, i int) bool {
		return !manet.IsIPLoopback(addr) && !manet.IsIPUnspecified(addr) && !isIPv4PrivateRange(addr)
	})

	cachedAnnouncementAddresses = announcementAddrs

	return announcementAddrs, nil
}

func isIPv4PrivateRange(addr multiaddr.Multiaddr) bool {
	ip4, err := addr.ValueForProtocol(multiaddr.P_IP4)
	if err != nil {
		return false
	}

	ip := net.ParseIP(ip4)
	if ip == nil {
		return false
	}

	// Check for private IPv4 ranges
	private10 := net.IPNet{IP: net.ParseIP("10.0.0.0"), Mask: net.CIDRMask(8, 32)}
	private172 := net.IPNet{IP: net.ParseIP("172.16.0.0"), Mask: net.CIDRMask(12, 32)}
	private192 := net.IPNet{IP: net.ParseIP("192.168.0.0"), Mask: net.CIDRMask(16, 32)}

	return private10.Contains(ip) || private172.Contains(ip) || private192.Contains(ip)
}
