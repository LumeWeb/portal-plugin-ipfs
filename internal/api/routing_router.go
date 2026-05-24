package api

import (
	"context"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/server"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

// pinnerDelegatedRouter implements server.DelegatedRouter. Read operations
// (GetIPNS, FindProviders) are backed by the local node; all others return
// ErrNotSupported.
type pinnerDelegatedRouter struct {
	api *API
}

var _ server.DelegatedRouter = (*pinnerDelegatedRouter)(nil)

// GetIPNS returns the published IPNS record for the given name.
func (r *pinnerDelegatedRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	record, err := r.api.ipfs.GetNode().GetPublisher().GetPublished(ctx, name, false)
	if err != nil {
		r.api.Logger().Error("routing: failed to get published IPNS record", zap.Error(err), zap.String("name", name.String()))
		return nil, err
	}
	if record == nil {
		return nil, routing.ErrNotFound
	}
	return record, nil
}

// FindProviders returns provider records for the given CID.
// It reports the local node as a provider if it has the block.
func (r *pinnerDelegatedRouter) FindProviders(ctx context.Context, c cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	hasBlock, err := r.api.ipfs.GetNode().HasBlock(ctx, c)
	if err != nil {
		r.api.Logger().Error("routing: failed to check block existence", zap.Error(err), zap.String("cid", c.String()))
		return nil, err
	}

	if !hasBlock {
		return iter.FromSlice([]iter.Result[types.Record]{}), nil
	}

	node := r.api.ipfs.GetNode()
	peerID := node.PeerID()

	addrs, err := node.ConnectionAddresses()
	if err != nil {
		r.api.Logger().Error("routing: failed to get connection addresses", zap.Error(err))
		return nil, err
	}

	maddrs := make([]types.Multiaddr, 0, len(addrs))
	for _, addr := range addrs {
		if transport, p2pPart := multiaddr.SplitLast(addr); p2pPart != nil && p2pPart.Protocol().Code == multiaddr.P_P2P {
			addr = transport
		}
		maddrs = append(maddrs, types.Multiaddr{Multiaddr: addr})
	}

	rec := &types.PeerRecord{
		Schema:    types.SchemaPeer,
		ID:        &peerID,
		Addrs:     maddrs,
		Protocols: []string{"transport-bitswap"},
	}

	return iter.FromSlice([]iter.Result[types.Record]{
		{Val: rec},
	}), nil
}

// FindPeers is not supported — the portal does not participate in DHT peer routing.
func (r *pinnerDelegatedRouter) FindPeers(_ context.Context, _ peer.ID, _ int) (iter.ResultIter[*types.PeerRecord], error) {
	return nil, routing.ErrNotSupported
}

// PutIPNS is not supported — the portal is a read-only routing server.
func (r *pinnerDelegatedRouter) PutIPNS(_ context.Context, _ ipns.Name, _ *ipns.Record) error {
	return routing.ErrNotSupported
}

// ProvideBitswap is deprecated and not supported.
func (r *pinnerDelegatedRouter) ProvideBitswap(_ context.Context, _ *server.BitswapWriteProvideRequest) (time.Duration, error) {
	return 0, routing.ErrNotSupported
}

// GetClosestPeers is not supported — the portal does not run a DHT.
func (r *pinnerDelegatedRouter) GetClosestPeers(_ context.Context, _ cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
	return nil, routing.ErrNotSupported
}


