package store

import (
	"context"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal/core"
)

// VirtualBlockStore is a wrapper around a CachedBlockstore that can bypass the cache
type VirtualBlockStore struct {
	cachedBS blockstore.Blockstore
	directBS blockstore.Blockstore
}

// NewVirtualBlockStore creates a new VirtualBlockStore
func NewVirtualBlockStore(ctx core.Context, directBS blockstore.Blockstore, cacheOpts blockstore.CacheOpts) (*VirtualBlockStore, error) {
	cachedBS, err := blockstore.CachedBlockstore(ctx, directBS, cacheOpts)
	if err != nil {
		return nil, err
	}

	return &VirtualBlockStore{
		cachedBS: cachedBS,
		directBS: directBS,
	}, nil
}

// DeleteBlock removes a given block from the blockstore
func (v *VirtualBlockStore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	if isVirtualReadEnabled(ctx) {
		return v.directBS.DeleteBlock(ctx, c)
	}
	return v.cachedBS.DeleteBlock(ctx, c)
}

// Has returns whether or not a given block is in the blockstore
func (v *VirtualBlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if isVirtualReadEnabled(ctx) {
		return v.directBS.Has(ctx, c)
	}
	return v.cachedBS.Has(ctx, c)
}

// Get returns a block by CID
func (v *VirtualBlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if isVirtualReadEnabled(ctx) {
		return v.directBS.Get(ctx, c)
	}
	return v.cachedBS.Get(ctx, c)
}

// GetSize returns the CIDs mapped BlockSize
func (v *VirtualBlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	if isVirtualReadEnabled(ctx) {
		return v.directBS.GetSize(ctx, c)
	}
	return v.cachedBS.GetSize(ctx, c)
}

// Put puts a given block to the underlying datastore
func (v *VirtualBlockStore) Put(ctx context.Context, b blocks.Block) error {
	if isVirtualReadEnabled(ctx) {
		return v.directBS.Put(ctx, b)
	}
	return v.cachedBS.Put(ctx, b)
}

// PutMany puts a slice of blocks at the same time using batching
func (v *VirtualBlockStore) PutMany(ctx context.Context, bs []blocks.Block) error {
	if isVirtualReadEnabled(ctx) {
		return v.directBS.PutMany(ctx, bs)
	}
	return v.cachedBS.PutMany(ctx, bs)
}

// AllKeysChan returns a channel from which the CIDs in the Blockstore can be read
func (v *VirtualBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	if isVirtualReadEnabled(ctx) {
		return v.directBS.AllKeysChan(ctx)
	}
	return v.cachedBS.AllKeysChan(ctx)
}

// HashOnRead specifies if every read block should be rehashed to make sure it matches its CID
func (v *VirtualBlockStore) HashOnRead(enabled bool) {
	v.cachedBS.HashOnRead(enabled)
	v.directBS.HashOnRead(enabled)
}
