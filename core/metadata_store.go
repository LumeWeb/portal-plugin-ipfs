package core

import (
	"context"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
)

// A MetadataStore is a store for IPFS block metadata. It is used to
// optimize block downloads by prefetching linked blocks.
type MetadataStore interface {
	BlockExists(ctx context.Context, c cid.Cid) (err error)
	BlockSiblings(ctx context.Context, c cid.Cid, max int) (siblings []cid.Cid, err error)
	BlockChildren(ctx context.Context, c cid.Cid, max *int) (siblings []cid.Cid, err error)

	Pin(ctx context.Context, pinnedBlock PinnedBlock) error
	Unpin(ctx context.Context, c cid.Cid) error
	Pinned(ctx context.Context, offset, limit int) (roots []cid.Cid, err error)
	Size(ctx context.Context, c cid.Cid) (uint64, error)
	ProcessMissingUnixFSNames(cids []cid.Cid) error
	UpdateUnixFSMetadata(c cid.Cid, metadata any) error
	MarkBlockReady(c cid.Cid, ready bool) error
}

type PinnedBlock struct {
	Cid   cid.Cid   `json:"cid"`
	Links []cid.Cid `json:"links"`
	Size  uint64    `json:"size"`
	Node  format.Node
}
