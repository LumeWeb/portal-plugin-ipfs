package core

import (
	"context"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"go.lumeweb.com/portal/core"
)

// A MetadataStore is a store for IPFS block metadata. It is used to
// optimize block downloads by prefetching linked blocks.
type MetadataStore interface {
	BlockExists(ctx context.Context, c cid.Cid) (err error)
	BlockSiblings(ctx context.Context, c cid.Cid, max int) (siblings []cid.Cid, err error)
	BlockChildren(ctx context.Context, c cid.Cid, max *int) (siblings []cid.Cid, err error)

	Pin(ctx context.Context, pinnedBlock PinnedBlock) error
	// BatchPin pins multiple blocks in a single database transaction,
	// reducing per-block transaction overhead for bulk uploads.
	BatchPin(ctx context.Context, pinnedBlocks []PinnedBlock) error
	Unpin(ctx context.Context, c cid.Cid) error
	Pinned(ctx context.Context, offset, limit int) (roots []cid.Cid, err error)
	Size(ctx context.Context, c cid.Cid) (uint64, error)
	ProcessMissingUnixFSNames(ctx context.Context, cids []cid.Cid) error
	UpdateUnixFSMetadata(c cid.Cid, metadata any) error
	MarkBlockReady(c cid.Cid, ready bool) error

	// ResolveDAG resolves the complete block graph rooted at rootCID in a single
	// recursive SQL query. Returns all blocks in the DAG with their sizes and
	// ordered parent→child link relationships.
	ResolveDAG(ctx context.Context, rootCID cid.Cid) ([]core.DAGBlockNode, error)
}

type PinnedBlock struct {
	Cid   cid.Cid   `json:"cid"`
	Links []cid.Cid `json:"links"`
	Size  uint64    `json:"size"`
	Node  format.Node
}
