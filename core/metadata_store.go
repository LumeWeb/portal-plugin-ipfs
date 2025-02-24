package core

import (
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
)

// A MetadataStore is a store for IPFS block metadata. It is used to
// optimize block downloads by prefetching linked blocks.
type MetadataStore interface {
	BlockExists(c cid.Cid) (err error)
	BlockSiblings(c cid.Cid, max int) (siblings []cid.Cid, err error)
	BlockChildren(c cid.Cid, max *int) (siblings []cid.Cid, err error)

	Pin(PinnedBlock) error
	Unpin(c cid.Cid) error
	Pinned(offset, limit int) (roots []cid.Cid, err error)
	Size(c cid.Cid) (uint64, error)
}

type PinnedBlock struct {
	Cid   cid.Cid   `json:"cid"`
	Links []cid.Cid `json:"links"`
	Size  uint64    `json:"size"`
	Node  format.Node
}
