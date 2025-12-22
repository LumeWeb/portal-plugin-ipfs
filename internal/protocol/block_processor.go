package protocol

import (
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// BlockProcessor defines a minimal interface for providing blocks
type BlockProcessor interface {
	// Next returns the next block from the source, or io.EOF when complete
	Next() (blocks.Block, error)

	// Roots returns the root CIDs
	Roots() []cid.Cid

	// Done informs the processor that a block with the given CID has been processed
	Done(cid.Cid)

	// GetDoneCIDs returns all CIDs that have been marked as done
	GetDoneCIDs() []cid.Cid

	// Release releases resources
	Release()
}
