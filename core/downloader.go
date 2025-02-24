package core

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// BlockDownloader defines the interface for downloading IPFS blocks with caching
// and request prioritization capabilities.
type BlockDownloader interface {
	// Get retrieves a block by its CID. The implementation should:
	// - Handle request prioritization
	// - Cache blocks to avoid redundant downloads
	// - Prefetch related blocks when appropriate
	// - Respect the provided context for cancellation
	Get(ctx context.Context, c cid.Cid) (blocks.Block, error)
}
