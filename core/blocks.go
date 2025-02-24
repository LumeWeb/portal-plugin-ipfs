package core

import (
	"context"
	"github.com/ipfs/go-cid"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
)

const BLOCK_SERVICE = "ipfs.block"

type BlockService interface {
	// GetBlockMeta retrieves metadata for a single block by CID
	GetBlockMeta(ctx context.Context, c cid.Cid) (*pluginDb.UnixFSNode, error)

	// GetBlockMetaBatch retrieves metadata for multiple blocks in a single request
	GetBlockMetaBatch(ctx context.Context, cids []cid.Cid) (map[string]*pluginDb.UnixFSNode, error)

	// Service marks this as a core service
	core.Service
}
