package block

import (
	"context"
	"fmt"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/db"
	"gorm.io/gorm"

	"github.com/ipfs/go-cid"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
)

var _ pluginCore.BlockService = (*BlockService)(nil)

type BlockService struct {
	ctx    core.Context
	db     *gorm.DB
	logger *core.Logger
	ipfs   core.Protocol
}

func NewBlockService() (core.Service, []core.ContextBuilderOption, error) {
	bs := &BlockService{}
	return bs, core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			bs.ctx = ctx
			bs.db = ctx.DB()
			bs.logger = ctx.ServiceLogger(bs)
			bs.ipfs = core.GetProtocol(internal.ProtocolName)
			return nil
		}),
	), nil
}

func (bs *BlockService) GetBlockMeta(ctx context.Context, c cid.Cid) (*pluginDb.UnixFSNode, error) {
	var unixFSNode pluginDb.UnixFSNode
	if err := db.RetryableTransaction(bs.ctx, bs.db, func(tx *gorm.DB) *gorm.DB {
		return tx.WithContext(ctx).
			Model(&pluginDb.UnixFSNode{}).
			Preload("Block"). // This will automatically join with IPFSBlock
			Joins("Block").   // This ensures the join condition is included in the main query
			Where("Block.cid = ?", c.Bytes()).
			First(&unixFSNode)
	}); err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get block meta: %w", err)
	}

	return &unixFSNode, nil
}

func (bs *BlockService) GetBlockMetaBatch(ctx context.Context, cids []cid.Cid) (map[string]*pluginDb.UnixFSNode, error) {
	metas := make(map[string]*pluginDb.UnixFSNode, len(cids))

	for _, c := range cids {
		meta, err := bs.GetBlockMeta(ctx, c)
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				continue
			}
			return nil, fmt.Errorf("failed to get block meta for %s: %w", c.String(), err)
		}

		metas[c.String()] = meta
	}

	return metas, nil
}

func (bs *BlockService) ID() string {
	return pluginCore.BLOCK_SERVICE
}
