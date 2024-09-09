package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/ipfs/boxo/ipld/unixfs"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/samber/lo"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/define"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/ipfs"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store/downloader"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.uber.org/zap"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

var _ downloader.MetadataStore = (*MetadataStoreDefault)(nil)
var _ MetadataStore = (*MetadataStoreDefault)(nil)

type (
	BlockDownloader interface {
		Get(ctx context.Context, c cid.Cid) (blocks.Block, error)
	}

	MetadataStore interface {
		BlockExists(c cid.Cid) (err error)
		Pin(PinnedBlock) error
		Unpin(c cid.Cid) error
		Pinned(offset, limit int) (roots []cid.Cid, err error)
		Size(c cid.Cid) (uint64, error)
	}

	PinnedBlock struct {
		Cid   cid.Cid   `json:"cid"`
		Links []cid.Cid `json:"links"`
		Size  uint64    `json:"size"`
		Node  format.Node
	}

	MetadataStoreDefault struct {
		ctx             core.Context
		metadataService core.MetadataService
		logger          *core.Logger
		db              *gorm.DB
	}
)

// Pin adds a block to the store.
func (s *MetadataStoreDefault) Pin(b PinnedBlock) error {
	b.Cid = encoding.NormalizeCid(b.Cid)
	s.logger.Debug("pinning block", zap.Stringer("cid", b.Cid))

	return db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		// Insert or update the parent block
		parentBlock := pluginDb.IPFSBlock{
			CID:              b.Cid.Bytes(),
			Size:             b.Size,
			LastAnnouncement: nil,
			Ready:            true,
		}

		if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
			return tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "cid"}},
				DoUpdates: clause.AssignmentColumns([]string{"updated_at", "size", "ready"}),
			}).Create(&parentBlock)
		}); err != nil {
			_ = tx.AddError(fmt.Errorf("failed to insert/update block: %w", err))
			return tx
		}

		unixfsNode, err := extractNodeMetadata(b)
		if err == nil {
			unixfsNode.BlockID = parentBlock.ID
			if err = db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
				return tx.Clauses(clause.OnConflict{
					Columns: []clause.Column{{Name: "block_id"}},
					DoUpdates: clause.Assignments(map[string]interface{}{
						"type":       unixfsNode.Type,
						"block_size": unixfsNode.BlockSize,
						"child_cid":  unixfsNode.ChildCID,
						"updated_at": time.Now(),
					}),
				}).Create(unixfsNode)
			}); err != nil {
				_ = tx.AddError(fmt.Errorf("failed to insert/update UnixFS node: %w", err))
				return tx
			}
		}

		nodeInfo, err := internal.AnalyzeNode(context.Background(), b.Node)
		if err != nil {
			return nil
		}

		for i, link := range b.Links {
			link = encoding.NormalizeCid(link)
			var childBlock pluginDb.IPFSBlock
			childBlock.CID = link.Bytes()
			childBlock.Size = 0
			if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
				return tx.FirstOrCreate(&childBlock)
			}); err != nil {
				_ = tx.AddError(fmt.Errorf("failed to find or create child block: %w", err))
				return tx
			}

			linkedBlock := pluginDb.IPFSLinkedBlock{
				ParentID:  parentBlock.ID,
				ChildID:   childBlock.ID,
				LinkIndex: i,
			}

			if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
				return tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "parent_id"}, {Name: "child_id"}, {Name: "link_index"}},
					DoNothing: true,
				}).Create(&linkedBlock)
			}); err != nil {
				_ = tx.AddError(fmt.Errorf("failed to insert linked block: %w", err))
				return tx
			}

			// Update any existing linked blocks with the correct parent ID
			if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
				return tx.Model(&pluginDb.IPFSLinkedBlock{}).
					Where("child_id = ? AND parent_id IS NULL", childBlock.ID).
					Update("parent_id", parentBlock.ID)
			}); err != nil {
				_ = tx.AddError(fmt.Errorf("failed to update linked block: %w", err))
				return tx
			}

			found := lo.Filter(nodeInfo.Links, func(n *format.Link, _ int) bool {
				return n.Cid.Equals(link)
			})

			if len(found) > 0 {
				if len(found[0].Name) > 0 {
					err := core.GetService[core.CronService](s.ctx, core.CRON_SERVICE).CreateJobIfNotExists(define.CronTaskUnixFSUpdateMetadataName, define.CronTaskUnixFSUpdateMetadataArgs{
						CID:  found[0].Cid.String(),
						Name: found[0].Name,
					})
					if err != nil {
						return nil
					}
				}
			}
		}

		return tx
	})
}

func (s *MetadataStoreDefault) Unpin(c cid.Cid) error {
	c = encoding.NormalizeCid(c)

	return db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		// Find the block to be unpinned
		var block pluginDb.IPFSBlock
		if err := tx.Where("cid = ?", c.Bytes()).First(&block).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Block not found, consider it already unpinned
				return tx
			}
			_ = tx.AddError(fmt.Errorf("failed to find block: %w", err))
			return tx
		}

		//
		if err := tx.Where("block_id = ?", block.ID).Delete(&pluginDb.UnixFSNode{}).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to delete UnixFS node: %w", err))
			return tx
		}

		// Hard delete related entries in IPFSLinkedBlock
		if err := tx.Unscoped().Where("parent_id = ? OR child_id = ?", block.ID, block.ID).Delete(&pluginDb.IPFSLinkedBlock{}).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to delete linked blocks: %w", err))
			return tx
		}

		// Hard delete the block itself
		if err := tx.Unscoped().Delete(&block).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to delete block: %w", err))
			return tx
		}

		s.logger.Debug("unpinned and hard deleted block", zap.Stringer("cid", c))
		return tx
	})
}

func (s *MetadataStoreDefault) BlockExists(c cid.Cid) error {
	var block pluginDb.IPFSBlock

	c = encoding.NormalizeCid(c)

	if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Where(&pluginDb.IPFSBlock{CID: c.Bytes()}).First(&block)
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// If the block doesn't exist, return format.ErrNotFound
			return format.ErrNotFound{Cid: c}
		} else if err != nil {
			return fmt.Errorf("failed to check block existence: %w", err)
		}
	}

	// If the block is not ready, return format.ErrNotFound
	if !block.Ready {
		return format.ErrNotFound{Cid: c}
	}

	return nil
}
func (s *MetadataStoreDefault) BlockChildren(c cid.Cid, max int) (children []cid.Cid, err error) {
	c = encoding.NormalizeCid(c)
	const query = `
WITH parent_block AS (
    SELECT id 
    FROM ipfs_blocks 
    WHERE cid = ?
)
SELECT b.cid
FROM ipfs_linked_blocks AS lb
INNER JOIN ipfs_blocks AS b ON (lb.child_id = b.id)
WHERE lb.parent_id = (SELECT id FROM parent_block)
ORDER BY lb.link_index ASC
LIMIT ?
`
	var rows *sql.Rows
	if err = db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		ret := tx.Raw(query, c.Bytes(), max)
		if ret.Error == nil {
			rows, _ = ret.Rows()
		}

		return ret
	}); err != nil || rows == nil {
		return nil, fmt.Errorf("failed to query children: %w", err)
	}

	defer func(rows *sql.Rows) {
		if rows == nil {
			return
		}
		err := rows.Close()
		if err != nil {
			s.logger.Error("failed to close rows", zap.Error(err))
		}
	}(rows)

	for rows.Next() {
		var childBytes []byte
		if err := rows.Scan(&childBytes); err != nil {
			return nil, fmt.Errorf("failed to scan child: %w", err)
		}
		child, err := cid.Parse(childBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse child CID: %w", err)
		}
		children = append(children, child)
	}

	return nil, rows.Err()
}

func (s *MetadataStoreDefault) BlockSiblings(c cid.Cid, max int) (siblings []cid.Cid, err error) {
	c = encoding.NormalizeCid(c)
	const query = `
WITH child_blocks AS (
    SELECT lb.parent_id, lb.link_index
    FROM ipfs_linked_blocks AS lb
    INNER JOIN ipfs_blocks AS b ON (lb.child_id = b.id)
    WHERE b.cid = ?
),
future_siblings AS (
    SELECT lb.child_id
    FROM ipfs_linked_blocks AS lb
    INNER JOIN child_blocks AS cb ON (lb.parent_id = cb.parent_id)
    WHERE lb.link_index > cb.link_index
    ORDER BY lb.link_index ASC
    LIMIT ?
)
SELECT b.cid
FROM future_siblings AS fs
INNER JOIN ipfs_blocks AS b ON (b.id = fs.child_id)
`
	var rows *sql.Rows

	if err = db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		ret := tx.Raw(query, c.Bytes(), max)
		if ret.Error == nil {
			rows, _ = ret.Rows()
		}
		return ret
	}); err != nil || rows == nil {
		return nil, fmt.Errorf("failed to query siblings: %w", err)
	}

	defer func(rows *sql.Rows) {
		if rows == nil {
			return
		}
		err := rows.Close()
		if err != nil {
			s.logger.Error("failed to close rows", zap.Error(err))
		}
	}(rows)

	for rows.Next() {
		var siblingBytes []byte
		if err := rows.Scan(&siblingBytes); err != nil {
			return nil, fmt.Errorf("failed to scan sibling: %w", err)
		}
		sibling, err := cid.Parse(siblingBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse sibling CID: %w", err)
		}
		siblings = append(siblings, sibling)
	}

	return siblings, rows.Err()
}

func (s *MetadataStoreDefault) ProvideCIDs(limit int) (cids []ipfs.PinnedCID, err error) {
	var _blocks []pluginDb.IPFSBlock
	if err = db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Order("last_announcement ASC").Limit(limit).Find(&_blocks)
	}); err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}

	for _, block := range _blocks {
		c, err := cid.Parse(block.CID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse CID: %w", err)
		}

		lastAnnouncement := time.Unix(0, 0)

		if block.LastAnnouncement != nil {
			lastAnnouncement = *block.LastAnnouncement
		}

		time.Unix(0, 0)

		cids = append(cids, ipfs.PinnedCID{
			CID:              c,
			LastAnnouncement: lastAnnouncement,
		})
	}
	return cids, nil
}

func (s *MetadataStoreDefault) SetLastAnnouncement(cids []cid.Cid, t time.Time) error {
	return db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		for _, c := range cids {

			c = encoding.NormalizeCid(c)

			block := &pluginDb.IPFSBlock{
				CID: c.Bytes(),
			}

			var rowsAffected int64

			if err := db.RetryOnLock(s.db, func(db *gorm.DB) *gorm.DB {
				ret := tx.Model(&block).
					Where(&block).
					Update("last_announcement", t)

				if ret.Error == nil {
					rowsAffected = ret.RowsAffected
				}

				return ret
			}); err != nil {
				_ = tx.AddError(fmt.Errorf("failed to update last announcement for %q: %w", c, err))
				return tx
			}
			if rowsAffected == 0 {
				_ = tx.AddError(fmt.Errorf("no block found with CID %q", c))
				return tx
			}
		}
		return tx
	})
}

func (s *MetadataStoreDefault) Pinned(offset, limit int) (roots []cid.Cid, err error) {
	var _blocks []pluginDb.IPFSBlock

	if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&pluginDb.IPFSBlock{}).
			Select("cid").
			Order("id ASC").
			Offset(offset).
			Limit(limit).
			Find(&_blocks)

	}); err != nil {
		s.logger.Error("failed to get pinned blocks", zap.Error(err))
	}

	for _, block := range _blocks {
		root, err := cid.Parse(block.CID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse root cid: %w", err)
		}
		root = encoding.NormalizeCid(root)
		roots = append(roots, root)
	}

	return roots, err
}

func (s *MetadataStoreDefault) Size(c cid.Cid) (uint64, error) {
	c = encoding.NormalizeCid(c)

	var size uint64
	if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&pluginDb.IPFSBlock{}).
			Select("size").
			Where("cid = ?", c.Bytes()).
			First(&size)
	}); err != nil {
		return 0, fmt.Errorf("failed to query block size: %w", err)
	}

	return size, nil
}

func (s *MetadataStoreDefault) UpdateUnixFSMetadata(c cid.Cid, metadata *pluginDb.UnixFSNode) error {
	c = encoding.NormalizeCid(c)

	return db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		var block pluginDb.IPFSBlock
		if err := tx.Where("cid = ?", c.Bytes()).First(&block).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to find block: %w", err))
			return tx
		}

		if err := tx.Model(&pluginDb.UnixFSNode{}).
			Where("block_id = ?", block.ID).
			Updates(metadata).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to update UnixFS metadata: %w", err))
			return tx
		}

		return tx
	})
}

func (s *MetadataStoreDefault) GetUnixFSMetadata(c cid.Cid) (*pluginDb.UnixFSNode, error) {
	c = encoding.NormalizeCid(c)

	var metadata pluginDb.UnixFSNode
	if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Joins("JOIN ipfs_blocks ON unixfs_nodes.block_id = ipfs_blocks.id").
			Where("ipfs_blocks.cid = ?", c.Bytes()).
			First(&metadata)
	}); err != nil {
		return nil, fmt.Errorf("failed to query UnixFS metadata: %w", err)
	}

	return &metadata, nil
}

// NewMetadataStore creates a new blockstore backed by a renterd node
func NewMetadataStore(ctx core.Context) *MetadataStoreDefault {
	return &MetadataStoreDefault{
		ctx:             ctx,
		metadataService: ctx.Service(core.METADATA_SERVICE).(core.MetadataService),
		db:              ctx.DB(),
		logger:          ctx.Logger(),
	}
}

func extractNodeMetadata(block PinnedBlock) (*pluginDb.UnixFSNode, error) {
	analyzedNode, err := internal.AnalyzeNode(context.Background(), block.Node)
	if err != nil {
		return nil, err
	}

	if !analyzedNode.IsUnixFS {
		return nil, fmt.Errorf("node is not a UnixFS node")
	}

	metadata := &pluginDb.UnixFSNode{}

	switch analyzedNode.UnixFSType {
	case unixfs.TFile:
		metadata.Type = 2
	case unixfs.TDirectory:
		metadata.Type = 1
	case unixfs.TSymlink:
		metadata.Type = 4
	case unixfs.THAMTShard:
		metadata.Type = 5
	default:
		return nil, fmt.Errorf("unsupported UnixFS type: %d", analyzedNode.UnixFSType)
	}

	if analyzedNode.UnixFSType == unixfs.TFile && analyzedNode.UnixFSBlockSizes != nil && len(analyzedNode.UnixFSBlockSizes) > 0 {
		metadata.BlockSize = int64(analyzedNode.UnixFSBlockSizes[0])
	}

	metadata.ChildCID = datatypes.NewJSONSlice(lo.Map(block.Links, func(c cid.Cid, _ int) cid.Cid {
		return encoding.NormalizeCid(c)
	}))

	return metadata, nil
}
