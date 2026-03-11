package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/ipfs"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.uber.org/zap"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var _ pluginCore.MetadataStore = (*MetadataStoreDefault)(nil)

type (
	MetadataStoreDefault struct {
		ctx    core.Context
		proto  ProtoNode
		upload core.UploadService
		logger *core.Logger
		db     *gorm.DB
	}
)

type ProtoNode interface {
	GetNode() ipfs.IPFSNode
}

// Pin adds a block to the store.
func (s *MetadataStoreDefault) Pin(ctx context.Context, b pluginCore.PinnedBlock) error {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.Pin")
	defer span.End()

	b.Cid = encoding.NormalizeCid(b.Cid)
	s.logger.Debug("pinning block", zap.Stringer("cid", b.Cid))

	// Deduplicate links while preserving order
	b.Links = lo.Uniq(lo.Map(b.Links, func(link cid.Cid, _ int) cid.Cid {
		return encoding.NormalizeCid(link)
	}))

	return db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		// Insert or update the parent block
		parentBlock := pluginDb.IPFSBlock{
			CID:              b.Cid.Bytes(),
			Size:             b.Size,
			LastAnnouncement: nil,
			Ready:            true,
		}

		if err := db.RetryableTransaction(ctx, tx, func(tx *gorm.DB) *gorm.DB {
			return tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "cid"}},
				DoUpdates: clause.AssignmentColumns([]string{"updated_at", "size", "ready"}),
			}).Create(&parentBlock)
		}); err != nil {
			_ = tx.AddError(fmt.Errorf("failed to insert/update block: %w", err))
			return tx
		}

		// Process UnixFS metadata if applicable
		s.logger.Debug("Extracting UnixFS metadata", zap.Stringer("cid", b.Cid))
		unixfsNode, err := ExtractNodeMetadata(s.logger, b)
		if err == nil {
			s.logger.Debug("UnixFS metadata extracted successfully", zap.Stringer("cid", b.Cid), zap.Any("metadata", unixfsNode))
			unixfsNode.BlockID = parentBlock.ID

			// Attempt to resolve the name immediately
			var existingNode pluginDb.UnixFSNode
			if err := tx.Where("block_id = ?", parentBlock.ID).First(&existingNode).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				_ = tx.AddError(fmt.Errorf("failed to check for existing UnixFS node: %w", err))
				return tx
			}

			if existingNode.Name == "" {
				s.logger.Debug("Attempting to resolve name from parent", zap.Stringer("cid", b.Cid))
				name, err := s.resolveNameFromParentWithBlock(b.Cid, &parentBlock, tx)
				if err == nil {
					unixfsNode.Name = name
					s.logger.Debug("Resolved name on the fly", zap.String("name", name), zap.Stringer("cid", b.Cid))
				} else {
					s.logger.Warn("Failed to resolve name on the fly", zap.Error(err), zap.Stringer("cid", b.Cid))
				}
			} else {
				unixfsNode.Name = existingNode.Name // Preserve existing name
				s.logger.Debug("Using existing name", zap.String("name", unixfsNode.Name), zap.Stringer("cid", b.Cid))
			}

			if err = db.RetryableTransaction(ctx, tx, func(tx *gorm.DB) *gorm.DB {
				return tx.Clauses(clause.OnConflict{
					Columns: []clause.Column{{Name: "block_id"}},
					DoUpdates: clause.Assignments(map[string]interface{}{
						"name":       unixfsNode.Name,
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
		} else {
			s.logger.Debug("Block is not a UnixFS node", zap.Stringer("cid", b.Cid), zap.Error(err))
		}

		// Process links
		s.logger.Debug("Processing links", zap.Stringer("cid", b.Cid), zap.Int("link_count", len(b.Links)))
		for i, link := range b.Links {
			link = encoding.NormalizeCid(link)
			var childBlock pluginDb.IPFSBlock
			if err = db.RetryableTransaction(ctx, tx, func(tx *gorm.DB) *gorm.DB {
				return tx.Where(&pluginDb.IPFSBlock{
					CID: link.Bytes(),
				}).FirstOrCreate(&childBlock, pluginDb.IPFSBlock{
					CID:   link.Bytes(),
					Ready: false, // Children start as not ready
				})
			}); err != nil {
				_ = tx.AddError(fmt.Errorf("failed to find or create child block: %w", err))
				return tx
			}

			// Create link relationship
			linkedBlock := pluginDb.IPFSLinkedBlock{
				ParentID:  parentBlock.ID,
				ChildID:   childBlock.ID,
				LinkIndex: i,
			}

			if err = db.RetryableTransaction(ctx, tx, func(tx *gorm.DB) *gorm.DB {
				return tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "parent_id"}, {Name: "child_id"}, {Name: "link_index"}},
					DoNothing: true,
				}).Create(&linkedBlock)
			}); err != nil {
				_ = tx.AddError(fmt.Errorf("failed to insert linked block: %w", err))
				return tx
			}

			// Update any existing linked blocks with the correct parent ID
			if err = db.RetryableTransaction(ctx, tx, func(tx *gorm.DB) *gorm.DB {
				return tx.Model(&pluginDb.IPFSLinkedBlock{}).
					Where("child_id = ? AND parent_id IS NULL", childBlock.ID).
					Update("parent_id", parentBlock.ID)
			}); err != nil {
				_ = tx.AddError(fmt.Errorf("failed to update linked block: %w", err))
				return tx
			}
		}

		s.logger.Debug("Block pinning completed", zap.Stringer("cid", b.Cid))
		return tx
	})
}

func (s *MetadataStoreDefault) resolveNameFromParentWithBlock(childCid cid.Cid, childBlock *pluginDb.IPFSBlock, tx *gorm.DB) (string, error) {
	childCid = encoding.NormalizeCid(childCid)

	s.logger.Debug("Resolving name from parent",
		zap.Stringer("child_cid", childCid),
		zap.Uint("child_block_id", childBlock.ID))

	// Find the linked block relationship using the child block ID
	var link pluginDb.IPFSLinkedBlock
	if err := tx.Where("child_id = ?", childBlock.ID).First(&link).Error; err != nil {
		s.logger.Debug("No parent found for node",
			zap.Stringer("child_cid", childCid),
			zap.Error(err))
		return "", fmt.Errorf("no parent found for node: %w", err)
	}

	s.logger.Debug("Found parent link",
		zap.Stringer("child_cid", childCid),
		zap.Uint("parent_id", link.ParentID),
		zap.Uint("child_id", link.ChildID),
		zap.Int("link_index", link.LinkIndex))

	// Get the parent block
	var parentBlock pluginDb.IPFSBlock
	if err := tx.First(&parentBlock, link.ParentID).Error; err != nil {
		s.logger.Debug("Failed to find parent block",
			zap.Stringer("child_cid", childCid),
			zap.Uint("parent_id", link.ParentID),
			zap.Error(err))
		return "", fmt.Errorf("failed to find parent block: %w", err)
	}

	// Parse the parent CID
	parentCid, err := cid.Parse(parentBlock.CID)
	if err != nil {
		s.logger.Debug("Failed to parse parent CID",
			zap.Stringer("child_cid", childCid),
			zap.Error(err))
		return "", fmt.Errorf("failed to parse parent CID: %w", err)
	}

	s.logger.Debug("Successfully parsed parent CID",
		zap.Stringer("child_cid", childCid),
		zap.Stringer("parent_cid", parentCid))

	// Get the parent block data
	block, err := s.proto.GetNode().GetBlock(s.ctx, parentCid)
	if err != nil {
		s.logger.Debug("Failed to get parent block",
			zap.Stringer("child_cid", childCid),
			zap.Stringer("parent_cid", parentCid),
			zap.Error(err))
		return "", fmt.Errorf("failed to get parent block: %w", err)
	}

	s.logger.Debug("Successfully retrieved parent block",
		zap.Stringer("child_cid", childCid),
		zap.Stringer("parent_cid", parentCid))

	// Decode the parent block
	ipldNode, err := encoding.DecodeBlock(s.ctx, block)
	if err != nil {
		s.logger.Debug("Failed to decode parent block",
			zap.Stringer("child_cid", childCid),
			zap.Stringer("parent_cid", parentCid),
			zap.Error(err))
		return "", fmt.Errorf("failed to decode parent block: %w", err)
	}

	s.logger.Debug("Successfully decoded parent block",
		zap.Stringer("child_cid", childCid),
		zap.Stringer("parent_cid", parentCid))

	// Find the link name that matches the child CID
	s.logger.Debug("Searching for child CID in parent links",
		zap.Stringer("child_cid", childCid),
		zap.Stringer("parent_cid", parentCid),
		zap.Int("parent_link_count", len(ipldNode.Links())))

	for _, _link := range ipldNode.Links() {
		_cid := encoding.NormalizeCid(_link.Cid)
		s.logger.Debug("Checking link",
			zap.Stringer("parent_cid", parentCid),
			zap.Stringer("link_cid", _link.Cid),
			zap.String("link_name", _link.Name))

		if _cid.Equals(childCid) {
			s.logger.Debug("Found matching link name",
				zap.Stringer("child_cid", _cid),
				zap.String("name", _link.Name))
			return _link.Name, nil
		}
	}

	s.logger.Debug("Name not found in parent links",
		zap.Stringer("child_cid", childCid),
		zap.Stringer("parent_cid", parentCid))

	return "", fmt.Errorf("name not found in parent links")
}

func (s *MetadataStoreDefault) resolveNameFromParent(childCid cid.Cid, tx *gorm.DB) (string, error) {
	// First find the child block ID
	childCid = encoding.NormalizeCid(childCid)
	var childBlock pluginDb.IPFSBlock
	if err := tx.Where("cid = ?", childCid.Bytes()).First(&childBlock).Error; err != nil {
		return "", fmt.Errorf("failed to find child block: %w", err)
	}

	return s.resolveNameFromParentWithBlock(childCid, &childBlock, tx)
}

func (s *MetadataStoreDefault) Unpin(ctx context.Context, c cid.Cid) error {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.Unpin")
	defer span.End()

	c = encoding.NormalizeCid(c)

	return db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
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

func (s *MetadataStoreDefault) BlockExists(ctx context.Context, c cid.Cid) error {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.BlockExists")
	defer span.End()

	var block pluginDb.IPFSBlock

	c = encoding.NormalizeCid(c)

	block.CID = c.Bytes()
	block.Ready = true

	if err := db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Where(&block).First(&block)
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
func (s *MetadataStoreDefault) BlockChildren(ctx context.Context, c cid.Cid, max *int) (children []cid.Cid, err error) {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.BlockChildren")
	defer core.EndSpanWithErr(span, err)

	c = encoding.NormalizeCid(c)
	query := `
        SELECT b.cid
        FROM ipfs_linked_blocks AS lb
        INNER JOIN ipfs_blocks AS b ON lb.child_id = b.id
        INNER JOIN ipfs_blocks AS p ON lb.parent_id = p.id
        WHERE p.cid = ?
          AND b.ready = 1
        ORDER BY lb.link_index ASC
    `
	args := []interface{}{c.Bytes()}

	if max != nil {
		query += "LIMIT ?"
		args = append(args, *max)
	}

	var rows *sql.Rows
	if err = db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		ret := tx.Raw(query, args...)
		if ret.Error == nil {
			rows, err = ret.Rows()
			if err != nil {
				_ = tx.AddError(err)
				return tx
			}
		}

		for rows.Next() {
			var childBytes []byte
			if err = rows.Scan(&childBytes); err != nil {
				_ = tx.AddError(fmt.Errorf("failed to scan child: %w", err))
				return tx
			}
			child, err := cid.Parse(childBytes)
			if err != nil {
				_ = tx.AddError(fmt.Errorf("failed to parse child CID: %w", err))
				return tx
			}
			children = append(children, child)
		}

		return ret
	}); err != nil || rows == nil {
		return nil, fmt.Errorf("failed to query children: %w", err)
	}

	return children, nil
}

func (s *MetadataStoreDefault) BlockSiblings(ctx context.Context, c cid.Cid, max int) (siblings []cid.Cid, err error) {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.BlockSiblings")
	defer core.EndSpanWithErr(span, err)

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
WHERE b.ready = true
`
	var rows *sql.Rows

	if err = db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		ret := tx.Raw(query, c.Bytes(), max)
		if ret.Error == nil {
			rows, err = ret.Rows()
			if err != nil {
				_ = tx.AddError(err)
				return tx
			}
			defer func(rows *sql.Rows) {
				err = rows.Close()
				if err != nil {
					s.logger.Error("Failed to close rows:", zap.Error(err))
				}
			}(rows)
		}

		for rows.Next() {
			var siblingBytes []byte
			if err := rows.Scan(&siblingBytes); err != nil {
				_ = tx.AddError(fmt.Errorf("failed to scan sibling: %w", err))
			}
			sibling, err := cid.Parse(siblingBytes)
			if err != nil {
				_ = tx.AddError(fmt.Errorf("failed to parse sibling CID: %w", err))
			}
			siblings = append(siblings, sibling)
		}

		return ret
	}); err != nil || rows == nil {
		return nil, fmt.Errorf("failed to query siblings: %w", err)
	}

	return siblings, nil
}

func (s *MetadataStoreDefault) ProvideCIDs(ctx context.Context, limit int) (cids []pluginCore.PinnedCID, err error) {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.ProvideCIDs")
	defer core.EndSpanWithErr(span, err)

	var _blocks []pluginDb.IPFSBlock
	if err = db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("ready = ?", true).Order("last_announcement ASC").Limit(limit).Find(&_blocks)
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

		cids = append(cids, pluginCore.PinnedCID{
			CID:              c,
			LastAnnouncement: lastAnnouncement,
		})
	}
	return cids, nil
}

func (s *MetadataStoreDefault) SetLastAnnouncement(ctx context.Context, cids []cid.Cid, t time.Time) error {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.SetLastAnnouncement")
	defer span.End()

	return db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		for _, c := range cids {

			c = encoding.NormalizeCid(c)

			block := &pluginDb.IPFSBlock{
				CID:   c.Bytes(),
				Ready: true,
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

func (s *MetadataStoreDefault) Pinned(ctx context.Context, offset, limit int) (roots []cid.Cid, err error) {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.Pinned")
	defer core.EndSpanWithErr(span, err)

	var _blocks []pluginDb.IPFSBlock

	if err := db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&pluginDb.IPFSBlock{}).
			Select("cid").
			Where("ready = ?", true).
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

func (s *MetadataStoreDefault) Size(ctx context.Context, c cid.Cid) (uint64, error) {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.Size")
	defer span.End()

	c = encoding.NormalizeCid(c)

	var size uint64
	if err := db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&pluginDb.IPFSBlock{}).
			Select("size").
			Where("cid = ?", c.Bytes()).
			First(&size)
	}); err != nil {
		return 0, fmt.Errorf("failed to query block size: %w", err)
	}

	return size, nil
}

func (s *MetadataStoreDefault) ProcessMissingUnixFSNames(cids []cid.Cid) error {
	for _, c := range cids {
		c = encoding.NormalizeCid(c)

		var unixfsNode pluginDb.UnixFSNode
		var block pluginDb.IPFSBlock

		if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
			if err := tx.Where("cid = ?", c.Bytes()).First(&block).Error; err != nil {
				_ = tx.AddError(fmt.Errorf("failed to find block for CID %s: %w", c.String(), err))
				return tx
			}

			if err := tx.Where("block_id = ?", block.ID).First(&unixfsNode).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					s.logger.Debug("No UnixFS node found for CID, skipping", zap.Stringer("cid", c))
					return tx // Not a UnixFS node, skip
				}
				_ = tx.AddError(fmt.Errorf("failed to find UnixFS node for CID %s: %w", c.String(), err))
				return tx
			}

			if unixfsNode.Name != "" {
				s.logger.Debug("Name already present, skipping", zap.Stringer("cid", c), zap.String("name", unixfsNode.Name))
				return tx // Name already exists, skip
			}

			name, err := s.resolveNameFromParentWithBlock(c, &block, tx)
			if err != nil {
				s.logger.Warn("Failed to resolve name for CID, skipping", zap.Stringer("cid", c), zap.Error(err))
				return tx // Failed to resolve, skip
			}

			if err := tx.Model(&unixfsNode).Update("name", name).Error; err != nil {
				_ = tx.AddError(fmt.Errorf("failed to update name for CID %s: %w", c.String(), err))
				return tx
			}

			s.logger.Debug("Successfully backfilled name", zap.Stringer("cid", c), zap.String("name", name))
			return tx
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *MetadataStoreDefault) UpdateUnixFSMetadata(c cid.Cid, metadata any) error {
	c = encoding.NormalizeCid(c)

	unixFSMetadata, ok := metadata.(*pluginDb.UnixFSNode)
	if !ok {
		return fmt.Errorf("metadata is not a UnixFSNode")
	}

	return db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		var block pluginDb.IPFSBlock
		if err := tx.Where(&pluginDb.IPFSBlock{CID: c.Bytes()}).First(&block).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to find block: %w", err))
			return tx
		}

		unixFSMetadata.BlockID = block.ID
		if err := tx.Where(&pluginDb.UnixFSNode{BlockID: block.ID}).FirstOrCreate(unixFSMetadata).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to upsert UnixFS metadata: %w", err))
			return tx
		}

		return tx
	})
}

func (s *MetadataStoreDefault) GetUnixFSMetadata(c cid.Cid) (*pluginDb.UnixFSNode, error) {
	c = encoding.NormalizeCid(c)

	var metadata pluginDb.UnixFSNode
	if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Joins("JOIN ipfs_blocks ON ipfs_unixfs_nodes.block_id = ipfs_blocks.id").
			Where("ipfs_blocks.cid = ?", c.Bytes()).
			First(&metadata)
	}); err != nil {
		return nil, fmt.Errorf("failed to query UnixFS metadata: %w", err)
	}

	return &metadata, nil
}

func (s *MetadataStoreDefault) MarkBlockReady(c cid.Cid, ready bool) error {
	c = encoding.NormalizeCid(c)

	return db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		var block pluginDb.IPFSBlock
		if err := tx.Where("cid = ?", c.Bytes()).First(&block).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to find block: %w", err))
			return tx
		}

		if err := tx.Model(&block).Update("ready", ready).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to mark readyness: %w", err))
			return tx
		}

		return tx
	})
}

// NewMetadataStore creates a new blockstore backed by a renterd node
func NewMetadataStore(ctx core.Context, proto ProtoNode) *MetadataStoreDefault {
	return &MetadataStoreDefault{
		ctx:    ctx,
		proto:  proto,
		upload: core.GetService[core.UploadService](ctx, core.UPLOAD_SERVICE),
		db:     ctx.DB(),
		logger: ctx.Logger(),
	}
}

func ExtractNodeMetadata(clogger *core.Logger, block pluginCore.PinnedBlock) (*pluginDb.UnixFSNode, error) {
	logger := clogger.Named("unixfs-extraction")

	logger.Debug("Starting UnixFS node analysis", zap.Stringer("cid", block.Cid))
	analyzedNode, err := internal.AnalyzeNode(context.Background(), block.Node)
	if err != nil {
		logger.Debug("Failed to analyze node", zap.Stringer("cid", block.Cid), zap.Error(err))
		return nil, err
	}

	if !analyzedNode.IsUnixFS {
		logger.Debug("Node is not a UnixFS node", zap.Stringer("cid", block.Cid))
		return nil, fmt.Errorf("node is not a UnixFS node")
	}

	logger.Debug("Node is a UnixFS node", zap.Stringer("cid", block.Cid), zap.String("type", analyzedNode.UnixFSType.String()))

	metadata := &pluginDb.UnixFSNode{}

	switch analyzedNode.UnixFSType {
	case unixfs.TFile:
		metadata.Type = 2
		logger.Debug("Processing UnixFS file", zap.Stringer("cid", block.Cid))
	case unixfs.TDirectory:
		metadata.Type = 1
		logger.Debug("Processing UnixFS directory", zap.Stringer("cid", block.Cid))
	case unixfs.TSymlink:
		metadata.Type = 4
		logger.Debug("Processing UnixFS symlink", zap.Stringer("cid", block.Cid))
	case unixfs.THAMTShard:
		metadata.Type = 5
		logger.Debug("Processing UnixFS HAMT shard", zap.Stringer("cid", block.Cid))
	default:
		logger.Debug("Unsupported UnixFS type", zap.Stringer("cid", block.Cid), zap.String("type", analyzedNode.UnixFSType.String()))
		return nil, fmt.Errorf("unsupported UnixFS type: %d", analyzedNode.UnixFSType)
	}

	if analyzedNode.UnixFSType == unixfs.TFile {
		if analyzedNode.ChunkSizes != nil && len(analyzedNode.ChunkSizes) > 0 {
			logger.Debug("Processing file block sizes", zap.Stringer("cid", block.Cid), zap.Int("block_count", len(analyzedNode.ChunkSizes)))
			var totalSize int64
			for _, size := range analyzedNode.ChunkSizes {
				totalSize += int64(size)
			}
			metadata.BlockSize = totalSize
			logger.Debug("File block size calculated", zap.Stringer("cid", block.Cid), zap.Int64("total_size", totalSize))
		} else {
			metadata.BlockSize = int64(analyzedNode.BlockSize)
			logger.Debug("File block size set from raw data", zap.Stringer("cid", block.Cid), zap.Int64("block_size", metadata.BlockSize))
		}
	}

	logger.Debug("Processing child CIDs", zap.Stringer("cid", block.Cid), zap.Int("child_count", len(block.Links)))

	// Validate that all link arrays have the same length to prevent index out of bounds
	if len(analyzedNode.LinkNames) != len(analyzedNode.LinkCIDs) || len(analyzedNode.LinkSizes) != len(analyzedNode.LinkCIDs) {
		return nil, fmt.Errorf("inconsistent link array lengths: CIDs=%d, Names=%d, Sizes=%d",
			len(analyzedNode.LinkCIDs), len(analyzedNode.LinkNames), len(analyzedNode.LinkSizes))
	}

	// Convert separate arrays to format.Link structs for compatibility, filtering out invalid CIDs
	var validLinks []*format.Link
	for i, linkCIDBytes := range analyzedNode.LinkCIDs {
		linkCID, err := cid.Cast(linkCIDBytes)
		if err != nil {
			logger.Error("Failed to cast link CID, skipping", zap.Stringer("parent_cid", block.Cid), zap.Error(err))
			continue
		}
		validLinks = append(validLinks, &format.Link{
			Cid:  linkCID,
			Name: analyzedNode.LinkNames[i],
			Size: analyzedNode.LinkSizes[i],
		})
	}

	metadata.ChildCID = datatypes.NewJSONSlice(lo.Map(validLinks, func(l *format.Link, _ int) cid.Cid {
		normalized := encoding.NormalizeCid(l.Cid)
		logger.Debug("Processing child link", zap.Stringer("parent_cid", block.Cid), zap.Stringer("child_cid", normalized), zap.String("link_name", l.Name))
		return normalized
	}))

	logger.Debug("UnixFS metadata extraction completed", zap.Stringer("cid", block.Cid), zap.Any("metadata", metadata))
	return metadata, nil
}
