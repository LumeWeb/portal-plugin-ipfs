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
	"go.lumeweb.com/ipfs-content/dagnode"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
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

// preparedBlock holds a PinnedBlock with its pre-extracted UnixFS metadata.
// Extracting metadata outside the transaction reduces tx hold time — the
// protobuf parse is pure CPU work that doesn't need the DB lock.
type preparedBlock struct {
	block      pluginCore.PinnedBlock
	unixfsNode *pluginDb.UnixFSNode // nil if not a UnixFS node
}

// Pin adds a block to the store.
func (s *MetadataStoreDefault) Pin(ctx context.Context, b pluginCore.PinnedBlock) error {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.Pin")
	defer span.End()

	b.Cid = encoding.NormalizeCid(b.Cid)
	b.Links = lo.Uniq(lo.Map(b.Links, func(link cid.Cid, _ int) cid.Cid {
		return encoding.NormalizeCid(link)
	}))

	// Pre-extract UnixFS metadata outside the transaction
	pb := preparedBlock{block: b}
	if node, err := ExtractNodeMetadata(s.logger, b); err == nil {
		pb.unixfsNode = node
	}

	return db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		// Ensure child block rows exist before looking them up.
		// In BatchPin this is done once for the whole batch; here we
		// do it per-block since Pin is the single-block path.
		if err := ensureChildBlocks(tx, pb); err != nil {
			_ = tx.AddError(err)
			return tx
		}
		if err := s.pinPreparedBlockInTx(tx, pb); err != nil {
			_ = tx.AddError(err)
			return tx
		}
		return tx
	})
}

// BatchPin pins multiple blocks in a single database transaction,
// reducing per-block transaction overhead for bulk uploads.
func (s *MetadataStoreDefault) BatchPin(ctx context.Context, pinnedBlocks []pluginCore.PinnedBlock) error {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.BatchPin")
	defer span.End()

	s.logger.Debug("batch pinning blocks", zap.Int("count", len(pinnedBlocks)))

	// Normalize CIDs and pre-extract UnixFS metadata outside the transaction.
	// This keeps the CPU-bound protobuf parsing out of the DB lock scope.
	prepared := make([]preparedBlock, len(pinnedBlocks))
	for i := range pinnedBlocks {
		pinnedBlocks[i].Cid = encoding.NormalizeCid(pinnedBlocks[i].Cid)
		pinnedBlocks[i].Links = lo.Uniq(lo.Map(pinnedBlocks[i].Links, func(link cid.Cid, _ int) cid.Cid {
			return encoding.NormalizeCid(link)
		}))

		prepared[i] = preparedBlock{block: pinnedBlocks[i]}
		if node, err := ExtractNodeMetadata(s.logger, pinnedBlocks[i]); err == nil {
			prepared[i].unixfsNode = node
		}
	}

	// Collect all unique child CIDs across the batch for bulk operations.
	childCIDSet := make(map[string]cid.Cid)
	for _, pb := range prepared {
		for _, link := range pb.block.Links {
			childCIDSet[string(link.Bytes())] = link
		}
	}

	return db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		// Bulk ensure child block rows exist (placeholder with Ready=false).
		// ON CONFLICT DO NOTHING preserves existing rows — whether Ready=true
		// (already pinned by this or another upload) or Ready=false (placeholder
		// from a previous batch). This is equivalent to the per-link FirstOrCreate
		// but avoids N SELECT+INSERT round-trips.
		if err := ensureChildBlocksFromSet(tx, childCIDSet); err != nil {
			_ = tx.AddError(err)
			return tx
		}

		for _, pb := range prepared {
			if err := s.pinPreparedBlockInTx(tx, pb); err != nil {
				_ = tx.AddError(err)
				return tx
			}
		}
		s.logger.Debug("batch pin completed", zap.Int("count", len(pinnedBlocks)))
		return tx
	})
}

// pinPreparedBlockInTx pins a single block within an existing transaction.
// UnixFS metadata must be pre-extracted (via preparedBlock) before calling this.
// No nested RetryableTransaction calls — the caller's transaction provides
// the retry boundary. Name resolution is deferred to ProcessMissingUnixFSNames().
func (s *MetadataStoreDefault) pinPreparedBlockInTx(tx *gorm.DB, pb preparedBlock) error {
	b := pb.block

	// Insert or update the parent block
	parentBlock := pluginDb.IPFSBlock{
		CID:              b.Cid.Bytes(),
		Size:             b.Size,
		LastAnnouncement: nil,
		Ready:            true,
	}

	result := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "cid"}},
		DoUpdates: clause.AssignmentColumns([]string{"updated_at", "size", "ready"}),
	}).Create(&parentBlock)
	if result.Error != nil {
		return fmt.Errorf("failed to insert/update block: %w", result.Error)
	}

	// GORM does not populate the auto-increment ID on the ON CONFLICT DO UPDATE
	// path (MySQL: LAST_INSERT_ID() only returns on INSERT, not UPDATE). When a
	// block was previously created as a placeholder by ensureChildBlocksFromSet
	// and is now being pinned, the upsert hits the update path and parentBlock.ID
	// remains 0. Query the row to get the actual ID.
	if parentBlock.ID == 0 {
		if err := tx.Where("cid = ?", b.Cid.Bytes()).First(&parentBlock).Error; err != nil {
			return fmt.Errorf("failed to find block after upsert: %w", err)
		}
	}

	// Write pre-extracted UnixFS metadata if applicable
	if pb.unixfsNode != nil {
		pb.unixfsNode.BlockID = parentBlock.ID

		// Name resolution is deferred to ProcessMissingUnixFSNames(),
		// which runs after all blocks are processed. Resolving names
		// during Pin() would require an S3 read + DAG decode of the
		// parent block per child — expensive in the hot path and
		// unnecessary since the post-upload pass already handles it.

		result := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "block_id"}},
			DoUpdates: clause.Assignments(map[string]any{
				"name":       pb.unixfsNode.Name,
				"type":       pb.unixfsNode.Type,
				"block_size": pb.unixfsNode.BlockSize,
				"child_cid":  pb.unixfsNode.ChildCID,
				"updated_at": time.Now(),
			}),
		}).Create(pb.unixfsNode)
		if result.Error != nil {
			return fmt.Errorf("failed to insert/update UnixFS node: %w", result.Error)
		}
	}

	// Process links — child blocks are already ensured by BatchPin's bulk INSERT.
	// We still need to look up their IDs for the linked_block rows.
	for i, link := range b.Links {
		var childBlock pluginDb.IPFSBlock
		result := tx.Where(&pluginDb.IPFSBlock{
			CID: link.Bytes(),
		}).First(&childBlock)
		if result.Error != nil {
			return fmt.Errorf("failed to find child block: %w", result.Error)
		}

		linkedBlock := pluginDb.IPFSLinkedBlock{
			ParentID:  parentBlock.ID,
			ChildID:   childBlock.ID,
			LinkIndex: i,
		}

		result = tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "parent_id"}, {Name: "child_id"}, {Name: "link_index"}},
			DoNothing: true,
		}).Create(&linkedBlock)
		if result.Error != nil {
			return fmt.Errorf("failed to insert linked block: %w", result.Error)
		}

		// Adopt orphaned linked blocks: if the INSERT was a no-op (RowsAffected == 0),
		// the link already exists with this parent. But if there's also an orphan row
		// (parent_id IS NULL) for this child, adopt it. Skip this if we just created
		// the link — the new row already has the correct parent_id.
		if result.RowsAffected == 0 {
			result = tx.Model(&pluginDb.IPFSLinkedBlock{}).
				Where("child_id = ? AND parent_id IS NULL", childBlock.ID).
				Update("parent_id", parentBlock.ID)
			if result.Error != nil {
				return fmt.Errorf("failed to update linked block: %w", result.Error)
			}
		}
	}

	return nil
}

// ensureChildBlocks ensures child block rows exist for a single preparedBlock.
// Used by Pin (single-block path). BatchPin uses ensureChildBlocksFromSet instead.
func ensureChildBlocks(tx *gorm.DB, pb preparedBlock) error {
	if len(pb.block.Links) == 0 {
		return nil
	}
	childCIDSet := make(map[string]cid.Cid, len(pb.block.Links))
	for _, link := range pb.block.Links {
		childCIDSet[string(link.Bytes())] = link
	}
	return ensureChildBlocksFromSet(tx, childCIDSet)
}

// ensureChildBlocksFromSet bulk-inserts placeholder rows for child blocks that
// don't yet exist. ON CONFLICT DO NOTHING preserves existing rows — whether
// Ready=true (already pinned by this or another upload) or Ready=false
// (placeholder from a previous batch). This replaces the per-link
// FirstOrCreate with a single bulk INSERT, avoiding N SELECT+INSERT round-trips.
func ensureChildBlocksFromSet(tx *gorm.DB, childCIDSet map[string]cid.Cid) error {
	if len(childCIDSet) == 0 {
		return nil
	}
	childBlocks := make([]pluginDb.IPFSBlock, 0, len(childCIDSet))
	for _, c := range childCIDSet {
		childBlocks = append(childBlocks, pluginDb.IPFSBlock{
			CID:   c.Bytes(),
			Ready: false,
		})
	}
	result := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "cid"}},
		DoNothing: true,
	}).Create(&childBlocks)
	if result.Error != nil {
		return fmt.Errorf("failed to ensure child blocks: %w", result.Error)
	}
	return nil
}

func (s *MetadataStoreDefault) resolveNameFromParentWithBlock(ctx context.Context, childCid cid.Cid, childBlock *pluginDb.IPFSBlock, tx *gorm.DB) (string, error) {
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
	// Skip quota check for internal name resolution - this is metadata extraction from already-pinned data
	getCtx := pc.SkipQuotaCheckOption(ctx, true)
	block, err := s.proto.GetNode().GetBlock(getCtx, parentCid)
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
	ipldNode, err := encoding.DecodeBlock(ctx, block)
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
			return format.ErrNotFound{Cid: c}
		}
		return fmt.Errorf("failed to check block existence: %w", err)
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
	args := []any{c.Bytes()}

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

func (s *MetadataStoreDefault) ProvideCIDs(ctx context.Context, since time.Time, limit int) (cids []pluginCore.PinnedCID, err error) {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.ProvideCIDs")
	defer core.EndSpanWithErr(span, err)

	var _blocks []pluginDb.IPFSBlock
	if err = db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("ready = ? AND (last_announcement IS NULL OR last_announcement < ?)", true, since).
			Order("last_announcement ASC").Limit(limit).Find(&_blocks)
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

func (s *MetadataStoreDefault) CountPinned(ctx context.Context, since time.Time) (stats pluginCore.PinnedCIDStats, err error) {
	ctx, span := core.TraceMethod(ctx, "MetadataStoreDefault.CountPinned")
	defer core.EndSpanWithErr(span, err)

	if err = db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&pluginDb.IPFSBlock{}).Where("ready = ?", true).Count(&stats.Total)
	}); err != nil {
		return stats, fmt.Errorf("failed to count pinned: %w", err)
	}

	if err = db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&pluginDb.IPFSBlock{}).Where("ready = ? AND last_announcement >= ?", true, since).Count(&stats.Announced)
	}); err != nil {
		return stats, fmt.Errorf("failed to count announced: %w", err)
	}

	stats.Pending = stats.Total - stats.Announced
	return stats, nil
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

func (s *MetadataStoreDefault) ProcessMissingUnixFSNames(ctx context.Context, cids []cid.Cid) error {
	// Skip quota check for internal name resolution - this is metadata extraction from already-pinned data
	ctx = pc.SkipQuotaCheckOption(ctx, true)
	for _, c := range cids {
		c = encoding.NormalizeCid(c)

		var unixfsNode pluginDb.UnixFSNode
		var block pluginDb.IPFSBlock

		if err := db.RetryableTransaction(ctx, s.db, func(tx *gorm.DB) *gorm.DB {
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

			name, err := s.resolveNameFromParentWithBlock(ctx, c, &block, tx)
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
	analyzedNode, err := dagnode.AnalyzeNode(context.Background(), block.Node)
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
		metadata.Type = pluginCore.UnixFSTypeFile.ToUint8()
		logger.Debug("Processing UnixFS file", zap.Stringer("cid", block.Cid))
	case unixfs.TDirectory:
		metadata.Type = pluginCore.UnixFSTypeDirectory.ToUint8()
		logger.Debug("Processing UnixFS directory", zap.Stringer("cid", block.Cid))
	case unixfs.TSymlink:
		metadata.Type = pluginCore.UnixFSTypeSymlink.ToUint8()
		logger.Debug("Processing UnixFS symlink", zap.Stringer("cid", block.Cid))
	case unixfs.THAMTShard:
		metadata.Type = pluginCore.UnixFSTypeHAMTShard.ToUint8()
		logger.Debug("Processing UnixFS HAMT shard", zap.Stringer("cid", block.Cid))
	default:
		logger.Debug("Unsupported UnixFS type", zap.Stringer("cid", block.Cid), zap.String("type", analyzedNode.UnixFSType.String()))
		return nil, fmt.Errorf("unsupported UnixFS type: %d", analyzedNode.UnixFSType)
	}

	if analyzedNode.UnixFSType == unixfs.TFile {
		// Use FileSize as the logical UnixFS file size (local size)
		// This is the original file size before chunking, not the encoded block size
		metadata.BlockSize = int64(analyzedNode.FileSize)
		logger.Debug("File block size set from UnixFS FileSize", zap.Stringer("cid", block.Cid), zap.Int64("file_size", metadata.BlockSize))
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
