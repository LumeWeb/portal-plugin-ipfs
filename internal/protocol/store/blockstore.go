package store

import (
	"bytes"
	"context"
	"fmt"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"

	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/ipfs"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/service"
	"go.uber.org/zap"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
)

type (
	// A BlockStore is a blockstore backed by a renterd node.
	BlockStore struct {
		ctx core.Context
		log *core.Logger

		bucket string

		metadata    pluginCore.MetadataStore
		downloader  pluginCore.BlockDownloader
		storage     core.StorageService
		upload      core.UploadService
		tracker     *ipfs.BlockRequestTracker

		proto   core.StorageProtocol
		batcher *metadataBatcher
	}
)

// DeleteBlock removes a given block from the blockstore.
func (bs *BlockStore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	ctx, span := core.TraceMethod(ctx, "BlockStore.DeleteBlock")
	defer span.End()

	key := cidKey(c)
	log := bs.log.Named("DeleteBlock").With(zap.Stack("stack"), zap.Stringer("cid", c), zap.String("key", key))

	if pc.IsVirtualReadEnabled(ctx) {
		log.Debug("virtual read enabled, skipping delete")
		return nil
	}

	if err := bs.metadata.Unpin(ctx, c); err != nil {
		return fmt.Errorf("failed to unpin block: %w", err)
	}

	start := time.Now()
	if err := bs.storage.DeleteObject(ctx, bs.proto, internal.NewIPFSHash(c)); err != nil {
		log.Debug("failed to delete block", zap.Error(err))
		return err
	}
	log.Debug("deleted block", zap.Duration("elapsed", time.Since(start)))
	return nil
}

// Has returns whether or not a given block is in the blockstore.
func (bs *BlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	ctx, span := core.TraceMethod(ctx, "BlockStore.Has")
	defer span.End()

	log := bs.log.Named("Has").With(zap.Stringer("cid", c))

	if pc.IsVirtualReadEnabled(ctx) {
		log.Debug("virtual read enabled, assuming block does not exist")
		return false, nil
	}

	start := time.Now()

	err := bs.metadata.BlockExists(ctx, c)
	if format.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to get block location: %w", err)
	}

	log.Debug("block exists", zap.Duration("elapsed", time.Since(start)))
	return true, nil
}

// Get returns a block by CID
func (bs *BlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, span := core.TraceMethod(ctx, "BlockStore.Get")
	defer span.End()

	if pc.IsVirtualReadEnabled(ctx) {
		bs.log.Debug("virtual read enabled, fetching block without storing")
		return bs.downloader.Get(ctx, c)
	}

	// Get block size for quota validation
	size, err := bs.metadata.Size(ctx, c)
	if err != nil {
		return nil, err
	}

	// Validate download quota - checks if any users pinning this content have sufficient quota
	// NOTE: Anonymous downloads (userID=0) check group availability instead of individual quota.
	// Usage will be distributed among users who have pinned this upload.
	if !pc.IsQuotaCheckSkipped(ctx) {
		bs.log.Debug("Performing anonymous download quota group availability check",
			zap.String("cid", c.String()),
			zap.Uint64("size", size))

		// Check if any pinners have sufficient quota to serve this block
		available, err := quota.CheckCIDGroupDownloadAvailability(ctx, bs.ctx, internal.NewIPFSHash(c), uint64(size))
		if err != nil {
			bs.log.Warn("Group quota availability check failed",
				zap.String("cid", c.String()),
				zap.Uint64("size", size),
				zap.Error(err))
			return nil, fmt.Errorf("failed to check group quota availability: %w", err)
		}

		if !available {
			bs.log.Debug("Group quota not available for anonymous download",
				zap.String("cid", c.String()),
				zap.Uint64("size", size))
			return nil, format.ErrNotFound{Cid: c}
		}

		bs.log.Debug("Group quota available for anonymous download",
			zap.String("cid", c.String()),
			zap.Uint64("size", size))
	}

	// Proceed with download
	block, err := bs.downloader.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	// Emit download completion event with upload tracking
	// Only emit if we can successfully retrieve upload information
	if !pc.IsQuotaCheckSkipped(ctx) {
		upload, err := bs.upload.GetUpload(ctx, internal.NewIPFSHash(c))
		if err != nil {
			bs.log.Debug("Failed to get upload for download tracking, skipping event emission",
				zap.Stringer("cid", c),
				zap.Error(err))
		} else if upload != nil {
			// Get client IP for direct requests (may be empty for internal/bitswap requests)
			clientIP := pc.GetClientIP(ctx)

			// Use probabilistic attribution if clientIP is empty
			attributionIP := clientIP
			if attributionIP == "" && bs.tracker != nil {
				if peerIP, ok := bs.tracker.GetAndRemoveRandomPeer(c); ok {
					attributionIP = peerIP
					bs.log.Debug("Using probabilistic peer attribution",
						zap.Stringer("cid", c),
						zap.String("attributed_ip", attributionIP))
				} else {
					bs.log.Debug("No peers available for attribution",
						zap.Stringer("cid", c))
				}
			}
			
				// Emit download event regardless of attribution IP availability
			// as long as quotas are not disabled
			if attributionIP == "" {
				bs.log.Debug("No attribution IP available, emitting download event without IP",
					zap.Stringer("cid", c))
			}
			quota.EmitDownloadCompleted(core.DetachContext(ctx), bs.ctx, &upload.UserID, upload.ID, uint64(len(block.RawData())), attributionIP, nil, true)
		} else {
			bs.log.Debug("Upload not found for CID, skipping download event emission",
				zap.Stringer("cid", c))
		}
	}

	return block, nil
}

// GetSize returns the CIDs mapped BlockSize
func (bs *BlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	ctx, span := core.TraceMethod(ctx, "BlockStore.GetSize")
	defer span.End()

	key := cidKey(c)
	log := bs.log.Named("GetSize").With(zap.Stringer("cid", c), zap.String("key", key))

	if pc.IsVirtualReadEnabled(ctx) {
		log.Debug("virtual read enabled, fetching block size without storing")
		block, err := bs.Get(ctx, c)
		if err != nil {
			return 0, err
		}
		return len(block.RawData()), nil
	}

	err := bs.metadata.BlockExists(ctx, c)
	if err != nil {
		return 0, err
	}

	size, err := bs.metadata.Size(ctx, c)
	if err != nil {
		return 0, err
	}

	log.Debug("got block size", zap.Uint64("size", size))
	return int(size), nil
}

// Put puts a given block to the underlying datastore
func (bs *BlockStore) Put(ctx context.Context, b blocks.Block) error {
	ctx, span := core.TraceMethod(ctx, "BlockStore.Put")
	defer span.End()

	key := cidKey(b.Cid())
	log := bs.log.Named("Put").With(zap.Stringer("cid", b.Cid()), zap.String("key", key), zap.Int("size", len(b.RawData())))

	if pc.IsVirtualReadEnabled(ctx) {
		log.Debug("virtual read enabled, skipping actual storage")
		return nil
	}

	start := time.Now()

	size := uint64(len(b.RawData()))

	// Upload to storage
	_, err := bs.storage.UploadObject(ctx, service.NewStorageUploadRequest(
		core.StorageUploadWithProtocol(bs.proto),
		core.StorageUploadWithData(bytes.NewReader(b.RawData())),
		core.StorageUploadWithSize(size),
		core.StorageUploadWithProof(internal.NewIPFSHash(b.Cid())),
	))

	if err != nil {
		return fmt.Errorf("failed to store block %q: %w", b.Cid(), err)
	}

	log.Debug("object uploaded", zap.Duration("elapsed", time.Since(start)))

	node, err := encoding.DecodeBlock(ctx, b)
	if err != nil {
		return fmt.Errorf("failed to decode block %q: %w", b.Cid(), err)
	}

	meta := pluginCore.PinnedBlock{
		Cid:  b.Cid(),
		Size: size,
		Node: node,
	}

	for _, link := range blockLinks(ctx, b) {
		meta.Links = append(meta.Links, link.Cid)
	}

	if err = bs.batcher.Add(ctx, meta); err != nil {
		log.Debug("failed to queue block metadata", zap.Error(err))
		return fmt.Errorf("failed to pin block %q: %w", b.Cid(), err)
	}

	log.Debug("put block", zap.Duration("duration", time.Since(start)))
	return nil
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (bs *BlockStore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	ctx, span := core.TraceMethod(ctx, "BlockStore.PutMany")
	defer span.End()

	log := bs.log.Named("PutMany").With(zap.Int("blocks", len(blocks)))

	for _, block := range blocks {
		log.Debug("putting block", zap.Stringer("cid", block.Cid()))
		if err := bs.Put(ctx, block); err != nil {
			return fmt.Errorf("failed to put block %q: %w", block.Cid(), err)
		}
	}

	return nil
}

// AllKeysChan returns a channel from which
// the CIDs in the Blockstore can be read. It should respect
// the given context, closing the channel if it becomes Done.
func (bs *BlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ctx, span := core.TraceMethod(ctx, "BlockStore.AllKeysChan")
	defer span.End()

	log := bs.log.Named("AllKeysChan")

	if pc.IsVirtualReadEnabled(ctx) {
		log.Debug("virtual read enabled, returning empty channel")
		ch := make(chan cid.Cid)
		close(ch)
		return ch, nil
	}

	ch := make(chan cid.Cid)
	go func() {
		for i := 0; ; i += 1000 {
			cids, err := bs.metadata.Pinned(ctx, i, 1000)
			if err != nil {
				bs.log.Error("failed to get root CIDs", zap.Error(err))
				close(ch)
				return
			} else if len(cids) == 0 {
				close(ch)
				return
			}

			log.Debug("got pinned CIDs", zap.Int("count", len(cids)))
			for _, c := range cids {
				select {
				case ch <- c:
				case <-ctx.Done():
					close(ch)
					return
				}

				// since only the v1 CID is stored, try to convert it to v0
				if c.Type() == uint64(multicodec.DagPb) && c.Prefix().MhType == multihash.SHA2_256 {
					cv0 := cid.NewCidV0(c.Hash())
					select {
					case ch <- cv0:
					case <-ctx.Done():
						close(ch)
						return
					}
				}
			}
		}
	}()
	return ch, nil
}

// Flush forces a flush of any accumulated block metadata to the database.
// This must be called after all blocks have been submitted via Put to ensure
// the final partial batch is written.
func (bs *BlockStore) Flush(ctx context.Context) error {
	return bs.batcher.Flush(ctx)
}

// NewBlockStore creates a new blockstore backed by a renterd node
func NewBlockStore(ctx core.Context, downloader pluginCore.BlockDownloader, metadata pluginCore.MetadataStore, tracker *ipfs.BlockRequestTracker) (*BlockStore, error) {
	proto, ok := core.GetProtocol(internal.ProtocolName).(core.StorageProtocol)
	if !ok {
		return nil, fmt.Errorf("protocol not found: %s", internal.ProtocolName)
	}

	storageSvc := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
	if storageSvc == nil {
		return nil, fmt.Errorf("storage service not initialized")
	}

	uploadSvc := core.GetService[core.UploadService](ctx, core.UPLOAD_SERVICE)
	if uploadSvc == nil {
		return nil, fmt.Errorf("upload service not initialized")
	}

	return &BlockStore{
		ctx:        ctx,
		log:        ctx.Logger(),
		metadata:   metadata,
		downloader: downloader,
		storage:    storageSvc,
		upload:     uploadSvc,
		tracker:    tracker,
		proto:      proto,
		batcher:    newMetadataBatcher(metadata, ctx.Logger(), defaultBatchSize),
	}, nil
}

func blockLinks(ctx context.Context, b blocks.Block) []*format.Link {
	ctx, span := core.TraceMethod(ctx, "blockLinks")
	defer span.End()

	pn, err := encoding.DecodeBlock(ctx, b)
	if err != nil {
		return nil
	}
	return pn.Links()
}
