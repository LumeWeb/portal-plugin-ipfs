package store

import (
	"bytes"
	"context"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/service"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
	"time"
)

type (
	// A BlockStore is a blockstore backed by a renterd node.
	BlockStore struct {
		log *core.Logger

		bucket string

		workerClient *worker.Client
		busClient    *bus.Client

		metadata   MetadataStore
		downloader BlockDownloader
		storage    core.StorageService
		proto      core.StorageProtocol
	}

	// ContextKey is a type for context keys
	ContextKey string
)

const (
	// VirtualReadKey is the context key for the virtual read option
	VirtualReadKey ContextKey = "virtualRead"
)

// VirtualReadOption sets the virtual read option in the context
func VirtualReadOption(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, VirtualReadKey, enabled)
}

// isVirtualReadEnabled checks if virtual read is enabled in the context
func isVirtualReadEnabled(ctx context.Context) bool {
	value, ok := ctx.Value(VirtualReadKey).(bool)
	return ok && value
}

func cidKey(c cid.Cid) string {
	return encoding.ToV1(c).String()
}

// DeleteBlock removes a given block from the blockstore.
func (bs *BlockStore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	key := cidKey(c)
	log := bs.log.Named("DeleteBlock").With(zap.Stack("stack"), zap.Stringer("cid", c), zap.String("key", key))

	if isVirtualReadEnabled(ctx) {
		log.Debug("virtual read enabled, skipping delete")
		return nil
	}

	if err := bs.metadata.Unpin(c); err != nil {
		return fmt.Errorf("failed to unpin block: %w", err)
	}

	start := time.Now()
	if err := bs.busClient.DeleteObject(ctx, bs.bucket, key, api.DeleteObjectOptions{}); err != nil {
		log.Debug("failed to delete block", zap.Error(err))
	}
	log.Debug("deleted block", zap.Duration("elapsed", time.Since(start)))
	return nil
}

// Has returns whether or not a given block is in the blockstore.
func (bs *BlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	log := bs.log.Named("Has").With(zap.Stringer("cid", c))

	if isVirtualReadEnabled(ctx) {
		log.Debug("virtual read enabled, assuming block does not exist")
		return false, nil
	}

	start := time.Now()

	err := bs.metadata.BlockExists(c)
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
	if isVirtualReadEnabled(ctx) {
		bs.log.Debug("virtual read enabled, fetching block without storing")
		return bs.downloader.Get(ctx, c)
	}
	return bs.downloader.Get(ctx, c)
}

// GetSize returns the CIDs mapped BlockSize
func (bs *BlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	key := cidKey(c)
	log := bs.log.Named("GetSize").With(zap.Stringer("cid", c), zap.String("key", key))

	if isVirtualReadEnabled(ctx) {
		log.Debug("virtual read enabled, fetching block size without storing")
		block, err := bs.Get(ctx, c)
		if err != nil {
			return 0, err
		}
		return len(block.RawData()), nil
	}

	err := bs.metadata.BlockExists(c)
	if err != nil {
		return 0, err
	}

	size, err := bs.metadata.Size(c)
	if err != nil {
		return 0, err
	}

	log.Debug("got block size", zap.Uint64("size", size))
	return int(size), nil
}

// Put puts a given block to the underlying datastore
func (bs *BlockStore) Put(ctx context.Context, b blocks.Block) error {
	key := cidKey(b.Cid())
	log := bs.log.Named("Put").With(zap.Stringer("cid", b.Cid()), zap.String("key", key), zap.Int("size", len(b.RawData())))

	if isVirtualReadEnabled(ctx) {
		log.Debug("virtual read enabled, skipping actual storage")
		return nil
	}

	start := time.Now()

	size := uint64(len(b.RawData()))

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

	meta := PinnedBlock{
		Cid:  b.Cid(),
		Size: size,
		Node: node,
	}

	for _, link := range blockLinks(ctx, b) {
		meta.Links = append(meta.Links, link.Cid)
	}

	if err := bs.metadata.Pin(meta); err != nil {
		log.Debug("failed to pin block", zap.Error(err))
		return fmt.Errorf("failed to pin block %q: %w", b.Cid(), err)
	}
	log.Debug("put block", zap.Duration("duration", time.Since(start)), zap.Error(err))
	return nil
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (bs *BlockStore) PutMany(ctx context.Context, blocks []blocks.Block) error {
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
	log := bs.log.Named("AllKeysChan")

	if isVirtualReadEnabled(ctx) {
		log.Debug("virtual read enabled, returning empty channel")
		ch := make(chan cid.Cid)
		close(ch)
		return ch, nil
	}

	ch := make(chan cid.Cid)
	go func() {
		for i := 0; ; i += 1000 {
			cids, err := bs.metadata.Pinned(i, 1000)
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

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (bs *BlockStore) HashOnRead(enabled bool) {
	// TODO: implement
}

// NewBlockStore creates a new blockstore backed by a renterd node
func NewBlockStore(ctx core.Context, downloader BlockDownloader, metadata MetadataStore) (*BlockStore, error) {
	proto, ok := core.GetProtocol(internal.ProtocolName).(core.StorageProtocol)
	if !ok {
		return nil, fmt.Errorf("protocol not found: %s", internal.ProtocolName)
	}

	return &BlockStore{
		log:        ctx.Logger(),
		metadata:   metadata,
		downloader: downloader,
		storage:    ctx.Service(core.STORAGE_SERVICE).(core.StorageService),
		proto:      proto,
	}, nil
}

func blockLinks(ctx context.Context, b blocks.Block) []*format.Link {
	pn, err := encoding.DecodeBlock(ctx, b)
	if err != nil {
		return nil
	}
	return pn.Links()
}
