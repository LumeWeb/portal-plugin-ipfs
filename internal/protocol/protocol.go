package protocol

import (
	"context"
	"dario.cat/mergo"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/ipfs/boxo/blockstore"
	levelds "github.com/ipfs/go-ds-leveldb"
	ipfsLog "github.com/ipfs/go-log/v2"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/ipfs"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store/downloader"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/event"
	"go.lumeweb.com/portal/service"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"io"
	"log"
	"path/filepath"
)

var _ core.Protocol = (*Protocol)(nil)
var _ core.StorageProtocol = (*Protocol)(nil)
var _ core.ProtocolRequestDataHandler = (*Protocol)(nil)
var _ core.ProtocolPinHandler = (*Protocol)(nil)

type Protocol struct {
	db            *gorm.DB
	node          *ipfs.Node
	metadataStore *store.MetadataStoreDefault
	pin           core.PinService
}

func (p Protocol) CompleteProtocolData(_ context.Context, _ uint) error {
	return nil
}

func (p Protocol) GetProtocolPinModel() any {
	return &pluginDb.IPFSPin{}
}

func (p Protocol) EncodeFileName(hash core.StorageHash) string {
	return hash.Multihash().B58String()
}

func (p Protocol) Hash(_ io.Reader, _ uint64) (core.StorageHash, error) {
	return nil, errors.New("not implemented")
}

func (p Protocol) GetNode() *ipfs.Node {
	return p.node
}

func (p Protocol) Name() string {
	return internal.ProtocolName
}

func (p Protocol) Config() config.ProtocolConfig {
	return &pluginConfig.Config{}
}

func (p Protocol) CreateProtocolData(ctx context.Context, id uint, data any) error {
	req := data.(*pluginDb.IPFSRequest)
	req.RequestID = id

	return p.db.Transaction(func(tx *gorm.DB) error {
		return db.RetryOnLock(tx, func(db *gorm.DB) *gorm.DB {
			return db.WithContext(ctx).FirstOrCreate(req, &pluginDb.IPFSRequest{
				RequestID: id,
			})
		})
	})
}

func (p Protocol) GetProtocolData(ctx context.Context, tx *gorm.DB, id uint) (any, error) {
	req := &pluginDb.IPFSRequest{
		RequestID: id,
	}
	err := tx.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return db.RetryOnLock(tx, func(db *gorm.DB) *gorm.DB {
			return db.Where(req).First(req)
		})
	})
	if err != nil {
		return nil, err
	}

	return req, nil
}

func (p Protocol) UpdateProtocolData(ctx context.Context, id uint, data any) error {
	req := data.(*pluginDb.IPFSRequest)
	req.RequestID = id

	curRequest := &pluginDb.IPFSRequest{}

	if err := p.db.Transaction(func(tx *gorm.DB) error {
		return db.RetryOnLock(tx, func(db *gorm.DB) *gorm.DB {
			return db.WithContext(ctx).Unscoped().Model(&pluginDb.IPFSRequest{}).Where(&pluginDb.IPFSRequest{RequestID: id}).First(curRequest)
		})
	}); err != nil {
		return err
	}

	req.ID = curRequest.ID

	if err := mergo.Merge(curRequest, req); err != nil {
		return err
	}

	if uuid.UUID(req.PinRequestID) != uuid.Nil {
		curRequest.PinRequestID = req.PinRequestID
	}

	if req.ParentPinRequestID != nil {
		curRequest.ParentPinRequestID = req.ParentPinRequestID
	}

	return p.db.Transaction(func(tx *gorm.DB) error {
		return db.RetryOnLock(tx, func(db *gorm.DB) *gorm.DB {
			return db.WithContext(ctx).Save(curRequest)
		})
	})
}

func (p Protocol) DeleteProtocolData(ctx context.Context, id uint) error {
	req := &pluginDb.IPFSRequest{}
	err := p.db.Transaction(func(tx *gorm.DB) error {
		return db.RetryOnLock(tx, func(db *gorm.DB) *gorm.DB {
			return db.WithContext(ctx).Delete(req, id)
		})
	})
	if err != nil {
		return err
	}

	return nil
}

func (p Protocol) QueryProtocolData(_ context.Context, tx *gorm.DB, query any) *gorm.DB {
	return tx.Where(query)
}

func (p Protocol) CreateProtocolPin(ctx context.Context, id uint, data any) error {
	req := data.(*pluginDb.IPFSPin)
	req.PinID = id

	return p.db.Transaction(func(tx *gorm.DB) error {
		return db.RetryOnLock(tx, func(db *gorm.DB) *gorm.DB {
			return db.WithContext(ctx).FirstOrCreate(req, req)
		})
	})
}

func (p Protocol) GetProtocolPin(ctx context.Context, tx *gorm.DB, id uint) (any, error) {
	req := &pluginDb.IPFSPin{
		PinID: id,
	}
	err := tx.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return db.RetryOnLock(tx, func(db *gorm.DB) *gorm.DB {
			return db.Preload("Request").Where(req).First(req)
		})
	})
	if err != nil {
		return nil, err
	}

	return req, nil
}

func (p Protocol) UpdateProtocolPin(ctx context.Context, id uint, data any) error {
	req := data.(*pluginDb.IPFSPin)
	req.PinID = id

	curPin := &pluginDb.IPFSPin{}

	if err := p.db.Transaction(func(tx *gorm.DB) error {
		return db.RetryOnLock(tx, func(db *gorm.DB) *gorm.DB {
			return db.WithContext(ctx).Model(&pluginDb.IPFSPin{}).Where(&pluginDb.IPFSPin{PinID: id}).First(curPin)
		})
	}); err != nil {
		return err
	}

	if err := mergo.Merge(curPin, req); err != nil {
		return err
	}

	if uuid.UUID(req.RequestID) != uuid.Nil {
		curPin.RequestID = req.RequestID
	}

	if req.ParentRequestID != nil {
		curPin.ParentRequestID = req.ParentRequestID
	}

	return p.db.Transaction(func(tx *gorm.DB) error {
		return db.RetryOnLock(tx, func(db *gorm.DB) *gorm.DB {
			return db.WithContext(ctx).Save(curPin)
		})
	})
}

func (p Protocol) DeleteProtocolPin(ctx context.Context, id uint) error {
	req := &pluginDb.IPFSPin{
		PinID: id,
	}
	err := p.db.Transaction(func(tx *gorm.DB) error {
		return db.RetryOnLock(tx, func(db *gorm.DB) *gorm.DB {
			return db.WithContext(ctx).Where(req).First(req)
		})
	})
	if err != nil {
		return err
	}

	return nil
}

func (p Protocol) QueryProtocolPin(ctx context.Context, query any) *gorm.DB {
	return p.db.WithContext(ctx).Where(query)
}

func (p Protocol) GetProtocolDataModel() any {
	return &pluginDb.IPFSRequest{}
}

func (p Protocol) GetMetadataStore() *store.MetadataStoreDefault {
	return p.metadataStore
}

func NewProtocol() (core.Protocol, []core.ContextBuilderOption, error) {
	proto := &Protocol{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			cfg := ctx.Config().GetProtocol(internal.ProtocolName).(*pluginConfig.Config)
			proto.db = ctx.DB()
			proto.pin = core.GetService[core.PinService](ctx, core.PIN_SERVICE)

			ms := store.NewMetadataStore(ctx)
			proto.metadataStore = ms

			bd, err := downloader.NewBlockDownloader(ctx, ms, cfg.BlockStore.MaxConcurrentFetches)
			if err != nil {
				return fmt.Errorf("failed to create block downloader: %w", err)
			}
			directBS, err := store.NewBlockStore(ctx, bd, ms)
			if err != nil {
				return fmt.Errorf("failed to create blockstore: %w", err)
			}

			cacheBsOpts := blockstore.DefaultCacheOpts()
			cacheBsOpts.HasTwoQueueCacheSize = cfg.BlockStore.CacheSize

			virtualBS, err := store.NewVirtualBlockStore(ctx, directBS, cacheBsOpts)
			if err != nil {
				return fmt.Errorf("failed to create virtual blockstore: %w", err)
			}

			ds, err := levelds.NewDatastore(filepath.Join(ctx.Config().ConfigDir(), internal.ProtocolName, "p2p.ldb"), nil)
			if err != nil {
				log.Fatal("failed to open leveldb datastore", zap.Error(err))
			}
			level := mapLogLevel(ctx.Config().Config().Core.Log.Level)

			if cfg.LogLevel != "" {
				level = mapLogLevel(cfg.LogLevel)
			}

			ipfsLog.SetAllLoggers(level)

			proto.node, err = ipfs.NewNode(ctx, cfg, ms, ds, virtualBS)
			if err != nil {
				return fmt.Errorf("failed to create ipfs node: %w", err)
			}

			return nil
		}),
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			event.Listen[*event.StorageObjectPinnedEvent](ctx, event.EVENT_STORAGE_OBJECT_UNPINNED, func(evt *event.StorageObjectPinnedEvent) error {
				return handlePinningChanged(proto, evt.Pin())
			})
			event.Listen[*event.StorageObjectUnpinnedEvent](ctx, event.EVENT_STORAGE_OBJECT_UNPINNED, func(evt *event.StorageObjectUnpinnedEvent) error {
				return handlePinningChanged(proto, evt.Pin())
			})

			return nil
		}),
		core.ContextWithExitFunc(func(ctx core.Context) error {
			if proto.node != nil {
				return proto.node.Close()
			}
			return nil
		}),
	)

	return proto, opts, nil
}

func mapLogLevel(level string) ipfsLog.LogLevel {
	switch level {
	case "debug":
		return ipfsLog.LevelDebug
	case "info":
		return ipfsLog.LevelInfo
	case "warn":
		return ipfsLog.LevelWarn
	default:
		return ipfsLog.LevelError
	}
}

func handlePinningChanged(proto *Protocol, pin *models.Pin) error {
	hash := service.NewStorageHashFromMultihashBytes(pin.Upload.Hash, pin.Upload.CIDType, nil)

	pinned, err := proto.pin.UploadPinnedGlobal(hash)
	if err != nil {
		return err
	}

	cid, err := internal.CIDFromHash(pin.Upload.Hash, pin.Upload.CIDType)
	if err != nil {
		return err
	}

	return proto.metadataStore.MarkBlockReady(cid, pinned)
}
