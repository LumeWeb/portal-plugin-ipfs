package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	levelds "github.com/ipfs/go-ds-leveldb"
	ipfsLog "github.com/ipfs/go-log/v2"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/ipfs"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store/downloader"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/models/data_models"
	"go.lumeweb.com/portal/service"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var _ core.Protocol = (*Protocol)(nil)
var _ core.StorageProtocol = (*Protocol)(nil)
var _ core.ProtocolGetPinHandler = (*Protocol)(nil)
var _ core.ProtocolPinHandler = (*pinHandler)(nil)

// Helper functions for operation names
func pinChildrenOperationName() string {
	return core.OperationName(internal.ProtocolName, "pin", "children")
}

func confirmOperationName() string {
	return core.OperationName(internal.ProtocolName, "confirm")
}

type Protocol struct {
	ctx           core.Context
	db            *gorm.DB
	node          *ipfs.Node
	metadataStore *store.MetadataStoreDefault
	pin           core.PinService
	coordinator   core.WorkflowCoordinator
}

type pinHandler struct {
}

func (p pinHandler) CreateProtocolPin(ctx context.Context, id uint, data any) error {
	return nil
}

func (p pinHandler) GetProtocolPin(ctx context.Context, tx *gorm.DB, id uint) (any, error) {
	return nil, nil
}

func (p pinHandler) UpdateProtocolPin(ctx context.Context, id uint, data any) error {
	return nil
}

func (p pinHandler) DeleteProtocolPin(ctx context.Context, id uint) error {
	return nil
}

func (p pinHandler) QueryProtocolPin(ctx context.Context, query any) *gorm.DB {
	return nil
}

func (p pinHandler) GetProtocolPinModel() data_models.PinDataModel {
	return nil
}

func (p Protocol) PinHandler() core.ProtocolPinHandler {
	return &pinHandler{}
}

func (p Protocol) Workflows() []core.WorkflowDefinition {
	return []core.WorkflowDefinition{
		p.newPinWorkflow(),
		p.newPinChildBlockWorkflow(),
		p.newUploadWorkflow(),
		p.newTUSUploadWorkflow(),
	}
}

func (p Protocol) pinWorkflowSteps() []core.OperationStep {
	return []core.OperationStep{
		p.newRetryStep(core.RetrieveOperationName(internal.ProtocolName)),
		p.newRetryStep(core.ScanOperationName(internal.ProtocolName)),
		p.newRetryStep(core.StoreOperationName(internal.ProtocolName)),
		p.newContinueStep(core.PublishOperationName(internal.ProtocolName)),
		p.newRetryStep(confirmOperationName()),
		p.newRetryStep(FilePathOperationName()),
	}
}

func (p Protocol) publishWorkflowSteps() []core.OperationStep {
	return []core.OperationStep{
		p.newRetryStep(core.PublishOperationName(internal.ProtocolName)),
	}
}

func (p Protocol) newPinWorkflow() core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name:                 PIN_WORKFLOW,
		AutoTriggerFirstStep: true,
		Steps:                p.pinWorkflowSteps(),
	}
}

func (p Protocol) newPinChildBlockWorkflow() core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name:                 PIN_CHILD_BLOCK_WORKFLOW,
		AutoTriggerFirstStep: true,
		Steps: append([]core.OperationStep{
			p.newRetryStep(pinChildrenOperationName()),
		}, p.publishWorkflowSteps()...),
	}
}

func (p Protocol) newUploadWorkflow() core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name:                 UPLOAD_WORKFLOW,
		AutoTriggerFirstStep: true,
		Steps: []core.OperationStep{
			p.newRetryStep(core.PostUploadOperationName(p.Name())),
		},
	}
}

func (p Protocol) newTUSUploadWorkflow() core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name:                 TUS_UPLOAD_WORKFLOW,
		AutoTriggerFirstStep: true,
		Steps: append([]core.OperationStep{
			p.newRetryStep(core.TUSUploadOperationName(p.Name())),
		}, p.publishWorkflowSteps()...),
	}
}

func (p Protocol) newRetryStep(operation string) core.OperationStep {
	return core.OperationStep{
		Operation:       operation,
		FailureBehavior: core.RetryStep,
		ID:              operation,
	}
}

func (p Protocol) newContinueStep(operation string) core.OperationStep {
	return core.OperationStep{
		Operation:       operation,
		FailureBehavior: core.ContinueWorkflow,
		ID:              operation,
	}
}

func (p Protocol) Operations() []core.Operation {
	return []core.Operation{
		NewRetrieveOperation(p.ctx),
		NewScanOperation(p.ctx),
		NewStoreOperation(p.ctx),
		NewPublishOperation(p.ctx),
		NewConfirmOperation(p.ctx),
		NewPinChildBlocksOperation(p.ctx),
		NewPostUploadOperation(p.ctx),
		NewFilePathOperation(p.ctx),
		service.NewTUSOperationHandler(p.ctx, p, func(ctx context.Context, helper core.OperationHelper, request *models.Request, tsReq *models.TUSRequest) error {
			tusHandler := core.GetAPI(internal.ProtocolName).(core.APITusHandler).GetTusHandler()

			reader, err := tusHandler.UploadReader(ctx, tsReq.TUSUploadID, p, 0)
			if err != nil {
				return fmt.Errorf("failed to get upload reader: %w", err)
			}
			defer func(reader io.ReadCloser) {
				if reader == nil {
					return
				}
				err = reader.Close()
				if err != nil {
					helper.Logger().Error("Failed to close upload reader", zap.Error(err))
				}
			}(reader)

			// Process the upload
			cids, err := ProcessCar(helper.Context(), reader)
			if err != nil {
				return fmt.Errorf("failed to process upload: %w", err)
			}

			// Prepare workflow data for publish operation
			workflowData := &PinWorkflowData{
				PinRequestID: uuid.New(),
				Cids:         lo.Map(cids, func(item cid.Cid, index int) string { return item.String() }),
			}

			err = helper.UpdateWorkflowDataStruct(request.ID, workflowData)
			if err != nil {
				return fmt.Errorf("failed to update workflow data: %w", err)
			}

			err = core.GetService[pluginCore.UploadService](helper.Context(), pluginCore.UPLOAD_SERVICE).ProcessCIDs(ctx, cids, lo.FromPtrOr(request.UserID, 0))
			if err != nil {
				return fmt.Errorf("failed to process upload: %w", err)
			}

			return nil
		}),
	}
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

func (p Protocol) DisplayName() string {
	return internal.ProtocolDisplayName
}

func (p Protocol) Config() config.ProtocolConfig {
	return &pluginConfig.ProtocolConfig{}
}

func (p Protocol) GetMetadataStore() *store.MetadataStoreDefault {
	return p.metadataStore
}

func NewProtocol() (core.Protocol, []core.ContextBuilderOption, error) {
	proto := &Protocol{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			proto.ctx = ctx
			cfg := ctx.Config().GetProtocol(internal.ProtocolName).(*pluginConfig.ProtocolConfig)
			proto.db = ctx.DB()
			proto.pin = core.GetService[core.PinService](ctx, core.PIN_SERVICE)

			ms := store.NewMetadataStore(ctx, proto)
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
