package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	ipfsLog "github.com/ipfs/go-log/v2"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pluginUpload "go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/ipfs"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store/downloader"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models/data_models"
	"go.lumeweb.com/portal/service"
	contentUnixFS "go.lumeweb.com/ipfs-content/unixfs"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var _ core.Protocol = (*Protocol)(nil)
var _ core.StorageProtocol = (*Protocol)(nil)
var _ core.ProtocolGetPinHandler = (*Protocol)(nil)
var _ core.ProtocolPinHandler = (*pinHandler)(nil)

// Helper functions for operation names
func confirmOperationName() string {
	return core.OperationName(internal.ProtocolName, "confirm")
}

type ProtoNode interface {
	core.Protocol
	core.StorageProtocol
	GetNode() ipfs.IPFSNode
	GetIPNSNode() pluginCore.IPNSNodeAccess
	GetMetadataStore() pluginCore.MetadataStore
	GetNodeFactory() *ipfs.NodeFactory
	RestartNode() error
}

// Ensure Protocol implements IPNSBoxoServices
var _ pluginCore.IPNSBoxoServices = (*Protocol)(nil)

type Protocol struct {
	*core.BaseComponent
	node          ipfs.IPFSNode
	metadataStore *store.MetadataStoreDefault
	pin           core.PinService
	coordinator   core.WorkflowCoordinator
	nodeFactory   *ipfs.NodeFactory
}

// GetIPNSNode returns the IPNS node access interface for IPNS operations
func (p *Protocol) GetIPNSNode() pluginCore.IPNSNodeAccess {
	return p.node
}

type pinHandler struct {
}

func (p pinHandler) CreateProtocolPin(ctx context.Context, id uint, data any) error {
	ctx, span := core.TraceMethod(ctx, "pinHandler.CreateProtocolPin")
	defer span.End()

	return nil
}

func (p pinHandler) GetProtocolPin(ctx context.Context, tx *gorm.DB, id uint) (any, error) {
	ctx, span := core.TraceMethod(ctx, "pinHandler.GetProtocolPin")
	defer span.End()

	return nil, nil
}

func (p pinHandler) UpdateProtocolPin(ctx context.Context, id uint, data any) error {
	ctx, span := core.TraceMethod(ctx, "pinHandler.UpdateProtocolPin")
	defer span.End()

	return nil
}

func (p pinHandler) DeleteProtocolPin(ctx context.Context, id uint) error {
	ctx, span := core.TraceMethod(ctx, "pinHandler.DeleteProtocolPin")
	defer span.End()

	return nil
}

func (p pinHandler) QueryProtocolPin(ctx context.Context, query any) *gorm.DB {
	ctx, span := core.TraceMethod(ctx, "pinHandler.QueryProtocolPin")
	defer span.End()

	return nil
}

func (p pinHandler) GetProtocolPinModel() data_models.PinDataModel {
	return nil
}

func (p Protocol) PinHandler() core.ProtocolPinHandler {
	return &pinHandler{}
}

func (p Protocol) ID() string {
	return p.Name()
}

// NewProtocolWorkflows creates the list of workflows for the IPFS protocol
func NewProtocolWorkflows(p core.Protocol) []core.WorkflowDefinition {
	return []core.WorkflowDefinition{
		newPinWorkflow(),
		newUploadWorkflow(p.Name()),
		newTUSUploadWorkflow(p.Name()),
	}
}

func (p Protocol) Workflows() []core.WorkflowDefinition {
	return NewProtocolWorkflows(&p)
}

func pinWorkflowSteps() []core.OperationStep {
	return []core.OperationStep{
		newRetryStep(core.RetrieveOperationName(internal.ProtocolName)),
		newRetryStep(core.ScanOperationName(internal.ProtocolName)),
		newRetryStep(core.StoreOperationName(internal.ProtocolName)),
		newContinueStep(core.PublishOperationName(internal.ProtocolName)),
		newRetryStep(confirmOperationName()),
		newRetryStep(FilePathOperationName()),
	}
}

func publishWorkflowSteps() []core.OperationStep {
	return []core.OperationStep{
		newRetryStep(core.PublishOperationName(internal.ProtocolName)),
	}
}

func newPinWorkflow() core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name:                 PIN_WORKFLOW,
		AutoTriggerFirstStep: true,
		Steps:                pinWorkflowSteps(),
	}
}

func newUploadWorkflow(protocolName string) core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name:                 UPLOAD_WORKFLOW,
		AutoTriggerFirstStep: true,
		Steps: []core.OperationStep{
			newRetryStep(core.PostUploadOperationName(protocolName)),
			newRetryStep(FilePathOperationName()),
		},
	}
}

func newTUSUploadWorkflow(protocolName string) core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name:                 TUS_UPLOAD_WORKFLOW,
		AutoTriggerFirstStep: true,
		Steps: append([]core.OperationStep{
			newRetryStep(core.TUSUploadOperationName(protocolName)),
			newRetryStep(FilePathOperationName()),
		}, publishWorkflowSteps()...),
	}
}

func newRetryStep(operation string) core.OperationStep {
	return core.OperationStep{
		Operation:       operation,
		FailureBehavior: core.RetryStep,
		ID:              operation,
	}
}

func newContinueStep(operation string) core.OperationStep {
	return core.OperationStep{
		Operation:       operation,
		FailureBehavior: core.ContinueWorkflow,
		ID:              operation,
	}
}

// NewProtocolOperations creates the list of operations for the IPFS protocol
func NewProtocolOperations(p core.Protocol) []core.Operation {
	return []core.Operation{
		NewRetrieveOperation(p.Context()),
		NewScanOperation(p.Context()),
		NewStoreOperation(p.Context()),
		NewPublishOperation(p.Context()),
		NewConfirmOperation(p.Context()),
		NewPostUploadOperation(p.Context()),
		NewFilePathOperation(p.Context()),
		NewTUSOperationHandler(p),
	}
}

// NewTUSOperationHandler creates the TUS upload operation handler
func NewTUSOperationHandler(p core.Protocol) core.Operation {
	return service.NewTUSOperationHandler(p.Context(), p, handleTUSUpload(p))
}

func (p Protocol) Operations() []core.Operation {
	return NewProtocolOperations(&p)
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

func (p Protocol) GetNode() ipfs.IPFSNode {
	return p.node
}

// RestartNode creates a new IPFS node instance using the factory
// This is useful for picking up new bootstrap peers or other configuration changes
func (p *Protocol) RestartNode() error {
	if p.nodeFactory == nil {
		return errors.New("node factory not initialized")
	}

	// Close the old node
	if p.node != nil {
		if err := p.node.Close(); err != nil {
			p.Context().Logger().Error("failed to close old node", zap.Error(err))
		}
	}

	// Create a new node using the factory
	newNode, err := p.nodeFactory.CreateNode()
	if err != nil {
		return fmt.Errorf("failed to create new node: %w", err)
	}

	// Update the node reference
	p.node = newNode

	p.Context().Logger().Info("IPFS node restarted successfully")
	return nil
}

func (p Protocol) GetPeerTracker() *ipfs.BlockRequestTracker {
	// For now, return nil - tracker is managed differently
	// Can be used to access tracker for testing/inspection
	return nil
}

func (p Protocol) GetMetadataStore() pluginCore.MetadataStore {
	return p.metadataStore
}

func (p Protocol) GetNodeFactory() *ipfs.NodeFactory {
	return p.nodeFactory
}

func (p Protocol) Name() string {
	return internal.ProtocolName
}

func (p Protocol) DisplayName() string {
	return internal.ProtocolDisplayName
}

func (p Protocol) GetConfig() config.ProtocolConfig {
	return &pluginConfig.ProtocolConfig{}
}

func NewProtocol() (core.Protocol, []core.ContextBuilderOption, error) {
	proto := &Protocol{}
	var _ds datastore.Batching
	var dsErr error

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			cfg := ctx.Config().GetProtocol(internal.ProtocolName).(*pluginConfig.ProtocolConfig)
			proto.pin = core.GetService[core.PinService](ctx, core.PIN_SERVICE)

			ms := store.NewMetadataStore(ctx, proto)
			proto.metadataStore = ms

			bd, err := downloader.NewBlockDownloader(ctx, ms, cfg.BlockStore.MaxConcurrentFetches)
			if err != nil {
				return fmt.Errorf("failed to create block downloader: %w", err)
			}

			// Create peer request tracker for probabilistic attribution
			peerTracker := ipfs.NewBlockRequestTracker()

			directBS, err := store.NewBlockStore(ctx, bd, ms, peerTracker)
			if err != nil {
				return fmt.Errorf("failed to create blockstore: %w", err)
			}

			cacheBsOpts := blockstore.DefaultCacheOpts()
			cacheBsOpts.HasTwoQueueCacheSize = cfg.BlockStore.CacheSize

			virtualBS, err := store.NewVirtualBlockStore(ctx, directBS, cacheBsOpts)
			if err != nil {
				return fmt.Errorf("failed to create virtual blockstore: %w", err)
			}

			_ds, dsErr = levelds.NewDatastore(filepath.Join(ctx.Config().ConfigDir(), internal.ProtocolName, "p2p"), nil)
			if dsErr != nil {
				ctx.Logger().Fatal("failed to open leveldb datastore", zap.Error(dsErr))
			}
			level := mapLogLevel(cfg.LogLevel)
			ipfsLog.SetAllLoggers(level)

			ctx.Logger().Debug("IPFS log level configured",
				zap.String("log_level", cfg.LogLevel),
				zap.String("mapped_level", level.String()))

			// Create node factory for managing node lifecycle and bootstrap peers
			proto.nodeFactory = ipfs.NewNodeFactory(ctx, cfg, ms, _ds, virtualBS, peerTracker)

			// Create initial node instance
			proto.node, err = proto.nodeFactory.CreateNode()
			if err != nil {
				return fmt.Errorf("failed to create ipfs node: %w", err)
			}

			return nil
		}),
		core.ContextWithExitFunc(func(ctx core.Context) error {
			if proto.node != nil {
				err := proto.node.Close()
				if err != nil {
					return err
				}
			}

			if _ds != nil {
				if err := _ds.Close(); err != nil {
					return err
				}
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

// KeyFromCID converts a CID to a datastore key
func KeyFromCID(c cid.Cid) ds.Key {
	return dshelp.MultihashToDsKey(c.Hash())
}

// KeyToCIDString converts a datastore key to CID string, removing leading slash if present
func KeyToCIDString(key ds.Key) string {
	key = ds.NewKey(key.Name())
	m, err := dshelp.DsKeyToMultihash(key)
	if err != nil {
		// Return empty string for invalid keys
		return ""
	}
	keyStr := cid.NewCidV0(m)

	return keyStr.String()
}

// KeyToCIDBinary converts a datastore key to binary CID string for space-efficient storage
func KeyToCIDBinary(key ds.Key) string {
	cidStr := KeyToCIDString(key)
	if cidObj, err := cid.Decode(cidStr); err == nil {
		return string(cidObj.Bytes()) // Use binary representation for space efficiency
	}
	return key.String() // Fallback to string key if not a valid CID
}

// createFileProcessorForTUS creates a file processor for TUS uploads (archives treated as single files)
func createFileProcessorForTUS(uploadFile io.ReadCloser, proto ProtoNode, logger *core.Logger) (BlockProcessor, error) {
	doneTracker := NewDoneTracker()
	bstore := proto.GetNode().GetBlockstore()
	dagService := proto.GetNode().DagService()

	// Create streaming blockstore with defaults
	bs := NewStreamingBlockstoreWithDefaults(logger, bstore, doneTracker, DEFAULT_BLOCK_QUEUE_SIZE)

	// Create UnixFS node generator
	nodeGenerator := contentUnixFS.NewUnixFSNodeGenerator(
		contentUnixFS.WithUnixFSNodeDAGService(dagService),
		contentUnixFS.WithUnixFSNodeBlockstore(bs),
	)

	// Create file block processor (not archive processor)
	return NewFileBlockProcessorWithDefaults(proto.Context(), bs, pluginUpload.NewUniversalReader(uploadFile), dagService, nodeGenerator, logger, doneTracker)
}
