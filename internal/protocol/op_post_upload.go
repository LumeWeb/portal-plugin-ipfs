package protocol

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/mholt/archives"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
)

const DEFAULT_BLOCK_QUEUE_SIZE = 10

// PostUploadOperationHandler handles post-upload processing
type PostUploadOperationHandler struct {
	core.OperationHelper
}

func (h *PostUploadOperationHandler) ValidateRequest(ctx context.Context, req *models.Request) error {
	if req.Hash == nil {
		return fmt.Errorf("upload hash is required")
	}
	return nil
}

func (h *PostUploadOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	workflow, err := h.getWorkflowData(req.ID)
	if err != nil {
		return err
	}

	uploadFile, err := h.getUpload(ctx, workflow.UploadID)
	if err != nil {
		return err
	}
	defer func(upload io.ReadCloser) {
		err := upload.Close()
		if err != nil {
			h.Logger().Error("failed to close upload", zap.Error(err))
		}
	}(uploadFile)

	uploadedFormat, err := upload.DetectFormat(uploadFile)
	if err != nil {
		return err
	}

	processor, err := h.createProcessor(uploadFile, uploadedFormat)
	if err != nil {
		return err
	}

	allCids, rootCids, err := ProcessBlocks(h.Context(), processor)
	if err != nil {
		return fmt.Errorf("failed to process CIDs from upload: %w", err)
	}

	userID := lo.FromPtrOr(req.UserID, 0)
	err = h.processCIDs(ctx, allCids, userID, req.SourceIP)
	if err != nil {
		return err
	}

	h.processMetadata(allCids)

	ipfsPin, err := h.createRootPin(ctx, rootCids[0], userID)
	if err != nil {
		return err
	}

	err = h.updateWorkflow(ctx, req.ID, rootCids, ipfsPin)
	if err != nil {
		return err
	}

	h.logProcessingResult(allCids, rootCids)
	return nil
}

// getWorkflowData retrieves workflow data for the request
func (h *PostUploadOperationHandler) getWorkflowData(requestID uint) (*PostUploadWorkflowData, error) {
	var workflow PostUploadWorkflowData
	err := h.StructuredWorkflowData(requestID, &workflow)
	if err != nil {
		return nil, err
	}
	return &workflow, nil
}

// getUpload retrieves the upload file from storage
func (h *PostUploadOperationHandler) getUpload(ctx context.Context, uploadID string) (io.ReadCloser, error) {
	storageSvc := core.GetService[core.StorageService](h.Context(), core.STORAGE_SERVICE)
	uploadFile, err := storageSvc.S3GetTemporaryUpload(ctx, h.Protocol().(core.StorageProtocol), uploadID)
	if err != nil {
		return nil, fmt.Errorf("failed to get upload: %w", err)
	}
	return uploadFile, nil
}

// createProcessor creates the appropriate block processor based on format
func (h *PostUploadOperationHandler) createProcessor(uploadFile io.ReadCloser, format upload.Format) (BlockProcessor, error) {
	if format.IsUploadFormat() {
		return NewCARBlockProcessor(uploadFile)
	}

	logger := h.Logger()
	doneTracker := NewDoneTracker()
	bstore := h.Protocol().(ProtoNode).GetNode().GetBlockstore()
	dagService := h.Protocol().(ProtoNode).GetNode().DagService()
	bs := NewStreamingBlockstoreWithDefaults(logger, bstore, doneTracker, DEFAULT_BLOCK_QUEUE_SIZE)

	if format.IsArchiveFormat() {
		return h.createArchiveProcessor(uploadFile, format, bs, dagService, logger, doneTracker)
	}

	return h.createFileProcessor(uploadFile, bs, dagService, logger, doneTracker)
}

// createArchiveProcessor creates a processor for archive formats
func (h *PostUploadOperationHandler) createArchiveProcessor(uploadFile io.ReadCloser, format upload.Format, sbs StreamingBlockstore, dagService format.DAGService, logger *core.Logger, doneTracker DoneTracker) (BlockProcessor, error) {
	seekableUpload, ok := uploadFile.(archives.ReaderAtSeeker)
	if !ok {
		return nil, fmt.Errorf("archive upload must be seekable for format detection and processing")
	}

	_, err := seekableUpload.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to start of archive: %w", err)
	}

	extractor, err := upload.NewArchiveExtractor(seekableUpload, format)
	if err != nil {
		return nil, fmt.Errorf("failed to create archive extractor: %w", err)
	}

	nodeGenerator := upload.NewUnixFSNodeGeneratorWithOptions(
		upload.WithUnixFSNodeGeneratorDAGService(dagService),
		upload.WithUnixFSNodeGeneratorBlockstore(sbs),
		upload.WithUnixFSNodeGeneratorLogger(logger),
	)

	streamProcessor := upload.NewStreamingProcessor(
		nodeGenerator,
		dagService,
		sbs,
		logger,
	)

	return NewArchiveBlockProcessor(h.Context(), sbs, extractor, streamProcessor, logger, doneTracker)
}

// createFileProcessor creates a processor for single file formats
func (h *PostUploadOperationHandler) createFileProcessor(uploadFile io.ReadCloser, sbs StreamingBlockstore, dagService format.DAGService, logger *core.Logger, doneTracker DoneTracker) (BlockProcessor, error) {
	nodeGenerator := upload.NewUnixFSNodeGeneratorWithOptions(
		upload.WithUnixFSNodeGeneratorDAGService(dagService),
		upload.WithUnixFSNodeGeneratorBlockstore(sbs),
		upload.WithUnixFSNodeGeneratorLogger(logger),
	)

	return NewFileBlockProcessorWithDefaults(h.Context(), sbs, upload.NewUniversalReader(uploadFile), dagService, nodeGenerator, logger, doneTracker)
}

// processCIDs processes all CIDs and creates upload records
func (h *PostUploadOperationHandler) processCIDs(ctx context.Context, allCids []cid.Cid, userID uint, sourceIP string) error {
	uploadSvc := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)
	if uploadSvc == nil {
		h.Logger().Error("Upload service not available")
		return fmt.Errorf("upload service not available")
	}

	ctx = store.ClientIPOption(ctx, sourceIP)
	err := uploadSvc.ProcessUpload(ctx, allCids, userID)
	if err != nil {
		return fmt.Errorf("failed to process upload: %w", err)
	}

	return nil
}

// processMetadata processes missing UnixFS metadata
func (h *PostUploadOperationHandler) processMetadata(allCids []cid.Cid) {
	metadataStore := h.Protocol().(*Protocol).GetMetadataStore()
	if metadataStore != nil {
		err := metadataStore.ProcessMissingUnixFSNames(allCids)
		if err != nil {
			h.Logger().Warn("Failed to process missing UnixFS names", zap.Error(err))
		}
	}
}

// createRootPin creates a root pin for the upload
func (h *PostUploadOperationHandler) createRootPin(ctx context.Context, rootCid cid.Cid, userID uint) (*db.IPFSPin, error) {
	uploadSvc := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)
	ipfsPin, err := uploadSvc.CreateRootPin(ctx, rootCid, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to create root pin: %w", err)
	}
	return ipfsPin, nil
}

// updateWorkflow updates workflow data with pin information
func (h *PostUploadOperationHandler) updateWorkflow(ctx context.Context, requestID uint, rootCids []cid.Cid, ipfsPin *db.IPFSPin) error {
	workflowData := &PinWorkflowData{
		PinRequestID: ipfsPin.RequestID.ToUUID(),
		Cids:         lo.Map(rootCids, func(item cid.Cid, _ int) string { return item.String() }),
	}

	err := h.UpdateWorkflowDataStruct(requestID, workflowData)
	if err != nil {
		return fmt.Errorf("failed to update workflow data: %w", err)
	}

	err = ValidateDAGCompletionAndUpdateWorkflow(ctx, h, requestID, ipfsPin, workflowData)
	if err != nil {
		h.Logger().Error("Failed to validate DAG completion and update workflow", zap.Error(err))
	}

	return nil
}

// logProcessingResult logs the final processing results
func (h *PostUploadOperationHandler) logProcessingResult(allCids, rootCids []cid.Cid) {
	h.Logger().Debug("Processed CAR file", zap.Int("num_cids", len(allCids)), zap.Int("num_roots", len(rootCids)))
}

func (h *PostUploadOperationHandler) GetStatus(ctx context.Context, req *models.Request) (*core.RequestStatus, error) {
	return &core.RequestStatus{
		ProgressPercent: 100,
		Message:         "Upload processed successfully",
	}, nil
}

func (h *PostUploadOperationHandler) Cleanup(ctx context.Context, req *models.Request) error {
	// Delete temporary upload
	storageSvc := core.GetService[core.StorageService](h.Context(), core.STORAGE_SERVICE)
	err := storageSvc.S3DeleteTemporaryUpload(ctx, h.Protocol().(core.StorageProtocol), req.Hash.String())
	if err != nil {
		return fmt.Errorf("failed to delete temporary upload: %w", err)
	}
	return nil
}

func NewPostUploadOperation(ctx core.Context) core.Operation {
	return core.NewPostUploadOperation(internal.ProtocolName,
		&PostUploadOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		},
	)
}
