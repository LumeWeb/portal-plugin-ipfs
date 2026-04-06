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
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
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
	ctx, span := core.TraceMethod(ctx, "PostUploadOperationHandler.ValidateRequest")
	defer span.End()

	if req.Hash == nil {
		return fmt.Errorf("upload hash is required")
	}
	return nil
}

func (h *PostUploadOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	ctx, span := core.TraceMethod(ctx, "PostUploadOperationHandler.Execute")
	defer span.End()

	// Initialize progress tracker with manual mode for simple milestones
	tracker, err := InitializeManualProgressTracker(h, req.ID, core.OpTypeUpload, 10)
	if err != nil {
		return err
	}

	workflow, err := h.getWorkflowData(req.ID)
	if err != nil {
		return err
	}

	// Set progress - loading upload
	if err := tracker.SetProgress(20); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	// Get upload file to read size for quota sanity check
	sizeCheckFile, err := h.getUpload(ctx, workflow.UploadID)
	if err != nil {
		return err
	}
	defer sizeCheckFile.Close()

	// Determine file size efficiently
	var uploadFileSize int64
	if seeker, ok := sizeCheckFile.(io.Seeker); ok {
		uploadFileSize, err = seeker.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("failed to seek to end of upload file: %w", err)
		}
		_, err = seeker.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("failed to seek to start of upload file: %w", err)
		}
	} else {
		// Fallback to reading all data only if seek not supported
		fileData, err := io.ReadAll(sizeCheckFile)
		if err != nil {
			return fmt.Errorf("failed to read upload file for size calculation: %w", err)
		}
		uploadFileSize = int64(len(fileData))
	}

	// Get fresh upload file handle for actual processing
	uploadFile, err := h.getUpload(ctx, workflow.UploadID)
	if err != nil {
		return fmt.Errorf("failed to get fresh upload file handle: %w", err)
	}
	defer func(upload io.ReadCloser) {
		err := upload.Close()
		if err != nil {
			h.Logger().Error("failed to close upload", zap.Error(err))
		}
	}(uploadFile)

	// Sanity check quota (no reservation) before processing
	userID := lo.FromPtrOr(req.UserID, 0)
	if userID == 0 {
		return fmt.Errorf("user ID is required")
	}

	if uploadFileSize > 0 {
		requestedBytes := uint64(uploadFileSize)

		// Validate upload quota without reservation (sanity check only)
		err = quota.ValidateUploadQuota(ctx, h.Context(), userID, requestedBytes)
		if err != nil {
			return err
		}

		// Validate storage quota without reservation (sanity check only)
		err = quota.ValidateStorageQuota(ctx, h.Context(), userID, requestedBytes)
		if err != nil {
			return err
		}
	}

	uploadedFormat, err := upload.DetectFormat(uploadFile)
	if err != nil {
		return err
	}

	// Set progress - creating processor
	if err := tracker.SetProgress(30); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	processor, err := h.createProcessor(uploadFile, uploadedFormat)
	if err != nil {
		return err
	}
	defer processor.Release()

	// Set progress - processing blocks
	if err := tracker.SetProgress(40); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	allCids, rootCids, err := ProcessBlocks(h.Context(), processor)
	if err != nil {
		return fmt.Errorf("failed to process CIDs from upload: %w", err)
	}

	// Check if any root CIDs were returned
	if len(rootCids) == 0 {
		return fmt.Errorf("no root CIDs found during block processing")
	}

	// Create per-block reservations for each block
	proto := h.Protocol().(ProtoNode)
	reservations, err := CreatePerBlockReservations(ctx, h.Context(), proto, allCids, userID)
	if err != nil {
		return err
	}

	err = h.processCIDs(ctx, allCids, userID, req.SourceIP, reservations)
	if err != nil {
		// Release all per-block reservations on error
		quota.ReleaseBlockReservationsMap(reservations)
		return err
	}

	// Set progress - processing metadata
	if err := tracker.SetProgress(60); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	h.processMetadata(ctx, allCids)

	// Set progress - creating root pin
	if err := tracker.SetProgress(70); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	ipfsPin, err := h.createRootPin(ctx, rootCids[0], userID)
	if err != nil {
		// Release all per-block reservations on error
		quota.ReleaseBlockReservationsMap(reservations)
		return err
	}

	// Set progress - updating pin status
	if err := tracker.SetProgress(80); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	// Update pin status to pinned
	pinSvc := core.GetService[pluginCore.IPFSPinService](h.Context(), pluginCore.PIN_SERVICE)
	if pinSvc != nil {
		err = pinSvc.UpdatePinStatus(ctx, ipfsPin.RequestID, db.PinningStatusPinned, nil)
		if err != nil {
			h.Logger().Error("Failed to update pin status to pinned", zap.Error(err))
			// Don't fail the whole operation for this
		}
	} else {
		h.Logger().Warn("Pin service not available for status update")
	}

	err = h.updateWorkflow(ctx, req.ID, rootCids, ipfsPin)
	if err != nil {
		// Release all per-block reservations on error
		quota.ReleaseBlockReservationsMap(reservations)
		return err
	}

	h.logProcessingResult(allCids, rootCids)

	// Complete
	if err := tracker.SetProgress(100); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

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
	ctx, span := core.TraceMethod(ctx, "PostUploadOperationHandler.getUpload")
	defer span.End()

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
		processor, err := h.createArchiveProcessor(uploadFile, format, bs, dagService, logger, doneTracker)
		if err != nil {
			// Clean up StreamingBlockstore on error
			if closeErr := bs.Close(); closeErr != nil && logger != nil {
				logger.Error("Failed to cleanup StreamingBlockstore after archive processor creation error", zap.Error(closeErr))
			}
			return nil, err
		}
		return processor, nil
	}

	processor, err := h.createFileProcessor(uploadFile, bs, dagService, logger, doneTracker)
	if err != nil {
		// Clean up StreamingBlockstore on error
		if closeErr := bs.Close(); closeErr != nil && logger != nil {
			logger.Error("Failed to cleanup StreamingBlockstore after file processor creation error", zap.Error(closeErr))
		}
		return nil, err
	}
	return processor, nil
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
func (h *PostUploadOperationHandler) processCIDs(ctx context.Context, allCids []cid.Cid, userID uint, sourceIP string, reservations map[cid.Cid]*quota.BlockReservations) error {
	ctx, span := core.TraceMethod(ctx, "PostUploadOperationHandler.processCIDs")
	defer span.End()

	uploadSvc := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)
	if uploadSvc == nil {
		h.Logger().Error("Upload service not available")
		return fmt.Errorf("upload service not available")
	}

	ctx = pc.ClientIPOption(ctx, sourceIP)
	err := uploadSvc.ProcessUpload(ctx, allCids, userID, reservations)
	if err != nil {
		return fmt.Errorf("failed to process upload: %w", err)
	}

	return nil
}

// processMetadata processes missing UnixFS metadata
func (h *PostUploadOperationHandler) processMetadata(ctx context.Context, allCids []cid.Cid) {
	metadataStore := h.Protocol().(*Protocol).GetMetadataStore()
	if metadataStore != nil {
		err := metadataStore.ProcessMissingUnixFSNames(ctx, allCids)
		if err != nil {
			h.Logger().Warn("Failed to process missing UnixFS names", zap.Error(err))
		}
	}
}

// createRootPin creates a root pin for the upload
func (h *PostUploadOperationHandler) createRootPin(ctx context.Context, rootCid cid.Cid, userID uint) (*db.IPFSPin, error) {
	ctx, span := core.TraceMethod(ctx, "PostUploadOperationHandler.createRootPin")
	defer span.End()

	uploadSvc := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)
	ipfsPin, err := uploadSvc.CreateRootPin(ctx, rootCid, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to create root pin: %w", err)
	}
	return ipfsPin, nil
}

// updateWorkflow updates workflow data with pin information
func (h *PostUploadOperationHandler) updateWorkflow(ctx context.Context, requestID uint, rootCids []cid.Cid, ipfsPin *db.IPFSPin) error {
	ctx, span := core.TraceMethod(ctx, "PostUploadOperationHandler.updateWorkflow")
	defer span.End()

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
	h.Logger().Debug("Processed upload file", zap.Int("num_cids", len(allCids)), zap.Int("num_roots", len(rootCids)))
}

func (h *PostUploadOperationHandler) GetStatus(ctx context.Context, req *models.Request) (*core.RequestStatus, error) {
	ctx, span := core.TraceMethod(ctx, "PostUploadOperationHandler.GetStatus")
	defer span.End()

	return h.GetStatusFromWorkflowData(req.ID, req)
}

func (h *PostUploadOperationHandler) Cleanup(ctx context.Context, req *models.Request) error {
	ctx, span := core.TraceMethod(ctx, "PostUploadOperationHandler.Cleanup")
	defer span.End()

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
