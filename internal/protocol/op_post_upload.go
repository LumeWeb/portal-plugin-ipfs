package protocol

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/mholt/archives"
	"github.com/samber/lo"
	contentArchive "go.lumeweb.com/ipfs-content/archive"
	contentUnixFS "go.lumeweb.com/ipfs-content/unixfs"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pluginErrors "go.lumeweb.com/portal-plugin-ipfs/internal/errors"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	pluginUpload "go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload/common"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
)

const DEFAULT_BLOCK_QUEUE_SIZE = 128

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

	h.Logger().Debug("upload operation started",
		zap.Uint("requestID", req.ID),
		zap.Uint("userID", lo.FromPtrOr(req.UserID, 0)))

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

	// Get upload file for processing and quota validation
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

	// Sanity check quota (no reservation) before processing
	userID := lo.FromPtrOr(req.UserID, 0)
	if userID == 0 {
		return fmt.Errorf("user ID is required")
	}

	// Get actual DAG block size from CAR file using ipfs-content ReadCAR API
	// This gives us the raw block data size, which is accurate for all upload types:
	// - CAR files: actual DAG block size (not CAR file size with headers)
	// - ArchivePreserve: size of archive wrapped in CAR
	// - ArchiveConvert: size of extracted files in CAR
	// - Single files: size of file wrapped in CAR
	seekableUpload, ok := uploadFile.(io.ReadSeeker)
	if !ok {
		return fmt.Errorf("upload file must be seekable for CAR processing")
	}

	dagSize, err := common.GetCARBlockDAGSizeWithDefaultLimit(ctx, seekableUpload, h.Logger())
	if err != nil {
		return fmt.Errorf("failed to get CAR DAG block size for quota validation: %w", err)
	}

	// Reset reader position to start for subsequent processing
	_, err = seekableUpload.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to reset CAR reader position: %w", err)
	}

	// Validate upload quota using actual DAG block size
	if err := quota.ValidateUploadQuota(ctx, h.Context(), userID, dagSize); err != nil {
		return err
	}

	// Validate storage quota using actual DAG block size
	if err := quota.ValidateStorageQuota(ctx, h.Context(), userID, dagSize); err != nil {
		return err
	}

	uploadedFormat, err := contentArchive.DetectFormat(uploadFile)
	if err != nil {
		if pluginUpload.IsUploadErrorType(err, pluginErrors.UploadErrUnsupportedFormat) {
			return pluginUpload.NewUnsupportedFormatError(err)
		}
		return pluginUpload.NewCorruptedFileError(err)
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

	allCids, rootCids, err := ProcessBlocks(h.Context(), ctx, processor, h.Protocol().(ProtoNode).GetBlockstoreFlusher())
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

	// Set progress - creating root pin
	if err := tracker.SetProgress(70); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	ipfsPin, err := h.createRootPin(ctx, rootCids[0], userID, workflow.Name)
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
			quota.ReleaseBlockReservationsMap(reservations)
			return fmt.Errorf("failed to update pin status to pinned: %w", err)
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

	if err := h.startFilePathWorkflow(ctx, req, allCids, ipfsPin); err != nil {
		h.Logger().Warn("Failed to start filepath workflow", zap.Error(err))
	}

	h.logProcessingResult(allCids, rootCids)

	// Check if context was cancelled during processing — this would cause
	// the cron job to fail before CompleteWorkflowStep can run, leaving
	// the operation stuck in "Processing" with progress=100.
	if ctxErr := ctx.Err(); ctxErr != nil {
		h.Logger().Warn("upload operation completed with cancelled context",
			zap.Uint("requestID", req.ID),
			zap.Int("numCids", len(allCids)),
			zap.Int("numRoots", len(rootCids)),
			zap.Error(ctxErr))
	} else {
		h.Logger().Debug("upload operation completed successfully",
			zap.Uint("requestID", req.ID),
			zap.Int("numCids", len(allCids)),
			zap.Int("numRoots", len(rootCids)))
	}

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
func (h *PostUploadOperationHandler) createProcessor(uploadFile io.ReadCloser, format contentArchive.Format) (BlockProcessor, error) {
	if format.IsUploadFormat() {
		return NewCARBlockProcessor(uploadFile)
	}

	logger := h.Logger()
	doneTracker := NewDoneTracker()
	bstore := h.Protocol().(ProtoNode).GetNode().GetBlockstore()
	bs := NewStreamingBlockstoreWithDefaults(logger, bstore, doneTracker, DEFAULT_BLOCK_QUEUE_SIZE)
	dagService := merkledag.NewDAGService(
		blockservice.New(bs, offline.Exchange(bs)),
	)

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
func (h *PostUploadOperationHandler) createArchiveProcessor(uploadFile io.ReadCloser, format contentArchive.Format, sbs StreamingBlockstore, dagService format.DAGService, logger *core.Logger, doneTracker DoneTracker) (BlockProcessor, error) {
	seekableUpload, ok := uploadFile.(archives.ReaderAtSeeker)
	if !ok {
		return nil, fmt.Errorf("archive upload must be seekable for format detection and processing")
	}

	_, err := seekableUpload.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to start of archive: %w", err)
	}

	extractor, err := contentArchive.NewArchiveExtractor(seekableUpload, format)
	if err != nil {
		return nil, fmt.Errorf("failed to create archive extractor: %w", err)
	}

	nodeGenerator := contentUnixFS.NewUnixFSNodeGenerator(
		contentUnixFS.WithUnixFSNodeDAGService(dagService),
		contentUnixFS.WithUnixFSNodeBlockstore(sbs),
	)

	streamProcessor := pluginUpload.NewStreamingProcessor(
		nodeGenerator,
		dagService,
		sbs,
		logger,
	)

	return NewArchiveBlockProcessor(h.Context(), sbs, extractor, streamProcessor, logger, doneTracker)
}

// createFileProcessor creates a processor for single file formats
func (h *PostUploadOperationHandler) createFileProcessor(uploadFile io.ReadCloser, sbs StreamingBlockstore, dagService format.DAGService, logger *core.Logger, doneTracker DoneTracker) (BlockProcessor, error) {
	nodeGenerator := contentUnixFS.NewUnixFSNodeGenerator(
		contentUnixFS.WithUnixFSNodeDAGService(dagService),
		contentUnixFS.WithUnixFSNodeBlockstore(sbs),
	)

	return NewFileBlockProcessorWithDefaults(h.Context(), sbs, pluginUpload.NewUniversalReader(uploadFile), dagService, nodeGenerator, logger, doneTracker)
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

// createRootPin creates a root pin for the upload
func (h *PostUploadOperationHandler) createRootPin(ctx context.Context, rootCid cid.Cid, userID uint, name string) (*db.IPFSPin, error) {
	ctx, span := core.TraceMethod(ctx, "PostUploadOperationHandler.createRootPin")
	defer span.End()

	uploadSvc := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)
	ipfsPin, err := uploadSvc.CreateRootPin(ctx, rootCid, userID, name)
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

// startFilePathWorkflow starts the dedicated FILE_PATH_WORKFLOW with the
// uploaded CIDs and related CIDs as workflow data. The workflow runs
// asynchronously as a separate request.
func (h *PostUploadOperationHandler) startFilePathWorkflow(
	ctx context.Context,
	req *models.Request,
	allCids []cid.Cid,
	ipfsPin *db.IPFSPin,
) error {
	userID := lo.FromPtrOr(req.UserID, 0)
	cids := lo.Map(allCids, func(c cid.Cid, _ int) string { return c.String() })

	if len(allCids) == 0 {
		return fmt.Errorf("no CIDs to process in filepath workflow")
	}

	var relatedCIDs []string
	if ipfsPin != nil {
		pinSvc := core.GetService[pluginCore.IPFSPinService](h.Context(), pluginCore.PIN_SERVICE)
		if pinSvc != nil {
			related, err := pinSvc.ValidateDAGCompletion(ctx, ipfsPin)
			if err != nil {
				h.Logger().Error("Failed to validate DAG completion for filepath workflow", zap.Error(err))
			} else {
				relatedCIDs = lo.FilterMap(related, func(b []byte, _ int) (string, bool) {
					c, err := cid.Cast(b)
					if err != nil {
						h.Logger().Error("Failed to cast related CID", zap.Binary("cid", b), zap.Error(err))
						return "", false
					}
					return c.String(), true
				})
			}
		}
	}

	_, err := h.StartWorkflow(
		FILE_PATH_WORKFLOW,
		filePathWorkflowOptions(req, userID, FilePathWorkflowInputData{
			CIDs:        cids,
			RelatedCIDs: relatedCIDs,
			UserID:      userID,
		})...,
	)
	return err
}
