package protocol

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
)

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
	var workflow PostUploadWorkflowData
	err := h.StructuredWorkflowData(req.ID, &workflow)
	if err != nil {
		return err
	}

	storageSvc := core.GetService[core.StorageService](h.Context(), core.STORAGE_SERVICE)
	// Get the upload from storage service
	upload, err := storageSvc.S3GetTemporaryUpload(ctx, h.Protocol().(core.StorageProtocol), workflow.UploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload: %w", err)
	}
	defer func(upload io.ReadCloser) {
		err = upload.Close()
		if err != nil {
			h.Logger().Error("failed to close upload", zap.Error(err))
		}
	}(upload)

	// Process the upload
	allCids, rootCids, err := ProcessCar(h.Context(), upload)
	if err != nil {
		return fmt.Errorf("failed to process CIDs from upload: %w", err)
	}

	userID := lo.FromPtrOr(req.UserID, 0)
	uploadSvc := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)

	if uploadSvc == nil {
		h.Logger().Error("Upload service not available")
		return fmt.Errorf("upload service not available")
	}

	// Set client IP in context for quota tracking
	ctx = store.ClientIPOption(ctx, req.SourceIP)

	// Process all CIDs to create upload and core pin records
	err = uploadSvc.ProcessUpload(ctx, allCids, userID)
	if err != nil {
		return fmt.Errorf("failed to process upload: %w", err)
	}

	// Fix any UnixFS metadata gaps before proceeding
	metadataStore := h.Protocol().(*Protocol).GetMetadataStore()
	if metadataStore != nil {
		err = metadataStore.ProcessMissingUnixFSNames(allCids)
		if err != nil {
			h.Logger().Warn("Failed to process missing UnixFS names", zap.Error(err))
		}
	}

	ipfsPin, err := uploadSvc.CreateRootPin(ctx, rootCids[0], userID)
	if err != nil {
		return fmt.Errorf("failed to create root pin: %w", err)
	}

	// Prepare workflow data using the request ID from the created IPFS pin
	workflowData := &PinWorkflowData{
		PinRequestID: ipfsPin.RequestID.ToUUID(),
		Cids:         lo.Map(rootCids, func(item cid.Cid, _ int) string { return item.String() }),
	}

	err = h.UpdateWorkflowDataStruct(req.ID, workflowData)
	if err != nil {
		return fmt.Errorf("failed to update workflow data: %w", err)
	}

	// Validate DAG completion and update workflow data with related CIDs
	err = ValidateDAGCompletionAndUpdateWorkflow(ctx, h, req.ID, ipfsPin, workflowData)
	if err != nil {
		h.Logger().Error("Failed to validate DAG completion and update workflow", zap.Error(err))
		// Don't fail the whole operation for DAG validation failure
	}

	h.Logger().Debug("Processed CAR file", zap.Int("num_cids", len(allCids)), zap.Int("num_roots", len(rootCids)))

	return nil
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
