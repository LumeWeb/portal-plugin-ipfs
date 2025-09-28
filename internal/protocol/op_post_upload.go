package protocol

import (
	"context"
	"fmt"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
	"io"
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
	cids, err := ProcessCar(h.Context(), upload)
	if err != nil {
		return fmt.Errorf("failed to process CIDs from upload: %w", err)
	}

	err = core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE).ProcessCIDs(ctx, cids, lo.FromPtrOr(req.UserID, 0))
	if err != nil {
		return fmt.Errorf("failed to process upload: %w", err)
	}

	h.Logger().Debug("Processed CAR file", zap.Int("num_cids", len(cids)))

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
