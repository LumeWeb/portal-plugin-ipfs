package protocol

import (
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/types"
	"go.uber.org/zap"
)

// ConfirmOperationHandler verifies all CIDs are ready before completing
type ConfirmOperationHandler struct {
	core.OperationHelper
}

func (h *ConfirmOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	if len(req.Hash) == 0 {
		return fmt.Errorf("hash is required")
	}
	return nil
}

func (h *ConfirmOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	var workflowData PinWorkflowData
	err := h.StructuredWorkflowData(req.ID, &workflowData)
	if err != nil {
		return fmt.Errorf("failed to get workflow data: %w", err)
	}

	pinSvc := core.GetService[pluginCore.IPFSPinService](h.Context(), pluginCore.PIN_SERVICE)

	proto := h.Protocol().(*Protocol)
	store := proto.GetMetadataStore()

	var cidList []cid.Cid

	for _, cidStr := range workflowData.Cids {
		c, err := cid.Parse(cidStr)
		if err != nil {
			return fmt.Errorf("failed to parse CID %s: %w", cidStr, err)
		}

		// Check if block exists and is ready
		err = store.BlockExists(c)
		if err != nil {
			return fmt.Errorf("block %s not ready: %w", c, err)
		}

		cidList = append(cidList, c)
	}

	// Process all CIDs to create upload/core pin records for all CIDs
	if len(cidList) > 0 {
		uploadSvc := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)
		if uploadSvc == nil {
			h.Logger().Error("Upload service not available")
			return fmt.Errorf("upload service not available")
		}
		
		err := uploadSvc.ProcessUpload(ctx, cidList, lo.FromPtrOr(req.UserID, 0))
		if err != nil {
			return fmt.Errorf("failed to process upload: %w", err)
		}

		// Fix any UnixFS metadata gaps before proceeding
		err = store.ProcessMissingUnixFSNames(cidList)
		if err != nil {
			h.Logger().Warn("Failed to process missing UnixFS names", zap.Error(err))
		}

		// Create IPFS pin record for the root CID (first CID in the list)
		_, err = uploadSvc.CreateRootPin(ctx, cidList[0], lo.FromPtrOr(req.UserID, 0))
		if err != nil {
			return fmt.Errorf("failed to create root pin: %w", err)
		}
	}

	err = pinSvc.UpdatePinStatus(ctx, types.FromUUID(workflowData.PinRequestID), db.PinningStatusPinned, nil)
	if err != nil {
		return fmt.Errorf("failed to update pin status: %w", err)
	}

	// Get the pin record for DAG validation
	pin, err := pinSvc.GetPinByRequestID(ctx, types.FromUUID(workflowData.PinRequestID))
	if err != nil {
		h.Logger().Error("Failed to get pin record for DAG validation", zap.Error(err))
		// Don't fail the whole operation for this
		return nil
	}

	// Validate DAG completion and update workflow data with related CIDs
	err = ValidateDAGCompletionAndUpdateWorkflow(ctx, h, req.ID, pin, &workflowData)
	if err != nil {
		h.Logger().Error("Failed to validate DAG completion and update workflow", zap.Error(err))
		// Don't fail the whole operation for DAG validation failure
	}

	return nil
}

func (h *ConfirmOperationHandler) GetStatus(_ context.Context, _ *models.Request) (*core.RequestStatus, error) {
	return &core.RequestStatus{
		ProgressPercent: 100,
		Message:         "All blocks confirmed ready",
	}, nil
}

func (h *ConfirmOperationHandler) Cleanup(_ context.Context, _ *models.Request) error {
	return nil
}

func NewConfirmOperation(ctx core.Context) core.Operation {
	return core.NewOperation(
		confirmOperationName(),
		"", // No global type for confirm
		&ConfirmOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		},
	)
}
