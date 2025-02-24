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

	cidList := make([]cid.Cid, 0)

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

	if len(cidList) > 0 {
		err = core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE).ProcessCIDs(ctx, cidList, lo.FromPtrOr(req.UserID, 0))
		if err != nil {
			return fmt.Errorf("failed to process upload: %w", err)
		}
	}

	err = pinSvc.UpdatePinStatus(ctx, types.FromUUID(workflowData.PinRequestID), db.PinningStatusPinned, nil)
	if err != nil {
		return fmt.Errorf("failed to update pin status: %w", err)
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
		fmt.Sprintf("%s.confirm", internal.ProtocolName),
		"", // No global type for confirm
		&ConfirmOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		},
	)
}
