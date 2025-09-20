package protocol

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
)

// PinChildBlockOperationHandler handles pinning child blocks of a CID
type PinChildBlockOperationHandler struct {
	core.OperationHelper
}

func (h *PinChildBlockOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	if len(req.Hash) == 0 {
		return fmt.Errorf("hash is required")
	}
	return nil
}

func (h *PinChildBlockOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	var workflowData PinChildBlockWorkflowData
	err := h.StructuredWorkflowData(req.ID, &workflowData)
	if err != nil {
		return fmt.Errorf("failed to get workflow data: %w", err)
	}

	proto := h.Protocol().(*Protocol)

	c, err := cid.Parse(workflowData.Cid)
	if err != nil {
		return fmt.Errorf("failed to parse CID %s: %w", workflowData.Cid, err)
	}

	// Pin each child block
	_, err = proto.GetNode().GetBlock(ctx, c)
	if err != nil {
		return fmt.Errorf("failed to pin child block %s: %w", c, err)
	}

	// Create core pin record for the child block
	err = core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE).ProcessCIDs(ctx, []cid.Cid{c}, lo.FromPtrOr(req.UserID, 0))
	if err != nil {
		return fmt.Errorf("failed to process child block pin %s: %w", c, err)
	}

	return nil
}

func (h *PinChildBlockOperationHandler) GetStatus(_ context.Context, _ *models.Request) (*core.RequestStatus, error) {
	return &core.RequestStatus{
		State:           "completed",
		ProgressPercent: 100,
		Message:         "Child block pinned successfully",
	}, nil
}

func (h *PinChildBlockOperationHandler) Cleanup(_ context.Context, _ *models.Request) error {
	return nil
}

func NewPinChildBlocksOperation(ctx core.Context) core.Operation {
	return core.NewOperation(
		fmt.Sprintf("%s.pin.children", internal.ProtocolName),
		"", // No global type for pin children
		&PinChildBlockOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		},
	)
}
