package protocol

import (
	"context"
	"fmt"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
)

// PublishOperationHandler handles announcing content to the IPFS network
type PublishOperationHandler struct {
	core.OperationHelper
}

func (h *PublishOperationHandler) ValidateRequest(ctx context.Context, req *models.Request) error {
	if len(req.Hash) == 0 {
		return fmt.Errorf("hash is required")
	}
	return nil
}

func (h *PublishOperationHandler) Execute(_ context.Context, _ *models.Request) error {
	// Trigger the reprovider to announce the content
	h.Protocol().(*Protocol).GetNode().TriggerReprovider()
	return nil
}

func (h *PublishOperationHandler) GetStatus(_ context.Context, req *models.Request) (*core.RequestStatus, error) {
	status := &core.RequestStatus{}

	if req.Status == models.RequestStatusCompleted {
		status.Message = "Content published to network"
		status.ProgressPercent = 100
	}

	return status, nil
}

func (h *PublishOperationHandler) Cleanup(_ context.Context, _ *models.Request) error {
	// No cleanup needed for publish operation
	return nil
}

func NewPublishOperation(ctx core.Context) core.Operation {
	return core.NewPublishOperation(internal.ProtocolName, &PublishOperationHandler{
		OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
	})
}
