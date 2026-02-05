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
	ctx, span := core.TraceMethod(ctx, "PublishOperationHandler.ValidateRequest")
	defer span.End()

	if len(req.Hash) == 0 {
		return fmt.Errorf("hash is required")
	}
	return nil
}

func (h *PublishOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	// Initialize progress tracker with manual mode for simple milestones
	tracker, err := h.NewProgressTracker(req.ID, core.ProgressModeManual, func(cfg *core.ProgressTrackerConfig) {
		cfg.MessageProvider = h.NewDefaultProgressMessageProvider(core.OpTypePublish)
	})
	if err != nil {
		return fmt.Errorf("failed to initialize progress tracker: %w", err)
	}

	if err := tracker.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize tracker: %w", err)
	}

	// Set initial progress
	if err := tracker.SetProgress(10); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	// Trigger the reprovider to announce the content
	h.Protocol().(*Protocol).GetNode().TriggerReprovider()

	// Complete
	if err := tracker.SetProgress(100); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	return nil
}

func (h *PublishOperationHandler) GetStatus(_ context.Context, req *models.Request) (*core.RequestStatus, error) {
	return h.GetStatusFromWorkflowData(req.ID, req)
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
