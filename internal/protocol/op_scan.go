package protocol

import (
	"context"
	"fmt"

	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
)

// ScanOperationHandler handles content validation/scanning
type ScanOperationHandler struct {
	core.OperationHelper
}

func (h *ScanOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	if len(req.Hash) == 0 {
		return fmt.Errorf("hash is required")
	}
	return nil
}

func (h *ScanOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	ctx, span := core.TraceMethod(ctx, "ScanOperationHandler.Execute")
	defer span.End()

	// Initialize progress tracker with manual mode for simple milestones
	tracker, err := h.NewProgressTracker(req.ID, core.ProgressModeManual, func(cfg *core.ProgressTrackerConfig) {
		cfg.MessageProvider = h.NewDefaultProgressMessageProvider(core.OpTypeScan)
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

	// TODO: implement content scan

	// Complete
	if err := tracker.SetProgress(100); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	return nil
}

func (h *ScanOperationHandler) GetStatus(_ context.Context, req *models.Request) (*core.RequestStatus, error) {
	return h.GetStatusFromWorkflowData(req.ID, req)
}

func (h *ScanOperationHandler) Cleanup(_ context.Context, _ *models.Request) error {
	// No cleanup needed for scan operation
	return nil
}

func NewScanOperation(ctx core.Context) core.Operation {
	return core.NewScanOperation(internal.ProtocolName, &ScanOperationHandler{
		OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
	})
}
