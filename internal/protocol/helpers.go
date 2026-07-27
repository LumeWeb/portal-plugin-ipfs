package protocol

import (
	"fmt"

	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// InitializeManualProgressTracker creates and initializes a manual mode progress tracker
// This is a shared utility function to reduce code duplication across operation handlers
// that use manual progress tracking with simple milestones
func InitializeManualProgressTracker(
	helper core.OperationHelper,
	reqID uint,
	opType core.OperationType,
	initialProgress int,
) (*core.ProgressTracker, error) {
	tracker, err := helper.NewProgressTracker(reqID, core.ProgressModeManual, func(cfg *core.ProgressTrackerConfig) {
		cfg.MessageProvider = helper.NewDefaultProgressMessageProvider(opType)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize progress tracker: %w", err)
	}

	if err := tracker.Initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize tracker: %w", err)
	}

	if err := tracker.SetProgress(float64(initialProgress)); err != nil {
		helper.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	return tracker, nil
}
