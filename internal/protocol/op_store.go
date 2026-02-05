package protocol

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
)

// StoreOperationHandler handles storing content locally
type StoreOperationHandler struct {
	core.OperationHelper
}

func (h *StoreOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	if len(req.Hash) == 0 {
		return fmt.Errorf("hash is required")
	}
	return nil
}

func (h *StoreOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	ctx, span := core.TraceMethod(ctx, "StoreOperationHandler.Execute")
	defer span.End()

	// Initialize progress tracker with manual mode for simple milestones
	tracker, err := InitializeManualProgressTracker(h, req.ID, core.OpTypeStore, 10)
	if err != nil {
		return err
	}

	var workflowData PinWorkflowData
	err = h.StructuredWorkflowData(req.ID, &workflowData)
	if err != nil {
		return fmt.Errorf("failed to get workflow data: %w", err)
	}

	// If CIDs are provided in workflow data, mark each block as ready
	if len(workflowData.Cids) > 0 {
		store := h.Protocol().(*Protocol).GetMetadataStore()

		// Set progress - marking blocks ready
		if err := tracker.SetProgress(50); err != nil {
			h.Logger().Warn("Failed to update progress", zap.Error(err))
		}

		for _, cidStr := range workflowData.Cids {
			c, err := cid.Parse(cidStr)
			if err != nil {
				// Log error but continue with other CIDs
				h.Logger().Warn("Failed to parse CID during store", zap.String("cid", cidStr), zap.Error(err))
				continue
			}
			err = store.MarkBlockReady(c, true)
			if err != nil {
				h.Logger().Warn("Failed to mark block ready", zap.Stringer("cid", c), zap.Error(err))
				continue
			}
		}
	}

	// Complete
	if err := tracker.SetProgress(100); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	return nil
}

func (h *StoreOperationHandler) GetStatus(_ context.Context, req *models.Request) (*core.RequestStatus, error) {
	return h.GetStatusFromWorkflowData(req.ID, req)
}

func (h *StoreOperationHandler) Cleanup(_ context.Context, req *models.Request) error {
	// No cleanup needed for store operation
	return nil
}

func NewStoreOperation(ctx core.Context) core.Operation {
	return core.NewStoreOperation(internal.ProtocolName, &StoreOperationHandler{
		OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
	})
}
