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
	var workflowData PinWorkflowData
	err := h.StructuredWorkflowData(req.ID, &workflowData)
	if err != nil {
		return fmt.Errorf("failed to get workflow data: %w", err)
	}

	// If CIDs are provided in workflow data, mark each block as ready
	if len(workflowData.Cids) > 0 {
		store := h.Protocol().(*Protocol).GetMetadataStore()
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
	} else {
		// Store operation marks content as pinned in the metadata store (backward compatibility)
		hash := h.StorageHash(req)
		_cid, err := internal.CIDFromStorageHash(hash)
		if err != nil {
			return err
		}
		return h.Protocol().(*Protocol).GetMetadataStore().MarkBlockReady(_cid, true)
	}
	return nil
}

func (h *StoreOperationHandler) GetStatus(_ context.Context, req *models.Request) (*core.RequestStatus, error) {
	status := &core.RequestStatus{
		ProgressPercent: 100,
	}

	if req.Status == models.RequestStatusCompleted {
		status.Message = "Content stored locally"
		status.ProgressPercent = 100
	}

	return status, nil
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
