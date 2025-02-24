package protocol

import (
	"context"
	"fmt"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
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
	// Store operation marks content as pinned in the metadata store
	hash := h.StorageHash(req)
	_cid, err := internal.CIDFromStorageHash(hash)
	if err != nil {
		return err
	}
	return h.Protocol().(*Protocol).GetMetadataStore().MarkBlockReady(_cid, true)
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
