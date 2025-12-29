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

	// TODO: implement content scan
	return nil
}

func (h *ScanOperationHandler) GetStatus(ctx context.Context, req *models.Request) (*core.RequestStatus, error) {
	ctx, span := core.TraceMethod(ctx, "ScanOperationHandler.GetStatus")
	defer span.End()

	status := &core.RequestStatus{
		ProgressPercent: 100,
	}

	if req.Status == models.RequestStatusCompleted {
		status.Message = "Content scanned successfully"
		status.ProgressPercent = 100
	}

	return status, nil
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
