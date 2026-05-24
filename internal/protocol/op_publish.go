package protocol

import (
	"context"
	"fmt"
	"time"

	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
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
	tracker, err := InitializeManualProgressTracker(h, req.ID, core.OpTypePublish, 10)
	if err != nil {
		return err
	}

	node := h.Protocol().(*Protocol).GetNode()

	if len(req.Hash) > 0 {
		c, cErr := internal.CIDFromHash(req.Hash, req.CIDType)
		if cErr == nil {
			go func() {
				provideCtx, cancel := context.WithTimeout(core.DetachContext(ctx), 30*time.Second)
				defer cancel()
				if err := node.ProvideCID(provideCtx, c); err != nil {
					h.Logger().Warn("direct DHT provide failed", zap.Error(err), zap.Stringer("cid", c))
				}
			}()
		}
	}

	node.TriggerReprovider()

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
