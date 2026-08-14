package protocol

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/types"
	"go.uber.org/zap"
)

// ConfirmOperationHandler verifies all CIDs are ready before completing
type ConfirmOperationHandler struct {
	core.OperationHelper
}

func (h *ConfirmOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	if len(req.Hash) == 0 {
		return fmt.Errorf("hash is required")
	}
	return nil
}

func (h *ConfirmOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	ctx, span := core.TraceMethod(ctx, "ConfirmOperationHandler.Execute")
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

	pinSvc := core.GetService[pluginCore.IPFSPinService](h.Context(), pluginCore.PIN_SERVICE)

	proto := h.Protocol().(*Protocol)
	metadataStore := proto.GetMetadataStore()

	// Set client IP in context for quota tracking before any operations
	ctx = pc.ClientIPOption(ctx, req.SourceIP)

	var cidList []cid.Cid

	for _, cidStr := range workflowData.Cids {
		c, err := cid.Parse(cidStr)
		if err != nil {
			return fmt.Errorf("failed to parse CID %s: %w", cidStr, err)
		}

		// Check if block exists and is ready
		err = metadataStore.BlockExists(ctx, c)
		if err != nil {
			return fmt.Errorf("block %s not ready: %w", c, err)
		}

		cidList = append(cidList, c)
	}

	// Guard: no CIDs means there is nothing to confirm. This must not be a
	// retryable error: the pin workflow's confirm step is configured with
	// FailureBehavior=RetryStep, and workflowData.Cids is only ever populated by
	// the prior retrieve step, so an empty list cannot be fixed by retrying.
	// In practice an empty Cids at this point means a stale/orphaned request
	// (created before retrieve began persisting Cids). Treat it as a no-op
	// success and let the workflow complete, rather than retrying forever and
	// re-queueing step-executor cron jobs indefinitely.
	if len(cidList) == 0 {
		h.Logger().Warn("confirm skipped: no CIDs in workflow data",
			zap.Uint("request_id", req.ID))
		return nil
	}

	// Process all CIDs to create upload/core pin records for all CIDs
	if len(cidList) > 0 {
		// Set progress - processing CIDs
		if err := tracker.SetProgress(30); err != nil {
			h.Logger().Warn("Failed to update progress", zap.Error(err))
		}

		uploadSvc := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)
		if uploadSvc == nil {
			h.Logger().Error("Upload service not available")
			return fmt.Errorf("upload service not available")
		}

		err := uploadSvc.ProcessUpload(ctx, cidList, lo.FromPtrOr(req.UserID, 0), nil)
		if err != nil {
			return fmt.Errorf("failed to process upload: %w", err)
		}

		// Set progress - creating root pin
		if err := tracker.SetProgress(60); err != nil {
			h.Logger().Warn("Failed to update progress", zap.Error(err))
		}

		// Create IPFS pin record for the root CID (first CID in the list)
		_, err = uploadSvc.CreateRootPin(ctx, cidList[0], lo.FromPtrOr(req.UserID, 0))
		if err != nil {
			return fmt.Errorf("failed to create root pin: %w", err)
		}
	}

	// Set progress - updating pin status
	if err := tracker.SetProgress(80); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	err = pinSvc.UpdatePinStatus(ctx, types.FromUUID(workflowData.PinRequestID), db.PinningStatusPinned, nil)
	if err != nil {
		return fmt.Errorf("failed to update pin status: %w", err)
	}

	// Get the pin record for DAG validation. After the pin status has been
	// committed, a transient lookup failure should not abort the operation,
	// because retrying would re-run non-idempotent upload/pin creation steps.
	pin, err := pinSvc.GetPinByRequestID(ctx, types.FromUUID(workflowData.PinRequestID))
	if err != nil {
		h.Logger().Warn("Failed to get pin record for DAG validation, continuing without related CIDs", zap.Error(err))
		pin = nil
	}

	// Start the dedicated filepath workflow asynchronously.
	// Name resolution and file path tree computation run as a separate
	// workflow request, independent of this confirm operation.
	if err := h.startFilePathWorkflow(ctx, req, cidList, pin); err != nil {
		h.Logger().Warn("Failed to start filepath workflow", zap.Error(err))
	}

	// Complete
	if err := tracker.SetProgress(100); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	return nil
}

func (h *ConfirmOperationHandler) GetStatus(_ context.Context, req *models.Request) (*core.RequestStatus, error) {
	return h.GetStatusFromWorkflowData(req.ID, req)
}

func (h *ConfirmOperationHandler) Cleanup(_ context.Context, _ *models.Request) error {
	return nil
}

// startFilePathWorkflow starts the dedicated FILE_PATH_WORKFLOW with the
// confirmed CIDs and related CIDs as workflow data. The workflow runs
// asynchronously as a separate request.
func (h *ConfirmOperationHandler) startFilePathWorkflow(
	ctx context.Context,
	req *models.Request,
	cidList []cid.Cid,
	pin *db.IPFSPin,
) error {
	userID := lo.FromPtrOr(req.UserID, 0)

	cids := lo.Map(cidList, func(c cid.Cid, _ int) string { return c.String() })

	if len(cidList) == 0 {
		return fmt.Errorf("no CIDs to process in filepath workflow")
	}

	var relatedCIDs []string
	if pin != nil {
		pinSvc := core.GetService[pluginCore.IPFSPinService](h.Context(), pluginCore.PIN_SERVICE)
		if pinSvc != nil {
			related, err := pinSvc.ValidateDAGCompletion(ctx, pin)
			if err != nil {
				h.Logger().Error("Failed to validate DAG completion for filepath workflow", zap.Error(err))
			} else {
				relatedCIDs = lo.FilterMap(related, func(b []byte, _ int) (string, bool) {
						c, err := cid.Cast(b)
						if err != nil {
							h.Logger().Warn("Failed to cast related CID", zap.Binary("cid", b), zap.Error(err))
							return "", false
						}
						return c.String(), true
					})
			}
		}
	}

	_, err := h.StartWorkflow(
		FILE_PATH_WORKFLOW,
		core.WithWorkflowStructData(FilePathWorkflowInputData{
			CIDs:         cids,
			RelatedCIDs: relatedCIDs,
			UserID:       userID,
		}, "json"),
		core.WithWorkflowUserID(userID),
	)
	return err
}

func NewConfirmOperation(ctx core.Context) core.Operation {
	return core.NewNamedOperation(
		confirmOperationName(),
		"", // No global type for confirm
		&ConfirmOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		},
		"Complete Pin",
	)
}
