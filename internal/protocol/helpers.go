package protocol

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// ValidateDAGCompletionAndUpdateWorkflow validates DAG completion for a pin and updates workflow data with related CIDs
// This helper function encapsulates the common pattern used across upload flows to:
// 1. Validate DAG completion using the provided IPFS pin to find related CIDs
// 2. Update workflow data with related CIDs for file path operations
func ValidateDAGCompletionAndUpdateWorkflow(
	ctx context.Context,
	helper core.OperationHelper,
	requestID uint,
	ipfsPin *db.IPFSPin,
	workflowData *PinWorkflowData,
) error {
	pinSvc := core.GetService[pluginCore.IPFSPinService](helper.Context(), pluginCore.PIN_SERVICE)
	if pinSvc == nil {
		helper.Logger().Error("Pin service not available")
		return nil // Don't fail the operation for this
	}

	var relatedCIDs [][]byte
	if ipfsPin != nil {
		var err error
		relatedCIDs, err = pinSvc.ValidateDAGCompletion(ctx, ipfsPin)
		if err != nil {
			helper.Logger().Error("Failed to validate DAG completion", zap.Error(err))
			// Don't fail the whole operation for DAG validation failure
		}
	}

	// Update workflow data with related CIDs for file path operation
	if len(relatedCIDs) > 0 {
		workflowData.Cids = append(workflowData.Cids, lo.Map(relatedCIDs, func(item []byte, index int) string {
			c, err := cid.Cast(item)
			if err != nil {
				helper.Logger().Error("Failed to cast related CID", zap.Error(err), zap.Binary("cid", item))
				return ""
			}
			return c.String()
		})...)

		// Filter out empty strings
		workflowData.Cids = lo.Filter(workflowData.Cids, func(item string, index int) bool {
			return item != ""
		})

		err := helper.UpdateWorkflowDataStruct(requestID, workflowData)
		if err != nil {
			helper.Logger().Error("Failed to update workflow data with related CIDs", zap.Error(err))
			// We don't return the error here as it's not critical to the operation
		}
	}

	return nil
}
