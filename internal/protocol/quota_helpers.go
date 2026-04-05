package protocol

import (
	"context"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
	quotaCore "go.lumeweb.com/portal-plugin-quota/core"
)

// CalculateTotalCIDSize calculates the total size of a list of CIDs
// Returns the total size and a map of individual CID sizes
func CalculateTotalCIDSize(ctx context.Context, proto ProtoNode, cids []cid.Cid, logger *core.Logger) (uint64, map[cid.Cid]uint64) {
	totalSize := uint64(0)
	cidSizes := make(map[cid.Cid]uint64, len(cids))

	for _, c := range cids {
		size, err := proto.GetMetadataStore().Size(ctx, c)
		if err != nil {
			logger.Warn("Failed to get size for quota check", zap.Stringer("cid", c), zap.Error(err))
			continue
		}
		cidSizes[c] = size
		totalSize += size
	}

	return totalSize, cidSizes
}

// CreateReservationMap creates a map of CIDs to a single reservation object
// This is used when a single quota check covers multiple CIDs
func CreateReservationMap(cids []cid.Cid, result *quotaCore.QuotaCheckResult) map[cid.Cid]*quotaCore.QuotaCheckResult {
	if result == nil || len(cids) == 0 {
		return nil
	}

	reservations := make(map[cid.Cid]*quotaCore.QuotaCheckResult, len(cids))
	for _, c := range cids {
		reservations[c] = result
	}
	return reservations
}
