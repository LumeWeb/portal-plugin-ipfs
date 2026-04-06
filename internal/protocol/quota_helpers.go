package protocol

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
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

// ReleasePerBlockReservations releases multiple quota reservations safely
// This is used for cleanup when per-block reservations are created
func ReleasePerBlockReservations(reservations []*quotaCore.QuotaCheckResult) {
	for _, result := range reservations {
		if result != nil {
			result.ReleaseReservation()
		}
	}
}

// CreatePerBlockReservations creates individual quota reservations for each block
// This ensures each CID maps to its own reservation for accurate download tracking.
// Returns a map of BlockReservations containing both upload and storage reservations for each CID.
// This version queries the blockstore for CID sizes.
func CreatePerBlockReservations(ctx context.Context, coreCtx core.Context, proto ProtoNode, cids []cid.Cid, userID uint) (map[cid.Cid]*quota.BlockReservations, error) {
	blockstore := proto.GetNode().GetBlockstore()

	// Build size map from blockstore
	sizes := make(map[cid.Cid]uint64, len(cids))
	for _, c := range cids {
		size, err := blockstore.GetSize(ctx, c)
		if err != nil {
			coreCtx.Logger().Warn("Failed to get block size for reservation, skipping",
				zap.String("cid", c.String()),
				zap.Error(err))
			continue
		}
		if size > 0 {
			sizes[c] = uint64(size)
		}
	}

	return CreatePerBlockReservationsWithSizes(ctx, coreCtx, userID, sizes)
}

// CreatePerBlockReservationsWithSizes creates individual quota reservations for each block
// This ensures each CID maps to its own reservation for accurate download tracking.
// Returns a map of BlockReservations containing both upload and storage reservations for each CID.
// Use this version when CID sizes are already known to avoid unnecessary blockstore queries.
func CreatePerBlockReservationsWithSizes(ctx context.Context, coreCtx core.Context, userID uint, sizes map[cid.Cid]uint64) (map[cid.Cid]*quota.BlockReservations, error) {
	// Create per-block reservations
	reservations := make(map[cid.Cid]*quota.BlockReservations, len(sizes))
	for c, blockSize := range sizes {
		if blockSize == 0 {
			continue
		}

		// Create upload quota reservation for this specific block
		uploadReservation, err := quota.CheckWithReservation(ctx, coreCtx, quota.CheckTypeUpload, userID, blockSize, quota.CheckUploadQuota)
		if err != nil {
			// Release all previously created reservations
			quota.ReleaseBlockReservationsMap(reservations)
			return nil, fmt.Errorf("failed to create upload reservation for block %s: %w", c.String(), err)
		}

		// Create storage quota reservation for this specific block
		storageReservation, err := quota.CheckWithReservation(ctx, coreCtx, quota.CheckTypeStorage, userID, blockSize, quota.CheckStorageQuota)
		if err != nil {
			// Release upload reservation before returning error
			uploadReservation.ReleaseReservation()
			// Release all previously created reservations
			quota.ReleaseBlockReservationsMap(reservations)
			return nil, fmt.Errorf("failed to create storage reservation for block %s: %w", c.String(), err)
		}

		// Map this CID to both upload and storage reservations
		reservations[c] = &quota.BlockReservations{
			UploadReservation:  uploadReservation,
			StorageReservation: storageReservation,
		}
	}

	return reservations, nil
}
