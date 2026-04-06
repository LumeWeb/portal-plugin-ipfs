package quota

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	quotaCore "go.lumeweb.com/portal-plugin-quota/core"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/event"
	"go.uber.org/zap"
)

// BlockReservations holds both upload and storage reservations for a block
type BlockReservations struct {
	UploadReservation  *quotaCore.QuotaCheckResult
	StorageReservation *quotaCore.QuotaCheckResult
}

// ReleaseAll releases both upload and storage reservations
func (br *BlockReservations) ReleaseAll() {
	if br.UploadReservation != nil {
		br.UploadReservation.ReleaseReservation()
	}
	if br.StorageReservation != nil {
		br.StorageReservation.ReleaseReservation()
	}
}

// ReleaseBlockReservationsMap releases all reservations in a BlockReservations map
func ReleaseBlockReservationsMap(reservations map[cid.Cid]*BlockReservations) {
	for _, blockRes := range reservations {
		if blockRes != nil {
			blockRes.ReleaseAll()
		}
	}
}

// WithQuotaService executes a function with quota service if available
func WithQuotaService(cctx context.Context, ctx core.Context, fn func(quotaCore.QuotaService, context.Context) error) error {
	return core.WithService[quotaCore.QuotaService](ctx, quotaCore.QUOTA_SERVICE, func(qs quotaCore.QuotaService) error {
		return fn(qs, cctx)
	})
}

// CheckUploadQuota checks upload quota if service is available
func CheckUploadQuota(cctx context.Context, ctx core.Context, userID uint, requestedBytes uint64, opts ...quotaCore.CheckOption) (*quotaCore.QuotaCheckResult, error) {
	var result *quotaCore.QuotaCheckResult

	err := WithQuotaService(cctx, ctx, func(qs quotaCore.QuotaService, c context.Context) error {
		res, err := qs.CheckUploadQuota(c, userID, requestedBytes, opts...)
		if err != nil {
			return err
		}
		result = &res
		return nil
	})

	return result, err
}

// CheckDownloadQuota checks download quota if service is available
func CheckDownloadQuota(cctx context.Context, ctx core.Context, userID uint, requestedBytes uint64, opts ...quotaCore.CheckOption) (*quotaCore.QuotaCheckResult, error) {
	var result *quotaCore.QuotaCheckResult

	err := WithQuotaService(cctx, ctx, func(qs quotaCore.QuotaService, c context.Context) error {
		res, err := qs.CheckDownloadQuota(c, userID, requestedBytes, opts...)
		if err != nil {
			return err
		}
		result = &res
		return nil
	})

	return result, err
}

// CheckStorageQuota checks storage quota if service is available
func CheckStorageQuota(cctx context.Context, ctx core.Context, userID uint, requestedBytes uint64, opts ...quotaCore.CheckOption) (*quotaCore.QuotaCheckResult, error) {
	var result *quotaCore.QuotaCheckResult

	err := WithQuotaService(cctx, ctx, func(qs quotaCore.QuotaService, c context.Context) error {
		res, err := qs.CheckStorageQuota(c, userID, requestedBytes, opts...)
		if err != nil {
			return err
		}
		result = &res
		return nil
	})

	return result, err
}

// EmitUploadCompleted emits an upload completed event for quota tracking
func EmitUploadCompleted(cctx context.Context, ctx core.Context, userID *uint, uploadID uint, bytes uint64, ip string, reservationID *string, successful bool) {
	core.Fire(ctx, event.EVENT_UPLOAD_COMPLETED, event.NewUploadCompletedEvent(cctx, uploadID, bytes, ip, userID, reservationID, successful))
}

// EmitDownloadCompleted emits a download completed event for quota tracking
func EmitDownloadCompleted(cctx context.Context, ctx core.Context, userID *uint, uploadID uint, bytes uint64, ip string, reservationID *string, successful bool) {
	core.Fire(ctx, event.EVENT_DOWNLOAD_COMPLETED, event.NewDownloadCompletedEvent(cctx, uploadID, bytes, ip, userID, reservationID, successful))
}

// EmitStorageObjectPinned emits a storage object pinned event for quota tracking
func EmitStorageObjectPinned(cctx context.Context, ctx core.Context, pin *models.Pin, ip string, reservationID *string) {
	core.Fire(ctx, event.EVENT_STORAGE_OBJECT_PINNED, event.NewStorageObjectPinnedEvent(cctx, pin, ip, reservationID))
}

// EmitStorageObjectUnpinned emits a storage object unpinned event for quota tracking
func EmitStorageObjectUnpinned(cctx context.Context, ctx core.Context, pin *models.Pin, ip string) {
	core.Fire(ctx, event.EVENT_STORAGE_OBJECT_UNPINNED, event.NewStorageObjectUnpinnedEvent(cctx, pin, ip))
}

// ValidateUploadQuota checks upload quota and returns an error if exceeded
func ValidateUploadQuota(cctx context.Context, ctx core.Context, userID uint, requestedBytes uint64) error {
	result, err := CheckUploadQuota(cctx, ctx, userID, requestedBytes)
	if err != nil {
		ctx.Logger().Warn("Upload quota check failed",
			zap.Uint("user_id", userID),
			zap.Uint64("requested_bytes", requestedBytes),
			zap.Error(err))
		return fmt.Errorf("quota check failed: %w", err)
	}
	if result != nil && !result.Allowed {
		ctx.Logger().Debug("Upload quota exceeded",
			zap.Uint("user_id", userID),
			zap.Uint64("requested_bytes", requestedBytes),
			zap.Uint64("current_usage", result.Details.CurrentUsage),
			zap.Any("limit", result.Details.Limit))
		return core.ErrUploadQuotaExceeded
	}
	if result != nil {
		ctx.Logger().Debug("Upload quota validated successfully",
			zap.Uint("user_id", userID),
			zap.Uint64("requested_bytes", requestedBytes),
			zap.Uint64("current_usage", result.Details.CurrentUsage))
	}
	return nil
}

// ValidateDownloadQuota checks download quota and returns an error if exceeded
func ValidateDownloadQuota(cctx context.Context, ctx core.Context, userID uint, requestedBytes uint64) error {
	result, err := CheckDownloadQuota(cctx, ctx, userID, requestedBytes)
	if err != nil {
		ctx.Logger().Warn("Download quota check failed",
			zap.Uint("user_id", userID),
			zap.Uint64("requested_bytes", requestedBytes),
			zap.Error(err))
		return fmt.Errorf("quota check failed: %w", err)
	}
	if result != nil && !result.Allowed {
		ctx.Logger().Debug("Download quota exceeded",
			zap.Uint("user_id", userID),
			zap.Uint64("requested_bytes", requestedBytes),
			zap.Uint64("current_usage", result.Details.CurrentUsage),
			zap.Any("limit", result.Details.Limit))
		return core.ErrDownloadQuotaExceeded
	}
	if result != nil {
		ctx.Logger().Debug("Download quota validated successfully",
			zap.Uint("user_id", userID),
			zap.Uint64("requested_bytes", requestedBytes),
			zap.Uint64("current_usage", result.Details.CurrentUsage))
	}
	return nil
}

// ValidateStorageQuota checks storage quota and returns an error if exceeded
func ValidateStorageQuota(cctx context.Context, ctx core.Context, userID uint, requestedBytes uint64) error {
	result, err := CheckStorageQuota(cctx, ctx, userID, requestedBytes)
	if err != nil {
		ctx.Logger().Warn("Storage quota check failed",
			zap.Uint("user_id", userID),
			zap.Uint64("requested_bytes", requestedBytes),
			zap.Error(err))
		return fmt.Errorf("quota check failed: %w", err)
	}
	if result != nil && !result.Allowed {
		ctx.Logger().Debug("Storage quota exceeded",
			zap.Uint("user_id", userID),
			zap.Uint64("requested_bytes", requestedBytes),
			zap.Uint64("current_usage", result.Details.CurrentUsage),
			zap.Any("limit", result.Details.Limit))
		return core.ErrStorageQuotaExceeded
	}
	if result != nil {
		ctx.Logger().Debug("Storage quota validated successfully",
			zap.Uint("user_id", userID),
			zap.Uint64("requested_bytes", requestedBytes),
			zap.Uint64("current_usage", result.Details.CurrentUsage))
	}
	return nil
}

// CheckCIDGroupDownloadAvailability checks if any users with pinned content have sufficient quota for anonymous downloads
func CheckCIDGroupDownloadAvailability(cctx context.Context, ctx core.Context, cid core.StorageHash, requiredBytes uint64) (bool, error) {
	available := true
	serviceFound := false

	core.WithService[quotaCore.QuotaService](ctx, quotaCore.QUOTA_SERVICE, func(qs quotaCore.QuotaService) error {
		serviceFound = true
		result, err := qs.CheckCIDGroupQuotaAvailability(cctx, cid, requiredBytes, quotaCore.UsageTypeDownload)
		if err == nil {
			available = result
		}
		return nil
	})

	if !serviceFound {
		ctx.Logger().Debug("Quota service not available, assuming group quota available for anonymous download",
			zap.String("cid", cid.String()),
			zap.Uint64("required_bytes", requiredBytes))
		return true, nil
	}

	return available, nil
}

// CheckWithReservation performs a quota check with reservation and provides unified error handling
// This encapsulates the common pattern of checking quota with reservation and returning appropriate errors
func CheckWithReservation(cctx context.Context, ctx core.Context, checkType string, userID uint, requestedBytes uint64, checkFunc func(context.Context, core.Context, uint, uint64, ...quotaCore.CheckOption) (*quotaCore.QuotaCheckResult, error)) (*quotaCore.QuotaCheckResult, error) {
	checkResult, err := checkFunc(cctx, ctx, userID, requestedBytes, quotaCore.WithCreateReservation())
	if err != nil {
		ctx.Logger().Warn("Failed to check quota", zap.String("check_type", checkType), zap.Uint("user_id", userID), zap.Uint64("requested_bytes", requestedBytes), zap.Error(err))
		return nil, fmt.Errorf("%s quota check failed: %w", checkType, err)
	}
	if checkResult != nil && !checkResult.Allowed {
		currentUsage := checkResult.Details.CurrentUsage
		quotaLimit := uint64(0)
		if checkResult.Details.Limit != nil {
			quotaLimit = *checkResult.Details.Limit
		}
		
		var quotaErr error
		switch checkType {
		case "upload":
			quotaErr = fmt.Errorf("%w (current: %d bytes, requested: %d bytes, limit: %d bytes)", core.ErrUploadQuotaExceeded, currentUsage, requestedBytes, quotaLimit)
		case "storage":
			quotaErr = fmt.Errorf("%w (current: %d bytes, requested: %d bytes, limit: %d bytes)", core.ErrStorageQuotaExceeded, currentUsage, requestedBytes, quotaLimit)
		case "download":
			quotaErr = fmt.Errorf("%w (current: %d bytes, requested: %d bytes, limit: %d bytes)", core.ErrDownloadQuotaExceeded, currentUsage, requestedBytes, quotaLimit)
		default:
			quotaErr = fmt.Errorf("%s quota exceeded: current usage %d bytes + requested %d bytes would exceed quota limit of %d bytes", checkType, currentUsage, requestedBytes, quotaLimit)
		}
		
		ctx.Logger().Warn("Quota exceeded", zap.String("check_type", checkType), zap.Uint("user_id", userID), zap.Uint64("requested_bytes", requestedBytes), zap.Uint64("current_usage", currentUsage), zap.Uint64("quota_limit", quotaLimit))
		checkResult.ReleaseReservation()
		return nil, quotaErr
	}
	return checkResult, nil
}

// ReleaseReservations releases multiple quota reservations safely
// This handles nil checks and releases all provided reservations
func ReleaseReservations(reservations ...*quotaCore.QuotaCheckResult) {
	for _, result := range reservations {
		if result != nil {
			result.ReleaseReservation()
		}
	}
}

// QuotaCheckResults holds the results of quota checks with reservations
type QuotaCheckResults struct {
	Upload  *quotaCore.QuotaCheckResult
	Storage *quotaCore.QuotaCheckResult
	Download *quotaCore.QuotaCheckResult
}

// ReleaseAll releases all reservations held in the QuotaCheckResults struct
func (q *QuotaCheckResults) ReleaseAll() {
	ReleaseReservations(q.Upload, q.Storage, q.Download)
}
