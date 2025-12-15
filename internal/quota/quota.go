package quota

import (
	"fmt"

	quotaCore "go.lumeweb.com/portal-plugin-quota/core"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/event"
)

// WithQuotaService executes a function with quota service if available
func WithQuotaService(ctx core.Context, fn func(quotaCore.QuotaService) error) error {
	return core.WithService[quotaCore.QuotaService](ctx, quotaCore.QUOTA_SERVICE, fn)
}

// CheckUploadQuota checks upload quota if service is available
func CheckUploadQuota(ctx core.Context, userID uint, requestedBytes uint64) (*quotaCore.QuotaCheckResult, error) {
	var result *quotaCore.QuotaCheckResult

	err := WithQuotaService(ctx, func(qs quotaCore.QuotaService) error {
		res, err := qs.CheckUploadQuota(userID, requestedBytes)
		if err != nil {
			return err
		}
		result = &res
		return nil
	})

	return result, err
}

// CheckDownloadQuota checks download quota if service is available
func CheckDownloadQuota(ctx core.Context, userID uint, requestedBytes uint64) (*quotaCore.QuotaCheckResult, error) {
	var result *quotaCore.QuotaCheckResult

	err := WithQuotaService(ctx, func(qs quotaCore.QuotaService) error {
		res, err := qs.CheckDownloadQuota(userID, requestedBytes)
		if err != nil {
			return err
		}
		result = &res
		return nil
	})

	return result, err
}

// CheckStorageQuota checks storage quota if service is available
func CheckStorageQuota(ctx core.Context, userID uint, requestedBytes uint64) (*quotaCore.QuotaCheckResult, error) {
	var result *quotaCore.QuotaCheckResult

	err := WithQuotaService(ctx, func(qs quotaCore.QuotaService) error {
		res, err := qs.CheckStorageQuota(userID, requestedBytes)
		if err != nil {
			return err
		}
		result = &res
		return nil
	})

	return result, err
}

// EmitUploadCompleted emits an upload completed event for quota tracking
func EmitUploadCompleted(ctx core.Context, userID *uint, uploadID uint, bytes uint64, ip string) {
	ctx.FireAsync(event.EVENT_UPLOAD_COMPLETED, event.NewUploadCompletedEvent(uploadID, bytes, ip, userID))
}

// EmitDownloadCompleted emits a download completed event for quota tracking
func EmitDownloadCompleted(ctx core.Context, uploadID uint, bytes uint64, ip string) {
	ctx.FireAsync(event.EVENT_DOWNLOAD_COMPLETED, event.NewDownloadCompletedEvent(uploadID, bytes, ip))
}

// EmitStorageObjectPinned emits a storage object pinned event for quota tracking
func EmitStorageObjectPinned(ctx core.Context, pin *models.Pin, ip string) {
	ctx.FireAsync(event.EVENT_STORAGE_OBJECT_PINNED, event.NewStorageObjectPinnedEvent(pin, ip))
}

// ValidateUploadQuota checks upload quota and returns an error if exceeded
func ValidateUploadQuota(ctx core.Context, userID uint, requestedBytes uint64) error {
	result, err := CheckUploadQuota(ctx, userID, requestedBytes)
	if err != nil {
		return fmt.Errorf("quota check failed: %w", err)
	}
	if result != nil && !result.Allowed {
		return core.ErrUploadQuotaExceeded
	}
	return nil
}

// ValidateDownloadQuota checks download quota and returns an error if exceeded
func ValidateDownloadQuota(ctx core.Context, userID uint, requestedBytes uint64) error {
	result, err := CheckDownloadQuota(ctx, userID, requestedBytes)
	if err != nil {
		return fmt.Errorf("quota check failed: %w", err)
	}
	if result != nil && !result.Allowed {
		return core.ErrDownloadQuotaExceeded
	}
	return nil
}

// ValidateStorageQuota checks storage quota and returns an error if exceeded
func ValidateStorageQuota(ctx core.Context, userID uint, requestedBytes uint64) error {
	result, err := CheckStorageQuota(ctx, userID, requestedBytes)
	if err != nil {
		return fmt.Errorf("quota check failed: %w", err)
	}
	if result != nil && !result.Allowed {
		return core.ErrStorageQuotaExceeded
	}
	return nil
}