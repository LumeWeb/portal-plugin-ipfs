package quota

import (
	"context"
	"fmt"

	quotaCore "go.lumeweb.com/portal-plugin-quota/core"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/event"
)

// WithQuotaService executes a function with quota service if available
func WithQuotaService(cctx context.Context, ctx core.Context, fn func(quotaCore.QuotaService, context.Context) error) error {
	return core.WithService[quotaCore.QuotaService](ctx, quotaCore.QUOTA_SERVICE, func(qs quotaCore.QuotaService) error {
		return fn(qs, cctx)
	})
}

// CheckUploadQuota checks upload quota if service is available
func CheckUploadQuota(cctx context.Context, ctx core.Context, userID uint, requestedBytes uint64) (*quotaCore.QuotaCheckResult, error) {
	var result *quotaCore.QuotaCheckResult

	err := WithQuotaService(cctx, ctx, func(qs quotaCore.QuotaService, c context.Context) error {
		res, err := qs.CheckUploadQuota(c, userID, requestedBytes)
		if err != nil {
			return err
		}
		result = &res
		return nil
	})

	return result, err
}

// CheckDownloadQuota checks download quota if service is available
func CheckDownloadQuota(cctx context.Context, ctx core.Context, userID uint, requestedBytes uint64) (*quotaCore.QuotaCheckResult, error) {
	var result *quotaCore.QuotaCheckResult

	err := WithQuotaService(cctx, ctx, func(qs quotaCore.QuotaService, c context.Context) error {
		res, err := qs.CheckDownloadQuota(c, userID, requestedBytes)
		if err != nil {
			return err
		}
		result = &res
		return nil
	})

	return result, err
}

// CheckStorageQuota checks storage quota if service is available
func CheckStorageQuota(cctx context.Context, ctx core.Context, userID uint, requestedBytes uint64) (*quotaCore.QuotaCheckResult, error) {
	var result *quotaCore.QuotaCheckResult

	err := WithQuotaService(cctx, ctx, func(qs quotaCore.QuotaService, c context.Context) error {
		res, err := qs.CheckStorageQuota(c, userID, requestedBytes)
		if err != nil {
			return err
		}
		result = &res
		return nil
	})

	return result, err
}

// EmitUploadCompleted emits an upload completed event for quota tracking
func EmitUploadCompleted(cctx context.Context, ctx core.Context, userID *uint, uploadID uint, bytes uint64, ip string) {
	ctx.FireAsync(event.EVENT_UPLOAD_COMPLETED, event.NewUploadCompletedEvent(cctx, uploadID, bytes, ip, userID))
}

// EmitDownloadCompleted emits a download completed event for quota tracking
func EmitDownloadCompleted(cctx context.Context, ctx core.Context, uploadID uint, bytes uint64, ip string, userID *uint) {
	ctx.FireAsync(event.EVENT_DOWNLOAD_COMPLETED, event.NewDownloadCompletedEvent(cctx, uploadID, bytes, ip, userID))
}

// EmitStorageObjectPinned emits a storage object pinned event for quota tracking
func EmitStorageObjectPinned(cctx context.Context, ctx core.Context, pin *models.Pin, ip string) {
	ctx.FireAsync(event.EVENT_STORAGE_OBJECT_PINNED, event.NewStorageObjectPinnedEvent(cctx, pin, ip))
}

// EmitStorageObjectUnpinned emits a storage object unpinned event for quota tracking
func EmitStorageObjectUnpinned(cctx context.Context, ctx core.Context, pin *models.Pin, ip string) {
	ctx.FireAsync(event.EVENT_STORAGE_OBJECT_UNPINNED, event.NewStorageObjectUnpinnedEvent(cctx, pin, ip))
}

// ValidateUploadQuota checks upload quota and returns an error if exceeded
func ValidateUploadQuota(cctx context.Context, ctx core.Context, userID uint, requestedBytes uint64) error {
	result, err := CheckUploadQuota(cctx, ctx, userID, requestedBytes)
	if err != nil {
		return fmt.Errorf("quota check failed: %w", err)
	}
	if result != nil && !result.Allowed {
		return core.ErrUploadQuotaExceeded
	}
	return nil
}

// ValidateDownloadQuota checks download quota and returns an error if exceeded
func ValidateDownloadQuota(cctx context.Context, ctx core.Context, userID uint, requestedBytes uint64) error {
	result, err := CheckDownloadQuota(cctx, ctx, userID, requestedBytes)
	if err != nil {
		return fmt.Errorf("quota check failed: %w", err)
	}
	if result != nil && !result.Allowed {
		return core.ErrDownloadQuotaExceeded
	}
	return nil
}

// ValidateStorageQuota checks storage quota and returns an error if exceeded
func ValidateStorageQuota(cctx context.Context, ctx core.Context, userID uint, requestedBytes uint64) error {
	result, err := CheckStorageQuota(cctx, ctx, userID, requestedBytes)
	if err != nil {
		return fmt.Errorf("quota check failed: %w", err)
	}
	if result != nil && !result.Allowed {
		return core.ErrStorageQuotaExceeded
	}
	return nil
}