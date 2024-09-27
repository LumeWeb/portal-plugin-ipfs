package tasks

import (
	"errors"
	"fmt"
	billingPluginService "go.lumeweb.com/portal-plugin-billing/service"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/define"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/event"
	"go.lumeweb.com/portal/service"
	"go.uber.org/zap"
	"io"
)

func CronTaskTusUpload(args *define.CronTaskTusUploadArgs, ctx core.Context) error {
	logger := ctx.Logger()
	tusService := core.GetService[core.TUSService](ctx, core.TUS_SERVICE)
	tusHandler := core.GetAPI(internal.ProtocolName).(*api.API).TusHandler()
	cronService := core.GetService[core.CronService](ctx, core.CRON_SERVICE)
	pinService := core.GetService[core.PinService](ctx, core.PIN_SERVICE)

	// Get the request
	found, request := tusService.UploadExists(ctx, args.UploadID)
	if !found {
		logger.Error("Failed to get request")
		return fmt.Errorf("request not found")
	}

	size, err := tusHandler.UploadSize(ctx, service.NewStorageHashFromMultihashBytes(request.Request.UploadHash, request.Request.UploadHashCIDType, nil))
	if err != nil {
		return err
	}

	if core.ServiceExists(ctx, billingPluginService.QUOTA_SERVICE) {
		quotaService := core.GetService[billingPluginService.QuotaService](ctx, billingPluginService.QUOTA_SERVICE)
		uploadAllowed, err := quotaService.CheckUploadQuota(request.Request.UserID, size)
		if err != nil {
			return err
		}

		storageAllowed, err := quotaService.CheckStorageQuota(request.Request.UserID, size)
		if err != nil {
			return err
		}

		if !uploadAllowed || !storageAllowed {
			err := cronService.CreateJobIfNotExists(define.CronTaskTusUploadCleanupName, define.CronTaskTusUploadCleanupArgs{
				UploadID: args.UploadID,
			})
			if err != nil {
				return err
			}

			return nil
		}
	}

	reader, err := tusHandler.UploadReader(ctx, args.UploadID, 0)
	if err != nil {
		logger.Error("Failed to get request reader", zap.Error(err))
		return err
	}
	defer func(reader io.ReadCloser) {
		err := reader.Close()
		if err != nil {
			logger.Error("Failed to close reader", zap.Error(err))
		}
	}(reader)

	// Process the car
	cids, err := processCar(ctx, reader, &request.Request)
	if err != nil {
		if errors.Is(err, core.ErrDuplicateRequest) {
			return createTusCleanupTask(ctx, args.UploadID)
		}

		return err
	}

	for _, cid := range cids {
		pin, err := pinService.QueryPin(ctx, nil, core.PinFilter{
			UserID:   request.Request.UserID,
			Hash:     internal.NewIPFSHash(cid),
			Protocol: internal.ProtocolName,
		})
		if err != nil {
			return err
		}
		err = event.FireStorageObjectUploadedEvent(ctx, pin, request.Request.SourceIP)
		if err != nil {
			logger.Error("Failed to fire storage object uploaded event", zap.Error(err))
		}
	}

	return createTusCleanupTask(ctx, args.UploadID)
}

func CronTaskTusUploadCleanup(args *define.CronTaskTusUploadCleanupArgs, ctx core.Context) error {
	logger := ctx.Logger()
	_api := core.GetAPI(internal.ProtocolName).(*api.API)

	tusService := core.GetService[core.TUSService](ctx, core.TUS_SERVICE)

	// Get the request
	found, request := tusService.UploadExists(ctx, args.UploadID)
	if !found {
		logger.Error("Failed to get request")
		return fmt.Errorf("request not found")
	}

	err := _api.TusHandler().CompleteUpload(ctx, service.NewStorageHashFromMultihash(request.Request.UploadHash, request.Request.UploadHashCIDType, nil))
	if err != nil {
		return err
	}

	return nil
}

func createTusCleanupTask(ctx core.Context, uploadID string) error {
	cronService := core.GetService[core.CronService](ctx, core.CRON_SERVICE)
	return cronService.CreateJobIfNotExists(define.CronTaskTusUploadCleanupName, define.CronTaskTusUploadCleanupArgs{
		UploadID: uploadID,
	})
}
