package tasks

import (
	"fmt"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/define"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/service"
	"go.uber.org/zap"
	"io"
)

func CronTaskTusUpload(args *define.CronTaskTusUploadArgs, ctx core.Context) error {
	logger := ctx.Logger()
	tusService := core.GetService[core.TUSService](ctx, core.TUS_SERVICE)
	tusHandler := core.GetAPI(internal.ProtocolName).(*api.API).TusHandler()

	// Get the request
	found, request := tusService.UploadExists(ctx, args.UploadID)
	if !found {
		logger.Error("Failed to get request")
		return fmt.Errorf("request not found")
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
	err = processCar(ctx, reader, &request.Request)
	if err != nil {
		return err
	}

	return nil
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

	err := _api.TusHandler().CompleteUpload(ctx, service.NewStorageHashFromMultihash(request.Request.UploadHash, nil))
	if err != nil {
		return err
	}

	return nil
}
