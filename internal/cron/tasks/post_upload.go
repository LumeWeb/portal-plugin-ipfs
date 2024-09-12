package tasks

import (
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/define"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/event"
	"go.uber.org/zap"
)

func CronTaskPostUpload(args *define.CronTaskPostUploadArgs, ctx core.Context) error {
	storageService := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
	proto := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)
	requestService := core.GetService[core.RequestService](ctx, core.REQUEST_SERVICE)
	cronService := core.GetService[core.CronService](ctx, core.CRON_SERVICE)
	pinService := core.GetService[core.PinService](ctx, core.PIN_SERVICE)
	logger := ctx.Logger()

	req, err := requestService.GetRequest(ctx, args.RequestID)
	if err != nil {
		return err
	}

	// Get the request
	upload, err := storageService.S3GetTemporaryUpload(ctx, proto, args.UploadID)
	if err != nil {
		return err
	}

	// Process the car
	cids, err := processCar(ctx, upload, req)
	if err != nil {
		return err
	}

	for _, cid := range cids {
		pin, err := pinService.QueryPin(ctx, nil, core.PinFilter{
			UserID:   req.UserID,
			Hash:     internal.NewIPFSHash(cid),
			Protocol: internal.ProtocolName,
		})
		if err != nil {
			return err
		}

		err = event.FireStorageObjectUploadedEvent(ctx, pin, req.SourceIP)
		if err != nil {
			logger.Error("Failed to fire storage object uploaded event", zap.Error(err))
		}
	}

	// Schedule the cleanup task
	err = cronService.CreateJobIfNotExists(define.CronTaskPostUploadCleanupName, define.CronTaskPostUploadCleanupArgs{
		RequestID: req.ID,
		UploadID:  args.UploadID,
	})

	if err != nil {
		return err
	}

	return nil
}

func CronTaskPostUploadCleanup(args *define.CronTaskPostUploadCleanupArgs, ctx core.Context) error {
	storageService := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
	proto := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)
	requestService := core.GetService[core.RequestService](ctx, core.REQUEST_SERVICE)

	err := requestService.CompleteRequest(ctx, args.RequestID)
	if err != nil {
		return err
	}

	err = requestService.DeleteRequest(ctx, args.RequestID)
	if err != nil {
		return err
	}

	err = storageService.S3DeleteTemporaryUpload(ctx, proto, args.UploadID)
	if err != nil {
		return err
	}

	return nil
}
