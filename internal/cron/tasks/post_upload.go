package tasks

import (
	"errors"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/define"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/event"
	"go.uber.org/zap"
)

func CronTaskPostUpload(args *define.CronTaskPostUploadArgs, ctx core.Context) error {
	storageService := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
	proto := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)
	requestService := core.GetService[core.RequestService](ctx, core.REQUEST_SERVICE)
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
		if errors.Is(err, core.ErrDuplicateRequest) {
			return createPostCleanupTask(ctx, req.ID, args.UploadID)
		}

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

	return createPostCleanupTask(ctx, req.ID, args.UploadID)
}

func CronTaskPostUploadCleanup(args *define.CronTaskPostUploadCleanupArgs, ctx core.Context) error {
	storageService := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
	proto := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)
	requestService := core.GetService[core.RequestService](ctx, core.REQUEST_SERVICE)

	request, err := requestService.GetRequest(ctx, args.RequestID)
	if err != nil {
		return err
	}

	if request.Status != models.RequestStatusDuplicate {
		err = requestService.CompleteRequest(ctx, args.RequestID)
		if err != nil {
			return err
		}
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

func createPostCleanupTask(ctx core.Context, reqID uint, uploadId string) error {
	cronService := core.GetService[core.CronService](ctx, core.CRON_SERVICE)

	err := cronService.CreateJobIfNotExists(define.CronTaskPostUploadCleanupName, define.CronTaskPostUploadCleanupArgs{
		RequestID: reqID,
		UploadID:  uploadId,
	})

	if err != nil {
		return err
	}

	return nil
}
