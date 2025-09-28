package upload

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"gorm.io/gorm"
)

var _ pluginCore.UploadService = (*UploadServiceDefault)(nil)

type UploadServiceDefault struct {
	ctx        core.Context
	db         *gorm.DB
	corePin    core.PinService
	coreUpload core.UploadService
	pin        pluginCore.IPFSPinService
	storage    core.StorageService
	ipfs       core.Protocol
}

func NewUploadService() (core.Service, []core.ContextBuilderOption, error) {
	_service := &UploadServiceDefault{}
	return _service, core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			_service.ctx = ctx
			_service.db = ctx.DB()
			_service.pin = core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
			_service.storage = core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
			_service.corePin = core.GetService[core.PinService](ctx, core.PIN_SERVICE)
			_service.coreUpload = core.GetService[core.UploadService](ctx, core.UPLOAD_SERVICE)
			_service.ipfs = core.GetProtocol(internal.ProtocolName)
			return nil
		}),
	), nil
}

func (s *UploadServiceDefault) HandleUpload(ctx context.Context, reader io.ReadSeekCloser, userId uint) (cid.Cid, string, error) {
	// Get the size of the reader
	size, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return cid.Undef, "", err
	}

	// Reset the reader to the beginning
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return cid.Undef, "", err
	}

	roots, err := internal.GetCarRoots(reader, false)
	if err != nil {
		return cid.Undef, "", err
	}
	// TODO: Handle multiple roots in the future?
	if len(roots) > 1 {
		// Either handle multiple roots or return error
		return cid.Undef, "", fmt.Errorf("CAR file has multiple roots, only single root CARs are supported")
	}

	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return cid.Undef, "", err
	}

	uploadId, err := s.storage.S3TemporaryUpload(ctx, reader, uint64(size), s.ipfs.(core.StorageProtocol))
	if err != nil {
		return cid.Undef, "", err
	}

	// TODO: check if we need to prevent duplication here or in AddPin
	_, err = s.pin.AddPin(ctx, &pluginDb.IPFSPin{
		UserID:    userId,
		CID:       roots[0].Bytes(),
		Name:      "",
		Origins:   nil,
		Meta:      nil,
		Delegates: nil,
		Info:      nil,
	})
	if err != nil {
		return cid.Undef, "", err
	}

	return roots[0], uploadId, nil
}

func (s *UploadServiceDefault) ProcessCIDs(ctx context.Context, cids []cid.Cid, userId uint) error {
	if len(cids) == 0 {
		return fmt.Errorf("no CIDs provided")
	}

	for _, _cid := range cids {
		size, err := s.ipfs.(*protocol.Protocol).GetMetadataStore().Size(_cid)
		if err != nil {
			return err
		}

		uploadMeta := &models.Upload{
			UserID:   userId,
			Protocol: s.ipfs.Name(),
			Hash:     _cid.Hash(),
			CIDType:  _cid.Type(),
			Size:     size,
		}

		// Create the upload record
		err = s.coreUpload.SaveUpload(ctx, uploadMeta)
		if err != nil {
			return fmt.Errorf("failed to save upload record: %w", err)
		}

		pinMeta := &models.Pin{
			UploadID: uploadMeta.ID,
			UserID:   uploadMeta.UserID,
		}

		// Create the core pin record
		_, err = s.corePin.CreatePin(ctx, pinMeta, nil)
		if err != nil {
			return fmt.Errorf("failed to create pin record: %w", err)
		}

		// Create the IPFS pin record
		_, err = s.pin.AddPin(ctx, &pluginDb.IPFSPin{
			UserID:    userId,
			CID:       _cid.Bytes(),
			Name:      "",
			Origins:   nil,
			Meta:      nil,
			Delegates: nil,
			Info:      nil,
		})
		if err != nil {
			return fmt.Errorf("failed to create IPFS pin record: %w", err)
		}
	}

	return nil
}

func (s *UploadServiceDefault) ID() string {
	return pluginCore.UPLOAD_SERVICE
}
