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
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
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

	return roots[0], uploadId, nil
}

func (s *UploadServiceDefault) ProcessUpload(ctx context.Context, cids []cid.Cid, userId uint) error {
	if len(cids) == 0 {
		return fmt.Errorf("no CIDs provided")
	}

	// Calculate total size for quota check and cache CID sizes
	var totalSize uint64
	cidSizes := make(map[string]uint64, len(cids))
	for _, c := range cids {
		size, err := s.ipfs.(*protocol.Protocol).GetMetadataStore().Size(c)
		if err != nil {
			s.ctx.Logger().Warn("Failed to get size for quota check", zap.Stringer("cid", c), zap.Error(err))
			continue
		}
		cidSizes[c.String()] = size
		totalSize += size
	}

	// Validate upload quota
	if totalSize > 0 {
		result, err := quota.CheckUploadQuota(s.ctx, userId, totalSize)
		if err != nil {
			s.ctx.Logger().Warn("Failed to check upload quota", zap.Uint("user_id", userId), zap.Uint64("total_size", totalSize), zap.Error(err))
			return fmt.Errorf("failed to check upload quota: %w", err)
		}
		if result != nil && !result.Allowed {
			currentUsage := result.Details.CurrentUsage
			quotaLimit := uint64(0)
			if result.Details.Limit != nil {
				quotaLimit = *result.Details.Limit
			}
			s.ctx.Logger().Warn("Upload quota exceeded", zap.Uint("user_id", userId), zap.Uint64("total_size", totalSize), zap.Uint64("current_usage", currentUsage), zap.Uint64("quota_limit", quotaLimit))
			return fmt.Errorf("upload quota exceeded: current usage %d bytes + requested %d bytes would exceed quota limit of %d bytes", currentUsage, totalSize, quotaLimit)
		}
	}

	// Create upload records and core pin records for ALL CIDs (both roots and children)
	for _, c := range cids {
		// Get size for this CID from cached map
		size, exists := cidSizes[c.String()]
		if !exists {
			return fmt.Errorf("size not found for CID %s in cache", c.String())
		}

		// Create upload record for this CID
		uploadMeta := &models.Upload{
			UserID:   userId,
			Protocol: s.ipfs.Name(),
			Hash:     c.Hash(),
			CIDType:  c.Type(),
			Size:     size,
		}

		err := s.coreUpload.SaveUpload(ctx, uploadMeta)
		if err != nil {
			return fmt.Errorf("failed to save upload record for CID %s: %w", c.String(), err)
		}

		// Create core pin record for this CID
		pinMeta := &models.Pin{
			UploadID: uploadMeta.ID,
			UserID:   uploadMeta.UserID,
		}

		createdPin, err := s.corePin.CreatePin(ctx, pinMeta, nil)
		if err != nil {
			return fmt.Errorf("failed to create pin record for CID %s: %w", c.String(), err)
		}

		// Emit storage object pinned event for quota tracking
		clientIP := store.GetClientIP(ctx)
		quota.EmitStorageObjectPinned(s.ctx, createdPin, clientIP)

		// Emit upload completion event for quota tracking
		// Get client IP from context using the shared helper
		ip := store.GetClientIP(ctx)

		quota.EmitUploadCompleted(s.ctx, &userId, uploadMeta.ID, size, ip)
	}

	return nil
}

func (s *UploadServiceDefault) CreateRootPin(ctx context.Context, c cid.Cid, userId uint) (*pluginDb.IPFSPin, error) {
	// Create IPFS pin record for the root CID and return it
	ipfsPin, err := s.pin.AddPin(ctx, &pluginDb.IPFSPin{
		UserID:    userId,
		CID:       c.Bytes(),
		Name:      "",
		Origins:   nil,
		Meta:      nil,
		Delegates: nil,
		Info:      nil,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create IPFS pin record: %w", err)
	}

	return ipfsPin, nil
}

func (s *UploadServiceDefault) ID() string {
	return pluginCore.UPLOAD_SERVICE
}
