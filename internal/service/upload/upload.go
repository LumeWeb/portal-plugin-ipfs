package upload

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pluginErrors "go.lumeweb.com/portal-plugin-ipfs/internal/errors"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
)

var _ pluginCore.UploadService = (*UploadServiceDefault)(nil)

// UploadResult holds the result of an upload operation
type UploadResult struct {
	CID      cid.Cid
	UploadID string
}

type UploadServiceDefault struct {
	*core.BaseComponent
	corePin    core.PinService
	coreUpload core.UploadService
	pin        pluginCore.IPFSPinService
	storage    core.StorageService
	ipfs       protocol.ProtoNode

	processorFactory upload.UploadProcessorFactory
}

func NewUploadService() (core.Service, []core.ContextBuilderOption, error) {
	_service := &UploadServiceDefault{}
	return _service, core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			_service.pin = core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
			_service.storage = core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
			_service.corePin = core.GetService[core.PinService](ctx, core.PIN_SERVICE)
			_service.coreUpload = core.GetService[core.UploadService](ctx, core.UPLOAD_SERVICE)
			protoInterface := core.GetProtocol(internal.ProtocolName)
			_service.ipfs = protoInterface.(protocol.ProtoNode)

			// Create processor factory
			_service.processorFactory = upload.NewUploadProcessorFactory(ctx.Logger(), _service.storage, _service.ipfs)

			return nil
		}),
	), nil
}

func (s *UploadServiceDefault) HandleUpload(ctx context.Context, reader io.ReadSeekCloser, userId uint) (cid.Cid, string, error) {
	ctx, span := core.TraceMethod(ctx, "UploadServiceDefault.HandleUpload")
	defer span.End()

	result, err := core.MetricTrackResult(
		HandleUploadDuration.WithLabelValues(),
		HandleUploadTotal.WithLabelValues(LabelStatusError),
		func() (UploadResult, error) {
			cid, uploadID, err := s.HandleUploadWithMode(ctx, reader, userId, upload.ArchiveConvert)
			return UploadResult{CID: cid, UploadID: uploadID}, err
		},
	)
	return result.CID, result.UploadID, err
}

func (s *UploadServiceDefault) HandleUploadWithMode(ctx context.Context, reader io.ReadSeekCloser, userId uint, mode upload.ArchiveMode) (cid.Cid, string, error) {
	ctx, span := core.TraceMethod(ctx, "UploadServiceDefault.HandleUploadWithMode")
	defer span.End()

	modeLabel := LabelModeConvert
	if mode == upload.ArchivePreserve {
		modeLabel = LabelModePreserve
	}

	result, err := core.MetricTrackResult(
		HandleUploadWithModeDuration.WithLabelValues(),
		HandleUploadWithModeTotal.WithLabelValues(LabelStatusError, modeLabel),
		func() (UploadResult, error) {
			// Detect file format
			format, err := upload.DetectFormat(reader)
			if err != nil {
				// Check if it's an unsupported format error
				if upload.IsUploadErrorType(err, pluginErrors.UploadErrUnsupportedFormat) {
					return UploadResult{}, upload.NewUnsupportedFormatError(err)
				}
				return UploadResult{}, upload.NewCorruptedFileError(err)
			}

			_, err = reader.Seek(0, io.SeekStart)
			if err != nil {
				return UploadResult{}, fmt.Errorf("failed to reset reader for processing: %w", err)
			}

			processor, err := s.processorFactory.CreateProcessor(format, mode, s.Context(), userId)
			if err != nil {
				return UploadResult{}, upload.NewProcessorError(format.String(), mode.String(), err)
			}

			rootCID, uploadID, err := processor.Process(ctx, reader)
			if err != nil {
				// If it's already an UploadError, return it as-is using the helper function
				if _, ok := upload.AsUploadError(err); ok {
					return UploadResult{}, err
				}
				// Otherwise wrap it in a generic processing error
				return UploadResult{}, upload.NewProcessingError(err)
			}

			return UploadResult{CID: rootCID, UploadID: uploadID}, nil
		},
	)
	return result.CID, result.UploadID, err
}

func (s *UploadServiceDefault) ProcessUpload(ctx context.Context, cids []cid.Cid, userId uint, reservations map[cid.Cid]*quota.BlockReservations) error {
	ctx, span := core.TraceMethod(ctx, "UploadServiceDefault.ProcessUpload")
	defer span.End()

	return core.MetricTrack(
		ProcessUploadDuration.WithLabelValues(),
		ProcessUploadTotal.WithLabelValues(LabelStatusError),
		func() error {
			if len(cids) == 0 {
				return fmt.Errorf("no CIDs provided")
			}

			// Cache CID sizes
			cidSizes := make(map[string]uint64, len(cids))
			for _, c := range cids {
				size, err := s.ipfs.GetMetadataStore().Size(ctx, c)
				if err != nil {
					s.Context().Logger().Warn("Failed to get size for quota check", zap.Stringer("cid", c), zap.Error(err))
					continue
				}
				cidSizes[c.String()] = size
			}

			// Create upload records and core pin records for ALL CIDs (both roots and children)
			clientIP := pc.GetClientIP(ctx)
			for _, c := range cids {

				size, exists := cidSizes[c.String()]
				if !exists {
					return fmt.Errorf("size not found for CID %s in cache", c.String())
				}

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

				pinMeta := &models.Pin{
					UploadID: uploadMeta.ID,
					UserID:   uploadMeta.UserID,
				}

				createdPin, err := s.corePin.CreatePin(ctx, pinMeta, nil)
				if err != nil {
					return fmt.Errorf("failed to create pin record for CID %s: %w", c.String(), err)
				}

				// Get reservation IDs for this CID
				var uploadResID *string
				var storageResID *string
				if reservations != nil {
					if blockRes, exists := reservations[c]; exists && blockRes != nil {
						// Extract upload reservation ID
						if blockRes.UploadReservation != nil && blockRes.UploadReservation.Reservation != nil {
							uploadUUID := blockRes.UploadReservation.Reservation.UUID()
							uploadResID = &uploadUUID
						}
						// Extract storage reservation ID
						if blockRes.StorageReservation != nil && blockRes.StorageReservation.Reservation != nil {
							storageUUID := blockRes.StorageReservation.Reservation.UUID()
							storageResID = &storageUUID
						}
					}
				}

				// Emit storage object pinned event for quota tracking
				if clientIP == "" {
					s.Context().Logger().Warn("Client IP not set in context for quota tracking", zap.String("cid", c.String()))
				}
				quota.EmitStorageObjectPinned(core.DetachContext(ctx), s.Context(), createdPin, clientIP, storageResID)

				// Emit upload completion event for quota tracking
				quota.EmitUploadCompleted(core.DetachContext(ctx), s.Context(), &userId, uploadMeta.ID, size, clientIP, uploadResID, true)
			}

			return nil
		},
	)
}

func (s *UploadServiceDefault) CreateRootPin(ctx context.Context, c cid.Cid, userId uint) (*pluginDb.IPFSPin, error) {
	ctx, span := core.TraceMethod(ctx, "UploadServiceDefault.CreateRootPin")
	defer span.End()

	return core.MetricTrackResult(
		CreateRootPinDuration.WithLabelValues(),
		CreateRootPinTotal.WithLabelValues(LabelStatusError),
		func() (*pluginDb.IPFSPin, error) {
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
		},
	)
}

func (s *UploadServiceDefault) ID() string {
	return pluginCore.UPLOAD_SERVICE
}
