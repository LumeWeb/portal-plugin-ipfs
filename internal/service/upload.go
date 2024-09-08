package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/gookit/event"
	"github.com/icza/gox/gox"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car/v2"
	"github.com/samber/lo"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/messages"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/define"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/types"
	_event "go.lumeweb.com/portal/event"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"io"
	"strings"
	"time"
)

const UPLOAD_SERVICE = "ipfs_upload_service"

var _ core.Service = (*UploadService)(nil)

type UploadService struct {
	ctx             core.Context
	db              *gorm.DB
	cron            core.CronService
	metadataService core.MetadataService
	storageService  core.StorageService
	ipfs            *protocol.Protocol
	pin             core.PinService
	requests        core.RequestService
	logger          *core.Logger
}

func NewUploadService() (core.Service, []core.ContextBuilderOption, error) {
	_service := &UploadService{}
	return _service, core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			_service.ctx = ctx
			_service.db = ctx.DB()
			_service.cron = core.GetService[core.CronService](ctx, core.CRON_SERVICE)
			_service.metadataService = core.GetService[core.MetadataService](ctx, core.METADATA_SERVICE)
			_service.storageService = core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
			_service.pin = core.GetService[core.PinService](ctx, core.PIN_SERVICE)
			_service.requests = core.GetService[core.RequestService](ctx, core.REQUEST_SERVICE)

			_service.logger = ctx.ServiceLogger(_service)

			ctx.Event().On(_event.EVENT_BOOT_COMPLETE, event.ListenerFunc(func(e event.Event) error {
				_, ok := e.(*_event.BootCompleteEvent)
				if !ok {
					return fmt.Errorf("invalid event type")
				}

				_service.ipfs = core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)

				return nil
			}))
			return nil
		}),
	), nil
}

func (s *UploadService) ID() string {
	return UPLOAD_SERVICE
}

func (s *UploadService) CreateQueuedPin(ctx context.Context, c cid.Cid, userId uint, uploaderIP string, name string, isInternal bool, parentRequestID *uuid.UUID, createCron bool) (*pluginDb.IPFSPinView, error) {
	existingPin, err := s.getExistingPinForUser(ctx, c, userId)
	if err != nil {
		return nil, fmt.Errorf("error checking existing pin: %w", err)
	}
	if existingPin != nil {
		return existingPin, nil
	}

	hash := internal.NewIPFSHash(c)

	reqId := types.BinaryUUID(uuid.New())

	_, err = s.requests.CreateRequest(ctx, &models.Request{
		Protocol:  internal.ProtocolName,
		Operation: models.RequestOperationPin,
		Hash:      hash.Multihash(),
		HashType:  hash.Type(),
		UserID:    userId,
		SourceIP:  uploaderIP,
		CIDType:   c.Type(),
	}, &pluginDb.IPFSRequest{
		Name:               name,
		Internal:           isInternal,
		PinRequestID:       reqId,
		ParentPinRequestID: (*types.BinaryUUID)(parentRequestID),
	}, nil)
	if err != nil {
		return nil, err
	}

	var pinRecord pluginDb.IPFSPinView

	if err = db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.WithContext(ctx).Model(&pluginDb.IPFSPinView{}).Where(&pluginDb.IPFSPinView{PinRequestID: reqId}).First(&pluginDb.IPFSPinView{})
	}); err != nil {
		return nil, fmt.Errorf("error creating pin: %w", err)
	}

	if createCron {
		err = s.cron.CreateJobIfNotExists(define.CronTaskPinName, define.CronTaskPinArgs{RequestID: uuid.UUID(reqId)})
		if err != nil {
			return nil, fmt.Errorf("error creating cron job: %w", err)
		}
	}

	return &pinRecord, nil
}

func (s *UploadService) CreatePinnedPin(ctx context.Context, c cid.Cid, operation models.RequestOperationType, size uint64, userId uint, uploaderIP string, name string, isInternal, isRoot bool, requestID, parentRequestID *uuid.UUID) (*pluginDb.IPFSPinView, bool, error) {
	hash := internal.NewIPFSHash(c)

	existing := false

	if requestID == nil || *requestID == uuid.Nil {
		_uuid := uuid.New()
		requestID = &_uuid
	}

	request, err := s.requests.QueryRequest(ctx, &models.Request{
		Hash:     hash.Multihash(),
		HashType: hash.Type(),
	}, core.RequestFilter{
		Protocol:  internal.ProtocolName,
		Operation: operation,
		UserID:    userId,
	})
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, err
	}

	if request != nil {
		existing = true
		ipfsReq, err := s.requests.GetProtocolData(ctx, request.ID)
		if err != nil {
			return nil, existing, err
		}

		ipfsReqData := ipfsReq.(*pluginDb.IPFSRequest)
		requestID = (*uuid.UUID)(&ipfsReqData.PinRequestID)
		parentRequestID = (*uuid.UUID)(ipfsReqData.ParentPinRequestID)
	}

	if !isRoot && request == nil {
		_, err := s.requests.CreateRequest(ctx, &models.Request{
			Status:    models.RequestStatusCompleted,
			Protocol:  internal.ProtocolName,
			Operation: operation,
			UserID:    userId,
			SourceIP:  uploaderIP,
			CIDType:   c.Type(),
			System:    true,
			Hash:      hash.Multihash(),
			HashType:  hash.Type(),
		}, &pluginDb.IPFSRequest{
			PinRequestID:       types.BinaryUUID(*requestID),
			ParentPinRequestID: (*types.BinaryUUID)(parentRequestID),
			Internal:           true,
		}, nil)

		if err != nil {
			return nil, existing, err
		}
	}

	err = s.metadataService.SaveUpload(ctx, core.UploadMetadata{
		UserID:     userId,
		Hash:       hash.Multihash(),
		HashType:   hash.Type(),
		Protocol:   internal.ProtocolName,
		UploaderIP: uploaderIP,
		Size:       size,
	})
	if err != nil {
		return nil, existing, err
	}

	s.logger.Debug("Creating pinned pin", zap.String("hash", c.String()), zap.String("requestID", requestID.String()), zap.String("name", name), zap.Bool("internal", isInternal))

	err = s.pin.PinByHash(hash, userId, &pluginDb.IPFSPin{
		Name:            name,
		Internal:        isInternal,
		RequestID:       types.BinaryUUID(*requestID),
		ParentRequestID: (*types.BinaryUUID)(parentRequestID),
	})

	if err != nil {
		return nil, existing, err
	}

	pin, err := s.GetPinByIdentifier(ctx, hash)
	if err != nil {
		return nil, existing, err
	}

	if pin == nil {
		return nil, existing, errors.New("pin not found")
	}

	s.logger.Debug("Created pinned pin", zap.String("hash", c.String()), zap.String("requestID", requestID.String()), zap.String("name", name), zap.Bool("internal", isInternal))

	return pin, existing, nil
}

func (s *UploadService) GetPinByIdentifier(ctx context.Context, identifier interface{}) (*pluginDb.IPFSPinView, error) {
	var pinQuery pluginDb.IPFSPinView

	switch v := identifier.(type) {
	case uuid.UUID:
		pinQuery.PinRequestID = types.BinaryUUID(v)
	case core.StorageHash:
		pinQuery.Hash = v.Multihash()
		pinQuery.HashType = v.Type()
	default:
		return nil, fmt.Errorf("unsupported identifier type")
	}

	return s.getPinEntityByQuery(ctx, pinQuery)
}

func (s *UploadService) getPinEntityByQuery(ctx context.Context, query pluginDb.IPFSPinView) (*pluginDb.IPFSPinView, error) {
	var pin pluginDb.IPFSPinView

	if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.WithContext(ctx).
			Model(&pin).
			Where(&query).
			First(&pin)
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("error fetching pin: %w", err)
	}

	return &pin, nil
}

func (s *UploadService) DeletePin(ctx context.Context, id uuid.UUID) error {
	pin, err := s.GetPinByIdentifier(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to fetch pin: %w", err)
	}
	if pin == nil {
		return fmt.Errorf("pin not found")
	}

	if internal.RequestStatusToPinStatus(pin.Status) == pluginDb.PinningStatusPinned {
		err = s.pin.DeletePin(ctx, pin.PinID)
		if err != nil {
			return fmt.Errorf("failed to unpin: %w", err)
		}
	}

	return s.DeletePinRequest(ctx, id)
}

func (s *UploadService) DeletePinRequest(ctx context.Context, id uuid.UUID) error {
	data, err := s.requests.QueryProtocolData(ctx, internal.ProtocolName, &pluginDb.IPFSRequest{
		PinRequestID: types.BinaryUUID(id),
	}, core.RequestFilter{
		Protocol: internal.ProtocolName,
	})

	if err != nil {
		return err
	}

	if data == nil {
		pin, err := s.pin.QueryProtocolPin(ctx, internal.ProtocolName, &pluginDb.IPFSPin{
			RequestID: types.BinaryUUID(id),
		}, core.PinFilter{Protocol: internal.ProtocolName})
		if err != nil {
			return err
		}

		if pin == nil {
			return nil
		}

		pinData := pin.(*pluginDb.IPFSPin)

		err = s.pin.DeletePin(ctx, pinData.PinID)
		if err != nil {
			return err
		}
	}

	reqData := data.(*pluginDb.IPFSRequest)

	err = s.requests.DeleteRequest(ctx, reqData.RequestID)
	if err != nil {
		return err
	}

	return nil
}

func (s *UploadService) GetPins(ctx context.Context, req messages.GetPinsRequest, userId uint) (*messages.PinResults, error) {
	var pins []pluginDb.IPFSPinView

	filterOpts := pinFilterOptions{
		UserID: userId,
		CIDs:   req.CID,
		Name:   req.Name,
		Match:  req.Match,
		Status: lo.Uniq(lo.Map(req.Status, func(status string, _ int) string {
			return string(internal.PinStatusToRequestStatus(pluginDb.PinningStatus(status)))
		})),
		Before:      req.Before,
		After:       req.After,
		BeforeCount: req.BeforeCount,
		AfterCount:  req.AfterCount,
		Limit:       req.Limit,
		Partial:     gox.NewBool(false),
	}

	// Apply general filters
	query := s.ctx.DB().WithContext(ctx).Model(&pluginDb.IPFSPinView{}).Scopes(applyGeneralPinFilters(filterOpts))

	// Count total matching records
	var totalCount int64
	if err := query.Count(&totalCount).Error; err != nil {
		return nil, fmt.Errorf("failed to count pins: %w", err)
	}

	// Apply paging filters
	query = query.Scopes(applyPagingPinFilters(filterOpts))

	if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return query.Find(&pins)
	}); err != nil {
		return nil, fmt.Errorf("failed to fetch pins: %w", err)
	}

	// Convert to PinStatus
	results := &messages.PinResults{
		Count:   uint64(totalCount),
		Results: make([]*messages.PinStatus, len(pins)),
	}

	for i, pin := range pins {
		results.Results[i] = s.convertToPinStatus(&pin)
	}

	return results, nil
}

func (s *UploadService) GetPinStatus(ctx context.Context, id uuid.UUID) (*messages.PinStatus, error) {
	pin, err := s.GetPinByIdentifier(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pin: %w", err)
	}
	if pin == nil {
		return nil, fmt.Errorf("pin not found")
	}
	return s.convertToPinStatus(pin), nil
}

func (s *UploadService) GetChildPins(ctx context.Context, parentID any) ([]*pluginDb.IPFSPinView, error) {
	var query pluginDb.IPFSPinView

	switch v := parentID.(type) {
	case uuid.UUID:
		query.ParentPinRequestID = (*types.BinaryUUID)(&v)
	case uint:
		query.RequestID = v
	default:
		return nil, fmt.Errorf("unsupported parent ID type")
	}

	var childPins []*pluginDb.IPFSPinView
	if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.WithContext(ctx).
			Model(&query).
			Where(&query).
			Find(&childPins)
	}); err != nil {
		return nil, fmt.Errorf("failed to get child pins: %w", err)
	}
	return childPins, nil
}

func (s *UploadService) AddQueuedPin(ctx context.Context, pin messages.Pin, userId uint, uploaderIP string) (*messages.PinStatus, error) {
	c, err := cid.Decode(pin.CID)
	if err != nil {
		return nil, fmt.Errorf("invalid CID: %w", err)
	}

	ipfsPin, err := s.CreateQueuedPin(ctx, c, userId, uploaderIP, pin.Name, false, nil, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create pin: %w", err)
	}

	return s.convertToPinStatus(ipfsPin), nil
}

func (s *UploadService) ReplacePin(ctx context.Context, id uuid.UUID, newPin messages.Pin) (*messages.PinStatus, error) {
	var newStatus *messages.PinStatus

	oldPin, err := s.GetPinByIdentifier(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch old pin: %w", err)
	}
	if oldPin == nil {
		return nil, fmt.Errorf("pin not found")
	}

	if requestStatusToPinStatus(oldPin.Status) != pluginDb.PinningStatusPinned {
		return nil, fmt.Errorf("can only replace pinned content")
	}
	if err = s.DeletePin(ctx, id); err != nil {
		return nil, fmt.Errorf("failed to delete old pin: %w", err)
	}

	// Add the new pin
	newCid, err := cid.Decode(newPin.CID)
	if err != nil {
		return nil, fmt.Errorf("invalid new CID: %w", err)
	}

	newIPFSPin, err := s.CreateQueuedPin(ctx, newCid, oldPin.UserID, oldPin.UploaderIP, newPin.Name, oldPin.Internal, nil, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create new pin: %w", err)
	}

	newStatus = s.convertToPinStatus(newIPFSPin)
	return newStatus, nil
}

func (s *UploadService) UpdatePinnedPinParent(ctx context.Context, requestID, parentRequestID uuid.UUID) error {
	reqData, err := s.requests.QueryProtocolData(ctx, internal.ProtocolName, &pluginDb.IPFSRequest{
		PinRequestID: types.BinaryUUID(requestID),
	}, core.RequestFilter{Protocol: internal.ProtocolName})

	if err != nil {
		return fmt.Errorf("failed to fetch request: %w", err)
	}

	reqDataItem := reqData.(*pluginDb.IPFSRequest)

	err = s.requests.UpdateProtocolData(ctx, reqDataItem.RequestID, &pluginDb.IPFSRequest{
		ParentPinRequestID: (*types.BinaryUUID)(&parentRequestID),
	})

	if err != nil {
		return fmt.Errorf("failed to update request parent: %w", err)
	}

	pinData, err := s.pin.QueryProtocolPin(ctx, internal.ProtocolName, &pluginDb.IPFSPin{
		RequestID: types.BinaryUUID(requestID),
	}, core.PinFilter{Protocol: internal.ProtocolName})

	if err != nil {
		return fmt.Errorf("failed to fetch pin: %w", err)
	}

	pinDataItem := pinData.(*pluginDb.IPFSPin)

	err = s.pin.UpdateProtocolPin(ctx, pinDataItem.PinID, &pluginDb.IPFSPin{
		ParentRequestID: (*types.BinaryUUID)(&parentRequestID),
	})
	if err != nil {
		return fmt.Errorf("failed to update pin parent: %w", err)
	}

	return nil
}

func (s *UploadService) HandlePostUpload(ctx context.Context, reader io.ReadSeekCloser, userId uint, uploaderIP string) error {
	// Get the size of the reader
	size, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to get reader size: %w", err)
	}

	// Reset the reader to the beginning
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to reset reader: %w", err)
	}

	carReader, err := car.NewReader(reader.(io.ReaderAt))
	if err != nil {
		return fmt.Errorf("failed to read car: %w", err)
	}

	_, err = carReader.Inspect(true)
	if err != nil {
		return fmt.Errorf("failed to read car: %w", err)
	}

	roots, err := carReader.Roots()
	if err != nil {
		return fmt.Errorf("failed to read car: %w", err)
	}

	if len(roots) == 0 {
		return fmt.Errorf("no roots found in car")
	}

	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to reset reader: %w", err)
	}

	// Create a new SHAReader
	hashReader := NewSHAReader(reader)

	upload, err := s.storageService.S3TemporaryUpload(ctx, hashReader, uint64(size), s.ipfs)
	if err != nil {
		return err
	}

	// Create a new request
	err = s.createUploadRequest(ctx, hashReader.CID(), roots[0], userId, uploaderIP, upload)

	if err != nil {
		return fmt.Errorf("failed to create pin: %w", err)
	}

	return nil
}

func (s *UploadService) PinRequestStatusQueued(ctx context.Context, id any) error {
	s.logger.Debug("Setting pin status to Queued", zap.Any("id", id))
	return s.UpdatePinRequestStatus(ctx, id, pluginDb.PinningStatusQueued)
}

func (s *UploadService) PinRequestStatusPinning(ctx context.Context, id any) error {
	s.logger.Debug("Setting pin status to Pinning", zap.Any("id", id))
	return s.UpdatePinRequestStatus(ctx, id, pluginDb.PinningStatusPinning)
}

func (s *UploadService) PinRequestStatusPinned(ctx context.Context, id any) error {
	s.logger.Debug("Setting pin status to Pinned", zap.Any("id", id))
	err := s.UpdatePinRequestStatus(ctx, id, pluginDb.PinningStatusPinned)
	if err != nil {
		return err
	}
	// Trigger reprovider for the newly pinned content
	s.ipfs.GetNode().TriggerReprovider()

	// Update child pins if this is a parent pin
	childPins, err := s.GetChildPins(ctx, id)
	if err != nil {
		s.logger.Warn("Failed to fetch child pins", zap.Error(err), zap.Any("id", id))
	} else {
		for _, childPin := range childPins {
			if internal.RequestStatusToPinStatus(childPin.Status) != pluginDb.PinningStatusPinned {
				err = s.PinRequestStatusPinned(ctx, uuid.UUID(childPin.PinRequestID))
				if err != nil {
					s.logger.Warn("Failed to update child pin status", zap.Error(err), zap.String("childID", childPin.PinRequestID.String()))
				}
			}
		}
	}

	switch v := id.(type) {
	case uint:
		err = s.requests.DeleteRequest(ctx, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *UploadService) PinRequestStatusFailed(ctx context.Context, id any) error {
	s.logger.Debug("Setting pin status to Failed", zap.Any("id", id))
	err := s.UpdatePinRequestStatus(ctx, id, pluginDb.PinningStatusFailed)
	if err != nil {
		return err
	}
	// Update child pins if this is a parent pin
	childPins, err := s.GetChildPins(ctx, id)
	if err != nil {
		s.logger.Warn("Failed to fetch child pins", zap.Error(err), zap.Any("id", id))
	} else {
		for _, childPin := range childPins {
			if internal.RequestStatusToPinStatus(childPin.Status) != pluginDb.PinningStatusFailed {
				err = s.PinRequestStatusFailed(ctx, uuid.UUID(childPin.PinRequestID))
				if err != nil {
					s.logger.Warn("Failed to update child pin status", zap.Error(err), zap.String("childID", childPin.PinRequestID.String()))
				}
			}
		}
	}

	return nil
}

func (s *UploadService) UpdatePinRequestStatus(ctx context.Context, id any, status pluginDb.PinningStatus) error {
	query := &pluginDb.IPFSRequest{}

	switch v := id.(type) {
	case uuid.UUID:
		query.PinRequestID = types.BinaryUUID(v)
	case uint:
		query.RequestID = v
	}

	data, err := s.requests.QueryProtocolData(ctx, internal.ProtocolName, query, core.RequestFilter{
		Protocol: internal.ProtocolName,
	})
	if err != nil {
		return err
	}

	pinRequest := data.(*pluginDb.IPFSRequest)

	err = s.requests.UpdateRequestStatus(ctx, pinRequest.RequestID, internal.PinStatusToRequestStatus(status))
	if err != nil {
		return err
	}

	return nil
}

func (s *UploadService) CompletePin(ctx context.Context, pin *pluginDb.IPFSPinView, node format.Node) error {
	hash := internal.NewIPFSHash(node.Cid())
	err := s.metadataService.SaveUpload(ctx, core.UploadMetadata{
		UserID:     pin.UserID,
		Hash:       pin.Hash,
		HashType:   pin.HashType,
		Protocol:   internal.ProtocolName,
		UploaderIP: pin.UploaderIP,
		Size:       uint64(len(node.RawData())),
	})
	if err != nil {
		return fmt.Errorf("error saving upload metadata: %w", err)
	}

	err = s.pin.PinByHash(hash, pin.UserID, &pluginDb.IPFSPin{
		Name: pin.Name,
	})
	if err != nil {
		return fmt.Errorf("error pinning: %w", err)
	}

	return nil
}

func (s *UploadService) UpdatePinParent(ctx context.Context, requestID uuid.UUID, parentRequestID uuid.UUID) error {

	err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.WithContext(ctx).
			Model(&pluginDb.IPFSPin{}).
			Where("request_id = ?", types.BinaryUUID(requestID)).
			Update("parent_request_id", types.BinaryUUID(parentRequestID))
	})

	if err != nil {
		return fmt.Errorf("failed to update pin parent: %w", err)
	}

	return nil
}

func (s *UploadService) SetTusUploadRequestID(ctx context.Context, requestId uint) error {
	err := s.requests.UpdateProtocolData(ctx, requestId, &pluginDb.IPFSRequest{PinRequestID: types.BinaryUUID(uuid.New())})
	if err != nil {
		return err
	}

	return nil
}

func (s *UploadService) DetectUpdatePartialStatus(ctx context.Context, block blocks.Block) error {
	hash := internal.NewIPFSHash(block.Cid())

	pin, err := s.pin.QueryProtocolPin(ctx, internal.ProtocolName, nil, core.PinFilter{Protocol: internal.ProtocolName, Hash: hash})
	if err != nil {
		return err
	}

	if pin == nil {
		return errors.New("pin not found")
	}

	pinData := pin.(*pluginDb.IPFSPin)

	partial, err := internal.DetectPartialFile(ctx, block)
	if err != nil {
		return err
	}

	err = s.pin.UpdateProtocolPin(ctx, pinData.PinID, &pluginDb.IPFSPin{Partial: partial})
	if err != nil {
		return err
	}

	return nil
}

func (s *UploadService) createUploadRequest(ctx context.Context, uploadCid, c cid.Cid, userId uint, uploaderIP string, uploadId string) error {
	uploadHash := internal.NewIPFSHash(c)
	hash := internal.NewIPFSHash(c)

	req, err := s.requests.CreateRequest(ctx, &models.Request{
		Protocol:   internal.ProtocolName,
		Operation:  models.RequestOperationUpload,
		UserID:     userId,
		SourceIP:   uploaderIP,
		CIDType:    uploadCid.Type(),
		UploadHash: uploadHash.Multihash(),
		Hash:       hash.Multihash(),
		HashType:   hash.Type(),
	}, &pluginDb.IPFSRequest{
		PinRequestID: types.BinaryUUID(uuid.New()),
	}, nil)
	if err != nil {
		return err
	}

	return s.cron.CreateJobIfNotExists(define.CronTaskPostUploadName, define.CronTaskPostUploadArgs{RequestID: req.ID, UploadID: uploadId})
}

func (s *UploadService) convertToPinStatus(imp *pluginDb.IPFSPinView) *messages.PinStatus {
	var c cid.Cid
	if len(imp.Hash) > 0 {
		c, _ = internal.CIDFromHash(imp.Hash)
	} else {
		c = cid.Undef
	}
	return &messages.PinStatus{
		RequestID: imp.PinRequestID.String(),
		Status:    requestStatusToPinStatus(imp.Status),
		Created:   imp.CreatedAt,
		Pin: messages.Pin{
			CID:  c.String(),
			Name: imp.Name,
		},
		Delegates: []string{}, // TODO: Implement delegates
		Info:      map[string]string{},
	}
}

func (s *UploadService) convertFromPinStatus(status *messages.PinStatus) (*pluginDb.IPFSPinView, error) {
	requestID, err := uuid.Parse(status.RequestID)
	if err != nil {
		return nil, fmt.Errorf("invalid request ID: %w", err)
	}

	c, err := cid.Decode(status.Pin.CID)
	if err != nil {
		return nil, fmt.Errorf("invalid CID: %w", err)
	}

	hash := internal.NewIPFSHash(c)

	return &pluginDb.IPFSPinView{
		PinRequestID: types.BinaryUUID(requestID),
		Name:         status.Pin.Name,
		Hash:         hash.Multihash(),
		HashType:     hash.Type(),
		Status:       pinStatusToRequestStatus(status.Status),
	}, nil
}

func (s *UploadService) getExistingPinForUser(ctx context.Context, c cid.Cid, userId uint) (*pluginDb.IPFSPinView, error) {
	var pin pluginDb.IPFSPinView

	hash := internal.NewIPFSHash(c)

	err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.WithContext(ctx).
			Where(&pluginDb.IPFSPinView{Hash: hash.Multihash(), HashType: hash.Type(), UserID: userId}).
			First(&pin)
	})

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("error querying existing pin: %w", err)
	}

	return &pin, nil
}

func (s *UploadService) GetBlockMeta(ctx context.Context, c cid.Cid) (*messages.BlockMetaResponse, error) {
	var unixFSNode pluginDb.UnixFSNode
	if err := db.RetryableTransaction(s.ctx, s.db, func(tx *gorm.DB) *gorm.DB {
		return tx.WithContext(ctx).
			Model(&pluginDb.UnixFSNode{}).
			Preload("Block"). // This will automatically join with IPFSBlock
			Joins("Block").   // This ensures the join condition is included in the main query
			Where("Block.cid = ?", c.Bytes()).
			First(&unixFSNode)
	}); err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get block meta: %w", err)
	}

	return &messages.BlockMetaResponse{
		Type:      unixFSNode.Type,
		BlockSize: unixFSNode.BlockSize,
		ChildCID: lo.Map(unixFSNode.ChildCID, func(c cid.Cid, _ int) string {
			return encoding.ToV1(c).String()
		}),
	}, nil
}
func requestStatusToPinStatus(status models.RequestStatusType) pluginDb.PinningStatus {
	switch status {
	case models.RequestStatusPending:
		return pluginDb.PinningStatusQueued
	case models.RequestStatusProcessing:
		return pluginDb.PinningStatusPinning
	case models.RequestStatusCompleted:
		return pluginDb.PinningStatusPinned
	case models.RequestStatusFailed:
		return pluginDb.PinningStatusFailed
	default:
		return pluginDb.PinningStatusQueued
	}
}

func pinStatusToRequestStatus(status pluginDb.PinningStatus) models.RequestStatusType {
	switch status {
	case pluginDb.PinningStatusQueued:
		return models.RequestStatusPending
	case pluginDb.PinningStatusPinning:
		return models.RequestStatusProcessing
	case pluginDb.PinningStatusPinned:
		return models.RequestStatusCompleted
	case pluginDb.PinningStatusFailed:
		return models.RequestStatusFailed
	default:
		return models.RequestStatusPending
	}
}

type pinFilterOptions struct {
	UserID      uint
	CIDs        []string
	Name        string
	Match       string
	Status      []string
	Before      *time.Time
	After       *time.Time
	BeforeCount *int64
	AfterCount  *int64
	Limit       int
	Internal    *bool
	Partial     *bool
}

func applyGeneralPinFilters(opts pinFilterOptions) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if opts.UserID != 0 {
			db = db.Where("user_id = ?", opts.UserID)
		}

		if len(opts.CIDs) > 0 {
			db = db.Where("hash IN ?", opts.CIDs)
		}

		if opts.Name != "" {
			switch strings.ToLower(opts.Match) {
			case "iexact":
				db = db.Where("LOWER(name) = LOWER(?)", opts.Name)
			case "partial":
				db = db.Where("name LIKE ?", "%"+opts.Name+"%")
			case "ipartial":
				db = db.Where("LOWER(name) LIKE LOWER(?)", "%"+opts.Name+"%")
			default: // exact
				db = db.Where("name = ?", opts.Name)
			}
		}

		if len(opts.Status) > 0 {
			db = db.Where("status IN ?", opts.Status)
		}

		if opts.Internal != nil {
			db = db.Where("internal = ?", opts.Internal)
		}

		if opts.Partial != nil {
			db = db.Where("partial = ?", opts.Partial)
		}

		db = db.Order("created_at DESC")

		return db
	}
}

func applyPagingPinFilters(opts pinFilterOptions) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		// Exclusive filtering: either time-based or count-based
		if opts.Before != nil || opts.After != nil {
			// Time-based filtering
			if opts.Before != nil {
				db = db.Where("created_at < ?", opts.Before)
			}
			if opts.After != nil {
				db = db.Where("created_at > ?", opts.After)
			}
		} else if opts.BeforeCount != nil || opts.AfterCount != nil {
			// Count-based filtering
			if opts.BeforeCount != nil {
				db = db.Where("id < ?", *opts.BeforeCount)
			}
			if opts.AfterCount != nil {
				db = db.Where("id > ?", *opts.AfterCount)
			}
		}

		if opts.Limit > 0 {
			db = db.Limit(opts.Limit)
		} else {
			db = db.Limit(10) // default limit
		}

		return db
	}
}
