package pin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-cid"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.lumeweb.com/portal/db/types"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	"go.uber.org/zap"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// PinServiceDefault implements the IPFSPinService interface
type PinServiceDefault struct {
	ctx      core.Context
	db       *gorm.DB
	logger   *core.Logger
	workflow core.WorkflowService
	ipfs     *protocol.Protocol
	pinSvc   core.PinService
}

// Ensure PinServiceDefault implements the interface
var _ pluginCore.IPFSPinService = (*PinServiceDefault)(nil)

// NewPinService creates a new pin service
func NewPinService() (core.Service, []core.ContextBuilderOption, error) {
	svc := &PinServiceDefault{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			svc.ctx = ctx
			svc.logger = ctx.ServiceLogger(svc)
			svc.db = ctx.DB()
			svc.workflow = core.GetService[core.WorkflowService](ctx, core.WORKFLOW_SERVICE)
			svc.pinSvc = core.GetService[core.PinService](ctx, core.PIN_SERVICE)
			proto := core.GetProtocol(internal.ProtocolName)
			ipfsProto, ok := proto.(*protocol.Protocol)
			if !ok {
				return fmt.Errorf("protocol %s is not of type *protocol.Protocol", internal.ProtocolName)
			}
			svc.ipfs = ipfsProto
			return nil
		}),
	)

	return svc, opts, nil
}

func (s *PinServiceDefault) ID() string {
	return pluginCore.PIN_SERVICE
}

// AddPin creates a new pin job record.
func (s *PinServiceDefault) AddPin(ctx context.Context, pin *pluginDb.IPFSPin) (*pluginDb.IPFSPin, error) {
	// Get delegate addresses and store them as JSON
	err := s.addDelegateAddresses(pin)
	if err != nil {
		s.logger.Error("Failed to get delegate addresses", zap.Error(err))
	}

	err = db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		return g.WithContext(ctx).Create(pin)
	})
	if err != nil {
		s.logger.Error("Failed to add pin", zap.Error(err), zap.Any("pin", pin))
		return nil, fmt.Errorf("failed to add pin: %w", err)
	}

	s.logger.Debug("Added pin", zap.String("request_id", pin.RequestID.String()), zap.Stringer("cid", cid.MustParse(pin.CID)))
	return pin, nil
}

// GetPinByRequestID retrieves a single pin job by its unique RequestID.
func (s *PinServiceDefault) GetPinByRequestID(ctx context.Context, requestID types.BinaryUUID) (*pluginDb.IPFSPin, error) {
	var pin pluginDb.IPFSPin

	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		return g.WithContext(ctx).Where("request_id = ?", requestID).First(&pin)
	})

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			s.logger.Debug("Pin not found", zap.String("request_id", requestID.String()))
			return nil, nil
		}
		s.logger.Error("Failed to get pin by request ID",
			zap.Error(err),
			zap.String("request_id", requestID.String()))
		return nil, fmt.Errorf("failed to get pin by request ID: %w", err)
	}

	s.logger.Debug("Retrieved pin",
		zap.String("request_id", pin.RequestID.String()),
		zap.Binary("cid", pin.CID))
	return &pin, nil
}

// ListPins retrieves a paginated and filtered list of pin jobs.
func (s *PinServiceDefault) ListPins(ctx context.Context, filter []queryutil.CrudFilter, sort []filter.Sort, pagination queryutil.Pagination) ([]*pluginDb.IPFSPin, int64, error) {
	var pins []*pluginDb.IPFSPin
	var total int64

	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		// Construct the query
		query := g.WithContext(ctx).Model(&pluginDb.IPFSPin{})
		query = queryutil.ApplyFilters(query, filter, nil)
		query = queryutil.ApplySort(query, sort)
		query = queryutil.ApplyPagination(query, pagination)

		// Get total count
		if err := query.Count(&total).Error; err != nil {
			_ = g.AddError(fmt.Errorf("failed to count pins: %w", err))
			return g
		}

		// Get the records
		if err := query.Find(&pins).Error; err != nil {
			_ = g.AddError(fmt.Errorf("failed to list pins: %w", err))
			return g
		}

		// Get delegate addresses once and reuse for all pins
		delegatesJSON, err := s.getDelegatesJSON()
		if err != nil {
			s.logger.Error("Failed to get delegate addresses", zap.Error(err))
			_ = g.AddError(fmt.Errorf("failed to get delegate addresses: %w", err))
			return g
		}

		// Set the pre-marshalled delegates JSON onto each pin
		for _, pin := range pins {
			pin.Delegates = delegatesJSON
		}

		return g
	})

	if err != nil {
		s.logger.Error("Failed to list pins",
			zap.Error(err),
			zap.Any("filters", filter),
			zap.Any("pagination", pagination))
		return nil, 0, err
	}

	s.logger.Debug("Listed pins",
		zap.Int("count", len(pins)),
		zap.Int64("total", total))
	return pins, total, nil
}

// ReplacePin creates a new pin job to replace an old one.
func (s *PinServiceDefault) ReplacePin(ctx context.Context, _ uint, _ string, oldRequestID types.BinaryUUID, newPin *pluginDb.IPFSPin) (*pluginDb.IPFSPin, error) {
	var replacedPin *pluginDb.IPFSPin

	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		// Delete the old pin
		if err := g.WithContext(ctx).
			Where("request_id = ?", oldRequestID).
			Delete(&pluginDb.IPFSPin{}).
			Error; err != nil {
			s.logger.Error("Failed to delete old pin",
				zap.Error(err),
				zap.String("old_request_id", oldRequestID.String()))
			_ = g.AddError(fmt.Errorf("failed to delete old pin: %w", err))
			return g
		}

		// Add the new pin
		if err := g.WithContext(ctx).Create(newPin).Error; err != nil {
			s.logger.Error("Failed to add new pin",
				zap.Error(err),
				zap.Any("new_pin", newPin))
			_ = g.AddError(fmt.Errorf("failed to add new pin: %w", err))
			return g
		}

		replacedPin = newPin
		return g
	})

	if err != nil {
		return nil, err
	}

	s.logger.Debug("Replaced pin",
		zap.String("old_request_id", oldRequestID.String()),
		zap.String("new_request_id", replacedPin.RequestID.String()),
		zap.Binary("new_cid", replacedPin.CID))
	return replacedPin, nil
}

// DeletePin soft-deletes a pin job by its RequestID.
func (s *PinServiceDefault) DeletePin(ctx context.Context, requestID types.BinaryUUID) error {
	// load + delete + re-check inside one txn with row lock; unpin after commit
	var (
		pin         pluginDb.IPFSPin
		shouldUnpin bool
		loaded      bool
	)
	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		// Lock the target row to serialize concurrent deletes on same request
		if err := g.WithContext(ctx).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("request_id = ?", requestID).
			First(&pin).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				// If pin doesn't exist, nothing to do
				return g
			}
			_ = g.AddError(err)
			return g
		}
		loaded = true
		// Soft-delete the target
		if err := g.WithContext(ctx).
			Where("request_id = ?", requestID).
			Delete(&pluginDb.IPFSPin{}).Error; err != nil {
			_ = g.AddError(err)
			return g
		}
		// Check if any other active pins remain for (user_id, cid)
		var cnt int64
		if err := g.WithContext(ctx).
			Model(&pluginDb.IPFSPin{}).
			Where("user_id = ? AND cid = ? AND request_id != ?", pin.UserID, pin.CID, requestID).
			Limit(1).Count(&cnt).Error; err != nil {
			_ = g.AddError(err)
			return g
		}
		shouldUnpin = (cnt == 0)
		return g
	})
	if err != nil {
		s.logger.Error("Failed to delete pin",
			zap.Error(err),
			zap.String("request_id", requestID.String()))
		return err
	}
	
	if !loaded {
		return nil
	}

	if shouldUnpin {
		c, err := cid.Cast(pin.CID)
		if err != nil {
			return fmt.Errorf("cid cast: %w", err)
		}
		if err := s.pinSvc.DeletePinByHash(internal.NewIPFSHash(c), pin.UserID); err != nil {
			s.logger.Error("Failed to unpin CID in core",
				zap.Error(err),
				zap.Stringer("cid", c),
				zap.Uint("user_id", pin.UserID))
			return fmt.Errorf("failed to unpin CID in core: %w", err)
		}
		s.logger.Debug("Core unpin operation completed", zap.Stringer("cid", c), zap.Uint("user_id", pin.UserID))
	}
	s.logger.Debug("Deleted pin record", zap.String("request_id", requestID.String()), zap.Uint("user_id", pin.UserID))
	return nil
}

// UpdatePinStatus updates the job's state.
func (s *PinServiceDefault) UpdatePinStatus(ctx context.Context, requestID types.BinaryUUID, status pluginDb.PinningStatus, info datatypes.JSON) error {
	var rowsAffected int64

	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		result := g.WithContext(ctx).
			Model(&pluginDb.IPFSPin{}).
			Where("request_id = ?", requestID).
			Updates(map[string]interface{}{
				"status": status,
				"info":   info,
			})

		if result.Error != nil {
			_ = g.AddError(fmt.Errorf("failed to update pin status: %w", result.Error))
			return g
		}

		rowsAffected = result.RowsAffected
		return g
	})

	if err != nil {
		s.logger.Error("Failed to update pin status",
			zap.Error(err),
			zap.String("request_id", requestID.String()),
			zap.String("status", string(status)))
		return err
	}

	if rowsAffected == 0 {
		s.logger.Warn("No pin found to update",
			zap.String("request_id", requestID.String()))
		return fmt.Errorf("no pin found with request ID: %s", requestID.String())
	}

	s.logger.Debug("Updated pin status",
		zap.String("request_id", requestID.String()),
		zap.String("status", string(status)),
		zap.Int64("rows_affected", rowsAffected))
	return nil
}

// countUserPinsByCID counts active pin records for a specific user and CID, excluding a specific request ID
func (s *PinServiceDefault) countUserPinsByCID(ctx context.Context, userID uint, cidBytes []byte, excludeRequestID types.BinaryUUID) (int64, error) {
	var count int64

	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		query := g.WithContext(ctx).Model(&pluginDb.IPFSPin{}).
			Where("user_id = ? AND cid = ? AND request_id != ?", userID, cidBytes, excludeRequestID)

		return query.Count(&count)
	})

	if err != nil {
		return 0, fmt.Errorf("failed to count user pins by CID: %w", err)
	}

	return count, nil
}

// addDelegateAddresses retrieves delegate addresses and marshals them to JSON, then sets them on the pin model
func (s *PinServiceDefault) addDelegateAddresses(pin *pluginDb.IPFSPin) error {
	delegatesJSON, err := s.getDelegatesJSON()
	if err != nil {
		return err
	}

	pin.Delegates = delegatesJSON
	return nil
}

// getDelegatesJSON retrieves delegate addresses once and marshals them to JSON
func (s *PinServiceDefault) getDelegatesJSON() ([]byte, error) {
	delegates, err := s.ipfs.GetNode().DelegateAddresses()
	if err != nil {
		return nil, err
	}

	delegatesJSON, err := json.Marshal(delegates)
	if err != nil {
		return nil, err
	}

	return delegatesJSON, nil
}
