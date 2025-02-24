package core

import (
	"context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/types"
	"go.lumeweb.com/queryutil"
	"gorm.io/datatypes"
)

const PIN_SERVICE = "ipfs.pin"

// IPFSPinService defines the interface for managing IPFS pin jobs.
type IPFSPinService interface {
	// AddPin creates a new pin job record.
	AddPin(ctx context.Context, pin *db.IPFSPin) (*db.IPFSPin, error)

	// GetPinByRequestID retrieves a single pin job by its unique RequestID.
	GetPinByRequestID(ctx context.Context, requestID types.BinaryUUID) (*db.IPFSPin, error)

	// ListPins retrieves a paginated and filtered list of pin jobs.
	ListPins(ctx context.Context, filter []queryutil.CrudFilter, pagination queryutil.Pagination) ([]*db.IPFSPin, int64, error)

	// ReplacePin creates a new pin job to replace an old one.
	ReplacePin(ctx context.Context, userId uint, userIp string, oldRequestID types.BinaryUUID, newPin *db.IPFSPin) (*db.IPFSPin, error)

	// DeletePin soft-deletes a pin job by its RequestID.
	DeletePin(ctx context.Context, requestID types.BinaryUUID) error

	// UpdatePinStatus updates the job's state.
	UpdatePinStatus(ctx context.Context, requestID types.BinaryUUID, status db.PinningStatus, info datatypes.JSON) error

	core.Service
}
