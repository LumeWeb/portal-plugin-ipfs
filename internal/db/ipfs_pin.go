package db

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal/db/types"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var _ schema.Tabler = (*IPFSPin)(nil)

type PinningStatus string

const (
	PinningStatusQueued  PinningStatus = "queued"
	PinningStatusPinning PinningStatus = "pinning"
	PinningStatusPinned  PinningStatus = "pinned"
	PinningStatusFailed  PinningStatus = "failed"
)

// MaxPinNameLength is the maximum allowed length of IPFSPin.Name, matching the
// varchar(255) size of the underlying column. Pin names must be capped to this
// length before being persisted.
const MaxPinNameLength = 255

// validStatuses is a map of valid pinning statuses for quick lookup
var validStatuses = map[PinningStatus]struct{}{
	PinningStatusQueued:  {},
	PinningStatusPinning: {},
	PinningStatusPinned:  {},
	PinningStatusFailed:  {},
}

type IPFSPin struct {
	gorm.Model
	RequestID types.BinaryUUID
	UserID    uint
	Status    PinningStatus
	CID       []byte `gorm:"column:cid"`
	Name      string
	Origins   datatypes.JSON
	Meta      datatypes.JSON
	Delegates datatypes.JSON
	Info      datatypes.JSON
}

func (I IPFSPin) TableName() string {
	return "ipfs_pins"
}

// BeforeCreate hook to set default values
func (pin *IPFSPin) BeforeCreate(_ *gorm.DB) error {
	// Set default RequestID if not provided
	if pin.RequestID.Empty() {
		pin.RequestID = types.NewBinUUID()
	}
	return nil
}

// BeforeSave hook to validate status and normalize CID
func (pin *IPFSPin) BeforeSave(_ *gorm.DB) error {
	// Set default status if not provided
	if pin.Status == "" {
		pin.Status = PinningStatusQueued
	}

	if _, ok := validStatuses[pin.Status]; !ok {
		return fmt.Errorf("invalid status: %s", pin.Status)
	}

	// Normalize CID if provided
	if len(pin.CID) > 0 {
		cidObj, err := cid.Cast(pin.CID)
		if err != nil {
			return fmt.Errorf("failed to parse CID: %w", err)
		}

		normalizedCid := encoding.NormalizeCid(cidObj)
		pin.CID = normalizedCid.Bytes()
	}

	return nil
}
