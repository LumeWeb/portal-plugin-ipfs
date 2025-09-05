package db

import (
	"fmt"
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
func (pin *IPFSPin) BeforeCreate(tx *gorm.DB) error {
	// Set default RequestID if not provided
	if pin.RequestID.Empty() {
		pin.RequestID = types.NewBinUUID()
	}

	// Set default status if not provided
	if pin.Status == "" {
		pin.Status = PinningStatusQueued
	}

	return nil
}

// BeforeSave hook to validate status
func (pin *IPFSPin) BeforeSave(tx *gorm.DB) error {
	if _, ok := validStatuses[pin.Status]; !ok {
		return fmt.Errorf("invalid status: %s", pin.Status)
	}
	return nil
}
