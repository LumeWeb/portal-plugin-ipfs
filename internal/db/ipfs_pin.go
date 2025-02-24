package db

import (
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
