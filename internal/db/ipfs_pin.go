package db

import (
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/types"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"time"
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
	ID              uint `gorm:"primarykey"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
	DeletedAt       gorm.DeletedAt `gorm:"uniqueIndex:idx_ipfs_pin_deleted_at_pin_id,idx_ipfs_pin_hash_request_id_deleted_at"`
	Name            string         `gorm:"index"`
	PinID           uint           `gorm:"uniqueIndex:idx_ipfs_pin_deleted_at_pin_id,idx_ipfs_pin_hash_request_id_deleted_at"`
	Pin             models.Pin
	RequestID       types.BinaryUUID  `gorm:"type:binary(16);uniqueIndex:idx_ipfs_pin_hash_request_id_deleted_at"`
	ParentRequestID *types.BinaryUUID `gorm:"type:binary(16);uniqueIndex:idx_ipfs_pin_hash_request_id_deleted_at"`
	Internal        bool              `gorm:"default:false"`
	Partial         bool              `gorm:"default:false"`
}

func (I IPFSPin) TableName() string {
	return "ipfs_pins"
}
