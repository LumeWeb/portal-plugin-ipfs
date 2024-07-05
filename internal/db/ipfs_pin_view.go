package db

import (
	mh "github.com/multiformats/go-multihash"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/types"
	"gorm.io/gorm"
)

type IPFSPinView struct {
	gorm.Model
	RequestID          uint                     `gorm:"column:request_id"`
	Status             models.RequestStatusType `gorm:"column:status"`
	Hash               mh.Multihash             `gorm:"column:hash;type:binary(64);"`
	HashType           uint64                   `gorm:"column:hash_type"`
	Name               string                   `gorm:"column:name"`
	UserID             uint                     `gorm:"column:user_id"`
	UploaderIP         string                   `gorm:"column:uploader_ip"`
	Internal           bool                     `gorm:"column:internal"`
	PinID              uint                     `gorm:"column:pin_id"`
	Pin                *models.Pin              `gorm:"foreignKey:PinID;references:ID"`
	PinRequestID       types.BinaryUUID         `gorm:"column:pin_request_id"`
	ParentPinRequestID *types.BinaryUUID        `gorm:"column:parent_pin_request_id"`
}

func (IPFSPinView) TableName() string {
	return "ipfs_pin_view"
}
