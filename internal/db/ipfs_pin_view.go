package db

import (
	mh "github.com/multiformats/go-multihash"
	"time"

	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/types"
	"gorm.io/gorm"
)

type IPFSPinView struct {
	gorm.Model
	CreatedAt          ViewTime                 `gorm:"type:datetime"`
	UpdatedAt          ViewTime                 `gorm:"type:datetime"`
	RequestID          uint                     `gorm:"column:request_id"`
	Status             models.RequestStatusType `gorm:"column:status"`
	Hash               mh.Multihash             `gorm:"column:hash;type:binary(64);"`
	HashType           uint64                   `gorm:"column:hash_type"`
	CIDType            uint64                   `gorm:"column:cid_type"`
	Name               string                   `gorm:"column:name"`
	UserID             uint                     `gorm:"column:user_id"`
	UploaderIP         string                   `gorm:"column:uploader_ip"`
	Internal           bool                     `gorm:"column:internal"`
	Partial            bool                     `gorm:"column:partial"`
	PinID              uint                     `gorm:"column:pin_id"`
	Pin                *models.Pin              `gorm:"foreignKey:PinID;references:ID"`
	PinRequestID       types.BinaryUUID         `gorm:"column:pin_request_id"`
	ParentPinRequestID *types.BinaryUUID        `gorm:"column:parent_pin_request_id"`
}

func (IPFSPinView) TableName() string {
	return "ipfs_pin_view"
}

func (ipv *IPFSPinView) BeforeCreate(tx *gorm.DB) error {
	now := time.Now()
	if ipv.CreatedAt.Time.IsZero() {
		ipv.CreatedAt.Time = now
	}
	if ipv.UpdatedAt.Time.IsZero() {
		ipv.UpdatedAt.Time = now
	}
	return nil
}

func (ipv *IPFSPinView) BeforeUpdate(tx *gorm.DB) error {
	ipv.UpdatedAt.Time = time.Now()
	return nil
}
