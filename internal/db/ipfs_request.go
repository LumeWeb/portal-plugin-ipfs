package db

import (
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/types"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var _ schema.Tabler = (*IPFSRequest)(nil)

type IPFSRequest struct {
	gorm.Model
	RequestID uint `gorm:"uniqueIndex:idx_ipfs_req_deleted_at_request_id"`
	Request   models.Request

	Name               string
	PinRequestID       types.BinaryUUID  `gorm:"type:binary(16);uniqueIndex:idx_ipfs_req_hash_request_id"`
	ParentPinRequestID *types.BinaryUUID `gorm:"type:binary(16);uniqueIndex:idx_ipfs_req_hash_request_id"`
	Internal           bool
}

func (I IPFSRequest) TableName() string {
	return "ipfs_requests"
}
