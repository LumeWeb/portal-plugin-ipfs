package db

import (
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"time"
)

var _ schema.Tabler = (*IPFSBlock)(nil)

type IPFSBlock struct {
	gorm.Model
	CID              []byte `gorm:"type:varbinary(64);uniqueIndex;column:cid"`
	Size             uint64
	Links            []IPFSLinkedBlock `gorm:"foreignKey:ParentID"`
	LastAnnouncement *time.Time        `gorm:"index"`
	Ready            bool              `gorm:"default:false"`
}

func (I IPFSBlock) TableName() string {
	return "ipfs_blocks"
}
