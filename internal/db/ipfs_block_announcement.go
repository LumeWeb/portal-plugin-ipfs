package db

import (
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"time"
)

var _ schema.Tabler = (*IPFSBlockAnnouncement)(nil)

type IPFSBlockAnnouncement struct {
	gorm.Model
	BlockID          uint              `gorm:"uniqueIndex:idx_ipfs_blok_announcement_block_node"`
	Block            IPFSBlock         `gorm:"foreignKey:BlockID"`
	NodeID           datatypes.BinUUID `gorm:"index,uniqueIndex:idx_ipfs_blok_announcement_block_node"`
	LastAnnouncement *time.Time        `gorm:"index"`
}

func (U IPFSBlockAnnouncement) TableName() string {
	return "ipfs_block_announcements"
}
