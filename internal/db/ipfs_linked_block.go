package db

import (
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var _ schema.Tabler = (*IPFSLinkedBlock)(nil)

type IPFSLinkedBlock struct {
	gorm.Model
	ParentID  uint      `gorm:"uniqueIndex:ipfs_idx_linked_blocks_unique"`
	ChildID   uint      `gorm:"uniqueIndex:ipfs_idx_linked_blocks_unique"`
	LinkIndex int       `gorm:"uniqueIndex:ipfs_idx_linked_blocks_unique"`
	Parent    IPFSBlock `gorm:"foreignKey:ParentID"`
	Child     IPFSBlock `gorm:"foreignKey:ChildID"`
}

func (I IPFSLinkedBlock) TableName() string {
	return "ipfs_linked_blocks"
}
