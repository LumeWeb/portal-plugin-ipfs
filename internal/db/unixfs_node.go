package db

import (
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var _ schema.Tabler = (*UnixFSNode)(nil)

type UnixFSNode struct {
	gorm.Model
	BlockID   uint
	Block     IPFSBlock `gorm:"foreignKey:BlockID"`
	Type      uint8
	Size      uint64
	BlockSize int64
}

func (U UnixFSNode) TableName() string {
	return "unixfs_nodes"
}
