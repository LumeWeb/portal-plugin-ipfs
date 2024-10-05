package db

import (
	"github.com/ipfs/go-cid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var _ schema.Tabler = (*UnixFSNode)(nil)

type UnixFSNode struct {
	gorm.Model
	BlockID   uint
	Block     IPFSBlock                    `gorm:"foreignKey:BlockID"`
	Name      string                       `gorm:"column:name"`
	Type      uint8                        `gorm:"column:type"`
	BlockSize int64                        `gorm:"column:block_size"`
	ChildCID  datatypes.JSONSlice[cid.Cid] `gorm:"column:child_cid"`
}

func (U UnixFSNode) TableName() string {
	return "ipfs_unixfs_nodes"
}
