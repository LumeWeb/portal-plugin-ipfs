package db

import (
	"fmt"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"time"
)

var _ schema.Tabler = (*IPFSBlock)(nil)

type IPFSBlock struct {
	gorm.Model
	CID              []byte `gorm:"column:cid"`
	Size             uint64
	Links            []IPFSLinkedBlock `gorm:"foreignKey:ParentID"`
	LastAnnouncement *time.Time
	Ready            bool `gorm:"default:false"`
}

func (I IPFSBlock) TableName() string {
	return "ipfs_blocks"
}

// BeforeSave hook to normalize CID before database operations
func (block *IPFSBlock) BeforeSave(_ *gorm.DB) error {
	// Normalize CID if provided
	if len(block.CID) > 0 {
		cidObj, err := cid.Cast(block.CID)
		if err != nil {
			return fmt.Errorf("failed to parse CID: %w", err)
		}

		normalizedCid := encoding.NormalizeCid(cidObj)
		block.CID = normalizedCid.Bytes()
	}

	return nil
}
