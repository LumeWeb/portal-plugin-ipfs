package db

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var _ schema.Tabler = (*IPFSIPNSKey)(nil)

// IPFSIPNSKey represents an IPNS key stored in the database
type IPFSIPNSKey struct {
	ID                  uint           `gorm:"primaryKey;autoIncrement"`
	UserID              uint           `gorm:"index:idx_ipfs_ipns_keys_user_id;not null"`
	Name                string         `gorm:"type:varchar(255);not null"` // User-friendly name
	IPNSName            string         `gorm:"column:ipns_name;type:varchar(255);index;not null"` // CIDv1 format
	PeerID              string         `gorm:"type:varchar(255);uniqueIndex:user_peer;not null"` // For uniqueness per user
	PrivateKeyEncrypted []byte         `gorm:"type:blob;not null"` // Encrypted with portal seed
	CreatedAt           time.Time      `gorm:"autoCreateTime"`
	DeletedAt           gorm.DeletedAt `gorm:"index:idx_ipfs_ipns_keys_deleted_at"`
}

func (I IPFSIPNSKey) TableName() string {
	return "ipfs_ipns_keys"
}

// BeforeSave hook to validate peer ID format
func (key *IPFSIPNSKey) BeforeSave(_ *gorm.DB) error {
	if _, err := peer.Decode(key.PeerID); err != nil {
		return err
	}
	return nil
}

// GetPeerID returns the peer ID as a libp2p peer.ID
func (key *IPFSIPNSKey) GetPeerID() (peer.ID, error) {
	return peer.Decode(key.PeerID)
}
