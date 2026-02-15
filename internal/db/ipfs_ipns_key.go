package db

import (
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var _ schema.Tabler = (*IPFSIPNSKey)(nil)

// IPFSIPNSKey represents an IPNS key stored in the database
type IPFSIPNSKey struct {
	ID                  uint           `gorm:"primaryKey;autoIncrement"`
	UserID              uint           `gorm:"index:idx_ipfs_ipns_keys_user_id;not null"`
	Name                string         `gorm:"type:varchar(255);not null"` // User-friendly name
	PeerIDMultihash     mh.Multihash   `gorm:"type:varbinary(64);uniqueIndex:user_peer;not null"`
	PrivateKeyEncrypted []byte         `gorm:"type:blob;not null"` // Encrypted with portal seed
	CreatedAt           time.Time      `gorm:"autoCreateTime"`
	DeletedAt           gorm.DeletedAt `gorm:"index:idx_ipfs_ipns_keys_deleted_at"`
}

func (I IPFSIPNSKey) TableName() string {
	return "ipfs_ipns_keys"
}

// BeforeSave hook to validate multihash format
func (key *IPFSIPNSKey) BeforeSave(_ *gorm.DB) error {
	if key.PeerIDMultihash == nil || len(key.PeerIDMultihash) == 0 {
		return fmt.Errorf("peer_id_multihash cannot be empty")
	}
	return nil
}

// PeerID returns the peer ID as a libp2p peer.ID
func (key *IPFSIPNSKey) PeerID() peer.ID {
	return peer.ID(key.PeerIDMultihash)
}

// IPNSName returns the IPNS name (CIDv1 with libp2p-key codec)
func (key *IPFSIPNSKey) IPNSName() string {
	c := cid.NewCidV1(cid.Libp2pKey, key.PeerIDMultihash)
	return c.String()
}

// GetPeerID alias for backward compatibility
func (key *IPFSIPNSKey) GetPeerID() peer.ID {
	return key.PeerID()
}
