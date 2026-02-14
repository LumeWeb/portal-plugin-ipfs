package db

import (
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
	"go.lumeweb.com/portal-plugin-ipfs/internal/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var _ schema.Tabler = (*Website)(nil)

// WebsiteStatus represents the status of a website
type WebsiteStatus string

const (
	WebsiteStatusPendingValidation WebsiteStatus = "pending_validation"
	WebsiteStatusActive            WebsiteStatus = "active"
	WebsiteStatusBroken            WebsiteStatus = "broken"
	WebsiteStatusBlocked           WebsiteStatus = "blocked"
)

// WebsiteTargetType represents the type of target (ipfs or ipns)
type WebsiteTargetType string

const (
	WebsiteTargetTypeIPFS WebsiteTargetType = "ipfs"
	WebsiteTargetTypeIPNS WebsiteTargetType = "ipns"
)

// validStatuses is a map of valid website statuses
var validWebsiteStatuses = map[WebsiteStatus]struct{}{
	WebsiteStatusPendingValidation: {},
	WebsiteStatusActive:            {},
	WebsiteStatusBroken:            {},
	WebsiteStatusBlocked:           {},
}

// validTargetTypes is a map of valid target types
var validTargetTypes = map[WebsiteTargetType]struct{}{
	WebsiteTargetTypeIPFS: {},
	WebsiteTargetTypeIPNS: {},
}

// Website represents a website configuration in the database
type Website struct {
	ID                  uint           `gorm:"primaryKey;autoIncrement"`
	UserID              uint           `gorm:"index:idx_ipfs_websites_user_id;not null"`
	Domain              string         `gorm:"type:varchar(255);index:idx_ipfs_websites_domain;not null"`
	TargetType          string         `gorm:"type:varchar(50);index:idx_ipfs_websites_status;not null"` // WebsiteTargetTypeIPFS or WebsiteTargetTypeIPNS
	TargetMultihash     mh.Multihash   `gorm:"type:varbinary(64);not null"`                              // CID multihash (IPFS) or peer ID multihash (IPNS)
	CIDVersion          *uint8         `gorm:"type:tinyint unsigned"`                                    // 0 = CIDv0, 1 = CIDv1; NULL for IPNS
	Status              string         `gorm:"type:varchar(50);index:idx_ipfs_websites_status;not null"` // pending_validation, active, broken
	ValidationToken     string         `gorm:"type:varchar(255);not null"`
	ValidationExpiresAt *time.Time     `gorm:"index"`
	LastCheckedAt       *time.Time     `gorm:"index:idx_ipfs_websites_last_checked_at"`
	CreatedAt           time.Time      `gorm:"autoCreateTime"`
	UpdatedAt           time.Time      `gorm:"autoUpdateTime"`
	DeletedAt           gorm.DeletedAt `gorm:"index:idx_ipfs_websites_deleted_at"`
}

func (W Website) TableName() string {
	return "ipfs_websites"
}

// BeforeSave hook to validate status, target type, and multihash
func (w *Website) BeforeSave(_ *gorm.DB) error {
	// Validate target type
	if _, ok := validTargetTypes[WebsiteTargetType(w.TargetType)]; !ok {
		return fmt.Errorf("%s: %s", errors.ErrInvalidTargetType, w.TargetType)
	}

	// Validate status
	if _, ok := validWebsiteStatuses[WebsiteStatus(w.Status)]; !ok {
		return fmt.Errorf("%s: %s", errors.ErrInvalidWebsiteStatus, w.Status)
	}

	// Validate multihash is set
	if w.TargetMultihash == nil || len(w.TargetMultihash) == 0 {
		return fmt.Errorf("target_multihash cannot be empty")
	}

	// Validate CID version constraints
	if w.TargetType == string(WebsiteTargetTypeIPFS) && w.CIDVersion == nil {
		return fmt.Errorf("cid_version must be set for IPFS targets")
	}
	if w.TargetType == string(WebsiteTargetTypeIPNS) && w.CIDVersion != nil {
		return fmt.Errorf("cid_version must be NULL for IPNS targets")
	}

	return nil
}

// IsExpired checks if the validation token has expired
func (w *Website) IsExpired() bool {
	if w.ValidationExpiresAt == nil {
		return false
	}
	return time.Now().After(*w.ValidationExpiresAt)
}

// ShouldCheck returns true if the website needs to be checked by the janitor
func (w *Website) ShouldCheck(interval time.Duration) bool {
	if w.LastCheckedAt == nil {
		return true
	}
	return time.Since(*w.LastCheckedAt) >= interval
}

// TargetHash returns the target hash as a string format based on target type
func (w *Website) TargetHash() string {
	if w.TargetType == string(WebsiteTargetTypeIPFS) {
		if w.CIDVersion != nil && *w.CIDVersion == 0 {
			// CIDv0: base58btc encoding
			return cid.NewCidV0(w.TargetMultihash).String()
		}
		// CIDv1: default to raw codec for IPFS content
		return cid.NewCidV1(cid.Raw, w.TargetMultihash).String()
	}
	// IPNS: base36 peer ID string
	peerID := peer.ID(w.TargetMultihash)
	return peerID.String()
}
