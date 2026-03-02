package db

import (
	"fmt"
	"strings"
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

	// IPFSPrefix is the prefix for IPFS target paths
	IPFSPrefix = "/ipfs/"

	// IPNSPrefix is the prefix for IPNS target paths
	IPNSPrefix = "/ipns/"
)

// ToDNSLinkPath returns the DNSLink path for this target type with the given hash
func (t WebsiteTargetType) ToDNSLinkPath(hash string) string {
	if t == WebsiteTargetTypeIPNS {
		return IPNSPrefix + hash
	}
	return IPFSPrefix + hash
}

// IPFSPath creates a properly formatted IPFS path from a CID string
func IPFSPath(cid string) string {
	return IPFSPrefix + trimPath(cid)
}

// IPNSPath creates a properly formatted IPNS path from a peer ID string
func IPNSPath(peerID string) string {
	return IPNSPrefix + trimPath(peerID)
}

// trimPath defensively trims leading and trailing slashes from path components
func trimPath(s string) string {
	return strings.Trim(s, "/")
}
}

// SSLStatus represents the SSL certificate status
type SSLStatus string

const (
	SSLStatusPending SSLStatus = "pending"
	SSLStatusIssuing SSLStatus = "issuing"
	SSLStatusReady   SSLStatus = "ready"
	SSLStatusFailed  SSLStatus = "failed"
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

// validSSLStatuses is a map of valid SSL statuses
var validSSLStatuses = map[SSLStatus]struct{}{
	SSLStatusPending: {},
	SSLStatusIssuing: {},
	SSLStatusReady:   {},
	SSLStatusFailed:  {},
}

// Website represents a website configuration in the database
type Website struct {
	ID                  uint           `gorm:"primaryKey;autoIncrement"`
	UserID              uint           `gorm:"index:idx_ipfs_websites_user_id;not null"`
	Domain              string         `gorm:"type:varchar(255);index:idx_ipfs_websites_domain;not null"`
	TargetType          string         `gorm:"type:varchar(50);index:idx_ipfs_websites_status;not null"` // WebsiteTargetTypeIPFS or WebsiteTargetTypeIPNS
	TargetMultihash     mh.Multihash   `gorm:"type:varbinary(64);not null"`                              // CID multihash (IPFS) or peer ID multihash (IPNS)
	CIDVersion          *uint8         `gorm:"column:cid_version;type:tinyint unsigned"`                // 0 = CIDv0, 1 = CIDv1; NULL for IPNS
	Status              string         `gorm:"type:varchar(50);index:idx_ipfs_websites_status;not null"` // pending_validation, active, broken
	ValidationToken     string         `gorm:"type:varchar(255);not null"`
	ValidationExpiresAt *time.Time     `gorm:"index"`
	LastCheckedAt       *time.Time     `gorm:"index:idx_ipfs_websites_last_checked_at"`
	DNSZoneID           *uint          `gorm:"column:dns_zone_id;index:idx_ipfs_websites_dns_zone_id"`    // Foreign key to DNS zone (if DNS hosting enabled)
	IPNSKeyID           *uint          `gorm:"column:ipns_key_id;index:idx_ipfs_websites_ipns_key_id"`     // Foreign key to IPNS key (if auto-created for managed DNS)
	Enabled             bool           `gorm:"column:dns_enabled;default:false"`                // Whether DNS hosting is enabled
	SSLStatus           string         `gorm:"column:ssl_status;type:varchar(50);index:idx_ipfs_websites_ssl_status;default:'pending'"`
	SSLError            string         `gorm:"column:ssl_error;type:text"`
	SSLIssuedAt         *time.Time     `gorm:"column:ssl_issued_at;index:idx_ipfs_websites_ssl_issued_at"`
	SSLLastUpdatedAt    *time.Time     `gorm:"column:ssl_last_updated_at;index:idx_ipfs_websites_ssl_last_updated_at"`
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

	// Validate SSL status
	if w.SSLStatus == "" {
		w.SSLStatus = string(SSLStatusPending)
	}
	if _, ok := validSSLStatuses[SSLStatus(w.SSLStatus)]; !ok {
		return fmt.Errorf("%s: %s", errors.ErrInvalidSSLStatus, w.SSLStatus)
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
