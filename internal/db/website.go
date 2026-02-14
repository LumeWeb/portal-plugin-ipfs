package db

import (
	"fmt"

	"time"

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
	TargetType          string         `gorm:"type:varchar(50);index:idx_ipfs_websites_status;not null"` // "ipfs" or "ipns"
	TargetHash          string         `gorm:"type:varchar(255);not null"` // CID or IPNS name
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

// BeforeSave hook to validate status and target type
func (w *Website) BeforeSave(_ *gorm.DB) error {
	// Validate target type
	if _, ok := validTargetTypes[WebsiteTargetType(w.TargetType)]; !ok {
		return fmt.Errorf("%w: %s", errors.ErrInvalidTargetType, w.TargetType)
	}

	// Validate status
	if _, ok := validWebsiteStatuses[WebsiteStatus(w.Status)]; !ok {
		return fmt.Errorf("%w: %s", errors.ErrInvalidWebsiteStatus, w.Status)
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
