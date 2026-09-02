package db

import (
	"time"

	"gorm.io/gorm/schema"
)

var _ schema.Tabler = (*WebsiteEvent)(nil)

// WebsiteEventType identifies the lifecycle transition recorded in the event log.
type WebsiteEventType string

const (
	// WebsiteEventPublished is recorded when a website is created or its
	// content is (re)published with a new target hash.
	WebsiteEventPublished WebsiteEventType = "published"
	// WebsiteEventRemoved is recorded when a website is deleted.
	WebsiteEventRemoved WebsiteEventType = "removed"
)

// WebsiteEvent is a durable, monotonically ordered record of a website
// lifecycle transition. It is the portal-side source of truth that the gateway
// can replay (through the SSE Last-Event-ID cursor or the
// /internal/websites/changes reconciliation endpoint) across gaps and
// restarts. The auto-increment ID doubles as the durable cursor / high-water
// mark consumers advance past.
type WebsiteEvent struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement"`
	EventType string    `gorm:"type:varchar(50);not null;index:idx_ipfs_website_events_event_type"` // published | removed
	Domain    string    `gorm:"type:varchar(255);not null;index:idx_ipfs_website_events_domain"`
	CID       string    `gorm:"column:cid;type:varchar(255);not null"` // target hash for published events; empty for removed
	WebsiteID uint      `gorm:"index:idx_ipfs_website_events_website_id"`
	UserID    uint      `gorm:"index:idx_ipfs_website_events_user_id"`
	CreatedAt time.Time `gorm:"autoCreateTime;index:idx_ipfs_website_events_created_at"`
}

func (WebsiteEvent) TableName() string {
	return "ipfs_website_events"
}
