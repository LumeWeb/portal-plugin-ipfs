package dto

import (
	"time"

	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// WebsiteChangeEvent is a single durable website lifecycle event returned by
// the reconciliation endpoint.
type WebsiteChangeEvent struct {
	ID        uint64              `json:"id"`
	EventType db.WebsiteEventType `json:"event_type" jsonschema:"enum=published,enum=removed"`
	Domain    string              `json:"domain"`
	CID       string              `json:"cid,omitempty"` // target hash for published events
	WebsiteID uint                `json:"website_id,omitempty"`
	UserID    uint                `json:"user_id,omitempty"`
	CreatedAt time.Time           `json:"created_at"`
}

// WebsiteChangesResponse is the payload of
// GET /internal/websites/changes?after=<cursor>.
//
// The gateway compares the last event ID it has fully processed (its cursor)
// against high_water_mark; when it is equal (or greater) it knows reconciliation
// is complete and the stream has caught up. Events are ordered ascending by ID.
type WebsiteChangesResponse struct {
	Events []WebsiteChangeEvent `json:"events"`

	// HighWaterMark is the largest durable event ID currently recorded. The
	// gateway persists it together with its processed cursor so a later SSE
	// reconnect can resume exactly where it left off.
	HighWaterMark uint64 `json:"high_water_mark"`

	// Truncated is true when more events exist beyond this batch. Consumers
	// should re-request with after set to the last event ID in Events.
	Truncated bool `json:"truncated"`
}

// SSEHighWaterMark is the terminal event of an SSE replay. It tells the gateway
// the largest durable event ID currently recorded so it knows the replay
// target against which to judge that catch-up is complete.
type SSEHighWaterMark struct {
	HighWaterMark uint64 `json:"high_water_mark"`
}
