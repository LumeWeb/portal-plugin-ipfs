package event

import (
	"context"
	"time"

	"go.lumeweb.com/portal/core"
)

const (
	// EVENT_WEBSITE_PUBLISHED is fired when a website is created or updated
	// with a new target hash (i.e., content is published/republished).
	EVENT_WEBSITE_PUBLISHED = "website.published"

	// EVENT_WEBSITE_REMOVED is fired when a website is deleted.
	EVENT_WEBSITE_REMOVED = "website.removed"
)

// WebsitePublishedEvent is fired when a website is created or updated
// with a new target hash.
type WebsitePublishedEvent struct {
	Ctx         context.Context `json:"-"`
	Domain      string          `json:"domain"`
	CID         string          `json:"cid"`
	UserID      uint            `json:"-"`
	WebsiteID   uint            `json:"-"`
	PublishedAt time.Time       `json:"published_at"`
}

// WebsiteRemovedEvent is fired when a website is deleted.
type WebsiteRemovedEvent struct {
	Ctx       context.Context `json:"-"`
	Domain    string          `json:"domain"`
	UserID    uint            `json:"-"`
	WebsiteID uint            `json:"-"`
	RemovedAt time.Time       `json:"removed_at"`
}

// NewWebsitePublishedEvent creates a WebsitePublishedEvent.
func NewWebsitePublishedEvent(ctx context.Context, domain, cid string, userID, websiteID uint) *WebsitePublishedEvent {
	return &WebsitePublishedEvent{
		Ctx:         ctx,
		Domain:      domain,
		CID:         cid,
		UserID:      userID,
		WebsiteID:   websiteID,
		PublishedAt: time.Now(),
	}
}

// NewWebsiteRemovedEvent creates a WebsiteRemovedEvent.
func NewWebsiteRemovedEvent(ctx context.Context, domain string, userID, websiteID uint) *WebsiteRemovedEvent {
	return &WebsiteRemovedEvent{
		Ctx:        ctx,
		Domain:     domain,
		UserID:     userID,
		WebsiteID:  websiteID,
		RemovedAt:  time.Now(),
	}
}

// OnWebsitePublished registers a listener for website publish events.
func OnWebsitePublished(ctx core.Context, handler func(context.Context, WebsitePublishedEvent) error, priority ...int) {
	core.Listen[WebsitePublishedEvent](ctx, EVENT_WEBSITE_PUBLISHED, func(e *core.CoreEvent[WebsitePublishedEvent]) error {
		return handler(e.Data.Ctx, e.Data)
	}, priority...)
}

// OnWebsiteRemoved registers a listener for website removal events.
func OnWebsiteRemoved(ctx core.Context, handler func(context.Context, WebsiteRemovedEvent) error, priority ...int) {
	core.Listen[WebsiteRemovedEvent](ctx, EVENT_WEBSITE_REMOVED, func(e *core.CoreEvent[WebsiteRemovedEvent]) error {
		return handler(e.Data.Ctx, e.Data)
	}, priority...)
}

// SSEEvent wraps a website event with a type field for client-side consumption.
type SSEEvent struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

// NewSSEEvent creates a new SSE event wrapper.
func NewSSEEvent(eventType string, data any) *SSEEvent {
	return &SSEEvent{
		Type: eventType,
		Data: data,
	}
}
