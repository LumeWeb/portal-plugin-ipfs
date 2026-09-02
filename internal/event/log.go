package event

import (
	"context"
	"time"

	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"gorm.io/gorm"
)

// Store is the durable, replayable website lifecycle event log. It backs both
// the SSE feed (whose event IDs are the durable IDs) and the
// /internal/websites/changes reconciliation endpoint the gateway uses to close
// SSE gaps without relying on visitor traffic.
//
// The auto-increment primary key is the monotonic cursor / high-water mark. IDs
// are never reused, so a consumer can safely advance its cursor past any event
// it has fully processed.
type Store struct {
	db *gorm.DB
}

// NewStore creates an event log Store backed by db.
func NewStore(db *gorm.DB) *Store {
	return &Store{db: db}
}

// Append durably records ev and returns its assigned durable ID. The returned
// ID is guaranteed to be greater than every previously appended event's ID.
func (s *Store) Append(ctx context.Context, ev pluginDb.WebsiteEvent) (uint64, error) {
	if err := s.db.WithContext(ctx).Create(&ev).Error; err != nil {
		return 0, err
	}
	return ev.ID, nil
}

// ListAfter returns events whose durable ID is greater than after, ordered
// ascending by ID. limit bounds the number of rows returned (<=0 means no
// limit). Consumers should use HighWaterMark to detect whether more events
// remain beyond the returned batch.
func (s *Store) ListAfter(ctx context.Context, after uint64, limit int) ([]pluginDb.WebsiteEvent, error) {
	if limit <= 0 {
		limit = 0
	}

	q := s.db.WithContext(ctx).Where("id > ?", after).Order("id ASC")
	if limit > 0 {
		q = q.Limit(limit)
	}

	var events []pluginDb.WebsiteEvent
	if err := q.Find(&events).Error; err != nil {
		return nil, err
	}
	return events, nil
}

// HighWaterMark returns the largest durable event ID currently recorded. It is
// the replay high-water mark the gateway compares against its cursor to decide
// when catch-up reconciliation is complete. Returns 0 when no events exist.
func (s *Store) HighWaterMark(ctx context.Context) (uint64, error) {
	var maxID *uint64
	err := s.db.WithContext(ctx).Model(&pluginDb.WebsiteEvent{}).
		Select("MAX(id)").Scan(&maxID).Error
	if err != nil {
		return 0, err
	}
	if maxID == nil {
		return 0, nil
	}
	return *maxID, nil
}

// PurgeBefore enforces the documented retention window so the log does not
// grow without bound. It removes rows whose durable cursor lies at or below the
// largest-id row that has aged past `before`, so the retention cut is aligned
// with the id-ordered replay cursor and high-water mark rather than
// created_at ordering (which is not guaranteed to match id order). Consumers
// must process events within the retention window.
func (s *Store) PurgeBefore(ctx context.Context, before time.Time) (int64, error) {
	// The id-bound keeps the cut aligned with the replay cursor/high-water mark;
	// the additional `created_at < before` constraint guarantees fresh rows still
	// inside the retention window survive even when a higher-id row has aged past
	// `before`. Together they delete exactly the expired rows within the low-id
	// tail and never evict a within-retention event.
	sub := s.db.Model(&pluginDb.WebsiteEvent{}).
		Select("MAX(id)").
		Where("created_at < ?", before)

	res := s.db.WithContext(ctx).
		Where("id <= (?)", sub).
		Where("created_at < ?", before).
		Delete(&pluginDb.WebsiteEvent{})
	if res.Error != nil {
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// ListSince returns events whose durable ID is greater than after, ordered
// ascending. It is identical to ListAfter but also reports the current
// high-water mark, which the reconciliation endpoint needs to give the gateway
// complete catch-up information.
func (s *Store) ListSince(ctx context.Context, after uint64, limit int) (events []pluginDb.WebsiteEvent, hwm uint64, err error) {
	hwm, err = s.HighWaterMark(ctx)
	if err != nil {
		return nil, hwm, err
	}

	events, err = s.ListAfter(ctx, after, limit)
	if err != nil {
		return nil, hwm, err
	}
	return events, hwm, nil
}
