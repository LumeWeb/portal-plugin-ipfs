package event

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newTestStore spins up an in-memory SQLite DB with the WebsiteEvent schema and
// returns a Store backed by it. Using AutoMigrate keeps the test focused on the
// store's behavior rather than the migration harness.
func newTestStore(t *testing.T) *Store {
	t.Helper()

	gormDB, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, gormDB.AutoMigrate(&db.WebsiteEvent{}))

	return NewStore(gormDB)
}

func TestStore_AppendAndListAfter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	id1, err := store.Append(ctx, db.WebsiteEvent{
		EventType: string(db.WebsiteEventPublished),
		Domain:    "alpha.example",
		CID:       "bafy1",
		WebsiteID: 10,
		UserID:    1,
	})
	require.NoError(t, err)

	id2, err := store.Append(ctx, db.WebsiteEvent{
		EventType: string(db.WebsiteEventRemoved),
		Domain:    "beta.example",
		WebsiteID: 11,
		UserID:    2,
	})
	require.NoError(t, err)

	id3, err := store.Append(ctx, db.WebsiteEvent{
		EventType: string(db.WebsiteEventPublished),
		Domain:    "gamma.example",
		CID:       "bafy2",
		WebsiteID: 12,
		UserID:    3,
	})
	require.NoError(t, err)

	// Durably monotonic: each appended ID is strictly greater than the last.
	assert.Less(t, id1, id2)
	assert.Less(t, id2, id3)

	// ListAfter from 0 returns everything in order.
	all, err := store.ListAfter(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, all, 3)
	assert.Equal(t, []uint64{id1, id2, id3}, []uint64{all[0].ID, all[1].ID, all[2].ID})

	// ListAfter from id1 excludes it.
	rest, err := store.ListAfter(ctx, id1, 0)
	require.NoError(t, err)
	require.Len(t, rest, 2)
	assert.Equal(t, []uint64{id2, id3}, []uint64{rest[0].ID, rest[1].ID})

	// Limit bounds the returned rows but preserves order.
	limited, err := store.ListAfter(ctx, 0, 2)
	require.NoError(t, err)
	require.Len(t, limited, 2)
	assert.Equal(t, id2, limited[1].ID)

	// ListSince reports the high-water mark alongside the events.
	events, hwm, err := store.ListSince(ctx, id1, 0)
	require.NoError(t, err)
	assert.Equal(t, id3, hwm)
	assert.Len(t, events, 2)
}

func TestStore_HighWaterMark(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Empty log -> 0.
	hwm, err := store.HighWaterMark(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), hwm)

	id, err := store.Append(ctx, db.WebsiteEvent{
		EventType: string(db.WebsiteEventPublished),
		Domain:    "alpha.example",
		CID:       "bafy1",
	})
	require.NoError(t, err)

	hwm, err = store.HighWaterMark(ctx)
	require.NoError(t, err)
	assert.Equal(t, id, hwm)
}

func TestStore_PurgeBefore(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	old := db.WebsiteEvent{
		EventType: string(db.WebsiteEventPublished),
		Domain:    "old.example",
		CID:       "bafy1",
		CreatedAt: time.Now().Add(-48 * time.Hour),
	}
	require.NoError(t, store.db.Create(&old).Error)

	id, err := store.Append(ctx, db.WebsiteEvent{
		EventType: string(db.WebsiteEventPublished),
		Domain:    "new.example",
		CID:       "bafy2",
	})
	require.NoError(t, err)

	// Purge events older than 24h: only the 48h-old row is removed.
	removed, err := store.PurgeBefore(ctx, time.Now().Add(-24*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, int64(1), removed)

	remaining, err := store.ListAfter(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, remaining, 1)
	assert.Equal(t, id, remaining[0].ID)
}

// TestStore_PurgeBefore_AlignedToID guards against two retention-purging bugs:
// (a) purging by created_at while the replay cursor/high-water mark key on the
// auto-increment id, which could drop a higher-id event before lower ids; and
// (b) the inverse — deleting fresh within-retention low-id rows because a
// higher-id row has aged past retention. Purge must delete exactly the expired
// rows, aligned to the id-order cut, and never evict a within-retention event.
func TestStore_PurgeBefore_AlignedToID(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	now := time.Now()
	old := now.Add(-48 * time.Hour)

	// id 1 is fresh, id 2 is expired, id 3 is fresh — a non-monotonic
	// timestamp/id ordering that must not cause a fresh row to be purged.
	e1 := db.WebsiteEvent{EventType: string(db.WebsiteEventPublished), Domain: "a.example", CreatedAt: now}
	e2 := db.WebsiteEvent{EventType: string(db.WebsiteEventPublished), Domain: "b.example", CreatedAt: old}
	e3 := db.WebsiteEvent{EventType: string(db.WebsiteEventPublished), Domain: "c.example", CreatedAt: now}
	require.NoError(t, store.db.Create(&e1).Error)
	require.NoError(t, store.db.Create(&e2).Error)
	require.NoError(t, store.db.Create(&e3).Error)

	removed, err := store.PurgeBefore(ctx, now.Add(-24*time.Hour))
	require.NoError(t, err)

	// Only the single expired row (id 2) is removed; fresh id 1 and id 3 survive
	// even though they sit within the id-order cut.
	assert.Equal(t, int64(1), removed)

	remaining, err := store.ListAfter(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, remaining, 2)
	assert.Equal(t, e1.ID, remaining[0].ID)
	assert.Equal(t, e3.ID, remaining[1].ID)

	// No expired row may remain, and the high-water mark is still correct
	// despite the id gap left at id 2.
	for _, ev := range remaining {
		assert.False(t, ev.CreatedAt.Before(now.Add(-24*time.Hour)))
	}
	hwm, err := store.HighWaterMark(ctx)
	require.NoError(t, err)
	assert.Equal(t, e3.ID, hwm)
}
