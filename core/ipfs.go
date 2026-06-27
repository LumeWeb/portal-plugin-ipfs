package core

import (
	"context"

	"github.com/ipfs/boxo/provider"
	"github.com/ipfs/go-cid"
	"time"
)

// A Provider provides CIDs to the IPFS network.
type Provider interface {
	provider.Ready
	provider.ProvideMany
}

// A ReprovideStore stores CIDs that need to be periodically announced.
type ReprovideStore interface {
	ProvideCIDs(ctx context.Context, since time.Time, limit int) ([]PinnedCID, error)
	SetLastAnnouncement(ctx context.Context, cids []cid.Cid, t time.Time) error
	// CountPinned returns stats about pinned CIDs and their DHT announcement state.
	// announced = CIDs whose last_announcement is at or after the given threshold.
	// pending = CIDs not yet announced since the threshold (failed or never tried).
	CountPinned(ctx context.Context, since time.Time) (PinnedCIDStats, error)
}

type PinnedCID struct {
	CID              cid.Cid   `json:"cid"`
	LastAnnouncement time.Time `json:"last_announcement"`
}

type PinnedCIDStats struct {
	Total     int64 `json:"total"`
	Announced int64 `json:"announced"`
	Pending   int64 `json:"pending"`
}
