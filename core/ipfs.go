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
	ProvideCIDs(ctx context.Context, limit int) ([]PinnedCID, error)
	SetLastAnnouncement(ctx context.Context, cids []cid.Cid, t time.Time) error
}

type PinnedCID struct {
	CID              cid.Cid   `json:"cid"`
	LastAnnouncement time.Time `json:"last_announcement"`
}
