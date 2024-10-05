package ipfs

import (
	"context"
	"sync"
	"time"

	"github.com/ipfs/boxo/provider"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"
)

type (
	// PinnedCID is a CID that needs to be periodically announced.
	PinnedCID struct {
		CID              cid.Cid   `json:"cid"`
		LastAnnouncement time.Time `json:"lastAnnouncement"`
	}

	// A Provider provides CIDs to the IPFS network.
	Provider interface {
		provider.Ready
		provider.ProvideMany
	}

	// A ReprovideStore stores CIDs that need to be periodically announced.
	ReprovideStore interface {
		ProvideCIDs(limit int) ([]PinnedCID, error)
		SetLastAnnouncement(cids []cid.Cid, t time.Time) error
	}
)

// A Reprovider periodically announces CIDs to the IPFS network.
type Reprovider struct {
	provider Provider
	store    ReprovideStore
	log      *zap.Logger

	triggerProvide       chan struct{}
	triggerDelayDuration time.Duration

	mu             sync.Mutex
	reprovideSleep time.Duration
}

// Trigger triggers the reprovider loop to run immediately.
func (r *Reprovider) Trigger() {
	select {
	case r.triggerProvide <- struct{}{}:
	default:
	}
}

// Run starts the reprovider loop, which periodically announces CIDs that
// have not been announced in the last interval.
func (r *Reprovider) Run(ctx context.Context, interval, timeout time.Duration, batchSize int) {
	for {
		if r.provider.Ready() {
			break
		}
		r.log.Debug("provider not ready")
		time.Sleep(30 * time.Second)
	}

	go r.handleTriggers(ctx, interval, timeout, batchSize)

	for {
		r.mu.Lock()
		sleepDuration := r.reprovideSleep
		r.mu.Unlock()

		r.log.Debug("sleeping until next reprovide time", zap.Duration("duration", sleepDuration))

		select {
		case <-ctx.Done():
			return
		case <-time.After(sleepDuration):
			r.log.Debug("reprovide sleep expired")
			nextSleep := r.performProvide(ctx, interval, timeout, batchSize)
			r.mu.Lock()
			r.reprovideSleep = nextSleep
			r.mu.Unlock()
		}
	}
}

func (r *Reprovider) handleTriggers(ctx context.Context, interval, timeout time.Duration, batchSize int) {
	var triggerTimer *time.Timer
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.triggerProvide:
			if triggerTimer != nil {
				triggerTimer.Stop()
			}
			triggerTimer = time.AfterFunc(r.triggerDelayDuration, func() {
				r.performProvide(ctx, interval, timeout, batchSize)
			})
			r.log.Debug("reprovide triggered")
		}
	}
}

func (r *Reprovider) performProvide(ctx context.Context, interval, timeout time.Duration, batchSize int) time.Duration {
	doProvide := func(ctx context.Context, keys []multihash.Multihash) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return r.provider.ProvideMany(ctx, keys)
	}

	reprovideSleep := 10 * time.Minute // Default sleep time if no CIDs to provide

	start := time.Now()

	cids, err := r.store.ProvideCIDs(batchSize)
	if err != nil {
		r.log.Error("failed to fetch CIDs to provide", zap.Error(err))
		return time.Minute // Return a shorter sleep time on error
	}

	if len(cids) == 0 {
		r.log.Debug("no CIDs to provide")
		return reprovideSleep
	}

	rem := time.Until(cids[0].LastAnnouncement.Add(interval))
	if rem > 0 {
		r.log.Debug("waiting for next provide interval")
		return rem
	}

	announced := make([]cid.Cid, 0, len(cids))
	keys := make([]multihash.Multihash, 0, len(cids))
	buffer := interval / 10
	minAnnouncement := time.Now().Add(-(interval - buffer))
	for _, c := range cids {
		if c.LastAnnouncement.After(minAnnouncement) {
			break
		}
		keys = append(keys, c.CID.Hash())
		announced = append(announced, c.CID)
	}

	if err := doProvide(ctx, keys); err != nil {
		r.log.Error("failed to provide CIDs", zap.Error(err))
		return time.Minute // Return a shorter sleep time on error
	}

	if err := r.store.SetLastAnnouncement(announced, time.Now()); err != nil {
		r.log.Error("failed to update last announcement time", zap.Error(err))
		return time.Minute // Return a shorter sleep time on error
	}

	r.log.Debug("provided CIDs", zap.Int("count", len(announced)), zap.Duration("elapsed", time.Since(start)))

	// If we've provided all CIDs, wait for the full interval before checking again
	if len(announced) < len(cids) {
		return time.Until(cids[len(announced)].LastAnnouncement.Add(interval))
	}

	return interval
}

// NewReprovider creates a new reprovider.
func NewReprovider(provider Provider, store ReprovideStore, log *zap.Logger) *Reprovider {
	return &Reprovider{
		provider:             provider,
		store:                store,
		log:                  log,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 30 * time.Second,
		reprovideSleep:       time.Duration(0),
	}
}
