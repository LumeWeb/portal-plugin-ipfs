package ipfs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal/core"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multihash"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

// basicDHTProvider is a wrapper around basic DHT that implements pluginCore.Provider
// For basic DHT which doesn't implement ProvideMany natively, we provide it by iterating
type basicDHTProvider struct {
	dht routing.ContentRouting
}

func (b *basicDHTProvider) Ready() bool {
	return true
}

func (b *basicDHTProvider) ProvideMany(ctx context.Context, keys []multihash.Multihash) error {
	for _, k := range keys {
		if err := b.dht.Provide(ctx, cid.NewCidV1(cid.Raw, k), true); err != nil {
			return err
		}
	}
	return nil
}

func newBasicDHTProvider(dht routing.ContentRouting) pluginCore.Provider {
	return &basicDHTProvider{dht: dht}
}

// A Reprovider periodically announces CIDs to the IPFS network.
type Reprovider struct {
	provider pluginCore.Provider
	store    pluginCore.ReprovideStore
	log      *zap.Logger

	triggerProvide       chan struct{}
	triggerDelayDuration time.Duration

	mu             sync.Mutex
	reprovideSleep time.Duration

	// cancelTrigger signals to stop timer callbacks
	cancelTrigger chan struct{}

	// cancelledFlag is an atomic flag to track cancellation state
	cancelledFlag atomic.Bool
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
	ctx, span := core.TraceMethod(ctx, "Reprovider.Run")
	defer span.End()

	for {
		if r.provider.Ready() {
			break
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		r.log.Debug("provider not ready")
		time.Sleep(30 * time.Second)
	}

	go func() {
		r.handleTriggers(ctx, interval, timeout, batchSize)
	}()

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
	ctx, span := core.TraceMethod(ctx, "Reprovider.handleTriggers")
	defer span.End()

	// Use a channel instead of AfterFunc for cleaner cancellation
	// When a trigger comes, we wait for the delay then check if cancelled
	for {
		select {
		case <-ctx.Done():
			r.cancelledFlag.Store(true)
			close(r.cancelTrigger)
			return

		case <-r.triggerProvide:
			// Wait for delay with cancellation support
			delayTimer := time.NewTimer(r.triggerDelayDuration)

			select {
			case <-ctx.Done():
				delayTimer.Stop()
				r.cancelledFlag.Store(true)
				close(r.cancelTrigger)
				return

			case <-delayTimer.C:
				// Timer fired, but context might have been cancelled during wait
				select {
				case <-ctx.Done():
					r.cancelledFlag.Store(true)
					close(r.cancelTrigger)
					return
				default:
					// Context still valid, proceed
				}

				// Double-check cancelled flag before calling performProvide
				if r.cancelledFlag.Load() {
					return
				}

				r.performProvide(ctx, interval, timeout, batchSize)
			}

			r.log.Debug("reprovide triggered")
		}
	}
}

func (r *Reprovider) performProvide(ctx context.Context, interval, timeout time.Duration, batchSize int) time.Duration {
	ctx, span := core.TraceMethod(ctx, "Reprovider.performProvide")
	defer span.End()

	// Check cancellation flag at start of performProvide
	// This prevents running after context is cancelled
	if r.cancelledFlag.Load() {
		return 10 * time.Minute
	}

	doProvide := func(ctx context.Context, keys []multihash.Multihash) error {
		ctx, span := core.TraceMethod(ctx, "anonymous")
		defer span.End()

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return r.provider.ProvideMany(ctx, keys)
	}

	reprovideSleep := 10 * time.Minute // Default sleep time if no CIDs to provide

	start := time.Now()

	// Check cancellation again before calling mocks to prevent race
	if r.cancelledFlag.Load() {
		return reprovideSleep
	}

	cids, err := r.store.ProvideCIDs(ctx, batchSize)
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

	buffer := interval / 10
	minAnnouncement := time.Now().Add(-(interval - buffer))
	eligibleCIDs := lo.Filter(cids, func(c pluginCore.PinnedCID, _ int) bool {
		return !c.LastAnnouncement.After(minAnnouncement)
	})

	announced := lo.Map(eligibleCIDs, func(c pluginCore.PinnedCID, _ int) cid.Cid {
		return c.CID
	})
	keys := lo.Map(eligibleCIDs, func(c pluginCore.PinnedCID, _ int) multihash.Multihash {
		return c.CID.Hash()
	})

	if err := doProvide(ctx, keys); err != nil {
		r.log.Error("failed to provide CIDs", zap.Error(err))
		return time.Minute // Return a shorter sleep time on error
	}

	if err := r.store.SetLastAnnouncement(ctx, announced, time.Now()); err != nil {
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
func NewReprovider(provider pluginCore.Provider, store pluginCore.ReprovideStore, log *zap.Logger) *Reprovider {
	return &Reprovider{
		provider:             provider,
		store:                store,
		log:                  log,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 30 * time.Second,
		reprovideSleep:       time.Duration(0),
		cancelTrigger:        make(chan struct{}),
	}
}
