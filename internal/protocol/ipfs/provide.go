package ipfs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multihash"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"go.uber.org/zap"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal/core"
)

// provideManyProvider is the minimal interface we need from a DHT that natively
// implements batched ProvideMany (e.g. FullRT). Using an interface here keeps
// provide.go free of fullrt imports.
type provideManyProvider interface {
	ProvideMany(ctx context.Context, keys []multihash.Multihash) error
}

// fullrtProvider delegates to FullRT.ProvideMany, which does keyspace-region
// batching internally: one GetClosestPeers lookup per key (local trie, no
// network round-trips), then groups keys by target peer so each peer receives
// multiple ADD_PROVIDER messages over a single connection. This eliminates the
// per-CID GCP bottleneck that basicDHTProvider has.
type fullrtProvider struct {
	dht   provideManyProvider
	ready func() bool
}

func (f *fullrtProvider) Ready() bool {
	if f.ready != nil {
		return f.ready()
	}
	return true
}

func (f *fullrtProvider) ProvideMany(ctx context.Context, keys []multihash.Multihash) error {
	start := time.Now()

	err := f.dht.ProvideMany(ctx, keys)

	elapsed := time.Since(start).Seconds()
	if err != nil {
		ReprovideCIDDuration.WithLabelValues(classifyProvideError(err)).Observe(elapsed)
		// FullRT.ProvideMany returns a single error, not per-CID. Treat all
		// keys as failed so the reprovider can retry them next cycle.
		ReprovideCIDsTotal.WithLabelValues(LabelResultFailure).Add(float64(len(keys)))
		ReprovideCIDFailures.WithLabelValues(classifyProvideError(err)).Add(float64(len(keys)))
		return &provideManyError{
			failed:     len(keys),
			total:      len(keys),
			err:        err,
			failedKeys: keys,
		}
	}

	ReprovideCIDDuration.WithLabelValues(LabelCIDResultSuccess).Observe(elapsed)
	ReprovideCIDsTotal.WithLabelValues(LabelResultSuccess).Add(float64(len(keys)))
	return nil
}

func newFullrtProvider(dht provideManyProvider, ready func() bool) pluginCore.Provider {
	return &fullrtProvider{dht: dht, ready: ready}
}

// basicDHTProvider wraps a basic DHT that doesn't implement ProvideMany
// natively. It iterates over keys with a per-CID timeout. Used in basic DHT
// mode (mainly for testing) and as a fallback when FullRT is unavailable.
type basicDHTProvider struct {
	dht            routing.ContentRouting
	ready          func() bool
	perCIDTimeout  time.Duration
	provideWorkers int
}

func (b *basicDHTProvider) Ready() bool {
	if b.ready != nil {
		return b.ready()
	}
	return true
}

func (b *basicDHTProvider) ProvideMany(ctx context.Context, keys []multihash.Multihash) error {
	var (
		mu     sync.Mutex
		failed []multihash.Multihash
	)

	workers := b.provideWorkers
	if workers <= 0 {
		workers = len(keys)
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for _, k := range keys {
		if gctx.Err() != nil {
			break
		}

		k := k
		g.Go(func() error {
			provideCtx := gctx
			var cancel context.CancelFunc
			if b.perCIDTimeout > 0 {
				provideCtx, cancel = context.WithTimeout(gctx, b.perCIDTimeout)
			}

			cidStart := time.Now()
			err := b.dht.Provide(provideCtx, cid.NewCidV1(cid.Raw, k), true)
			cidElapsed := time.Since(cidStart).Seconds()
			if cancel != nil {
				cancel()
			}

			if err != nil {
				ReprovideCIDDuration.WithLabelValues(classifyProvideError(err)).Observe(cidElapsed)
				ReprovideCIDFailures.WithLabelValues(classifyProvideError(err)).Inc()
				ReprovideCIDsTotal.WithLabelValues(LabelResultFailure).Inc()
				mu.Lock()
				failed = append(failed, k)
				mu.Unlock()
			} else {
				ReprovideCIDDuration.WithLabelValues(LabelCIDResultSuccess).Observe(cidElapsed)
				ReprovideCIDsTotal.WithLabelValues(LabelResultSuccess).Inc()
			}
			return nil
		})
	}

	_ = g.Wait()

	if len(failed) > 0 {
		return &provideManyError{
			failed:     len(failed),
			total:      len(keys),
			err:        errors.New("provide failed for some CIDs"),
			failedKeys: failed,
		}
	}

	return nil
}

func newBasicDHTProvider(dht routing.ContentRouting, ready func() bool, perCIDTimeout time.Duration, provideWorkers int) pluginCore.Provider {
	return &basicDHTProvider{dht: dht, ready: ready, perCIDTimeout: perCIDTimeout, provideWorkers: provideWorkers}
}

// NewDHTProvider is the facade factory for DHT providers. It abstracts the
// fullrt vs basic DHT choice behind a single call so node.go never branches
// on DHT mode.
//
// When fullrt is non-nil, it delegates to FullRT.ProvideMany (keyspace-region
// batching, local GetClosestPeers, bulk ADD_PROVIDER per peer).
//
// When fullrt is nil, it uses basicDHTProvider with the provided DHT (the
// primary DHT in basic mode, or the companion DHT in fullrt mode without
// fullRT available). The ready function gates health if needed.
func NewDHTProvider(fullrt provideManyProvider, dht routing.ContentRouting, ready func() bool, cfg config.IPFSProvider) pluginCore.Provider {
	if fullrt != nil {
		return newFullrtProvider(fullrt, ready)
	}
	return newBasicDHTProvider(dht, ready, cfg.PerCIDTimeout, cfg.ProvideWorkers)
}

// provideManyError is returned by ProvideMany when some CIDs fail.
// It carries the set of failed multihashes so callers can track per-CID state.
type provideManyError struct {
	failed     int
	total      int
	err        error
	failedKeys []multihash.Multihash
}

func (e *provideManyError) Error() string {
	return fmt.Sprintf("provideMany: %d/%d CIDs failed, last error: %v", e.failed, e.total, e.err)
}

func (e *provideManyError) Unwrap() error {
	return e.err
}

// FailedKeys returns the multihashes that failed to provide.
func (e *provideManyError) FailedKeys() []multihash.Multihash {
	return e.failedKeys
}

func classifyProvideError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "context_cancelled"
	}
	msg := err.Error()
	if strings.Contains(msg, "routing") || strings.Contains(msg, "no peers") {
		return "routing"
	}
	return "other"
}

// A Reprovider periodically announces CIDs to the IPFS network.
type Reprovider struct {
	provider pluginCore.Provider
	store    pluginCore.ReprovideStore
	log      *zap.Logger
	cfg      config.IPFSProvider

	triggerProvide chan struct{}
}

// Trigger signals the reprovider loop to run immediately.
func (r *Reprovider) Trigger() {
	select {
	case r.triggerProvide <- struct{}{}:
	default:
	}
}

// Run starts the reprovider loop, which periodically announces CIDs that
// have not been announced in the last interval.
func (r *Reprovider) Run(ctx context.Context) {
	ctx, span := core.TraceMethod(ctx, "Reprovider.Run")
	defer span.End()

	// Wait for the DHT provider to become ready.
	for {
		if r.provider.Ready() {
			ReprovideProviderReady.Set(1)
			break
		}
		ReprovideProviderReady.Set(0)
		select {
		case <-ctx.Done():
			return
		default:
		}
		r.log.Debug("provider not ready")
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.cfg.NotReadySleep):
		}
	}

	// Initial sleep is 0 so the first provide runs immediately. This is the
	// "boot sweep": all CIDs are stale, ProvideCIDs returns full batches, and
	// the BacklogSleep logic in performProvide causes rapid cycling until the
	// backlog drains, then it settles into the normal interval.
	sleepDuration := time.Duration(0)

	for {
		r.log.Debug("sleeping until next reprovide time", zap.Duration("duration", sleepDuration))

		select {
		case <-ctx.Done():
			return
		case <-time.After(sleepDuration):
			sleepDuration = r.performProvide(ctx, LabelTriggerScheduled)
		case <-r.triggerProvide:
			// Debounce: wait for TriggerDelay, then provide. If another
			// trigger arrives during the delay, the channel buffers it and
			// the next loop iteration picks it up immediately.
			delayTimer := time.NewTimer(r.cfg.TriggerDelay)
			select {
			case <-ctx.Done():
				delayTimer.Stop()
				return
			case <-delayTimer.C:
				sleepDuration = r.performProvide(ctx, LabelTriggerManual)
			}
		}
	}
}

func (r *Reprovider) performProvide(ctx context.Context, trigger string) time.Duration {
	ctx, span := core.TraceMethod(ctx, "Reprovider.performProvide")
	defer span.End()

	ReprovideAttemptsTotal.WithLabelValues(trigger).Inc()

	// Update global pinned-state gauges.
	// Use a buffer slightly larger than the interval to avoid a timestamp
	// precision race: SetLastAnnouncement sets last_announcement = T at the
	// end of the previous cycle, and the next cycle's since = now - interval
	// is computed a few milliseconds later, making since > T. Without the
	// buffer, CIDs announced in the previous cycle fall just outside the
	// window and are counted as pending (and re-provided) every time.
	since := time.Now().Add(-r.cfg.Interval - time.Minute)
	if stats, err := r.store.CountPinned(ctx, since); err != nil {
		r.log.Warn("failed to count pinned CIDs", zap.Error(err))
	} else {
		ReprovidePinnedTotal.Set(float64(stats.Total))
		ReprovideAnnouncedTotal.Set(float64(stats.Announced))
		ReprovidePendingTotal.Set(float64(stats.Pending))
	}

	// Only fetch CIDs whose last_announcement is older than the interval.
	// Use the same buffer as CountPinned to avoid re-broadcasting CIDs that
	// were announced in the previous cycle.
	cutoff := time.Now().Add(-r.cfg.Interval - time.Minute)
	cids, err := r.store.ProvideCIDs(ctx, cutoff, r.cfg.BatchSize)
	if err != nil {
		r.log.Error("failed to fetch CIDs to provide", zap.Error(err))
		ReprovideFailuresTotal.Inc()
		return r.cfg.ErrorSleep
	}

	if len(cids) == 0 {
		r.log.Debug("no CIDs to provide")
		return r.cfg.EmptySleep
	}

	announced := lo.Map(cids, func(c pluginCore.PinnedCID, _ int) cid.Cid {
		return c.CID
	})
	keys := lo.Map(cids, func(c pluginCore.PinnedCID, _ int) multihash.Multihash {
		return c.CID.Hash()
	})

	ReprovideBatchSize.WithLabelValues().Observe(float64(len(keys)))

	start := time.Now()

	provideCtx, provideCancel := context.WithTimeout(ctx, r.cfg.ProvideManyTimeout)
	defer provideCancel()

	if err := r.provider.ProvideMany(provideCtx, keys); err != nil {
		ReprovideDuration.WithLabelValues().Observe(time.Since(start).Seconds())
		ReprovideFailuresTotal.Inc()

		// Extract per-CID failure info
		var failedKeys []multihash.Multihash
		var pme *provideManyError
		if errors.As(err, &pme) {
			failedKeys = pme.FailedKeys()
		} else {
			// Unknown error: mark all attempted keys as failed
			failedKeys = keys
		}

		// Mark successfully-provided CIDs even on partial failure.
		// Without this, CIDs that succeeded are never marked as announced,
		// causing the reprovider to re-broadcast them on every cycle.
		failedSet := make(map[string]struct{}, len(failedKeys))
		for _, fk := range failedKeys {
			failedSet[string(fk)] = struct{}{}
		}
		succeededCIDs := lo.Filter(announced, func(c cid.Cid, _ int) bool {
			_, failed := failedSet[string(c.Hash())]
			return !failed
		})
		if len(succeededCIDs) > 0 {
			if err := r.store.SetLastAnnouncement(ctx, succeededCIDs, time.Now()); err != nil {
				r.log.Warn("failed to update last announcement for partial success",
					zap.Int("count", len(succeededCIDs)),
					zap.Error(err))
			}
		}

		r.log.Error("failed to provide CIDs",
			zap.Error(err),
			zap.Int("succeeded", len(succeededCIDs)),
			zap.Int("failed", len(failedKeys)))

		return r.cfg.ErrorSleep
	}

	ReprovideDuration.WithLabelValues().Observe(time.Since(start).Seconds())

	if err := r.store.SetLastAnnouncement(ctx, announced, time.Now()); err != nil {
		r.log.Error("failed to update last announcement time", zap.Error(err))
		return r.cfg.ErrorSleep
	}

	ReprovideSuccessesTotal.Inc()

	r.log.Debug("provided CIDs",
		zap.Int("count", len(announced)),
		zap.Duration("elapsed", time.Since(start)))

	// If the batch was full, there are likely more pending CIDs.
	// Return a short sleep to drain the backlog quickly instead of waiting
	// the full interval between batches.
	if len(cids) >= r.cfg.BatchSize {
		return r.cfg.BacklogSleep
	}

	return r.cfg.Interval
}

// NewReprovider creates a new reprovider.
func NewReprovider(provider pluginCore.Provider, store pluginCore.ReprovideStore, log *zap.Logger, cfg config.IPFSProvider) *Reprovider {
	return &Reprovider{
		provider:       provider,
		store:          store,
		log:            log,
		cfg:            cfg,
		triggerProvide: make(chan struct{}, 1),
	}
}
