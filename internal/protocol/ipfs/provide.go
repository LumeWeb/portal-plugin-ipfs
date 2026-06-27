package ipfs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal/core"

	"github.com/avast/retry-go/v5"
	"github.com/gammazero/workerpool"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multihash"
	"github.com/samber/lo"
	"go.uber.org/zap"
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

// basicDHTProvider is a wrapper around basic DHT that implements pluginCore.Provider.
// For basic DHT which doesn't implement ProvideMany natively, we provide it by iterating.
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
	workers := b.provideWorkers
	if workers <= 0 {
		workers = 1
	}

	var failed atomic.Int64
	var mu sync.Mutex
	var lastErr error
	failedKeys := make(map[string]struct{})

	wp := workerpool.New(workers)

	for _, k := range keys {
		if ctx.Err() != nil {
			break
		}

		k := k
		wp.Submit(func() {
			err := retry.New(
				retry.Attempts(3),
				retry.Delay(1*time.Second),
				retry.DelayType(retry.BackOffDelay),
				retry.Context(ctx),
				retry.RetryIf(func(err error) bool {
					// Don't retry on timeout or cancellation: likely to fail again
					// and just burn another per-CID timeout.
					return !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled)
				}),
			).Do(func() error {
				provideCtx := ctx
				var cancel context.CancelFunc
				if b.perCIDTimeout > 0 {
					provideCtx, cancel = context.WithTimeout(ctx, b.perCIDTimeout)
				}

				cidStart := time.Now()
				err := b.dht.Provide(provideCtx, cid.NewCidV1(cid.Raw, k), true)
				cidElapsed := time.Since(cidStart).Seconds()
				if cancel != nil {
					cancel()
				}

				if err != nil {
					ReprovideCIDDuration.WithLabelValues(classifyProvideError(err)).Observe(cidElapsed)
				} else {
					ReprovideCIDDuration.WithLabelValues(LabelCIDResultSuccess).Observe(cidElapsed)
				}
				return err
			})

			if err != nil {
				failed.Add(1)
				mu.Lock()
				lastErr = err
				failedKeys[string(k)] = struct{}{}
				mu.Unlock()

				errorType := classifyProvideError(err)
				ReprovideCIDFailures.WithLabelValues(errorType).Inc()
				ReprovideCIDsTotal.WithLabelValues(LabelResultFailure).Inc()
				return
			}
			ReprovideCIDsTotal.WithLabelValues(LabelResultSuccess).Inc()
		})
	}

	wp.StopWait()

	f := int(failed.Load())
	if f > 0 {
		mu.Lock()
		le := lastErr
		fk := make([]multihash.Multihash, 0, len(failedKeys))
		for ks := range failedKeys {
			fk = append(fk, multihash.Multihash(ks))
		}
		mu.Unlock()
		return &provideManyError{
			failed:     f,
			total:      len(keys),
			err:        le,
			failedKeys: fk,
		}
	}
	return nil
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

func newBasicDHTProvider(dht routing.ContentRouting, ready func() bool, perCIDTimeout time.Duration, provideWorkers int) pluginCore.Provider {
	return &basicDHTProvider{dht: dht, ready: ready, perCIDTimeout: perCIDTimeout, provideWorkers: provideWorkers}
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

	triggerProvide       chan struct{}
	triggerDelayDuration time.Duration

	mu             sync.Mutex
	reprovideSleep time.Duration

	// cancelTrigger signals to stop timer callbacks
	cancelTrigger chan struct{}

	// cancelledFlag is an atomic flag to track cancellation state
	cancelledFlag atomic.Bool

	// Circuit breaker
	consecutiveFailures atomic.Uint32
	circuitOpenUntil    atomic.Int64 // unix nano timestamp

	// Boot-cycle tracking: tracks per-CID success/failure during the initial
	// reprovide sweep after boot. When a CID fails then later succeeds on retry,
	// it is removed from the failed set.
	bootCycle bootCycle
}

type bootCycle struct {
	mu        sync.Mutex
	started   bool
	attempted int64
	succeeded int64
	failed    map[string]struct{} // multihash string -> failed
}

func (bc *bootCycle) markAttempted(n int) {
	bc.mu.Lock()
	bc.attempted += int64(n)
	bc.mu.Unlock()
}

func (bc *bootCycle) markSucceeded(keys []multihash.Multihash) {
	bc.mu.Lock()
	bc.succeeded += int64(len(keys))
	for _, k := range keys {
		delete(bc.failed, string(k))
	}
	bc.mu.Unlock()
}

func (bc *bootCycle) markFailed(keys []multihash.Multihash) {
	bc.mu.Lock()
	for _, k := range keys {
		bc.failed[string(k)] = struct{}{}
	}
	bc.mu.Unlock()
}

func (bc *bootCycle) reset() {
	bc.mu.Lock()
	bc.attempted = 0
	bc.succeeded = 0
	bc.failed = make(map[string]struct{})
	bc.started = true
	bc.mu.Unlock()
}

func (bc *bootCycle) snapshot() (attempted, succeeded, failedCount int64) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return bc.attempted, bc.succeeded, int64(len(bc.failed))
}

func (bc *bootCycle) isStarted() bool {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return bc.started
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
func (r *Reprovider) Run(ctx context.Context, interval time.Duration, batchSize int) {
	ctx, span := core.TraceMethod(ctx, "Reprovider.Run")
	defer span.End()

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
		time.Sleep(30 * time.Second)
	}

	go func() {
		r.handleTriggers(ctx, interval, batchSize)
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
			nextSleep := r.performProvide(ctx, interval, batchSize, LabelTriggerScheduled)
			r.mu.Lock()
			r.reprovideSleep = nextSleep
			r.mu.Unlock()
		}
	}
}

func (r *Reprovider) handleTriggers(ctx context.Context, interval time.Duration, batchSize int) {
	ctx, span := core.TraceMethod(ctx, "Reprovider.handleTriggers")
	defer span.End()

	for {
		select {
		case <-ctx.Done():
			r.cancelledFlag.Store(true)
			close(r.cancelTrigger)
			return

		case <-r.triggerProvide:
			delayTimer := time.NewTimer(r.triggerDelayDuration)

			select {
			case <-ctx.Done():
				delayTimer.Stop()
				r.cancelledFlag.Store(true)
				close(r.cancelTrigger)
				return

			case <-delayTimer.C:
				select {
				case <-ctx.Done():
					r.cancelledFlag.Store(true)
					close(r.cancelTrigger)
					return
				default:
				}

				if r.cancelledFlag.Load() {
					return
				}

				r.performProvide(ctx, interval, batchSize, LabelTriggerManual)
			}

			r.log.Debug("reprovide triggered")
		}
	}
}

func (r *Reprovider) performProvide(ctx context.Context, interval time.Duration, batchSize int, trigger string) time.Duration {
	ctx, span := core.TraceMethod(ctx, "Reprovider.performProvide")
	defer span.End()

	ReprovideAttemptsTotal.WithLabelValues(trigger).Inc()

	// Start boot cycle on first performProvide after boot
	if !r.bootCycle.isStarted() {
		r.bootCycle.reset()
		ReprovideBootCycleAttempted.Set(0)
		ReprovideBootCycleSucceeded.Set(0)
		ReprovideBootCycleFailed.Set(0)
		r.log.Info("starting boot reprovide cycle")
	}

	// Update global pinned-state gauges
	since := time.Now().Add(-interval)
	if stats, err := r.store.CountPinned(ctx, since); err != nil {
		r.log.Warn("failed to count pinned CIDs", zap.Error(err))
	} else {
		ReprovidePinnedTotal.Set(float64(stats.Total))
		ReprovideAnnouncedTotal.Set(float64(stats.Announced))
		ReprovidePendingTotal.Set(float64(stats.Pending))
	}

	if r.cancelledFlag.Load() {
		return 10 * time.Minute
	}

	// Circuit breaker: if open, skip until cooldown expires
	if openUntil := r.circuitOpenUntil.Load(); openUntil > 0 {
		now := time.Now().UnixNano()
		if now < openUntil {
			retryAfter := time.Unix(0, openUntil)
			r.log.Debug("circuit breaker open, skipping provide",
				zap.Time("retry_after", retryAfter))
			return time.Until(retryAfter)
		}
		// Cooldown expired -- half-open state, try again
		r.circuitOpenUntil.Store(0)
		ReprovideCircuitOpen.Set(0)
	}

	doProvide := func(ctx context.Context, keys []multihash.Multihash) error {
		ctx, span := core.TraceMethod(ctx, "anonymous")
		defer span.End()

		return r.provider.ProvideMany(ctx, keys)
	}

	reprovideSleep := 10 * time.Minute // Default sleep time if no CIDs to provide

	// Check cancellation again before calling mocks to prevent race
	if r.cancelledFlag.Load() {
		return reprovideSleep
	}

	// Only fetch CIDs whose last_announcement is older than the interval.
	// This avoids re-broadcasting CIDs that are already fresh in the DHT.
	cutoff := time.Now().Add(-interval)
	cids, err := r.store.ProvideCIDs(ctx, cutoff, batchSize)
	if err != nil {
		r.log.Error("failed to fetch CIDs to provide", zap.Error(err))
		ReprovideFailuresTotal.Inc()
		r.recordFailure()
		return time.Minute
	}

	if len(cids) == 0 {
		r.log.Debug("no CIDs to provide")
		// Boot cycle complete: all CIDs have been processed
		attempted, succeeded, failedCount := r.bootCycle.snapshot()
		if attempted > 0 {
			r.log.Info("boot reprovide cycle complete",
				zap.Int64("attempted", attempted),
				zap.Int64("succeeded", succeeded),
				zap.Int64("failed", failedCount))
		}
		return reprovideSleep
	}

	announced := lo.Map(cids, func(c pluginCore.PinnedCID, _ int) cid.Cid {
		return c.CID
	})
	keys := lo.Map(cids, func(c pluginCore.PinnedCID, _ int) multihash.Multihash {
		return c.CID.Hash()
	})

	ReprovideBatchSize.WithLabelValues().Observe(float64(len(keys)))

	// Track boot-cycle attempted
	r.bootCycle.markAttempted(len(keys))

	start := time.Now()

	if err := doProvide(ctx, keys); err != nil {
		ReprovideDuration.WithLabelValues().Observe(time.Since(start).Seconds())
		ReprovideFailuresTotal.Inc()
		failures := r.recordFailure()

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
			zap.Int("failed", len(failedKeys)),
			zap.Uint32("consecutive_failures", failures))

		// Track per-CID failures in boot cycle
		r.bootCycle.markFailed(failedKeys)
		// Track per-CID successes in boot cycle (remove from failed set)
		succeededKeys := lo.Filter(keys, func(k multihash.Multihash, _ int) bool {
			_, failed := failedSet[string(k)]
			return !failed
		})
		if len(succeededKeys) > 0 {
			r.bootCycle.markSucceeded(succeededKeys)
		}

		// Update boot-cycle gauges
		attempted, succeededCount, failedCount := r.bootCycle.snapshot()
		ReprovideBootCycleAttempted.Set(float64(attempted))
		ReprovideBootCycleSucceeded.Set(float64(succeededCount))
		ReprovideBootCycleFailed.Set(float64(failedCount))

		if failures >= 3 {
			cooldown := 15 * time.Minute
			r.circuitOpenUntil.Store(time.Now().Add(cooldown).UnixNano())
			ReprovideCircuitOpen.Set(1)
			r.log.Error("circuit breaker opened",
				zap.Uint32("failures", failures),
				zap.Duration("cooldown", cooldown))
			return cooldown
		}
		return time.Minute
	}

	ReprovideDuration.WithLabelValues().Observe(time.Since(start).Seconds())

	// Track boot-cycle succeeded: remove from failed set if previously failed
	r.bootCycle.markSucceeded(keys)
	attempted, succeeded, failedCount := r.bootCycle.snapshot()
	ReprovideBootCycleAttempted.Set(float64(attempted))
	ReprovideBootCycleSucceeded.Set(float64(succeeded))
	ReprovideBootCycleFailed.Set(float64(failedCount))

	if err := r.store.SetLastAnnouncement(ctx, announced, time.Now()); err != nil {
		r.log.Error("failed to update last announcement time", zap.Error(err))
		return time.Minute
	}

	ReprovideSuccessesTotal.Inc()
	r.recordSuccess()

	r.log.Debug("provided CIDs",
		zap.Int("count", len(announced)),
		zap.Duration("elapsed", time.Since(start)))

	return interval
}

// recordFailure increments the consecutive failure counter and returns the new value.
func (r *Reprovider) recordFailure() uint32 {
	v := r.consecutiveFailures.Add(1)
	ReprovideConsecutiveFailures.Set(float64(v))
	return v
}

// recordSuccess resets the consecutive failure counter.
func (r *Reprovider) recordSuccess() {
	r.consecutiveFailures.Store(0)
	ReprovideConsecutiveFailures.Set(0)
}

// NewReprovider creates a new reprovider.
func NewReprovider(provider pluginCore.Provider, store pluginCore.ReprovideStore, log *zap.Logger) *Reprovider {
	return &Reprovider{
		provider:             provider,
		store:                store,
		log:                  log,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 2 * time.Second,
		reprovideSleep:       time.Duration(0),
		cancelTrigger:        make(chan struct{}),
	}
}
