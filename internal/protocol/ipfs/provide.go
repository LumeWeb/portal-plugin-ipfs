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

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multihash"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

// basicDHTProvider is a wrapper around basic DHT that implements pluginCore.Provider.
// For basic DHT which doesn't implement ProvideMany natively, we provide it by iterating.
type basicDHTProvider struct {
	dht           routing.ContentRouting
	ready         func() bool
	perCIDTimeout time.Duration
}

func (b *basicDHTProvider) Ready() bool {
	if b.ready != nil {
		return b.ready()
	}
	return true
}

func (b *basicDHTProvider) ProvideMany(ctx context.Context, keys []multihash.Multihash) error {
	var failed int
	var lastErr error

	for _, k := range keys {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		provideCtx := ctx
		var cancel context.CancelFunc
		if b.perCIDTimeout > 0 {
			provideCtx, cancel = context.WithTimeout(ctx, b.perCIDTimeout)
		}

		err := b.dht.Provide(provideCtx, cid.NewCidV1(cid.Raw, k), true)
		if cancel != nil {
			cancel()
		}

		if err != nil {
			failed++
			lastErr = err

			errorType := classifyProvideError(err)
			ReprovideCIDFailures.WithLabelValues(errorType).Inc()
			ReprovideCIDsTotal.WithLabelValues(LabelResultFailure).Inc()
			continue
		}
		ReprovideCIDsTotal.WithLabelValues(LabelResultSuccess).Inc()
	}

	if failed > 0 {
		return fmt.Errorf("provideMany: %d/%d CIDs failed, last error: %w", failed, len(keys), lastErr)
	}
	return nil
}

func newBasicDHTProvider(dht routing.ContentRouting, ready func() bool, perCIDTimeout time.Duration) pluginCore.Provider {
	return &basicDHTProvider{dht: dht, ready: ready, perCIDTimeout: perCIDTimeout}
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
			nextSleep := r.performProvide(ctx, interval, timeout, batchSize, LabelTriggerScheduled)
			r.mu.Lock()
			r.reprovideSleep = nextSleep
			r.mu.Unlock()
		}
	}
}

func (r *Reprovider) handleTriggers(ctx context.Context, interval, timeout time.Duration, batchSize int) {
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

				r.performProvide(ctx, interval, timeout, batchSize, LabelTriggerManual)
			}

			r.log.Debug("reprovide triggered")
		}
	}
}

func (r *Reprovider) performProvide(ctx context.Context, interval, timeout time.Duration, batchSize int, trigger string) time.Duration {
	ctx, span := core.TraceMethod(ctx, "Reprovider.performProvide")
	defer span.End()

	ReprovideAttemptsTotal.WithLabelValues(trigger).Inc()

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

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return r.provider.ProvideMany(ctx, keys)
	}

	reprovideSleep := 10 * time.Minute // Default sleep time if no CIDs to provide

	// Check cancellation again before calling mocks to prevent race
	if r.cancelledFlag.Load() {
		return reprovideSleep
	}

	cids, err := r.store.ProvideCIDs(ctx, batchSize)
	if err != nil {
		r.log.Error("failed to fetch CIDs to provide", zap.Error(err))
		ReprovideFailuresTotal.Inc()
		r.recordFailure()
		return time.Minute
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

	ReprovideBatchSize.WithLabelValues().Observe(float64(len(keys)))

	start := time.Now()

	if err := doProvide(ctx, keys); err != nil {
		ReprovideDuration.WithLabelValues().Observe(time.Since(start).Seconds())
		ReprovideFailuresTotal.Inc()
		failures := r.recordFailure()
		r.log.Error("failed to provide CIDs",
			zap.Error(err),
			zap.Uint32("consecutive_failures", failures))

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

	if err := r.store.SetLastAnnouncement(ctx, announced, time.Now()); err != nil {
		r.log.Error("failed to update last announcement time", zap.Error(err))
		return time.Minute
	}

	ReprovideSuccessesTotal.Inc()
	r.recordSuccess()

	r.log.Debug("provided CIDs",
		zap.Int("count", len(announced)),
		zap.Duration("elapsed", time.Since(start)))

	if len(announced) < len(cids) {
		return time.Until(cids[len(announced)].LastAnnouncement.Add(interval))
	}

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
