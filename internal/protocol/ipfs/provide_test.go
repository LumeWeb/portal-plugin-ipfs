package ipfs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.lumeweb.com/portal-plugin-ipfs/core"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.uber.org/zap"
)

func newTestReprovider(t *testing.T) (*Reprovider, *mocks.MockProvider, *mocks.MockReprovideStore) {
	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	return NewReprovider(mockProvider, mockStore, logger), mockProvider, mockStore
}

// runReproviderAndWait runs the reprovider in a goroutine and waits for it to complete
func runReproviderAndWait(ctx context.Context, cancel context.CancelFunc, reprovider *Reprovider, interval time.Duration, batchSize int, waitDuration time.Duration) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		reprovider.Run(ctx, interval, batchSize)
	}()

	time.Sleep(waitDuration)
	cancel()
	wg.Wait()
}

func TestReprovider_Run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	interval := 1 * time.Second
	batchSize := 10

	reprovider, mockProvider, mockStore := newTestReprovider(t)

	// Mock provider ready
	mockProvider.EXPECT().Ready().Return(true)

	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
	}
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Maybe()
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	runReproviderAndWait(ctx, cancel, reprovider, interval, batchSize, 2*interval)
}

func TestReprovider_Run_ProviderNotReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	interval := 1 * time.Second
	batchSize := 10

	reprovider, mockProvider, mockStore := newTestReprovider(t)

	// Mock provider not ready initially, then ready
	mockProvider.EXPECT().Ready().Return(false).Once()
	mockProvider.EXPECT().Ready().Return(true)

	// Mock store calls that happen after provider becomes ready
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return([]core.PinnedCID{}, nil).Maybe()
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Wait longer to account for the 30-second sleep in the Run function
	runReproviderAndWait(ctx, cancel, reprovider, interval, batchSize, 2*interval+30*time.Second)
}

func TestReprovider_Run_ProvideCIDsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	interval := 1 * time.Second
	batchSize := 10

	reprovider, mockProvider, mockStore := newTestReprovider(t)

	// Mock provider ready
	mockProvider.EXPECT().Ready().Return(true)

	// Mock store returning error
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(nil, errors.New("test error")).Once()

	runReproviderAndWait(ctx, cancel, reprovider, interval, batchSize, 2*interval)
}

func TestReprovider_Run_ProvideManyError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	interval := 1 * time.Second
	batchSize := 10

	reprovider, mockProvider, mockStore := newTestReprovider(t)

	// Mock provider ready
	mockProvider.EXPECT().Ready().Return(true)

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
	}
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany returning error
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(errors.New("test error")).Once()

	runReproviderAndWait(ctx, cancel, reprovider, interval, batchSize, 2*interval)
}

func TestReprovider_Run_SetLastAnnouncementError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	interval := 1 * time.Second
	batchSize := 10

	reprovider, mockProvider, mockStore := newTestReprovider(t)

	// Mock provider ready
	mockProvider.EXPECT().Ready().Return(true)

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
	}
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Once()

	// Mock store SetLastAnnouncement returning error
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("test error")).Once()

	runReproviderAndWait(ctx, cancel, reprovider, interval, batchSize, 2*interval)
}

func TestReprovider_Trigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	batchSize := 10

	reprovider := NewReprovider(mockProvider, mockStore, logger)

	// Mock provider ready
	mockProvider.EXPECT().Ready().Return(true)

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
	}
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Maybe()

	// Mock provider ProvideMany
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Mock store SetLastAnnouncement
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	go reprovider.Run(ctx, interval, batchSize)

	// Trigger the reprovider
	reprovider.Trigger()

	// Wait for the reprovider to run once
	time.Sleep(2 * interval)

	// Clean up the goroutine
	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestNewReprovider(t *testing.T) {
	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	reprovider := NewReprovider(mockProvider, mockStore, logger)

	assert.NotNil(t, reprovider)
	assert.NotNil(t, reprovider.triggerProvide)
	assert.Equal(t, 2*time.Second, reprovider.triggerDelayDuration)
	assert.Equal(t, time.Duration(0), reprovider.reprovideSleep)
}

func TestReprovider_performProvide_NoCIDs(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	batchSize := 10

	reprovider := &Reprovider{
		provider:             mockProvider,
		store:                mockStore,
		log:                  logger,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 100 * time.Millisecond,
		reprovideSleep:       0,
		mu:                   sync.Mutex{},
	}

	// Mock store returning no CIDs
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return([]core.PinnedCID{}, nil).Once()

	sleepDuration := reprovider.performProvide(ctx, interval, batchSize, LabelTriggerScheduled)

	assert.Equal(t, 10*time.Minute, sleepDuration)
}

func TestReprovider_performProvide_ProvideCIDsError(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	batchSize := 10

	reprovider := &Reprovider{
		provider:             mockProvider,
		store:                mockStore,
		log:                  logger,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 100 * time.Millisecond,
		reprovideSleep:       0,
		mu:                   sync.Mutex{},
	}

	// Mock store returning error
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(nil, errors.New("test error")).Once()

	sleepDuration := reprovider.performProvide(ctx, interval, batchSize, LabelTriggerScheduled)

	assert.Equal(t, time.Minute, sleepDuration)
}

func TestReprovider_performProvide_WaitingForNextInterval(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	batchSize := 10

	reprovider := &Reprovider{
		provider:             mockProvider,
		store:                mockStore,
		log:                  logger,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 100 * time.Millisecond,
		reprovideSleep:       0,
		mu:                   sync.Mutex{},
	}

	// Mock store returning CIDs with future LastAnnouncement
	futureTime := time.Now().Add(2 * interval)
	testCIDs := []core.PinnedCID{{CID: cid.Cid{}, LastAnnouncement: futureTime}}
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Once()

	sleepDuration := reprovider.performProvide(ctx, interval, batchSize, LabelTriggerScheduled)

	assert.Less(t, time.Until(futureTime), sleepDuration)
}

func TestReprovider_performProvide_Success(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	batchSize := 10

	reprovider := &Reprovider{
		provider:             mockProvider,
		store:                mockStore,
		log:                  logger,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 100 * time.Millisecond,
		reprovideSleep:       0,
		mu:                   sync.Mutex{},
	}

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
	}
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Once()

	// Mock store SetLastAnnouncement
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	sleepDuration := reprovider.performProvide(ctx, interval, batchSize, LabelTriggerScheduled)

	assert.Equal(t, interval, sleepDuration)
}

func TestReprovider_performProvide_Success_NotAllAnnounced(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	batchSize := 10

	reprovider := &Reprovider{
		provider:             mockProvider,
		store:                mockStore,
		log:                  logger,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 100 * time.Millisecond,
		reprovideSleep:       0,
		mu:                   sync.Mutex{},
	}

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(2 * interval)}, // Future announcement
	}
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Once()

	// Mock store SetLastAnnouncement
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	sleepDuration := reprovider.performProvide(ctx, interval, batchSize, LabelTriggerScheduled)
	assert.Less(t, time.Until(testCIDs[2].LastAnnouncement.Add(interval)), sleepDuration)
}

func TestReprovider_performProvide_ProvideManyError(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	batchSize := 10

	reprovider := &Reprovider{
		provider:             mockProvider,
		store:                mockStore,
		log:                  logger,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 100 * time.Millisecond,
		reprovideSleep:       0,
		mu:                   sync.Mutex{},
	}

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
	}
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany returning error
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(errors.New("test error")).Once()

	sleepDuration := reprovider.performProvide(ctx, interval, batchSize, LabelTriggerScheduled)

	assert.Equal(t, time.Minute, sleepDuration)
}

func TestReprovider_performProvide_SetLastAnnouncementError(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	batchSize := 10

	reprovider := &Reprovider{
		provider:             mockProvider,
		store:                mockStore,
		log:                  logger,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 100 * time.Millisecond,
		reprovideSleep:       0,
		mu:                   sync.Mutex{},
	}

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
	}
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Once()

	// Mock store SetLastAnnouncement returning error
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("test error")).Once()

	sleepDuration := reprovider.performProvide(ctx, interval, batchSize, LabelTriggerScheduled)

	assert.Equal(t, time.Minute, sleepDuration)
}

func TestReprovider_performProvide_MinAnnouncement(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	batchSize := 10

	reprovider := &Reprovider{
		provider:             mockProvider,
		store:                mockStore,
		log:                  logger,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 100 * time.Millisecond,
		reprovideSleep:       0,
		mu:                   sync.Mutex{},
	}

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
	}
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Once()

	// Mock store SetLastAnnouncement
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	sleepDuration := reprovider.performProvide(ctx, interval, batchSize, LabelTriggerScheduled)

	assert.Equal(t, interval, sleepDuration)
}

func TestReprovider_performProvide_MinAnnouncement_NotAllAnnounced(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	batchSize := 10

	reprovider := &Reprovider{
		provider:             mockProvider,
		store:                mockStore,
		log:                  logger,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 100 * time.Millisecond,
		reprovideSleep:       0,
		mu:                   sync.Mutex{},
	}

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(2 * interval)}, // Future announcement
	}
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Once()

	// Mock store SetLastAnnouncement
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	sleepDuration := reprovider.performProvide(ctx, interval, batchSize, LabelTriggerScheduled)

	assert.Less(t, time.Until(testCIDs[2].LastAnnouncement.Add(interval)), sleepDuration)
}

func TestBasicDHTProvider_ProvideMany(t *testing.T) {
	t.Run("ready_returns_false_when_fn_returns_false", func(t *testing.T) {
		provider := newBasicDHTProvider(&stubContentRouting{}, func() bool { return false }, 0, 0)
		assert.False(t, provider.Ready())
	})

	t.Run("ready_returns_true_when_fn_returns_true", func(t *testing.T) {
		provider := newBasicDHTProvider(&stubContentRouting{}, func() bool { return true }, 0, 0)
		assert.True(t, provider.Ready())
	})

	t.Run("ready_defaults_to_true_when_fn_is_nil", func(t *testing.T) {
		provider := newBasicDHTProvider(&stubContentRouting{}, nil, 0, 0)
		assert.True(t, provider.Ready())
	})

	t.Run("provide_many_calls_provide_for_each_key", func(t *testing.T) {
		c1 := cid.NewCidV1(cid.Raw, mustMultihash(t, "key1"))
		c2 := cid.NewCidV1(cid.Raw, mustMultihash(t, "key2"))

		scr := &stubContentRouting{}
		provider := newBasicDHTProvider(scr, nil, 0, 0)

		keys := []multihash.Multihash{c1.Hash(), c2.Hash()}
		err := provider.ProvideMany(context.Background(), keys)
		assert.NoError(t, err)
		assert.Equal(t, 2, scr.getProvideCount())
	})

	t.Run("provide_many_continues_past_errors", func(t *testing.T) {
		c1 := cid.NewCidV1(cid.Raw, mustMultihash(t, "key1"))
		c2 := cid.NewCidV1(cid.Raw, mustMultihash(t, "key2"))

		testErr := errors.New("provide failed")
		scr := &stubContentRouting{err: testErr}
		provider := newBasicDHTProvider(scr, nil, 0, 0)

		keys := []multihash.Multihash{c1.Hash(), c2.Hash()}
		err := provider.ProvideMany(context.Background(), keys)
		assert.Error(t, err)
		// 2 CIDs × 3 attempts each (retries on non-timeout errors)
		assert.Equal(t, 6, scr.getProvideCount())
	})

	t.Run("provide_many_retries_transient_then_succeeds", func(t *testing.T) {
		key := mustMultihash(t, "retry-key")

		var attempts atomic.Int32
		scr := &stubContentRouting{
			provideFn: func() error {
				if attempts.Add(1) < 3 {
					return errors.New("transient routing error")
				}
				return nil
			},
		}
		provider := newBasicDHTProvider(scr, nil, 0, 0)

		err := provider.ProvideMany(context.Background(), []multihash.Multihash{key})
		assert.NoError(t, err)
		assert.Equal(t, int32(3), attempts.Load())
	})

	t.Run("provide_many_does_not_retry_timeout", func(t *testing.T) {
		key := mustMultihash(t, "timeout-key")

		var attempts atomic.Int32
		scr := &stubContentRouting{
			provideFn: func() error {
				attempts.Add(1)
				return context.DeadlineExceeded
			},
		}
		provider := newBasicDHTProvider(scr, nil, 0, 0)

		err := provider.ProvideMany(context.Background(), []multihash.Multihash{key})
		assert.Error(t, err)
		assert.Equal(t, int32(1), attempts.Load()) // no retry on timeout
	})

	t.Run("provide_many_runs_in_parallel", func(t *testing.T) {
		// Generate 100 CIDs and verify they're all provided with 16 workers.
		keys := make([]multihash.Multihash, 100)
		for i := 0; i < 100; i++ {
			keys[i] = mustMultihash(t, fmt.Sprintf("parallel-key-%d", i))
		}

		scr := &stubContentRouting{}
		provider := newBasicDHTProvider(scr, nil, 0, 16)

		err := provider.ProvideMany(context.Background(), keys)
		assert.NoError(t, err)
		assert.Equal(t, 100, scr.getProvideCount())
	})

	t.Run("provide_many_respects_context_cancellation", func(t *testing.T) {
		// With 1 worker and a blocked provide, context cancel should stop dispatching.
		keys := make([]multihash.Multihash, 50)
		for i := 0; i < 50; i++ {
			keys[i] = mustMultihash(t, fmt.Sprintf("cancel-key-%d", i))
		}

		scr := &stubContentRouting{}
		provider := newBasicDHTProvider(scr, nil, 0, 1)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		_ = provider.ProvideMany(ctx, keys)
		// With ctx already cancelled, the loop should break before submitting all tasks.
		assert.Less(t, scr.getProvideCount(), 50)
	})
}

func TestReprovider_CircuitBreaker_Open(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	reprovider := &Reprovider{
		provider:             mockProvider,
		store:                mockStore,
		log:                  logger,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 100 * time.Millisecond,
		reprovideSleep:       0,
		mu:                   sync.Mutex{},
	}

	// Set circuit breaker to open state with a future timestamp
	futureTime := time.Now().Add(15 * time.Minute)
	reprovider.circuitOpenUntil.Store(futureTime.UnixNano())
	ReprovideCircuitOpen.Set(1)

	sleepDuration := reprovider.performProvide(ctx, time.Second, 10, LabelTriggerScheduled)

	// Should return the time until the circuit opens, without calling store or provider
	assert.InDelta(t, 15*time.Minute, sleepDuration, float64(time.Second))
	mockProvider.AssertNotCalled(t, "ProvideMany")
	mockStore.AssertNotCalled(t, "ProvideCIDs")
}

func TestReprovider_CircuitBreaker_OpenAfter3Failures(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	batchSize := 10

	reprovider := &Reprovider{
		provider:             mockProvider,
		store:                mockStore,
		log:                  logger,
		triggerProvide:       make(chan struct{}, 1),
		triggerDelayDuration: 100 * time.Millisecond,
		reprovideSleep:       0,
		mu:                   sync.Mutex{},
	}

	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
	}

	// Simulate 3 consecutive failures
	for i := 0; i < 3; i++ {
		mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Once()
		mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(errors.New("test error")).Once()

		sleepDuration := reprovider.performProvide(ctx, interval, batchSize, LabelTriggerScheduled)

		if i < 2 {
			// First two failures: return time.Minute, circuit not yet open
			assert.Equal(t, time.Minute, sleepDuration, "attempt %d", i)
		} else {
			// Third failure: circuit opens, return 15 minutes cooldown
			assert.Equal(t, 15*time.Minute, sleepDuration, "attempt %d", i)
		}
	}

	// Circuit breaker should now be open
	assert.True(t, reprovider.circuitOpenUntil.Load() > 0)
}

// stubContentRouting is a minimal routing.ContentRouting stub for testing.
type stubContentRouting struct {
	mu           sync.Mutex
	provideCount int
	err          error
	provideFn    func() error // if set, called instead of returning err
}

func (s *stubContentRouting) Provide(_ context.Context, _ cid.Cid, _ bool) error {
	s.mu.Lock()
	s.provideCount++
	s.mu.Unlock()
	if s.provideFn != nil {
		return s.provideFn()
	}
	return s.err
}

func (s *stubContentRouting) getProvideCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.provideCount
}

func (s *stubContentRouting) FindProvidersAsync(_ context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
	return nil
}

func mustMultihash(t *testing.T, data string) multihash.Multihash {
	t.Helper()
	h, err := multihash.Sum([]byte(data), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return h
}
