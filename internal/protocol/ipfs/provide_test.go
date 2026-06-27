package ipfs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"

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

	cfg := config.IPFSProvider{
		BatchSize:     10,
		Interval:      1 * time.Second,
		PerCIDTimeout: 0,
		TriggerDelay:  100 * time.Millisecond,
		NotReadySleep: 50 * time.Millisecond,
		EmptySleep:    10 * time.Minute,
		ErrorSleep:    time.Minute,
		BacklogSleep:  time.Minute,
	}

	return NewReprovider(mockProvider, mockStore, logger, cfg), mockProvider, mockStore
}

// runReproviderAndWait runs the reprovider in a goroutine and waits for it to complete
func runReproviderAndWait(ctx context.Context, cancel context.CancelFunc, reprovider *Reprovider, waitDuration time.Duration) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		reprovider.Run(ctx)
	}()

	time.Sleep(waitDuration)
	cancel()
	wg.Wait()
}

func TestReprovider_Run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	reprovider, mockProvider, mockStore := newTestReprovider(t)

	// Mock provider ready
	mockProvider.EXPECT().Ready().Return(true)

	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
	}
	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return(testCIDs, nil).Maybe()
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	runReproviderAndWait(ctx, cancel, reprovider, 2*time.Second)
}

func TestReprovider_Run_ProviderNotReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	reprovider, mockProvider, mockStore := newTestReprovider(t)

	// Mock provider not ready initially, then ready
	mockProvider.EXPECT().Ready().Return(false).Once()
	mockProvider.EXPECT().Ready().Return(true)

	// Mock store calls that happen after provider becomes ready
	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return([]core.PinnedCID{}, nil).Maybe()
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// NotReadySleep is 50ms in test config, so 1s is enough
	runReproviderAndWait(ctx, cancel, reprovider, 2*time.Second)
}

func TestReprovider_Run_ProvideCIDsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	reprovider, mockProvider, mockStore := newTestReprovider(t)

	// Mock provider ready
	mockProvider.EXPECT().Ready().Return(true)

	// Mock store returning error
	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return(nil, errors.New("test error")).Once()

	runReproviderAndWait(ctx, cancel, reprovider, 2*time.Second)
}

func TestReprovider_Run_ProvideManyError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	reprovider, mockProvider, mockStore := newTestReprovider(t)

	// Mock provider ready
	mockProvider.EXPECT().Ready().Return(true)

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
	}
	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany returning error
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(errors.New("test error")).Once()

	runReproviderAndWait(ctx, cancel, reprovider, 2*time.Second)
}

func TestReprovider_Run_SetLastAnnouncementError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	reprovider, mockProvider, mockStore := newTestReprovider(t)

	// Mock provider ready
	mockProvider.EXPECT().Ready().Return(true)

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
	}
	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Once()

	// Mock store SetLastAnnouncement returning error
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("test error")).Once()

	runReproviderAndWait(ctx, cancel, reprovider, 2*time.Second)
}

func TestReprovider_Trigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	cfg := config.IPFSProvider{
		BatchSize:     10,
		Interval:      1 * time.Second,
		TriggerDelay:  100 * time.Millisecond,
		NotReadySleep: 50 * time.Millisecond,
		EmptySleep:    10 * time.Minute,
		ErrorSleep:    time.Minute,
		BacklogSleep:  time.Minute,
	}

	reprovider := NewReprovider(mockProvider, mockStore, logger, cfg)

	// Mock provider ready
	mockProvider.EXPECT().Ready().Return(true)

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
	}
	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return(testCIDs, nil).Maybe()

	// Mock provider ProvideMany
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Mock store SetLastAnnouncement
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	go reprovider.Run(ctx)

	// Trigger the reprovider
	reprovider.Trigger()

	// Wait for the reprovider to run once
	time.Sleep(2 * time.Second)

	// Clean up the goroutine
	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestNewReprovider(t *testing.T) {
	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	cfg := config.IPFSProvider{
		BatchSize:     500,
		Interval:      4 * time.Hour,
		TriggerDelay:  2 * time.Second,
		NotReadySleep: 30 * time.Second,
		EmptySleep:    10 * time.Minute,
		ErrorSleep:    time.Minute,
		BacklogSleep:  time.Minute,
	}

	reprovider := NewReprovider(mockProvider, mockStore, logger, cfg)

	assert.NotNil(t, reprovider)
	assert.NotNil(t, reprovider.triggerProvide)
	assert.Equal(t, cfg, reprovider.cfg)
}

func newTestReproviderCfg(interval time.Duration, batchSize int) config.IPFSProvider {
	return config.IPFSProvider{
		BatchSize:     batchSize,
		Interval:      interval,
		TriggerDelay:  100 * time.Millisecond,
		NotReadySleep: 50 * time.Millisecond,
		EmptySleep:    10 * time.Minute,
		ErrorSleep:    time.Minute,
		BacklogSleep:  time.Minute,
	}
}

func TestReprovider_performProvide_NoCIDs(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	cfg := newTestReproviderCfg(1*time.Second, 10)

	reprovider := &Reprovider{
		provider:       mockProvider,
		store:          mockStore,
		log:            logger,
		cfg:            cfg,
		triggerProvide: make(chan struct{}, 1),
	}

	// Mock store returning no CIDs
	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return([]core.PinnedCID{}, nil).Once()

	sleepDuration := reprovider.performProvide(ctx, LabelTriggerScheduled)

	assert.Equal(t, 10*time.Minute, sleepDuration)
}

func TestReprovider_performProvide_ProvideCIDsError(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	cfg := newTestReproviderCfg(1*time.Second, 10)

	reprovider := &Reprovider{
		provider:       mockProvider,
		store:          mockStore,
		log:            logger,
		cfg:            cfg,
		triggerProvide: make(chan struct{}, 1),
	}

	// Mock store returning error
	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return(nil, errors.New("test error")).Once()

	sleepDuration := reprovider.performProvide(ctx, LabelTriggerScheduled)

	assert.Equal(t, time.Minute, sleepDuration)
}

func TestReprovider_performProvide_ProvideManyError(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	cfg := newTestReproviderCfg(1*time.Second, 10)

	reprovider := &Reprovider{
		provider:       mockProvider,
		store:          mockStore,
		log:            logger,
		cfg:            cfg,
		triggerProvide: make(chan struct{}, 1),
	}

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
	}
	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany returning error
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(errors.New("test error")).Once()

	sleepDuration := reprovider.performProvide(ctx, LabelTriggerScheduled)

	assert.Equal(t, time.Minute, sleepDuration)
}

func TestReprovider_performProvide_Success(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	cfg := newTestReproviderCfg(1*time.Second, 10)

	reprovider := &Reprovider{
		provider:       mockProvider,
		store:          mockStore,
		log:            logger,
		cfg:            cfg,
		triggerProvide: make(chan struct{}, 1),
	}

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
	}
	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Once()

	// Mock store SetLastAnnouncement
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	sleepDuration := reprovider.performProvide(ctx, LabelTriggerScheduled)

	assert.Equal(t, 1*time.Second, sleepDuration)
}

func TestReprovider_performProvide_SetLastAnnouncementError(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	cfg := newTestReproviderCfg(1*time.Second, 10)

	reprovider := &Reprovider{
		provider:       mockProvider,
		store:          mockStore,
		log:            logger,
		cfg:            cfg,
		triggerProvide: make(chan struct{}, 1),
	}

	// Mock store returning CIDs
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * time.Second)},
	}
	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Once()

	// Mock store SetLastAnnouncement returning error
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("test error")).Once()

	sleepDuration := reprovider.performProvide(ctx, LabelTriggerScheduled)

	assert.Equal(t, time.Minute, sleepDuration)
}

func TestReprovider_performProvide_BacklogDrain(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	cfg := newTestReproviderCfg(1*time.Second, 3)

	reprovider := &Reprovider{
		provider:       mockProvider,
		store:          mockStore,
		log:            logger,
		cfg:            cfg,
		triggerProvide: make(chan struct{}, 1),
	}

	// Mock store returning full batch (3 CIDs = batch size)
	testCIDs := []core.PinnedCID{
		{CID: cid.NewCidV1(cid.Raw, mustMultihash(t, "backlog1")), LastAnnouncement: time.Now().Add(-2 * time.Second)},
		{CID: cid.NewCidV1(cid.Raw, mustMultihash(t, "backlog2")), LastAnnouncement: time.Now().Add(-2 * time.Second)},
		{CID: cid.NewCidV1(cid.Raw, mustMultihash(t, "backlog3")), LastAnnouncement: time.Now().Add(-2 * time.Second)},
	}
	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 3).Return(testCIDs, nil).Once()

	// Mock provider ProvideMany
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Once()

	// Mock store SetLastAnnouncement
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	sleepDuration := reprovider.performProvide(ctx, LabelTriggerScheduled)

	// Full batch → BacklogSleep (1m), not Interval
	assert.Equal(t, time.Minute, sleepDuration)
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
		// No retries: 1 call per key
		assert.Equal(t, 2, scr.getProvideCount())
	})

	t.Run("provide_many_respects_context_cancellation", func(t *testing.T) {
		keys := make([]multihash.Multihash, 50)
		for i := 0; i < 50; i++ {
			keys[i] = mustMultihash(t, fmt.Sprintf("cancel-key-%d", i))
		}

		scr := &stubContentRouting{}
		provider := newBasicDHTProvider(scr, nil, 0, 0)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		_ = provider.ProvideMany(ctx, keys)
		// With ctx already cancelled, the loop should break before submitting all tasks.
		assert.Less(t, scr.getProvideCount(), 50)
	})
}

// stubProvideMany is a minimal provideManyProvider stub for testing fullrtProvider.
type stubProvideMany struct {
	mu        sync.Mutex
	calls     int
	keys      []multihash.Multihash
	err       error
	provideFn func(keys []multihash.Multihash) error
}

func (s *stubProvideMany) ProvideMany(_ context.Context, keys []multihash.Multihash) error {
	s.mu.Lock()
	s.calls++
	s.keys = keys
	fn := s.provideFn
	s.mu.Unlock()
	if fn != nil {
		return fn(keys)
	}
	return s.err
}

func (s *stubProvideMany) getCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestFullrtProvider_ProvideMany(t *testing.T) {
	t.Run("ready_returns_false_when_fn_returns_false", func(t *testing.T) {
		provider := newFullrtProvider(&stubProvideMany{}, func() bool { return false })
		assert.False(t, provider.Ready())
	})

	t.Run("ready_returns_true_when_fn_returns_true", func(t *testing.T) {
		provider := newFullrtProvider(&stubProvideMany{}, func() bool { return true })
		assert.True(t, provider.Ready())
	})

	t.Run("ready_defaults_to_true_when_fn_is_nil", func(t *testing.T) {
		provider := newFullrtProvider(&stubProvideMany{}, nil)
		assert.True(t, provider.Ready())
	})

	t.Run("provide_many_delegates_all_keys", func(t *testing.T) {
		c1 := cid.NewCidV1(cid.Raw, mustMultihash(t, "frt-key1"))
		c2 := cid.NewCidV1(cid.Raw, mustMultihash(t, "frt-key2"))

		stub := &stubProvideMany{}
		provider := newFullrtProvider(stub, nil)

		keys := []multihash.Multihash{c1.Hash(), c2.Hash()}
		err := provider.ProvideMany(context.Background(), keys)
		assert.NoError(t, err)
		assert.Equal(t, 1, stub.getCalls())
		assert.Equal(t, keys, stub.keys)
	})

	t.Run("provide_many_returns_provideManyError_on_failure", func(t *testing.T) {
		c1 := cid.NewCidV1(cid.Raw, mustMultihash(t, "frt-err1"))
		c2 := cid.NewCidV1(cid.Raw, mustMultihash(t, "frt-err2"))

		testErr := errors.New("fullrtbulk send failed")
		stub := &stubProvideMany{err: testErr}
		provider := newFullrtProvider(stub, nil)

		keys := []multihash.Multihash{c1.Hash(), c2.Hash()}
		err := provider.ProvideMany(context.Background(), keys)

		require.Error(t, err)

		var pme *provideManyError
		require.ErrorAs(t, err, &pme)
		assert.Equal(t, 2, pme.failed)
		assert.Equal(t, 2, pme.total)
		assert.Equal(t, testErr, pme.err)
		assert.Equal(t, keys, pme.failedKeys)
	})

	t.Run("provide_many_returns_all_keys_as_failed_on_error", func(t *testing.T) {
		c1 := cid.NewCidV1(cid.Raw, mustMultihash(t, "frt-fail1"))
		c2 := cid.NewCidV1(cid.Raw, mustMultihash(t, "frt-fail2"))
		c3 := cid.NewCidV1(cid.Raw, mustMultihash(t, "frt-fail3"))

		stub := &stubProvideMany{err: errors.New("network error")}
		provider := newFullrtProvider(stub, nil)

		keys := []multihash.Multihash{c1.Hash(), c2.Hash(), c3.Hash()}
		err := provider.ProvideMany(context.Background(), keys)

		var pme *provideManyError
		require.ErrorAs(t, err, &pme)
		// All 3 keys should be in failedKeys for retry
		assert.Equal(t, 3, len(pme.failedKeys))
	})

	t.Run("provide_many_with_empty_keys_returns_nil", func(t *testing.T) {
		stub := &stubProvideMany{}
		provider := newFullrtProvider(stub, nil)

		err := provider.ProvideMany(context.Background(), []multihash.Multihash{})
		assert.NoError(t, err)
		assert.Equal(t, 1, stub.getCalls())
	})

	t.Run("provide_many_respects_context_cancellation", func(t *testing.T) {
		key := mustMultihash(t, "frt-cancel-key")

		stub := &stubProvideMany{
			provideFn: func(_ []multihash.Multihash) error {
				return context.Canceled
			},
		}
		provider := newFullrtProvider(stub, nil)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := provider.ProvideMany(ctx, []multihash.Multihash{key})
		require.Error(t, err)

		var pme *provideManyError
		require.ErrorAs(t, err, &pme)
		assert.Equal(t, 1, pme.failed)
	})
}

func TestReprovider_performProvide_PartialFailure_AllFail(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	cfg := newTestReproviderCfg(1*time.Second, 10)

	reprovider := &Reprovider{
		provider:       mockProvider,
		store:          mockStore,
		log:            logger,
		cfg:            cfg,
		triggerProvide: make(chan struct{}, 1),
	}

	c1 := cid.NewCidV1(cid.Raw, mustMultihash(t, "allfail-key1"))
	c2 := cid.NewCidV1(cid.Raw, mustMultihash(t, "allfail-key2"))

	testCIDs := []core.PinnedCID{
		{CID: c1, LastAnnouncement: time.Now().Add(-2 * time.Second)},
		{CID: c2, LastAnnouncement: time.Now().Add(-2 * time.Second)},
	}

	// All keys fail
	pme := &provideManyError{
		failed:     2,
		total:      2,
		err:        errors.New("timeout"),
		failedKeys: []multihash.Multihash{c1.Hash(), c2.Hash()},
	}

	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return(testCIDs, nil).Once()
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(pme).Once()
	// SetLastAnnouncement should NOT be called (no successes)
	mockStore.AssertNotCalled(t, "SetLastAnnouncement")

	sleepDuration := reprovider.performProvide(ctx, LabelTriggerScheduled)

	assert.Equal(t, time.Minute, sleepDuration)
	mockStore.AssertNotCalled(t, "SetLastAnnouncement")
}

func TestReprovider_performProvide_PartialFailure_SetLastAnnouncementError(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	cfg := newTestReproviderCfg(1*time.Second, 10)

	reprovider := &Reprovider{
		provider:       mockProvider,
		store:          mockStore,
		log:            logger,
		cfg:            cfg,
		triggerProvide: make(chan struct{}, 1),
	}

	c1 := cid.NewCidV1(cid.Raw, mustMultihash(t, "pme-err-key1"))
	c2 := cid.NewCidV1(cid.Raw, mustMultihash(t, "pme-err-key2"))

	testCIDs := []core.PinnedCID{
		{CID: c1, LastAnnouncement: time.Now().Add(-2 * time.Second)},
		{CID: c2, LastAnnouncement: time.Now().Add(-2 * time.Second)},
	}

	// 1 of 2 fails
	pme := &provideManyError{
		failed:     1,
		total:      2,
		err:        errors.New("timeout"),
		failedKeys: []multihash.Multihash{c1.Hash()},
	}

	mockStore.EXPECT().CountPinned(mock.Anything, mock.Anything).Return(core.PinnedCIDStats{}, nil).Maybe()
	mockStore.EXPECT().ProvideCIDs(mock.Anything, mock.Anything, 10).Return(testCIDs, nil).Once()
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(pme).Once()
	// SetLastAnnouncement fails (e.g. DB error) — should not block error handling
	// Called for c2 (the succeeded CID)
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.MatchedBy(func(cids []cid.Cid) bool {
		return len(cids) == 1 && cids[0].Equals(c2)
	}), mock.Anything).Return(errors.New("db error")).Once()

	sleepDuration := reprovider.performProvide(ctx, LabelTriggerScheduled)

	// Should still return ErrorSleep (failure path), not crash
	assert.Equal(t, time.Minute, sleepDuration)
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


