package ipfs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.lumeweb.com/portal-plugin-ipfs/core"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.uber.org/zap"
)

func newTestReprovider(t *testing.T) (*Reprovider, *mocks.MockProvider, *mocks.MockReprovideStore) {
	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	return NewReprovider(mockProvider, mockStore, logger), mockProvider, mockStore
}

func TestReprovider_Run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
	batchSize := 10

	reprovider, mockProvider, mockStore := newTestReprovider(t)

	// Mock provider ready
	mockProvider.EXPECT().Ready().Return(true)

	// Mock store returning CIDs - expect multiple calls since reprovider runs in a loop
	testCIDs := []core.PinnedCID{
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
		{CID: cid.Cid{}, LastAnnouncement: time.Now().Add(-2 * interval)},
	}
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(testCIDs, nil).Times(2).Maybe()

	// Mock provider ProvideMany - expect multiple calls
	mockProvider.EXPECT().ProvideMany(mock.Anything, mock.Anything).Return(nil).Times(2).Maybe()

	// Mock store SetLastAnnouncement - expect multiple calls
	mockStore.EXPECT().SetLastAnnouncement(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2).Maybe()

	go reprovider.Run(ctx, interval, timeout, batchSize)

	// Wait for the reprovider to run once
	time.Sleep(2 * interval)
}

func TestReprovider_Run_ProviderNotReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
	batchSize := 10

	reprovider, mockProvider, _ := newTestReprovider(t)

	// Mock provider not ready initially, then ready
	mockProvider.EXPECT().Ready().Return(false).Once()
	mockProvider.EXPECT().Ready().Return(true)

	go reprovider.Run(ctx, interval, timeout, batchSize)

	// Wait for the reprovider to run once
	time.Sleep(2 * interval)
}

func TestReprovider_Run_ProvideCIDsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
	batchSize := 10

	reprovider, mockProvider, mockStore := newTestReprovider(t)

	// Mock provider ready
	mockProvider.EXPECT().Ready().Return(true)

	// Mock store returning error
	mockStore.EXPECT().ProvideCIDs(mock.Anything, batchSize).Return(nil, errors.New("test error")).Once()

	go reprovider.Run(ctx, interval, timeout, batchSize)

	// Wait for the reprovider to run once
	time.Sleep(2 * interval)
}

func TestReprovider_Run_ProvideManyError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
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

	go reprovider.Run(ctx, interval, timeout, batchSize)

	// Wait for the reprovider to run once
	time.Sleep(2 * interval)
}

func TestReprovider_Run_SetLastAnnouncementError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
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

	go reprovider.Run(ctx, interval, timeout, batchSize)

	// Wait for the reprovider to run once
	time.Sleep(2 * interval)
}

func TestReprovider_Trigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
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

	go reprovider.Run(ctx, interval, timeout, batchSize)

	// Trigger the reprovider
	reprovider.Trigger()

	// Wait for the reprovider to run once
	time.Sleep(2 * interval)
}

func TestNewReprovider(t *testing.T) {
	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	reprovider := NewReprovider(mockProvider, mockStore, logger)

	assert.NotNil(t, reprovider)
	assert.NotNil(t, reprovider.triggerProvide)
	assert.Equal(t, 30*time.Second, reprovider.triggerDelayDuration)
	assert.Equal(t, time.Duration(0), reprovider.reprovideSleep)
}

func TestReprovider_performProvide_NoCIDs(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
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

	sleepDuration := reprovider.performProvide(ctx, interval, timeout, batchSize)

	assert.Equal(t, 10*time.Minute, sleepDuration)
}

func TestReprovider_performProvide_ProvideCIDsError(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
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

	sleepDuration := reprovider.performProvide(ctx, interval, timeout, batchSize)

	assert.Equal(t, time.Minute, sleepDuration)
}

func TestReprovider_performProvide_WaitingForNextInterval(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
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

	sleepDuration := reprovider.performProvide(ctx, interval, timeout, batchSize)

	assert.Less(t, time.Until(futureTime), sleepDuration)
}

func TestReprovider_performProvide_Success(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
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

	sleepDuration := reprovider.performProvide(ctx, interval, timeout, batchSize)

	assert.Equal(t, interval, sleepDuration)
}

func TestReprovider_performProvide_Success_NotAllAnnounced(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
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

	sleepDuration := reprovider.performProvide(ctx, interval, timeout, batchSize)

	assert.Less(t, time.Until(testCIDs[2].LastAnnouncement.Add(interval)), sleepDuration)
}

func TestReprovider_performProvide_ProvideManyError(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
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

	sleepDuration := reprovider.performProvide(ctx, interval, timeout, batchSize)

	assert.Equal(t, time.Minute, sleepDuration)
}

func TestReprovider_performProvide_SetLastAnnouncementError(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
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

	sleepDuration := reprovider.performProvide(ctx, interval, timeout, batchSize)

	assert.Equal(t, time.Minute, sleepDuration)
}

func TestReprovider_performProvide_MinAnnouncement(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
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

	sleepDuration := reprovider.performProvide(ctx, interval, timeout, batchSize)

	assert.Equal(t, interval, sleepDuration)
}

func TestReprovider_performProvide_MinAnnouncement_NotAllAnnounced(t *testing.T) {
	ctx := context.Background()

	mockProvider := mocks.NewMockProvider(t)
	mockStore := mocks.NewMockReprovideStore(t)
	logger, _ := zap.NewDevelopment()

	interval := 1 * time.Second
	timeout := 500 * time.Millisecond
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

	sleepDuration := reprovider.performProvide(ctx, interval, timeout, batchSize)

	assert.Less(t, time.Until(testCIDs[2].LastAnnouncement.Add(interval)), sleepDuration)
}
