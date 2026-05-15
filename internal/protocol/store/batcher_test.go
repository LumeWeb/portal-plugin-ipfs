package store

import (
	"context"
	"testing"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// mockBatchStore is a mock MetadataStore that only implements BatchPin and Pin.
type mockBatchStore struct {
	mock.Mock
}

func (m *mockBatchStore) BatchPin(ctx context.Context, blocks []pluginCore.PinnedBlock) error {
	args := m.Called(ctx, blocks)
	return args.Error(0)
}

func (m *mockBatchStore) Pin(ctx context.Context, b pluginCore.PinnedBlock) error {
	args := m.Called(ctx, b)
	return args.Error(0)
}

func (m *mockBatchStore) BlockExists(ctx context.Context, c cid.Cid) error        { return nil }
func (m *mockBatchStore) BlockSiblings(ctx context.Context, c cid.Cid, max int) ([]cid.Cid, error) {
	return nil, nil
}
func (m *mockBatchStore) BlockChildren(ctx context.Context, c cid.Cid, max *int) ([]cid.Cid, error) {
	return nil, nil
}
func (m *mockBatchStore) Unpin(ctx context.Context, c cid.Cid) error                 { return nil }
func (m *mockBatchStore) Pinned(ctx context.Context, offset, limit int) ([]cid.Cid, error) {
	return nil, nil
}
func (m *mockBatchStore) Size(ctx context.Context, c cid.Cid) (uint64, error)        { return 0, nil }
func (m *mockBatchStore) ProcessMissingUnixFSNames(ctx context.Context, cids []cid.Cid) error {
	return nil
}
func (m *mockBatchStore) UpdateUnixFSMetadata(c cid.Cid, metadata any) error { return nil }
func (m *mockBatchStore) MarkBlockReady(c cid.Cid, ready bool) error        { return nil }

func makePinnedBlock(data string) pluginCore.PinnedBlock {
	c, _ := cid.V1Builder{Codec: 0x55, MhType: 0x12}.Sum([]byte(data))
	return pluginCore.PinnedBlock{Cid: c, Size: uint64(len(data))}
}

func TestMetadataBatcher_AddAndFlush(t *testing.T) {
	mockStore := new(mockBatchStore)
	logger := &core.Logger{Logger: zap.NewNop()}
	batcher := newMetadataBatcher(mockStore, logger, 50)

	block := makePinnedBlock("test-data")

	// Add a block — should be buffered, not flushed yet
	err := batcher.Add(context.Background(), block)
	require.NoError(t, err)

	// Flush should trigger BatchPin with 1 block
	mockStore.On("BatchPin", mock.Anything, mock.Anything).Return(nil).Once()
	err = batcher.Flush(context.Background())
	require.NoError(t, err)
	mockStore.AssertExpectations(t)
}

func TestMetadataBatcher_AutoFlushOnBatchSize(t *testing.T) {
	mockStore := new(mockBatchStore)
	logger := &core.Logger{Logger: zap.NewNop()}
	batcher := newMetadataBatcher(mockStore, logger, 3) // small batch size for testing

	// Add 2 blocks — should be buffered
	err := batcher.Add(context.Background(), makePinnedBlock("block-1"))
	require.NoError(t, err)
	err = batcher.Add(context.Background(), makePinnedBlock("block-2"))
	require.NoError(t, err)

	// 3rd block should trigger auto-flush (batchSize reached)
	mockStore.On("BatchPin", mock.Anything, mock.Anything).Return(nil).Once()
	err = batcher.Add(context.Background(), makePinnedBlock("block-3"))
	require.NoError(t, err)
	mockStore.AssertExpectations(t)

	// Flush after auto-flush should be a no-op (batch is empty)
	err = batcher.Flush(context.Background())
	require.NoError(t, err)
}

func TestMetadataBatcher_FlushErrorStopsSubsequentAdds(t *testing.T) {
	mockStore := new(mockBatchStore)
	logger := &core.Logger{Logger: zap.NewNop()}
	batcher := newMetadataBatcher(mockStore, logger, 2)

	// Add 2 blocks to trigger auto-flush
	mockStore.On("BatchPin", mock.Anything, mock.Anything).Return(assert.AnError).Once()
	err := batcher.Add(context.Background(), makePinnedBlock("block-1"))
	require.NoError(t, err)
	err = batcher.Add(context.Background(), makePinnedBlock("block-2"))
	require.Error(t, err) // auto-flush on batch full returns the error
	mockStore.AssertExpectations(t)

	// Subsequent Add should return the sticky error
	err = batcher.Add(context.Background(), makePinnedBlock("block-3"))
	require.Error(t, err)
	assert.Equal(t, assert.AnError, err)
}

func TestMetadataBatcher_FlushEmptyBatch(t *testing.T) {
	mockStore := new(mockBatchStore)
	logger := &core.Logger{Logger: zap.NewNop()}
	batcher := newMetadataBatcher(mockStore, logger, 50)

	// Flush with no blocks added — should be a no-op
	err := batcher.Flush(context.Background())
	require.NoError(t, err)
	mockStore.AssertNotCalled(t, "BatchPin")
}

func TestMetadataBatcher_Close(t *testing.T) {
	mockStore := new(mockBatchStore)
	logger := &core.Logger{Logger: zap.NewNop()}
	batcher := newMetadataBatcher(mockStore, logger, 50)

	err := batcher.Add(context.Background(), makePinnedBlock("block-1"))
	require.NoError(t, err)

	// Close should flush the pending batch
	mockStore.On("BatchPin", mock.Anything, mock.Anything).Return(nil).Once()
	err = batcher.Close(context.Background())
	require.NoError(t, err)
	mockStore.AssertExpectations(t)

	// Add after close should be a no-op
	err = batcher.Add(context.Background(), makePinnedBlock("block-2"))
	require.NoError(t, err)
}

func TestMetadataBatcher_MultipleFlushes(t *testing.T) {
	mockStore := new(mockBatchStore)
	logger := &core.Logger{Logger: zap.NewNop()}
	batcher := newMetadataBatcher(mockStore, logger, 50)

	// Add blocks and flush
	err := batcher.Add(context.Background(), makePinnedBlock("block-1"))
	require.NoError(t, err)
	mockStore.On("BatchPin", mock.Anything, mock.Anything).Return(nil).Once()
	err = batcher.Flush(context.Background())
	require.NoError(t, err)

	// Add more blocks and flush again
	err = batcher.Add(context.Background(), makePinnedBlock("block-2"))
	require.NoError(t, err)
	mockStore.On("BatchPin", mock.Anything, mock.Anything).Return(nil).Once()
	err = batcher.Flush(context.Background())
	require.NoError(t, err)

	mockStore.AssertNumberOfCalls(t, "BatchPin", 2)
}

func TestMetadataBatcher_BatchContainsCorrectBlocks(t *testing.T) {
	mockStore := new(mockBatchStore)
	logger := &core.Logger{Logger: zap.NewNop()}
	batcher := newMetadataBatcher(mockStore, logger, 50)

	block1 := makePinnedBlock("block-1")
	block2 := makePinnedBlock("block-2")

	err := batcher.Add(context.Background(), block1)
	require.NoError(t, err)
	err = batcher.Add(context.Background(), block2)
	require.NoError(t, err)

	// Verify the batch contains exactly the 2 blocks we added
	mockStore.On("BatchPin", mock.Anything, mock.MatchedBy(func(blocks []pluginCore.PinnedBlock) bool {
		return len(blocks) == 2 && blocks[0].Cid.Equals(block1.Cid) && blocks[1].Cid.Equals(block2.Cid)
	})).Return(nil).Once()

	err = batcher.Flush(context.Background())
	require.NoError(t, err)
	mockStore.AssertExpectations(t)
}
