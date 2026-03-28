package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/core/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
	localMocks "go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
)

func TestBlockStore_Get(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData := "test data"
		testCid := generateCid(t, testData)
		expectedBlock := blocks.NewBlock([]byte(testData))

		// Mock the metadata's Size method (called for quota validation)
		mockMetadata.EXPECT().Size(mock.Anything, testCid).Return(uint64(len(testData)), nil).Once()

		// Mock the downloader's Get method
		mockDownloader.EXPECT().Get(mock.Anything, testCid).Return(expectedBlock, nil).Once()

		// Mock the upload service's GetUpload method for download attribution
		mockUploadService := core.GetService[*mocks.MockUploadService](ctx, core.UPLOAD_SERVICE)
		if mockUploadService != nil {
			mockUploadService.EXPECT().GetUpload(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
		}

		// Act
		block, err := bs.Get(context.Background(), testCid)

		// Assert
		require.NoError(tb, err)
		assert.Equal(tb, expectedBlock, block)
	}, ipfsTestConfig)
}

func TestBlockStore_GetSize(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData := "test data"
		testCid := generateCid(t, testData)
		expectedSize := uint64(1234)

		// Mock the metadata's Size method
		mockMetadata.EXPECT().BlockExists(mock.Anything, testCid).Return(nil).Once()
		mockMetadata.EXPECT().Size(mock.Anything, testCid).Return(expectedSize, nil).Once()

		// Act
		size, err := bs.GetSize(context.Background(), testCid)

		// Assert
		require.NoError(tb, err)
		assert.Equal(tb, int(expectedSize), size)
	}, ipfsTestConfig)
}

func TestBlockStore_GetSize_BlockExistsError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData := "test data"
		testCid := generateCid(t, testData)
		expectedError := errors.New("block does not exist")

		// Mock the metadata's BlockExists method to return an error
		mockMetadata.EXPECT().BlockExists(mock.Anything, testCid).Return(expectedError).Once()

		// Act
		_, err = bs.GetSize(context.Background(), testCid)

		// Assert
		require.Error(tb, err)
		assert.Contains(t, err.Error(), expectedError.Error())
	}, ipfsTestConfig)
}

func TestBlockStore_GetSize_SizeError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData := "test data"
		testCid := generateCid(t, testData)
		expectedError := errors.New("failed to get size")

		// Mock the metadata's Size method to return an error
		mockMetadata.EXPECT().BlockExists(mock.Anything, testCid).Return(nil).Once()
		mockMetadata.EXPECT().Size(mock.Anything, testCid).Return(0, expectedError).Once()

		// Act
		_, err = bs.GetSize(context.Background(), testCid)

		// Assert
		require.Error(tb, err)
		assert.Contains(t, err.Error(), expectedError.Error())
	}, ipfsTestConfig)
}

func TestBlockStore_Has(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData := "test data"
		testCid := generateCid(t, testData)

		// Mock the metadata's BlockExists method
		mockMetadata.EXPECT().BlockExists(mock.Anything, testCid).Return(nil).Once()

		// Act
		exists, err := bs.Has(context.Background(), testCid)

		// Assert
		require.NoError(tb, err)
		assert.True(tb, exists)
	}, ipfsTestConfig)
}

func TestBlockStore_Has_NotFound(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData := "test data"
		testCid := generateCid(t, testData)
		expectedError := errors.New("not found")

		// Mock the metadata's BlockExists method to return an error
		mockMetadata.EXPECT().BlockExists(mock.Anything, testCid).Return(expectedError).Once()

		// Act
		exists, err := bs.Has(context.Background(), testCid)

		// Assert
		require.Error(tb, err)
		assert.False(tb, exists)
		assert.Contains(t, err.Error(), expectedError.Error())
	}, ipfsTestConfig)
}

func TestBlockStore_Put(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData := "test data"
		testCid := generateCid(t, testData)
		testBlock, err := blocks.NewBlockWithCid([]byte(testData), testCid)
		require.NoError(t, err)

		// Mock the storage's UploadObject method
		mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).Return(nil, nil).Once()

		// Mock the metadata's Pin method
		mockMetadata.EXPECT().Pin(mock.Anything, mock.Anything).Return(nil).Once()

		// Act
		err = bs.Put(context.Background(), testBlock)

		// Assert
		require.NoError(tb, err)
	}, ipfsTestConfig)
}

func TestBlockStore_Put_UploadError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData := "test data"
		testCid := generateCid(t, testData)
		testBlock, err := blocks.NewBlockWithCid([]byte(testData), testCid)
		require.NoError(t, err)
		expectedError := errors.New("upload failed")

		// Mock the storage's UploadObject method to return an error
		mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).Return(nil, expectedError).Once()

		// Act
		err = bs.Put(context.Background(), testBlock)

		// Assert
		require.Error(tb, err)
		assert.Contains(t, err.Error(), expectedError.Error())
	}, ipfsTestConfig)
}

func TestBlockStore_Put_PinError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData := "test data"
		testCid := generateCid(t, testData)
		testBlock, err := blocks.NewBlockWithCid([]byte(testData), testCid)
		require.NoError(t, err)
		expectedError := errors.New("pin failed")

		// Mock the storage's UploadObject method
		mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).Return(nil, nil).Once()

		// Mock the metadata's Pin method to return an error
		mockMetadata.EXPECT().Pin(mock.Anything, mock.Anything).Return(expectedError).Once()

		// Act
		err = bs.Put(context.Background(), testBlock)

		// Assert
		require.Error(tb, err)
		assert.Contains(t, err.Error(), expectedError.Error())
	}, ipfsTestConfig)
}

func TestBlockStore_DeleteBlock(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		testData := "test data"
		testCid := generateCid(t, testData)

		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		// Mock the metadata's Unpin method
		mockMetadata.EXPECT().Unpin(mock.Anything, testCid).Return(nil).Once()

		// Mock the storage's DeleteObject call
		mockStorage.EXPECT().DeleteObject(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Act
		err = bs.DeleteBlock(context.Background(), testCid)

		// Assert
		require.NoError(tb, err)
	}, ipfsTestConfig)
}

func TestBlockStore_DeleteBlock_UnpinError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)

		testData := "test data"
		testCid := generateCid(t, testData)
		expectedError := errors.New("unpin failed")

		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		// Mock the metadata's Unpin method to return an error
		mockMetadata.EXPECT().Unpin(mock.Anything, testCid).Return(expectedError).Once()

		// Act
		err = bs.DeleteBlock(context.Background(), testCid)

		// Assert
		require.Error(tb, err)
		assert.Contains(t, err.Error(), expectedError.Error())
	}, ipfsTestConfig)
}

func TestBlockStore_PutMany(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData1 := "test data 1"
		testCid1 := generateCid(t, testData1)
		testBlock1, err := blocks.NewBlockWithCid([]byte(testData1), testCid1)
		require.NoError(t, err)

		testData2 := "test data 2"
		testCid2 := generateCid(t, testData2)
		testBlock2, err := blocks.NewBlockWithCid([]byte(testData2), testCid2)
		require.NoError(t, err)

		_blocks := []blocks.Block{testBlock1, testBlock2}

		// Mock the storage's UploadObject method for each block
		mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).Return(nil, nil).Times(2)

		// Mock the metadata's Pin method for each block
		mockMetadata.EXPECT().Pin(mock.Anything, mock.Anything).Return(nil).Times(2)

		// Act
		err = bs.PutMany(context.Background(), _blocks)

		// Assert
		require.NoError(tb, err)
	}, ipfsTestConfig)
}

func TestBlockStore_PutMany_PutError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
	mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData1 := "test data 1"
		testCid1 := generateCid(t, testData1)
		testBlock1, err := blocks.NewBlockWithCid([]byte(testData1), testCid1)
		require.NoError(t, err)

		testData2 := "test data 2"
		testCid2 := generateCid(t, testData2)
		testBlock2, err := blocks.NewBlockWithCid([]byte(testData2), testCid2)
		require.NoError(t, err)

		_blocks := []blocks.Block{testBlock1, testBlock2}
		expectedError := errors.New("upload failed")

		// Mock the storage's UploadObject method to return an error for the first block
		mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).Return(nil, expectedError).Once()

		// Act
		err = bs.PutMany(context.Background(), _blocks)

		// Assert
		require.Error(tb, err)
		assert.Contains(t, err.Error(), expectedError.Error())
	}, ipfsTestConfig)
}

func TestBlockStore_AllKeysChan(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData1 := "test data 1"
		testCid1 := generateCid(t, testData1)
		testData2 := "test data 2"
		testCid2 := generateCid(t, testData2)
		expectedCIDs := []cid.Cid{testCid1, testCid2}

		// Mock the metadata's Pinned method
		mockMetadata.EXPECT().Pinned(mock.Anything, 0, 1000).Return(expectedCIDs, nil).Once()
		mockMetadata.EXPECT().Pinned(mock.Anything, 1000, 1000).Return([]cid.Cid{}, nil).Once()

		// Act
		ch, err := bs.AllKeysChan(context.Background())
		require.NoError(tb, err)

		// Collect CIDs from the channel
		var receivedCIDs []cid.Cid
		for c := range ch {
			receivedCIDs = append(receivedCIDs, c)
		}

		// Assert
		assert.ElementsMatch(tb, expectedCIDs, receivedCIDs)
	}, ipfsTestConfig)
}

func TestBlockStore_AllKeysChan_MetadataError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		expectedError := errors.New("failed to get pinned CIDs")

		// Mock the metadata's Pinned method to return an error
		mockMetadata.EXPECT().Pinned(mock.Anything, 0, 1000).Return(nil, expectedError).Once()

		// Act
		ch, err := bs.AllKeysChan(context.Background())
		require.NoError(tb, err)

		// Collect CIDs from the channel (should be empty due to the error)
		var receivedCIDs []cid.Cid
		for c := range ch {
			receivedCIDs = append(receivedCIDs, c)
		}

		// Assert
		assert.Empty(tb, receivedCIDs)
	}, ipfsTestConfig)
}

func TestNewBlockStore_ProtocolNotFound(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore and BlockDownloader
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)

		// Attempt to create a new BlockStore - should return an error
		_, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		assert.Error(tb, err)
		assert.Contains(t, err.Error(), "protocol not found")
	}, coreTesting.WithConfig("core.protocols."+internal.ProtocolName+".enabled", false))
}

func TestBlockStore_VirtualReadEnabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)

		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData := "test data"
		testCid := generateCid(t, testData)
		// Use raw block to avoid protobuf decoding issues
		testBlock, err := blocks.NewBlockWithCid([]byte(testData), testCid)
		require.NoError(t, err)

		// Enable virtual read
		readCtx := pc.VirtualReadOption(context.Background(), true)

		// Test Get
		mockDownloader.EXPECT().Get(mock.Anything, testCid).Return(testBlock, nil).Once()
		
		// Mock the upload service's GetUpload method for download attribution
		mockUploadService := core.GetService[*mocks.MockUploadService](ctx, core.UPLOAD_SERVICE)
		if mockUploadService != nil {
			mockUploadService.EXPECT().GetUpload(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
		}
		
		block, err := bs.Get(readCtx, testCid)
		require.NoError(tb, err)
		assert.Equal(tb, testBlock, block)

		// Test Has
		has, err := bs.Has(readCtx, testCid)
		require.NoError(tb, err)
		assert.False(tb, has)

		// Test GetSize
		mockDownloader.EXPECT().Get(mock.Anything, testCid).Return(testBlock, nil).Once()
		size, err := bs.GetSize(readCtx, testCid)
		require.NoError(tb, err)
		assert.Equal(tb, len(testData), size)

		// Test Put
		err = bs.Put(readCtx, testBlock)
		require.NoError(tb, err)

		// Test DeleteBlock
		err = bs.DeleteBlock(readCtx, testCid)
		require.NoError(tb, err)

		// Test AllKeysChan
		ch, err := bs.AllKeysChan(readCtx)
		require.NoError(tb, err)
		// Verify the channel is closed immediately
		_, ok := <-ch
		assert.False(tb, ok, "AllKeysChan should return a closed channel when virtual read is enabled")

	}, ipfsTestConfig)
}

func TestBlockStore_VirtualReadDisabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		testData := "test data"
		testCid := generateCid(t, testData)
		testBlock, err := blocks.NewBlockWithCid([]byte(testData), testCid)
		require.NoError(tb, err)

		// Disable virtual read (default)
		normalCtx := context.Background()

		// Test Get
		mockMetadata.EXPECT().Size(mock.Anything, testCid).Return(uint64(len(testData)), nil).Once()
		mockDownloader.EXPECT().Get(mock.Anything, testCid).Return(testBlock, nil).Once()
		
		// Mock the upload service's GetUpload method for download attribution
		mockUploadService := core.GetService[*mocks.MockUploadService](ctx, core.UPLOAD_SERVICE)
		if mockUploadService != nil {
			mockUploadService.EXPECT().GetUpload(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
		}
		
		block, err := bs.Get(normalCtx, testCid)
		require.NoError(tb, err)
		assert.Equal(tb, testBlock, block)

		// Test Has
		mockMetadata.EXPECT().BlockExists(mock.Anything, testCid).Return(nil).Once()
		has, err := bs.Has(normalCtx, testCid)
		require.NoError(tb, err)
		assert.True(tb, has)

		// Test GetSize
		mockMetadata.EXPECT().BlockExists(mock.Anything, testCid).Return(nil).Once()
		mockMetadata.EXPECT().Size(mock.Anything, testCid).Return(uint64(len(testData)), nil).Once()
		size, err := bs.GetSize(normalCtx, testCid)
		require.NoError(tb, err)
		assert.Equal(tb, len(testData), size)

		// Test Put
		mockStorage.EXPECT().UploadObject(mock.Anything, mock.Anything).Return(nil, nil).Once()
		mockMetadata.EXPECT().Pin(mock.Anything, mock.Anything).Return(nil).Once()
		err = bs.Put(normalCtx, testBlock)
		require.NoError(tb, err)

		// Test DeleteBlock
		mockMetadata.EXPECT().Unpin(mock.Anything, testCid).Return(nil).Once()
		mockStorage.EXPECT().DeleteObject(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		err = bs.DeleteBlock(normalCtx, testCid)
		require.NoError(tb, err)

		// Test AllKeysChan
		mockMetadata.EXPECT().Pinned(mock.Anything, 0, 1000).Return([]cid.Cid{}, nil).Once()
		ch, err := bs.AllKeysChan(normalCtx)
		require.NoError(tb, err)
		_, ok := <-ch
		assert.False(tb, ok, "AllKeysChan should return a closed channel when no keys are pinned")

	}, ipfsTestConfig)
}

func TestBlockStore_AllKeysChan_ContextDone(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		mockDownloader := localMocks.NewMockBlockDownloader(t)
		mockMetadata := localMocks.NewMockMetadataStore(t)
		bs, err := store.NewBlockStore(ctx, mockDownloader, mockMetadata)
		require.NoError(tb, err)

		// Create a context that is already done
		cctx, ccancel := context.WithCancel(context.Background())
		ccancel()

		// Mock the initial Pinned call that will happen in the goroutine
		mockMetadata.EXPECT().Pinned(mock.Anything, 0, 1000).Return([]cid.Cid{}, nil).Once()

		// Act
		ch, err := bs.AllKeysChan(cctx)
		require.NoError(tb, err)

		// Verify channel is closed immediately
		select {
		case _, ok := <-ch:
			assert.False(tb, ok, "Channel should be closed immediately when context is done")
		case <-time.After(100 * time.Millisecond):
			assert.Fail(tb, "Channel should be closed immediately when context is done")
		}

		// Verify only the expected calls were made
		mockMetadata.AssertExpectations(tb)
	}, ipfsTestConfig)
}

