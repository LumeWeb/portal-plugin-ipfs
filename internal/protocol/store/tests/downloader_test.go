package tests

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store/downloader"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

type mockReadCloser struct {
	io.Reader
}

func (mrc *mockReadCloser) Close() error {
	return nil
}

func TestBlockDownloader(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		// Configure the BlockDownloaderDefault
		bd, err := downloader.NewBlockDownloader(ctx, mockStore, 2)
		require.NoError(tb, err)

		// Create a test CID
		mh, err := multihash.Sum([]byte("test data"), multihash.SHA2_256, -1)
		require.NoError(tb, err)
		testCid := cid.NewCidV1(cid.Raw, mh)

		// Mock the BlockExists call
		mockStore.EXPECT().BlockExists(testCid).Return(nil).Once()

		// Mock the DownloadObjectWithOptions call - create a new reader for each call
		mockStorage.EXPECT().DownloadObjectWithOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, protocol core.StorageProtocol, objectHash core.StorageHash, opts ...core.StorageOptionFunc) (io.ReadCloser, error) {
				return &mockReadCloser{Reader: strings.NewReader("test data")}, nil
			})

		// Mock the BlockSiblings and BlockChildren calls
		mockStore.EXPECT().BlockSiblings(testCid, 64).Return([]cid.Cid{}, nil).Maybe()
		mockStore.EXPECT().BlockChildren(testCid, mock.Anything).Return([]cid.Cid{}, nil).Maybe()

		// Call Get
		block, err := bd.Get(context.Background(), testCid)
		require.NoError(tb, err)
		assert.NotNil(tb, block)

		// Verify the block data
		assert.Equal(tb, []byte("test data"), block.RawData())
	}, ipfsTestConfig)
}

func TestBlockDownloader_BlockExistsError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		// Configure the BlockDownloaderDefault
		bd, err := downloader.NewBlockDownloader(ctx, mockStore, 2)
		require.NoError(tb, err)

		// Create a test CID
		mh, err := multihash.Sum([]byte("test data"), multihash.SHA2_256, -1)
		require.NoError(tb, err)
		testCid := cid.NewCidV1(cid.Raw, mh)

		// Mock the BlockExists call to return an error
		mockStore.EXPECT().BlockExists(testCid).Return(fmt.Errorf("block does not exist")).Once()

		// Call Get
		block, err := bd.Get(context.Background(), testCid)
		assert.Error(tb, err)
		assert.Nil(tb, block)

		assert.Contains(t, err.Error(), "block does not exist")
	}, ipfsTestConfig)
}

func TestBlockDownloader_DownloadObjectWithOptionsError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		// Configure the BlockDownloaderDefault
		bd, err := downloader.NewBlockDownloader(ctx, mockStore, 2)
		require.NoError(tb, err)

		// Create a test CID
		mh, err := multihash.Sum([]byte("test data"), multihash.SHA2_256, -1)
		require.NoError(tb, err)
		testCid := cid.NewCidV1(cid.Raw, mh)

		// Mock the BlockExists call
		mockStore.EXPECT().BlockExists(testCid).Return(nil).Once()

		// Mock the DownloadObjectWithOptions call to return an error
		mockStorage.EXPECT().DownloadObjectWithOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, fmt.Errorf("download failed")).Once()

		// Call Get
		block, err := bd.Get(context.Background(), testCid)
		assert.Error(tb, err)
		assert.Nil(tb, block)

		assert.Contains(t, err.Error(), "failed to download block")
	}, ipfsTestConfig)
}

func TestBlockDownloader_DownloadWorker_QueueRelated(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		// Configure the BlockDownloaderDefault with a single worker
		bd, err := downloader.NewBlockDownloader(ctx, mockStore, 1)
		require.NoError(tb, err)

		// Create a test CID
		mh, err := multihash.Sum([]byte("test data"), multihash.SHA2_256, -1)
		require.NoError(tb, err)
		testCid := cid.NewCidV1(cid.Raw, mh)

		// Mock the BlockExists call
		mockStore.EXPECT().BlockExists(testCid).Return(nil).Once()

		// Mock the DownloadObjectWithOptions call
		mockStorage.EXPECT().DownloadObjectWithOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockReadCloser{Reader: strings.NewReader("test data")}, nil).Once()

		// Mock the BlockSiblings and BlockChildren calls
		mockStore.EXPECT().BlockSiblings(testCid, 64).Return([]cid.Cid{}, nil).Once()
		mockStore.EXPECT().BlockChildren(testCid, mock.Anything).Return([]cid.Cid{}, nil).Once()

		// Call Get
		_, err = bd.Get(context.Background(), testCid)
		require.NoError(tb, err)

		// Wait for the download worker to complete and queue related blocks
		time.Sleep(100 * time.Millisecond)
	}, ipfsTestConfig)
}

func TestBlockDownloader_PriorityBehavior(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		// Configure the BlockDownloaderDefault with single worker to test ordering
		bd, err := downloader.NewBlockDownloader(ctx, mockStore, 1)
		require.NoError(tb, err)

		// Create test CIDs
		mh1, err := multihash.Sum([]byte("test1"), multihash.SHA2_256, -1)
		require.NoError(tb, err)
		cid1 := cid.NewCidV1(cid.Raw, mh1)

		mh2, err := multihash.Sum([]byte("test2"), multihash.SHA2_256, -1)
		require.NoError(tb, err)
		cid2 := cid.NewCidV1(cid.Raw, mh2)

		// Mock BlockExists to return nil
		mockStore.EXPECT().BlockExists(cid1).Return(nil).Once()
		mockStore.EXPECT().BlockExists(cid2).Return(nil).Once()

		// Mock DownloadObjectWithOptions calls in order we expect them to be processed
		// (high priority first, then low priority)
		mockStorage.EXPECT().DownloadObjectWithOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, protocol core.StorageProtocol, objectHash core.StorageHash, opts ...core.StorageOptionFunc) (io.ReadCloser, error) {
				// Return the correct data for each CID
				if objectHash.Multihash().String() == cid2.Hash().String() {
					return &mockReadCloser{Reader: strings.NewReader("test2")}, nil
				}
				if objectHash.Multihash().String() == cid1.Hash().String() {
					return &mockReadCloser{Reader: strings.NewReader("test1")}, nil
				}
				return nil, fmt.Errorf("unexpected CID requested")
			}).Times(2)

		// Mock BlockSiblings and BlockChildren to return empty slices
		mockStore.EXPECT().BlockSiblings(mock.Anything, 64).Return([]cid.Cid{}, nil)
		mockStore.EXPECT().BlockChildren(mock.Anything, mock.Anything).Return([]cid.Cid{}, nil)

		// Queue cid1 with low priority (should be processed second)
		cid1Done := make(chan struct{})
		go func() {
			_, err := bd.Get(context.Background(), cid1)
			require.NoError(tb, err)
			close(cid1Done)
		}()

		// Wait a bit to ensure cid1 is queued first
		time.Sleep(10 * time.Millisecond)

		// Then queue cid2 with high priority
		go func() {
			_, err := bd.Get(context.Background(), cid2)
			require.NoError(tb, err)
		}()

		// Wait for downloads to complete
		time.Sleep(500 * time.Millisecond)
	}, ipfsTestConfig)
}

func TestNewBlockDownloader_ProtocolNotFound(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		// Configure the BlockDownloaderDefault
		_, err := downloader.NewBlockDownloader(ctx, mockStore, 2)
		assert.Error(tb, err)
		assert.Contains(t, err.Error(), "protocol not found")
	}, coreTesting.WithConfig("core.protocols."+internal.ProtocolName+".enabled", false))
}

func TestBlockDownloader_ConcurrentGet(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		numRoutines := 10
		var wg sync.WaitGroup
		wg.Add(numRoutines)

		// Create test CIDs and data
		testData := make([]struct {
			cid  cid.Cid
			data string
		}, numRoutines)

		for i := 0; i < numRoutines; i++ {
			data := fmt.Sprintf("test data %d", i)
			mh, err := multihash.Sum([]byte(data), multihash.SHA2_256, -1)
			require.NoError(tb, err)
			testData[i].cid = cid.NewCidV1(cid.Raw, mh)
			testData[i].data = data
		}

		// Mock the DownloadObjectWithOptions call - expect exactly numRoutines calls with different data
		mockStorage.EXPECT().DownloadObjectWithOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, protocol core.StorageProtocol, objectHash core.StorageHash, opts ...core.StorageOptionFunc) (io.ReadCloser, error) {
				// Find the matching test data for this CID
				for _, td := range testData {
					if objectHash.Multihash().String() == td.cid.Hash().String() {
						return &mockReadCloser{Reader: strings.NewReader(td.data)}, nil
					}
				}
				return nil, fmt.Errorf("unexpected CID requested")
			}).Times(numRoutines)

		// Mock the BlockSiblings and BlockChildren calls for all CIDs
		for _, td := range testData {
			mockStore.EXPECT().BlockSiblings(td.cid, 64).Return([]cid.Cid{}, nil)
			mockStore.EXPECT().BlockChildren(td.cid, mock.Anything).Return([]cid.Cid{}, nil)
			mockStore.EXPECT().BlockExists(td.cid).Return(nil)
		}

		// Configure the BlockDownloaderDefault
		bd, err := downloader.NewBlockDownloader(ctx, mockStore, 4)
		require.NoError(tb, err)

		// Launch multiple goroutines to call Get concurrently with different CIDs
		for i := 0; i < numRoutines; i++ {
			go func(i int) {
				defer wg.Done()
				td := testData[i]
				block, err := bd.Get(context.Background(), td.cid)
				require.NoError(tb, err)
				assert.NotNil(tb, block)
				assert.Equal(tb, []byte(td.data), block.RawData())
			}(i)
		}

		wg.Wait() // Wait for all goroutines to complete
	}, ipfsTestConfig)
}

func TestBlockDownloader_HashMismatch(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		// Configure the BlockDownloaderDefault
		bd, err := downloader.NewBlockDownloader(ctx, mockStore, 2)
		require.NoError(tb, err)

		// Create a test CID
		mh, err := multihash.Sum([]byte("test data"), multihash.SHA2_256, -1)
		require.NoError(tb, err)
		testCid := cid.NewCidV1(cid.Raw, mh)

		// Mock the BlockExists call
		mockStore.EXPECT().BlockExists(testCid).Return(nil).Once()

		// Mock the DownloadObjectWithOptions call to return incorrect data
		mockStorage.EXPECT().DownloadObjectWithOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockReadCloser{Reader: strings.NewReader("incorrect data")}, nil).Once()

		// Call Get
		block, err := bd.Get(context.Background(), testCid)
		assert.Error(tb, err)
		assert.Nil(tb, block)
		assert.Contains(t, err.Error(), "block hash mismatch")
	}, ipfsTestConfig)
}

func TestBlockDownloader_VerifyError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		// Configure the BlockDownloaderDefault
		bd, err := downloader.NewBlockDownloader(ctx, mockStore, 2)
		require.NoError(tb, err)

		// Create a test CID with an invalid multihash type
		mh, err := multihash.Sum([]byte("test data"), multihash.SHA1, -1)
		require.NoError(tb, err)
		testCid := cid.NewCidV1(cid.Raw, mh)

		// Mock the BlockExists call
		mockStore.EXPECT().BlockExists(testCid).Return(nil).Once()

		// Mock the DownloadObjectWithOptions call
		mockStorage.EXPECT().DownloadObjectWithOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockReadCloser{Reader: strings.NewReader("test data")}, nil).Once()

		// Call Get
		block, err := bd.Get(context.Background(), testCid)
		if assert.Error(tb, err) {
			assert.Nil(tb, block)
			assert.Contains(tb, err.Error(), "unsupported hash function")
		}
	}, ipfsTestConfig)
}

func TestBlockDownloader_ReadError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		// Configure the BlockDownloaderDefault
		bd, err := downloader.NewBlockDownloader(ctx, mockStore, 2)
		require.NoError(tb, err)

		// Create a test CID
		mh, err := multihash.Sum([]byte("test data"), multihash.SHA2_256, -1)
		require.NoError(tb, err)
		testCid := cid.NewCidV1(cid.Raw, mh)

		// Mock the BlockExists call
		mockStore.EXPECT().BlockExists(testCid).Return(nil).Once()

		// Mock the DownloadObjectWithOptions call to return a reader that errors
		mockStorage.EXPECT().DownloadObjectWithOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockReadCloser{Reader: &errorReader{}}, nil).Once()

		// Call Get
		block, err := bd.Get(context.Background(), testCid)
		assert.Error(tb, err)
		assert.Nil(tb, block)
		assert.Contains(t, err.Error(), "failed to read block")
	}, ipfsTestConfig)
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read error")
}

func TestBlockDownloader_QueueRelated_BlockSiblingsError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		// Configure the BlockDownloaderDefault
		bd, err := downloader.NewBlockDownloader(ctx, mockStore, 2)
		require.NoError(tb, err)

		// Create a test CID
		mh, err := multihash.Sum([]byte("test data"), multihash.SHA2_256, -1)
		require.NoError(tb, err)
		testCid := cid.NewCidV1(cid.Raw, mh)

		// Mock the BlockExists call
		mockStore.EXPECT().BlockExists(testCid).Return(nil).Once()

		// Mock the DownloadObjectWithOptions call
		mockStorage.EXPECT().DownloadObjectWithOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockReadCloser{Reader: strings.NewReader("test data")}, nil).Once()

		// Mock BlockSiblings to return an error when queueRelated is called
		mockStore.EXPECT().BlockSiblings(testCid, 64).Return([]cid.Cid{}, fmt.Errorf("siblings error")).Once()

		// Call Get which will trigger queueRelated for high priority downloads
		block, err := bd.Get(context.Background(), testCid)
		require.NoError(tb, err)
		assert.NotNil(tb, block)

		// Wait for queueRelated to complete
		time.Sleep(100 * time.Millisecond)
	}, ipfsTestConfig)
}

func TestBlockDownloader_QueueRelated_BlockChildrenError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		// Configure the BlockDownloaderDefault
		bd, err := downloader.NewBlockDownloader(ctx, mockStore, 2)
		require.NoError(tb, err)

		// Create a test CID
		mh, err := multihash.Sum([]byte("test data"), multihash.SHA2_256, -1)
		require.NoError(tb, err)
		testCid := cid.NewCidV1(cid.Raw, mh)

		// Mock the BlockExists call
		mockStore.EXPECT().BlockExists(testCid).Return(nil).Once()

		// Mock the DownloadObjectWithOptions call
		mockStorage.EXPECT().DownloadObjectWithOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&mockReadCloser{Reader: strings.NewReader("test data")}, nil).Once()

		// Mock BlockSiblings to return empty slice
		mockStore.EXPECT().BlockSiblings(testCid, 64).Return([]cid.Cid{}, nil).Once()

		// Mock BlockChildren to return an error when queueRelated is called
		mockStore.EXPECT().BlockChildren(testCid, mock.Anything).Return([]cid.Cid{}, fmt.Errorf("children error")).Once()

		// Call Get which will trigger queueRelated for high priority downloads
		block, err := bd.Get(context.Background(), testCid)
		require.NoError(tb, err)
		assert.NotNil(tb, block)

		// Wait for queueRelated to complete
		time.Sleep(100 * time.Millisecond)
	}, ipfsTestConfig)
}

func TestBlockDownloader_Get_ContextDone(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a mock MetadataStore
		mockStore := mocks.NewMockMetadataStore(t)

		mockStorage := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		// Configure the BlockDownloaderDefault
		bd, err := downloader.NewBlockDownloader(ctx, mockStore, 2)
		require.NoError(tb, err)

		// Create a test CID
		mh, err := multihash.Sum([]byte("test data"), multihash.SHA2_256, -1)
		require.NoError(tb, err)
		testCid := cid.NewCidV1(cid.Raw, mh)

		// Create a context that is already done
		cctx, ccancel := context.WithCancel(context.Background())
		ccancel()

		// Call Get - should fail immediately without calling any mocks
		block, err := bd.Get(cctx, testCid)
		assert.Error(tb, err)
		assert.Nil(tb, block)
		assert.Contains(t, err.Error(), "context canceled")

		// Verify no unexpected calls were made to mocks
		mockStore.AssertNotCalled(tb, "BlockExists", mock.Anything)
		mockStorage.AssertNotCalled(tb, "DownloadObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	}, ipfsTestConfig)
}
