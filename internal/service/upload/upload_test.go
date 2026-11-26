package upload

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	quota "go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	quotaCore "go.lumeweb.com/portal-plugin-quota/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"

	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/fixtures"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/types"
	"go.lumeweb.com/portal/event"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/service"
)

func TestMain(m *testing.M) {
	coreTesting.WithDBAndOptions(m, coreTesting.WithMockServiceFactory(pluginCore.PIN_SERVICE, mocks.NewMockIPFSPinService),
		coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, NewUploadService),
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
		coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
		coreTesting.WithConfig("plugin.ipfs.protocol.port", 0),
		coreTesting.WithConfig("plugin.ipfs.protocol.ws_port", 0),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		))
}

func TestUploadService_HandleUpload(t *testing.T) {
	t.Skip("Skipping test - requires valid CAR fixture file")
	// TODO: Generate valid CAR file programmatically or provide test fixture
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		internal.RegisterHashes()
		err := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol).GetNode().Close()
		require.NoError(t, err)
		// Arrange
		uploadService := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, uploadService)

		mockStorageService := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		testReader, err := os.Open(filepath.Join(fixtures.FixturesDir, "cars/filetree.car"))
		require.NoError(tb, err)

		userId := uint(123)

		// Set expectations on the mock services
		// HandleUpload only calls S3TemporaryUpload, AddPin is called in CreateRootPin via workflow
		mockStorageService.EXPECT().S3TemporaryUpload(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", nil).Once()

		// Act
		_, _, err = uploadService.HandleUpload(context.Background(), testReader, userId)

		// Assert
		assert.NoError(tb, err)
	})
}

func TestUploadService_HandleUpload_WithMode(t *testing.T) {
	tests := []struct {
		name            string
		archiveMode     upload.ArchiveMode
		useHandleUpload bool
	}{
		{
			name:            "ZIPConvert",
			archiveMode:     upload.ArchiveConvert,
			useHandleUpload: false,
		},
		{
			name:            "ZIPPreserve",
			archiveMode:     upload.ArchivePreserve,
			useHandleUpload: false,
		},
		{
			name:            "DefaultMode",
			archiveMode:     upload.ArchiveConvert, // Default should be convert
			useHandleUpload: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				internal.RegisterHashes()
				err := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol).GetNode().Close()
				require.NoError(tb, err, "Failed to close protocol node")

				// Arrange
				uploadService := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
				require.NotNil(tb, uploadService)

				mockStorageService := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

				// Create test ZIP file using upload package
				testFiles := []upload.TestFile{
					{
						Name:     "test.txt",
						Content:  "Hello, World!",
						IsDir:    false,
						Mode:     0644,
						Modified: time.Now(),
					},
				}
				zipData := bytes.NewReader(upload.CreateZIPArchive(t, ctx, testFiles))
				reader := io.NopCloser(zipData)

				userId := uint(123)

				// Set expectations on mock services
				mockStorageService.EXPECT().S3TemporaryUpload(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-upload-id", nil).Once()

				// Act
				var rootCID cid.Cid
				var uploadId string
				if tt.useHandleUpload {
					rootCID, uploadId, err = uploadService.HandleUpload(ctx, upload.NewUniversalReader(reader), userId)
				} else {
					rootCID, uploadId, err = uploadService.HandleUploadWithMode(ctx, upload.NewUniversalReader(reader), userId, tt.archiveMode)
				}

				// Assert
				assert.NoError(tb, err)
				assert.NotEqual(tb, cid.Undef, rootCID)
				assert.Equal(tb, "test-upload-id", uploadId)

				mockStorageService.AssertExpectations(tb)
			})
		})
	}
}

func TestUploadService_CreateRootPin_Idempotent(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		uploadSvc := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, uploadSvc)

		mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		require.NotNil(tb, mockPinSvc)

		testCID, err := cid.NewPrefixV1(cid.Raw, 0x12).Sum([]byte("test content for idempotency"))
		require.NoError(tb, err)
		userID := uint(123)

		existingPin := &pluginDb.IPFSPin{
			RequestID: types.NewBinUUID(),
			UserID:    userID,
			CID:       testCID.Bytes(),
		}

		// First call: no existing pin found, AddPin creates one
		mockPinSvc.EXPECT().GetPinByCIDAndUser(mock.Anything, testCID, userID).Return(nil, nil).Once()
		mockPinSvc.EXPECT().AddPin(mock.Anything, mock.Anything).Return(existingPin, nil).Once()

		pin1, err := uploadSvc.CreateRootPin(ctx, testCID, userID, "test-pin")
		require.NoError(tb, err)
		require.NotNil(tb, pin1)
		require.Equal(tb, existingPin.RequestID, pin1.RequestID)

		// Second call: existing pin found, AddPin should NOT be called
		mockPinSvc.EXPECT().GetPinByCIDAndUser(mock.Anything, testCID, userID).Return(existingPin, nil).Once()

		pin2, err := uploadSvc.CreateRootPin(ctx, testCID, userID, "test-pin")
		require.NoError(tb, err)
		require.NotNil(tb, pin2)
		require.Equal(tb, pin1.RequestID, pin2.RequestID, "second call should return same pin")

		mockPinSvc.AssertExpectations(tb)
	})
}

// TestUploadService_ProcessUpload_IdempotentQuotaEvents verifies that calling
// ProcessUpload twice for the same CIDs emits each quota event only once per
// CID. The pin workflow processes child blocks in the retrieve operation and
// the full CID list again in confirm; without the already-pinned skip, child
// upload and storage usage would be recorded twice.
func TestUploadService_ProcessUpload_IdempotentQuotaEvents(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		internal.RegisterHashes()
		metaStore := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol).GetMetadataStore()

		userID := uint(321)
		data1 := []byte("process-upload-idempotent-1")
		data2 := []byte("process-upload-idempotent-2")
		hash1, err := multihash.Sum(data1, multihash.SHA2_256, -1)
		require.NoError(tb, err)
		hash2, err := multihash.Sum(data2, multihash.SHA2_256, -1)
		require.NoError(tb, err)
		c1 := cid.NewCidV1(cid.Raw, hash1)
		c2 := cid.NewCidV1(cid.Raw, hash2)
		require.NoError(tb, metaStore.Pin(ctx, pluginCore.PinnedBlock{Cid: c1, Size: 100, Node: merkledag.NewRawNode(data1)}))
		require.NoError(tb, metaStore.Pin(ctx, pluginCore.PinnedBlock{Cid: c2, Size: 200, Node: merkledag.NewRawNode(data2)}))

		var uploadEvents, storageEvents int32
		core.Listen[event.UploadCompletedEvent](ctx, event.EVENT_UPLOAD_COMPLETED, func(e *core.CoreEvent[event.UploadCompletedEvent]) error {
			atomic.AddInt32(&uploadEvents, 1)
			return nil
		})
		core.Listen[event.StorageObjectPinnedEvent](ctx, event.EVENT_STORAGE_OBJECT_PINNED, func(e *core.CoreEvent[event.StorageObjectPinnedEvent]) error {
			atomic.AddInt32(&storageEvents, 1)
			return nil
		})

		uploadService := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, uploadService)

		// Both core services are mocked in this environment. The pin mock is
		// backed by in-test state so pin rows persist across the two
		// ProcessUpload calls and the second pass must skip them.
		mockUploadSvc := coreTesting.GetMockUploadService(ctx)
		require.NotNil(tb, mockUploadSvc)
		uploadIDs := map[string]uint{} // find-or-create, like the real service
		var nextUploadID uint
		mockUploadSvc.EXPECT().SaveUpload(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, upload *models.Upload) error {
				key := string(upload.Hash)
				if id, ok := uploadIDs[key]; ok {
					upload.ID = id
					return nil
				}
				nextUploadID++
				uploadIDs[key] = nextUploadID
				upload.ID = nextUploadID
				return nil
			}).Maybe()

		mockPinSvc := coreTesting.GetMockPinService(ctx)
		require.NotNil(tb, mockPinSvc)
		var createdPins []*models.Pin
		mockPinSvc.EXPECT().GetPinsByUploadID(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, uploadID uint) ([]*models.Pin, error) {
				var pins []*models.Pin
				for _, p := range createdPins {
					if p.UploadID == uploadID {
						pins = append(pins, p)
					}
				}
				return pins, nil
			}).Maybe()
		mockPinSvc.EXPECT().CreatePin(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, pin *models.Pin, _ any) (*models.Pin, error) {
				pin.ID = uint(len(createdPins) + 1)
				createdPins = append(createdPins, pin)
				return pin, nil
			}).Maybe()

		cids := []cid.Cid{c1, c2}
		require.NoError(tb, uploadService.ProcessUpload(context.Background(), cids, userID, nil))
		require.NoError(tb, uploadService.ProcessUpload(context.Background(), cids, userID, nil))

		assert.Equal(tb, int32(2), atomic.LoadInt32(&uploadEvents), "one upload event per CID across both passes")
		assert.Equal(tb, int32(2), atomic.LoadInt32(&storageEvents), "one storage event per CID across both passes")
	})
}

// fakeReservation records Release calls so tests can assert that skipped
// CIDs do not leak their quota reservations.
type fakeReservation struct {
	usageType quotaCore.UsageType
	released  int32
}

func newFakeReservation(usageType quotaCore.UsageType) *fakeReservation {
	return &fakeReservation{usageType: usageType}
}

func (f *fakeReservation) Release()     { atomic.AddInt32(&f.released, 1) }
func (f *fakeReservation) UUID() string { return "test-uuid" }
func (f *fakeReservation) UserID() uint { return 1 }
func (f *fakeReservation) UsageType() quotaCore.UsageType {
	return f.usageType
}
func (f *fakeReservation) Bytes() int64 { return 100 }

// TestUploadService_ProcessUpload_SkippedCIDReleasesReservations verifies that
// when a CID is skipped because a pin already exists, its per-block quota
// reservations are released instead of being left held, and no quota events
// are emitted for the skipped CID.
func TestUploadService_ProcessUpload_SkippedCIDReleasesReservations(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		internal.RegisterHashes()
		metaStore := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol).GetMetadataStore()

		userID := uint(654)
		data1 := []byte("process-upload-release-1")
		data2 := []byte("process-upload-release-2")
		hash1, err := multihash.Sum(data1, multihash.SHA2_256, -1)
		require.NoError(tb, err)
		hash2, err := multihash.Sum(data2, multihash.SHA2_256, -1)
		require.NoError(tb, err)
		c1 := cid.NewCidV1(cid.Raw, hash1)
		c2 := cid.NewCidV1(cid.Raw, hash2)
		require.NoError(tb, metaStore.Pin(ctx, pluginCore.PinnedBlock{Cid: c1, Size: 100, Node: merkledag.NewRawNode(data1)}))
		require.NoError(tb, metaStore.Pin(ctx, pluginCore.PinnedBlock{Cid: c2, Size: 200, Node: merkledag.NewRawNode(data2)}))

		var uploadEvents, storageEvents int32
		core.Listen[event.UploadCompletedEvent](ctx, event.EVENT_UPLOAD_COMPLETED, func(e *core.CoreEvent[event.UploadCompletedEvent]) error {
			atomic.AddInt32(&uploadEvents, 1)
			return nil
		})
		core.Listen[event.StorageObjectPinnedEvent](ctx, event.EVENT_STORAGE_OBJECT_PINNED, func(e *core.CoreEvent[event.StorageObjectPinnedEvent]) error {
			atomic.AddInt32(&storageEvents, 1)
			return nil
		})

		mockUploadSvc := coreTesting.GetMockUploadService(ctx)
		require.NotNil(tb, mockUploadSvc)
		uploadIDs := map[string]uint{} // find-or-create, like the real service
		var nextUploadID uint
		mockUploadSvc.EXPECT().SaveUpload(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, upload *models.Upload) error {
				key := string(upload.Hash)
				if id, ok := uploadIDs[key]; ok {
					upload.ID = id
					return nil
				}
				nextUploadID++
				uploadIDs[key] = nextUploadID
				upload.ID = nextUploadID
				return nil
			}).Maybe()

		// Both CIDs already have pins, so every CID is skipped.
		mockPinSvc := coreTesting.GetMockPinService(ctx)
		require.NotNil(tb, mockPinSvc)
		existingPins := map[uint][]*models.Pin{}
		mockPinSvc.EXPECT().GetPinsByUploadID(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, uploadID uint) ([]*models.Pin, error) {
				return existingPins[uploadID], nil
			}).Maybe()
		mockPinSvc.EXPECT().CreatePin(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, pin *models.Pin, _ any) (*models.Pin, error) {
				pin.ID = uint(len(existingPins[pin.UploadID]) + 1)
				existingPins[pin.UploadID] = append(existingPins[pin.UploadID], pin)
				return pin, nil
			}).Maybe()

		// Pre-create the pins the skip check will find. This pass emits one
		// event pair per CID; record the counts as the baseline.
		require.NoError(tb, uploadServiceProcessForTest(ctx, []cid.Cid{c1, c2}, userID, nil))
		baseUploadEvents := atomic.LoadInt32(&uploadEvents)
		baseStorageEvents := atomic.LoadInt32(&storageEvents)

		uploadService := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, uploadService)

		upRes1 := newFakeReservation(quotaCore.UsageTypeUpload)
		storRes1 := newFakeReservation(quotaCore.UsageTypeStorageAdd)
		upRes2 := newFakeReservation(quotaCore.UsageTypeUpload)
		storRes2 := newFakeReservation(quotaCore.UsageTypeStorageAdd)
		reservations := map[cid.Cid]*quota.BlockReservations{
			c1: {UploadReservation: &quotaCore.QuotaCheckResult{Reservation: upRes1}, StorageReservation: &quotaCore.QuotaCheckResult{Reservation: storRes1}},
			c2: {UploadReservation: &quotaCore.QuotaCheckResult{Reservation: upRes2}, StorageReservation: &quotaCore.QuotaCheckResult{Reservation: storRes2}},
		}

		require.NoError(tb, uploadService.ProcessUpload(context.Background(), []cid.Cid{c1, c2}, userID, reservations))

		assert.Equal(tb, baseUploadEvents, atomic.LoadInt32(&uploadEvents), "no new upload events for already-pinned CIDs")
		assert.Equal(tb, baseStorageEvents, atomic.LoadInt32(&storageEvents), "no new storage events for already-pinned CIDs")
		assert.Equal(t, int32(1), atomic.LoadInt32(&upRes1.released), "upload reservation released once for c1")
		assert.Equal(t, int32(1), atomic.LoadInt32(&storRes1.released), "storage reservation released once for c1")
		assert.Equal(t, int32(1), atomic.LoadInt32(&upRes2.released), "upload reservation released once for c2")
		assert.Equal(t, int32(1), atomic.LoadInt32(&storRes2.released), "storage reservation released once for c2")
	})
}

func uploadServiceProcessForTest(ctx coreTesting.TestContext, cids []cid.Cid, userID uint, reservations map[cid.Cid]*quota.BlockReservations) error {
	uploadService := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
	return uploadService.ProcessUpload(context.Background(), cids, userID, reservations)
}
