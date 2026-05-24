package upload

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"

	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/fixtures"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
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
