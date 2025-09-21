package upload

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	"go.lumeweb.com/portal/service"
	"os"
	"path"
	"runtime"
	"testing"
)

var (
	_, currentFile, _, _ = runtime.Caller(0)
	carFileName          = "../../testing/fixtures/cars/filetree.car"
)

func TestUploadService_HandleUpload(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		internal.RegisterHashes()
		err := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol).GetNode().Close()
		// Arrange
		uploadService := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
		require.NotNil(tb, uploadService)

		mockPinService := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		mockStorageService := core.GetService[*coreMocks.MockStorageService](ctx, core.STORAGE_SERVICE)

		testReader, err := os.Open(path.Join(path.Dir(currentFile), carFileName))
		require.NoError(tb, err)

		userId := uint(123)

		roots, err := internal.GetCarRoots(testReader, false)
		require.NoError(tb, err)

		// Set expectations on the mock services
		mockStorageService.EXPECT().S3TemporaryUpload(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", nil).Once()
		mockPinService.EXPECT().AddPin(context.Background(), mock.AnythingOfType("*db.IPFSPin")).Return(&pluginDb.IPFSPin{
			UserID:    userId,
			CID:       roots[0].Bytes(),
			Name:      "",
			Origins:   nil,
			Meta:      nil,
			Delegates: nil,
			Info:      nil,
		}, nil).Once()

		// Act
		_, _, err = uploadService.HandleUpload(context.Background(), testReader, userId)

		// Assert
		assert.NoError(tb, err)
	}, coreTesting.CombineOptions(
		coreTesting.WithMockServiceFactory(pluginCore.PIN_SERVICE, mocks.NewMockIPFSPinService),
		coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, NewUploadService),
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
		coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
	))
}
