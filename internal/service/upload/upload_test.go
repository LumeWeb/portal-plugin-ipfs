package upload

import (
	"context"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"

	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/service"
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


		mockStorageService := core.GetService[*coreTesting.MockStorageService](ctx, core.STORAGE_SERVICE)

		testReader, err := os.Open(path.Join(path.Dir(currentFile), carFileName))
		require.NoError(tb, err)

		userId := uint(123)



		// Set expectations on the mock services
		// HandleUpload only calls S3TemporaryUpload, AddPin is called in CreateRootPin via workflow
		mockStorageService.EXPECT().S3TemporaryUpload(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", nil).Once()

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
