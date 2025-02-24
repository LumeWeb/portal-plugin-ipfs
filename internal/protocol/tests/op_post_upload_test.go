package tests

import (
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/pin"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/upload"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/service"
	"io/fs"
	"os"
	"path"
	"testing"
)

func TestPostUploadOperationHandler_Execute_Integration(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		// 1. Read the CAR file content from the fixture.
		carFile, err := os.OpenFile(path.Join(path.Dir(currentFile), carFileName), os.O_RDONLY, fs.ModePerm)
		defer func(carFile *os.File) {
			_ = carFile.Close()
		}(carFile)
		require.NoError(tb, err)

		// 3. Create a test user account
		userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
		testUser, err := userSvc.CreateAccount("test@example.com", "testpassword123", false)
		require.NoError(tb, err)

		// 5. Create a WorkflowTest instance.
		wfTest := coreTesting.NewWorkflowTest(ctx)

		uploadService := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)

		root, uploadId, err := uploadService.HandleUpload(ctx, carFile, testUser.ID)
		require.NoError(tb, err)

		// 8. Start the workflow with the upload hash.
		req := wfTest.StartOperationWorkflow("ipfs.post.upload",
			core.WithWorkflowStorageHash(internal.NewIPFSHash(root)),
			core.WithWorkflowUserID(testUser.ID),
			core.WithWorkflowSourceIP("127.0.0.1"),
			core.WithWorkflowStructData(protocol.PostUploadWorkflowData{
				UploadID: uploadId,
			}, "json"),
		)

		// Act
		// Execute the workflow step.
		wfTest.ExecuteWorkflowStep(req)

		// Assert
		// Assertions
		wfTest.AssertOperationSuccess(req)
		wfTest.AssertOperationStatusMessageContains(req, "Upload processed successfully")
		wfTest.AssertOperationStatusProgress(req, 100)

	},
		coreTesting.WithStatefulMockRenterService(),
		coreTesting.WithServiceFactory(core.UPLOAD_SERVICE, service.NewMetadataService),
		coreTesting.WithServiceFactory(core.PIN_SERVICE, service.NewPinService),
		coreTesting.WithServiceFactory(core.STORAGE_SERVICE, service.NewStorageService),
		coreTesting.WithServiceFactory(core.CRON_SERVICE, service.NewCronService),
		coreTesting.WithServiceFactory(core.REQUEST_SERVICE, service.NewRequestService),
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithServiceFactory(core.USER_SERVICE, service.NewUserService),
		coreTesting.WithServiceFactory(pluginCore.PIN_SERVICE, pin.NewPinService),
		coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, upload.NewUploadService),
		coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
		coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
		coreTesting.WithMockS3(),
	)
}
