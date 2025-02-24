package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tus/tusd/v2/pkg/handler"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/pin"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/upload"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/service"
	"io"
	"io/fs"
	"os"
	"path"
	"testing"
)

func TestTUSUploadOperationHandler_Execute_Integration(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// --- Test Setup ---
		// Initialize services and test dependencies
		wfTest := coreTesting.NewWorkflowTest(ctx)
		tusService := core.GetService[core.TUSService](ctx, core.TUS_SERVICE)
		storageSvc := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
		userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
		proto := core.GetProtocol(internal.ProtocolName)

		// --- Test Data Preparation ---
		// 1. Load CAR file fixture
		carFile, err := os.OpenFile(path.Join(path.Dir(currentFile), carFileName), os.O_RDONLY, fs.ModePerm)
		defer func() { _ = carFile.Close() }()
		require.NoError(tb, err)

		carSize, err := carFile.Stat()
		require.NoError(tb, err)

		roots, err := internal.GetCarRoots(carFile)
		require.NoError(tb, err)

		// 2. Create test user
		testUser, err := userSvc.CreateAccount("test@example.com", "testpassword123", false)
		require.NoError(tb, err)

		// --- TUS Upload Setup ---
		objectId := uuid.New().String()
		uploadId := uuid.New().String()
		fullId := fmt.Sprintf("%s+%s", objectId, uploadId)
		uploaderIp := "127.0.0.1"

		// Create TUS upload
		tusUpload, err := tusService.CreateUpload(
			ctx,
			internal.NewIPFSHash(roots[0]),
			fullId,
			testUser.ID,
			uploaderIp,
			proto.(core.StorageProtocol),
		)
		require.NoError(tb, err)

		err = tusService.UploadProcessing(ctx, proto.(core.StorageProtocol), tusUpload.TUSUploadID)
		require.NoError(tb, err)

		// --- Storage Upload ---
		// Upload file info
		fileInfo := handler.FileInfo{ID: objectId, Size: carSize.Size()}
		infoData := io.NopCloser(bytes.NewReader(mustMarshal(tb, fileInfo)))
		err = storageSvc.S3MultipartUpload(
			ctx,
			infoData,
			ctx.Config().Config().Core.Storage.S3.BufferBucket,
			storageSvc.GetTemporaryUploadPath(proto.(core.StorageProtocol), fmt.Sprintf("%s.info", objectId)),
			uint64(len(mustMarshal(tb, fileInfo))),
		)
		require.NoError(tb, err)

		// Upload CAR file
		err = storageSvc.S3MultipartUpload(
			ctx,
			carFile,
			ctx.Config().Config().Core.Storage.S3.BufferBucket,
			storageSvc.GetTemporaryUploadPath(proto.(core.StorageProtocol), objectId),
			uint64(carSize.Size()),
		)
		require.NoError(tb, err)

		// --- Workflow Execution ---
		wf := wfTest.NewOperationWorkflow(core.TUSUploadOperationName(internal.ProtocolName))
		wfTest.MustConvertRequestToWorkflow(
			tusUpload.GetRequestID(),
			wf,
			0,
			core.WithWorkflowStorageHash(internal.NewIPFSHash(roots[0])),
			core.WithWorkflowUserID(testUser.ID),
			core.WithWorkflowSourceIP(uploaderIp),
		)

		req := wfTest.GetRequest(tusUpload.RequestID)
		wfTest.ExecuteWorkflowStep(req)

		// --- Assertions ---
		wfTest.AssertOperationSuccess(req)
		wfTest.AssertOperationStatusMessageContains(req, "Request completed successfully")
		wfTest.AssertOperationStatusProgress(req, 100)
	},
		coreTesting.WithStatefulMockRenterService(),
		coreTesting.WithServiceFactory(core.REQUEST_SERVICE, service.NewRequestService),
		coreTesting.WithServiceFactory(core.TUS_SERVICE, service.NewTUSService),
		coreTesting.WithServiceFactory(core.UPLOAD_SERVICE, service.NewMetadataService),
		coreTesting.WithServiceFactory(core.PIN_SERVICE, service.NewPinService),
		coreTesting.WithServiceFactory(core.STORAGE_SERVICE, service.NewStorageService),
		coreTesting.WithServiceFactory(core.CRON_SERVICE, service.NewCronService),
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithServiceFactory(core.USER_SERVICE, service.NewUserService),
		coreTesting.WithServiceFactory(pluginCore.PIN_SERVICE, pin.NewPinService),
		coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, upload.NewUploadService),
		coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
		coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
		coreTesting.WithAPI(internal.ProtocolName, api.NewAPI),
		coreTesting.WithAPIConfig(internal.ProtocolName, &pluginConfig.APIConfig{}),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
		coreTesting.WithMockS3(),
	)
}
func mustMarshal(tb coreTesting.TB, v interface{}) []byte {
	data, err := json.Marshal(v)
	require.NoError(tb, err)
	return data
}
