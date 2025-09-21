package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tus/tusd/v2/pkg/handler"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
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

		roots, err := internal.GetCarRoots(carFile, false)
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
		GetTUSUploadTestOptions()...,
	)
}
func mustMarshal(tb coreTesting.TB, v interface{}) []byte {
	data, err := json.Marshal(v)
	require.NoError(tb, err)
	return data
}
