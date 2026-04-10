package tus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tus/tusd/v2/pkg/handler"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
)

func mustMarshal(tb coreTesting.TB, v interface{}) []byte {
	data, err := json.Marshal(v)
	require.NoError(tb, err)
	return data
}

// assertTUSWorkflowSuccess performs TUS-specific workflow assertions with expected message
func assertTUSWorkflowSuccess(wfTest *coreTesting.WorkflowTest, req *models.Request) {
	wfTest.AssertOperationSuccess(req)
	wfTest.AssertOperationStatusMessageContains(req, "Successfully completed")
	wfTest.AssertOperationStatusProgress(req, 100)
}

// SetupTUSUpload creates a TUS upload with optional hash and returns protocol and request ID
// hash can be nil for files where hash is not yet known (e.g., non-CAR files)
func SetupTUSUpload(tb coreTesting.TB, ctx coreTesting.TestContext, uploadFile *os.File, hash core.StorageHash) (core.StorageProtocol, uint) {
	tb.Helper()
	tusService := core.GetService[core.TUSService](ctx, core.TUS_SERVICE)
	storageSvc := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
	proto := core.GetProtocol(internal.ProtocolName)

	// TUS Upload Setup
	objectId := uuid.New().String()
	uploadId := uuid.New().String()
	fullId := fmt.Sprintf("%s+%s", objectId, uploadId)

	// Create TUS upload
	testUser, err := core.GetService[core.UserService](ctx, core.USER_SERVICE).CreateAccount(ctx, "test@example.com", "testpassword123", false)
	require.NoError(tb, err)

	tusUpload, err := tusService.CreateUpload(
		ctx,
		hash,
		fullId,
		testUser.ID,
		"127.0.0.1",
		proto.(core.StorageProtocol),
	)
	require.NoError(tb, err)

	err = tusService.UploadProcessing(ctx, proto.(core.StorageProtocol), tusUpload.TUSUploadID)
	require.NoError(tb, err)

	// Get file stats for S3 upload
	fileSize, err := uploadFile.Stat()
	require.NoError(tb, err)

	// S3 Upload
	fileInfo := handler.FileInfo{ID: objectId, Size: fileSize.Size()}
	infoData := io.NopCloser(bytes.NewReader(mustMarshal(tb, fileInfo)))
	err = storageSvc.S3MultipartUpload(
		ctx,
		infoData,
		ctx.Config().Config().Core.Storage.S3.BufferBucket,
		storageSvc.GetTemporaryUploadPath(proto.(core.StorageProtocol), fmt.Sprintf("%s.info", objectId)),
		uint64(len(mustMarshal(tb, fileInfo))),
	)
	require.NoError(tb, err)

	// Upload file to S3
	err = storageSvc.S3MultipartUpload(
		ctx,
		uploadFile,
		ctx.Config().Config().Core.Storage.S3.BufferBucket,
		storageSvc.GetTemporaryUploadPath(proto.(core.StorageProtocol), objectId),
		uint64(fileSize.Size()),
	)
	require.NoError(tb, err)

	return proto.(core.StorageProtocol), tusUpload.RequestID
}

// AssertTUSWorkflowSuccess is exported version for test packages
func AssertTUSWorkflowSuccess(wfTest *coreTesting.WorkflowTest, req *models.Request) {
	assertTUSWorkflowSuccess(wfTest, req)
}
