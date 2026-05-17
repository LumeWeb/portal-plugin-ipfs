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

func assertTUSWorkflowSuccess(wfTest *coreTesting.WorkflowTest, req *models.Request) {
	wfTest.AssertOperationSuccess(req)
	wfTest.AssertOperationStatusMessageContains(req, "Successfully completed")
	wfTest.AssertOperationStatusProgress(req, 100)
}

type TusUploadOption func(*tusUploadConfig)

type tusUploadConfig struct {
	existingUser *models.User
}

func WithExistingUser(user *models.User) TusUploadOption {
	return func(cfg *tusUploadConfig) {
		cfg.existingUser = user
	}
}

func SetupTUSUpload(tb coreTesting.TB, ctx coreTesting.TestContext, uploadFile *os.File, hash core.StorageHash, opts ...TusUploadOption) (core.StorageProtocol, uint, uint) {
	tb.Helper()

	var cfg tusUploadConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	tusService := core.GetService[core.TUSService](ctx, core.TUS_SERVICE)
	storageSvc := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
	proto := core.GetProtocol(internal.ProtocolName)

	objectId := uuid.New().String()
	uploadId := uuid.New().String()
	fullId := fmt.Sprintf("%s+%s", objectId, uploadId)

	var testUser *models.User
	if cfg.existingUser != nil {
		testUser = cfg.existingUser
	} else {
		var err error
		testUser, err = core.GetService[core.UserService](ctx, core.USER_SERVICE).CreateAccount(ctx, "test@example.com", "testpassword123", false)
		require.NoError(tb, err)
	}

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

	fileSize, err := uploadFile.Stat()
	require.NoError(tb, err)

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

	err = storageSvc.S3MultipartUpload(
		ctx,
		uploadFile,
		ctx.Config().Config().Core.Storage.S3.BufferBucket,
		storageSvc.GetTemporaryUploadPath(proto.(core.StorageProtocol), objectId),
		uint64(fileSize.Size()),
	)
	require.NoError(tb, err)

	return proto.(core.StorageProtocol), tusUpload.RequestID, testUser.ID
}

func AssertTUSWorkflowSuccess(wfTest *coreTesting.WorkflowTest, req *models.Request) {
	assertTUSWorkflowSuccess(wfTest, req)
}
