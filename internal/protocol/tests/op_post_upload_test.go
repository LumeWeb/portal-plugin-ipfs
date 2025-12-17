package tests

import (
	"io/fs"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
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
		wfTest.CompleteWorkflowStep(req)

		// Assert
		// Assertions
		wfTest.AssertOperationSuccess(req)
		wfTest.AssertOperationStatusMessageContains(req, "Upload processed successfully")
		wfTest.AssertOperationStatusProgress(req, 100)

	},
		coreTesting.CombineOptions(GetCommonTestOptions(), GetDbTestOptions()),
	)
}
