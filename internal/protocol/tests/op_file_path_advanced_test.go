package tests

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestFilePathOperationHandler_ComputePathsRecursive_MultiLevelDirectory(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		// Create a multi-level directory structure:
		// root_dir/
		//     subdir1/
		//         subdir2/
		//             file1.txt
		//             file2.txt
		rootCID := util.GenerateTestCID(t, "root_dir data")
		subdir1CID := util.GenerateTestCID(t, "subdir1 data")
		subdir2CID := util.GenerateTestCID(t, "subdir2 data")
		file1CID := util.GenerateTestCID(t, "file1 data")
		file2CID := util.GenerateTestCID(t, "file2 data")

		// Create UnixFS nodes
		_, rootUnixFS := util.CreateTestBlockAndNode(t, ctx, rootCID, "root_dir", 1, 0, []cid.Cid{subdir1CID})
		util.CreateTestBlockAndNode(t, ctx, subdir1CID, "subdir1", 1, 0, []cid.Cid{subdir2CID})
		util.CreateTestBlockAndNode(t, ctx, subdir2CID, "subdir2", 1, 0, []cid.Cid{file1CID, file2CID})
		util.CreateTestBlockAndNode(t, ctx, file1CID, "file1.txt", 0, 512, []cid.Cid{})
		util.CreateTestBlockAndNode(t, ctx, file2CID, "file2.txt", 0, 256, []cid.Cid{})

		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		require.NotNil(tb, fileManagerSvc)

		processed := make(map[string]bool)

		// Act
		err := handler.ComputePathsRecursive(context.Background(), fileManagerSvc, rootUnixFS, userID, rootCID, "", 0, processed, false)

		// Assert
		require.NoError(tb, err)

		// Verify all file paths were created
		var filePaths []pluginDb.FilePath
		result := ctx.DB().Where("user_id = ?", userID).Order("path ASC").Find(&filePaths)
		require.NoError(tb, result.Error)
		assert.Len(tb, filePaths, 5)

		// Verify the paths and other attributes
		expectedPaths := []string{
			"/root_dir",
			"/root_dir/subdir1",
			"/root_dir/subdir1/subdir2",
			"/root_dir/subdir1/subdir2/file1.txt",
			"/root_dir/subdir1/subdir2/file2.txt",
		}
		expectedNames := []string{
			"root_dir",
			"subdir1",
			"subdir2",
			"file1.txt",
			"file2.txt",
		}
		expectedDirectories := []bool{true, true, true, false, false}
		expectedParentPaths := []string{"", "/root_dir", "/root_dir/subdir1", "/root_dir/subdir1/subdir2", "/root_dir/subdir1/subdir2"}
		expectedDepths := []int{0, 1, 2, 3, 3}

		for i, path := range expectedPaths {
			assert.Equal(tb, path, filePaths[i].Path, "Path mismatch for index %d", i)
			assert.Equal(tb, expectedNames[i], filePaths[i].Name, "Name mismatch for index %d", i)
			assert.Equal(tb, expectedDirectories[i], filePaths[i].IsDirectory, "IsDirectory mismatch for index %d", i)
			assert.Equal(tb, expectedParentPaths[i], filePaths[i].ParentPath, "ParentPath mismatch for index %d", i)
			assert.Equal(tb, expectedDepths[i], filePaths[i].Depth, "Depth mismatch for index %d", i)
			assert.Equal(tb, userID, filePaths[i].UserID, "UserID mismatch for index %d", i)
			assert.False(tb, filePaths[i].IsOrphan, "IsOrphan should be false for index %d", i)
		}

		// Verify CIDs
		assert.Equal(tb, rootCID.Bytes(), filePaths[0].CID)
		assert.Equal(tb, subdir1CID.Bytes(), filePaths[1].CID)
		assert.Equal(tb, subdir2CID.Bytes(), filePaths[2].CID)
		assert.Equal(tb, file1CID.Bytes(), filePaths[3].CID)
		assert.Equal(tb, file2CID.Bytes(), filePaths[4].CID)

		// Verify that the processed map contains all CIDs
		assert.True(tb, processed[rootCID.String()])
		assert.True(tb, processed[subdir1CID.String()])
		assert.True(tb, processed[subdir2CID.String()])
		assert.True(tb, processed[file1CID.String()])
		assert.True(tb, processed[file2CID.String()])
	}, TestOptions)
}
