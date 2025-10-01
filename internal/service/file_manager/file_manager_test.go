package filemanager

import (
	"context"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/block"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/db/types"
	"go.lumeweb.com/queryutil"
)

var TestOptions = coreTesting.CombineOptions(
	coreTesting.WithServiceFactory(pluginCore.FILE_MANAGER_SERVICE, func() (core.Service, []core.ContextBuilderOption, error) {
		return NewFileManagerService()
	}),
	coreTesting.WithServiceFactory(pluginCore.BLOCK_SERVICE, func() (core.Service, []core.ContextBuilderOption, error) {
		return block.NewBlockService()
	}),
	coreTesting.WithSQLitePluginMigrations(
		internal.ProtocolName, migrations.GetSQLite(),
	),
)

func createTestFilePath(t *testing.T, ctx coreTesting.TestContext, userID uint, testCID cid.Cid, path, name string, isDirectory bool) *pluginDb.FilePath {
	// Calculate parent path
	var parentPath string
	if path == pluginDb.RootPath || path == "" {
		parentPath = pluginDb.RootPath
	} else {
		// Find the last slash to get the parent directory
		lastSlash := strings.LastIndex(path, "/")
		if lastSlash > 0 {
			parentPath = path[:lastSlash]
		} else if lastSlash == 0 {
			parentPath = pluginDb.RootPath
		}
	}

	filePath := &pluginDb.FilePath{
		UserID:      userID,
		CID:         testCID.Bytes(),
		Path:        path,
		Name:        name,
		Type:        0,
		Size:        1024,
		IsDirectory: isDirectory,
		IsOrphan:    false,
		ParentPath:  parentPath,
		Depth:       0,
	}

	err := ctx.DB().Create(filePath).Error
	require.NoError(t, err)
	return filePath
}

func createTestPin(t *testing.T, ctx coreTesting.TestContext, userID uint, testCID cid.Cid) *pluginDb.IPFSPin {
	pin := &pluginDb.IPFSPin{
		UserID:    userID,
		CID:       testCID.Bytes(),
		RequestID: types.NewBinUUID(),
		Status:    pluginDb.PinningStatusPinned,
	}

	err := ctx.DB().Create(pin).Error
	require.NoError(t, err)
	return pin
}

func TestFileManagerService_ListFiles(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		// Create test file paths
		createTestFilePath(t, ctx, userID, testCID1, "/file1.txt", "file1.txt", false)
		createTestFilePath(t, ctx, userID, testCID2, "/file2.txt", "file2.txt", false)

		// Act
		files, total, err := fileManagerSvc.ListFiles(context.Background(), userID, nil, nil, queryutil.DefaultPagination)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, files, 2)
		assert.Equal(tb, int64(2), total)

		// Verify file contents
		fileNames := []string{files[0].Name, files[1].Name}
		assert.Contains(tb, fileNames, "file1.txt")
		assert.Contains(tb, fileNames, "file2.txt")
	}, TestOptions)
}

func TestFileManagerService_ListFiles_Empty(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)

		// Act
		files, total, err := fileManagerSvc.ListFiles(context.Background(), userID, nil, nil, queryutil.DefaultPagination)

		// Assert
		require.NoError(tb, err)
		assert.Empty(tb, files)
		assert.Equal(tb, int64(0), total)
	}, TestOptions)
}

func TestFileManagerService_ListDirectoryContents(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		testCID3 := util.GenerateTestCID(t, "test data 3")

		// Create test file paths in root directory
		createTestFilePath(t, ctx, userID, testCID1, "/file1.txt", "file1.txt", false)
		createTestFilePath(t, ctx, userID, testCID2, "/dir1", "dir1", true)
		createTestFilePath(t, ctx, userID, testCID3, "/dir1/file2.txt", "file2.txt", false)

		// Act - list root directory
		contents, err := fileManagerSvc.ListDirectoryContents(context.Background(), userID, pluginDb.RootPath)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, contents, 2) // Should have file1.txt and dir1

		// Verify directories come first, then files
		assert.True(tb, contents[0].IsDirectory)  // dir1 should be first
		assert.False(tb, contents[1].IsDirectory) // file1.txt should be second

		assert.Equal(tb, "dir1", contents[0].Name)
		assert.Equal(tb, "file1.txt", contents[1].Name)
	}, TestOptions)
}

func TestFileManagerService_ListDirectoryContents_WithOrphans(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		// Create a normal file path
		createTestFilePath(t, ctx, userID, testCID1, "/file1.txt", "file1.txt", false)

		// Create an orphan file path
		orphanPath := &pluginDb.FilePath{
			UserID:      userID,
			CID:         testCID2.Bytes(),
			Path:        "/" + testCID2.String(),
			Name:        testCID2.String(),
			Type:        0,
			Size:        0,
			IsDirectory: false,
			IsOrphan:    true,
			ParentPath:  pluginDb.RootPath,
			Depth:       0,
		}
		err := ctx.DB().Create(orphanPath).Error
		require.NoError(t, err)

		// Act - list root directory (should include orphans)
		contents, err := fileManagerSvc.ListDirectoryContents(context.Background(), userID, pluginDb.RootPath)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, contents, 2) // Should have both normal and orphan files

		// Verify both files are present
		fileNames := []string{contents[0].Name, contents[1].Name}
		assert.Contains(tb, fileNames, "file1.txt")
		assert.Contains(tb, fileNames, testCID2.String())
	}, TestOptions)
}

func TestFileManagerService_GetBreadcrumbs(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		testCID3 := util.GenerateTestCID(t, "test data 3")

		// Create test file paths for breadcrumb hierarchy
		createTestFilePath(t, ctx, userID, testCID1, "/dir1", "dir1", true)
		createTestFilePath(t, ctx, userID, testCID2, "/dir1/subdir", "subdir", true)
		createTestFilePath(t, ctx, userID, testCID3, "/dir1/subdir/file.txt", "file.txt", false)

		targetPath := "/dir1/subdir/file.txt"

		// Act
		breadcrumbs, err := fileManagerSvc.GetBreadcrumbs(context.Background(), userID, targetPath)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, breadcrumbs, 3) // Should have dir1, subdir, and file.txt

		// Verify breadcrumb order (should be ordered by depth)
		assert.Equal(tb, "/dir1", breadcrumbs[0].Path)
		assert.Equal(tb, "/dir1/subdir", breadcrumbs[1].Path)
		assert.Equal(tb, "/dir1/subdir/file.txt", breadcrumbs[2].Path)

		assert.Equal(tb, "dir1", breadcrumbs[0].Name)
		assert.Equal(tb, "subdir", breadcrumbs[1].Name)
		assert.Equal(tb, "file.txt", breadcrumbs[2].Name)
	}, TestOptions)
}

func TestFileManagerService_GetBreadcrumbs_RootPath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create a root level file
		createTestFilePath(t, ctx, userID, testCID, "/file.txt", "file.txt", false)

		targetPath := "/file.txt"

		// Act
		breadcrumbs, err := fileManagerSvc.GetBreadcrumbs(context.Background(), userID, targetPath)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, breadcrumbs, 1) // Should have just the file itself

		assert.Equal(tb, "/file.txt", breadcrumbs[0].Path)
		assert.Equal(tb, "file.txt", breadcrumbs[0].Name)
	}, TestOptions)
}

func TestFileManagerService_CreateFilePath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		filePath := &pluginDb.FilePath{
			UserID:      userID,
			CID:         testCID.Bytes(),
			Path:        "/test/file.txt",
			Name:        "file.txt",
			Type:        0,
			Size:        1024,
			IsDirectory: false,
			IsOrphan:    false,
			ParentPath:  "/test",
			Depth:       1,
		}

		// Act
		err := fileManagerSvc.CreateFilePath(context.Background(), filePath)

		// Assert
		require.NoError(tb, err)

		// Verify the file path was created in the database
		var retrievedPath pluginDb.FilePath
		result := ctx.DB().Where("user_id = ? AND path = ?", userID, "/test/file.txt").First(&retrievedPath)
		require.NoError(tb, result.Error)

		assert.Equal(tb, filePath.Name, retrievedPath.Name)
		assert.Equal(tb, filePath.Path, retrievedPath.Path)
		assert.Equal(tb, filePath.CID, retrievedPath.CID)
	}, TestOptions)
}

func TestFileManagerService_ValidatePathCompleteness_Complete(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create both pin and file path
		createTestPin(t, ctx, userID, testCID)
		createTestFilePath(t, ctx, userID, testCID, "/file.txt", "file.txt", false)

		// Act
		isComplete, err := fileManagerSvc.ValidatePathCompleteness(context.Background())

		// Assert
		require.NoError(tb, err)
		assert.True(tb, isComplete)
	}, TestOptions)
}

func TestFileManagerService_ValidatePathCompleteness_Incomplete(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create only pin, no file path
		createTestPin(t, ctx, userID, testCID)

		// Act
		isComplete, err := fileManagerSvc.ValidatePathCompleteness(context.Background())

		// Assert
		require.NoError(tb, err)
		assert.False(tb, isComplete)
	}, TestOptions)
}

func TestFileManagerService_GetIncompletePins(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		// Create pin without file path (incomplete)
		createTestPin(t, ctx, userID, testCID1)

		// Create pin with file path (complete)
		createTestPin(t, ctx, userID, testCID2)
		createTestFilePath(t, ctx, userID, testCID2, "/file2.txt", "file2.txt", false)

		// Act
		incompletePins, err := fileManagerSvc.GetIncompletePins(context.Background())

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, incompletePins, 1)
		assert.Equal(tb, testCID1.Bytes(), incompletePins[0].CID)
	}, TestOptions)
}

func TestFileManagerService_GetOrphanedPaths(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		// Create file path without pin (orphaned)
		createTestFilePath(t, ctx, userID, testCID1, "/file1.txt", "file1.txt", false)

		// Create file path with pin (not orphaned)
		createTestPin(t, ctx, userID, testCID2)
		createTestFilePath(t, ctx, userID, testCID2, "/file2.txt", "file2.txt", false)

		// Act
		orphanedPaths, err := fileManagerSvc.GetOrphanedPaths(context.Background())

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, orphanedPaths, 1)
		assert.Equal(tb, testCID1.Bytes(), orphanedPaths[0].CID)
	}, TestOptions)
}

func TestFileManagerService_DeleteFilePathSmart_NoReferences(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create file path without any pin references
		filePath := createTestFilePath(t, ctx, userID, testCID, "/file.txt", "file.txt", false)

		// Act
		err := fileManagerSvc.DeleteFilePathSmart(context.Background(), userID, testCID.Bytes())

		// Assert
		require.NoError(tb, err)

		// Verify the file path was deleted
		var retrievedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", filePath.ID).First(&retrievedPath)
		assert.Error(tb, result.Error) // Should not exist
	}, TestOptions)
}

func TestFileManagerService_DeleteFilePathSmart_WithReferences(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create file path with pin reference
		filePath := createTestFilePath(t, ctx, userID, testCID, "/file.txt", "file.txt", false)
		createTestPin(t, ctx, userID, testCID)

		// Act
		err := fileManagerSvc.DeleteFilePathSmart(context.Background(), userID, testCID.Bytes())

		// Assert
		require.NoError(tb, err)

		// Verify the file path was NOT deleted (because it's referenced by a pin)
		var retrievedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", filePath.ID).First(&retrievedPath)
		require.NoError(tb, result.Error)
		assert.Equal(tb, filePath.ID, retrievedPath.ID)
	}, TestOptions)
}

func TestFileManagerService_DeleteFilePath_ForceDelete(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create file path with pin reference
		filePath := createTestFilePath(t, ctx, userID, testCID, "/file.txt", "file.txt", false)
		createTestPin(t, ctx, userID, testCID)

		// Act - force delete should remove even with references
		err := fileManagerSvc.DeleteFilePath(context.Background(), userID, testCID.Bytes())

		// Assert
		require.NoError(tb, err)

		// Verify the file path was deleted despite having references
		var retrievedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", filePath.ID).First(&retrievedPath)
		assert.Error(tb, result.Error) // Should not exist
	}, TestOptions)
}

func TestFileManagerService_DeleteFilePathsByUserID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID1 := uint(123)
		userID2 := uint(456)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		// Create file paths for different users
		filePath1 := createTestFilePath(t, ctx, userID1, testCID1, "/file1.txt", "file1.txt", false)
		filePath2 := createTestFilePath(t, ctx, userID2, testCID2, "/file2.txt", "file2.txt", false)

		// Act - delete only for user 1
		err := fileManagerSvc.DeleteFilePathsByUserID(context.Background(), userID1)

		// Assert
		require.NoError(tb, err)

		// Verify user 1's file path was deleted
		var retrievedPath1 pluginDb.FilePath
		result1 := ctx.DB().Where("id = ?", filePath1.ID).First(&retrievedPath1)
		assert.Error(tb, result1.Error) // Should not exist

		// Verify user 2's file path still exists
		var retrievedPath2 pluginDb.FilePath
		result2 := ctx.DB().Where("id = ?", filePath2.ID).First(&retrievedPath2)
		require.NoError(tb, result2.Error)
		assert.Equal(tb, filePath2.ID, retrievedPath2.ID)
	}, TestOptions)
}

func TestFileManagerService_ListFiles_InvalidFilters(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		// Create invalid filter with unsupported field
		invalidFilters := []queryutil.CrudFilter{
			queryutil.NewLogicalFilter("nonexistent_field", queryutil.OpEq, "test_value"),
		}

		userID := uint(123)

		// Act
		files, total, err := fileManagerSvc.ListFiles(context.Background(), userID, invalidFilters, nil, queryutil.DefaultPagination)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, files)
		assert.Equal(tb, int64(0), total)
	}, TestOptions)
}

func TestFileManagerService_ListDirectoryContents_InvalidUserID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		// Act - use invalid user ID
		contents, err := fileManagerSvc.ListDirectoryContents(context.Background(), uint(999999), pluginDb.RootPath)

		// Assert
		require.NoError(tb, err) // Should not error, just return empty list
		assert.Empty(tb, contents)
	}, TestOptions)
}

func TestFileManagerService_ListDirectoryContents_NonExistentPath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)

		// Act - use non-existent path
		contents, err := fileManagerSvc.ListDirectoryContents(context.Background(), userID, "/nonexistent/path")

		// Assert
		require.NoError(tb, err) // Should not error, just return empty list
		assert.Empty(tb, contents)
	}, TestOptions)
}

func TestFileManagerService_GetBreadcrumbs_InvalidPath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)

		// Act - use invalid path (empty string)
		breadcrumbs, err := fileManagerSvc.GetBreadcrumbs(context.Background(), userID, "")

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, breadcrumbs)

		// Act - use invalid path (no leading slash)
		breadcrumbs, err = fileManagerSvc.GetBreadcrumbs(context.Background(), userID, "invalid/path")

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, breadcrumbs)
	}, TestOptions)
}

func TestFileManagerService_CreateFilePath_DuplicatePath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		filePath := &pluginDb.FilePath{
			UserID:      userID,
			CID:         testCID.Bytes(),
			Path:        "/test/file.txt",
			Name:        "file.txt",
			Type:        0,
			Size:        1024,
			IsDirectory: false,
			IsOrphan:    false,
			ParentPath:  "/test",
			Depth:       1,
		}

		// Create the file path first time
		err := fileManagerSvc.CreateFilePath(context.Background(), filePath)
		require.NoError(tb, err)

		// Try to create the same path again
		// Act
		err = fileManagerSvc.CreateFilePath(context.Background(), filePath)

		// Assert
		assert.Error(tb, err)
	}, TestOptions)
}

func TestFileManagerService_DeleteFilePathSmart_DatabaseError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create file path
		createTestFilePath(t, ctx, userID, testCID, "/file.txt", "file.txt", false)

		// Close the database to simulate connection error
		db, err := ctx.DB().DB()
		require.NoError(tb, err)
		err = db.Close()
		require.NoError(tb, err)

		// Act
		err = fileManagerSvc.DeleteFilePathSmart(context.Background(), userID, testCID.Bytes())

		// Assert
		assert.Error(tb, err)
	}, TestOptions)
}

func TestFileManagerService_ListFiles_EmptyPagination(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		// Create test file paths
		createTestFilePath(t, ctx, userID, testCID1, "/file1.txt", "file1.txt", false)
		createTestFilePath(t, ctx, userID, testCID2, "/file2.txt", "file2.txt", false)

		// Act with empty pagination
		emptyPagination := queryutil.Pagination{}
		files, total, err := fileManagerSvc.ListFiles(context.Background(), userID, nil, nil, emptyPagination)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, files, 2)
		assert.Equal(tb, int64(2), total)
	}, TestOptions)
}

func TestFileManagerService_ListFiles_PaginationBeyondData(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create only one test file path
		createTestFilePath(t, ctx, userID, testCID, "/file.txt", "file.txt", false)

		// Act with pagination beyond available data
		beyondPagination := queryutil.Pagination{
			Start: 100, // Start beyond available data
			End:   110,
		}
		files, total, err := fileManagerSvc.ListFiles(context.Background(), userID, nil, nil, beyondPagination)

		// Assert
		require.NoError(tb, err)
		assert.Empty(tb, files) // Should be empty since we're beyond the data
		assert.Equal(tb, int64(1), total)
	}, TestOptions)
}

func TestFileManagerService_ListFiles_WithFilters(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		// Create test file paths
		createTestFilePath(t, ctx, userID, testCID1, "/file1.txt", "file1.txt", false)
		createTestFilePath(t, ctx, userID, testCID2, "/file2.txt", "file2.txt", false)

		// Test filter by name
		nameFilters := []queryutil.CrudFilter{
			queryutil.NewLogicalFilter("name", queryutil.OpEq, "file1.txt"),
		}

		// Act
		files, total, err := fileManagerSvc.ListFiles(context.Background(), userID, nameFilters, nil, queryutil.DefaultPagination)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, files, 1)
		assert.Equal(tb, int64(1), total)
		assert.Equal(tb, "file1.txt", files[0].Name)
	}, TestOptions)
}

func TestFileManagerService_ListFiles_WithSorting(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		// Create test file paths
		createTestFilePath(t, ctx, userID, testCID1, "/a_file.txt", "a_file.txt", false)
		createTestFilePath(t, ctx, userID, testCID2, "/z_file.txt", "z_file.txt", false)

		// Test sorting by name ascending
		ascSort := []queryutil.Sort{
			{
				Field: "name",
				Order: queryutil.OrderAsc,
			},
		}

		// Act
		files, total, err := fileManagerSvc.ListFiles(context.Background(), userID, nil, ascSort, queryutil.DefaultPagination)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, files, 2)
		assert.Equal(tb, int64(2), total)
		assert.Equal(tb, "a_file.txt", files[0].Name) // Should be first
		assert.Equal(tb, "z_file.txt", files[1].Name) // Should be second

		// Test sorting by name descending
		descSort := []queryutil.Sort{
			{
				Field: "name",
				Order: queryutil.OrderDesc,
			},
		}

		// Act
		files, total, err = fileManagerSvc.ListFiles(context.Background(), userID, nil, descSort, queryutil.DefaultPagination)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, files, 2)
		assert.Equal(tb, int64(2), total)
		assert.Equal(tb, "z_file.txt", files[0].Name) // Should be first
		assert.Equal(tb, "a_file.txt", files[1].Name) // Should be second
	}, TestOptions)
}

func TestFileManagerService_GetBreadcrumbs_RootPathOnly(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create a root level file
		createTestFilePath(t, ctx, userID, testCID, "/file.txt", "file.txt", false)

		targetPath := "/file.txt"

		// Act
		breadcrumbs, err := fileManagerSvc.GetBreadcrumbs(context.Background(), userID, targetPath)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, breadcrumbs, 1) // Should have just the file itself

		assert.Equal(tb, "/file.txt", breadcrumbs[0].Path)
		assert.Equal(tb, "file.txt", breadcrumbs[0].Name)
	}, TestOptions)
}

func TestFileManagerService_GetBreadcrumbs_DeepNestedPath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		testCID3 := util.GenerateTestCID(t, "test data 3")
		testCID4 := util.GenerateTestCID(t, "test data 4")
		testCID5 := util.GenerateTestCID(t, "test data 5")

		// Create deeply nested file paths
		createTestFilePath(t, ctx, userID, testCID1, "/level1", "level1", true)
		createTestFilePath(t, ctx, userID, testCID2, "/level1/level2", "level2", true)
		createTestFilePath(t, ctx, userID, testCID3, "/level1/level2/level3", "level3", true)
		createTestFilePath(t, ctx, userID, testCID4, "/level1/level2/level3/level4", "level4", true)
		createTestFilePath(t, ctx, userID, testCID5, "/level1/level2/level3/level4/file.txt", "file.txt", false)

		targetPath := "/level1/level2/level3/level4/file.txt"

		// Act
		breadcrumbs, err := fileManagerSvc.GetBreadcrumbs(context.Background(), userID, targetPath)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, breadcrumbs, 5) // Should have all levels

		// Verify breadcrumb order (should be ordered by depth)
		assert.Equal(tb, "/level1", breadcrumbs[0].Path)
		assert.Equal(tb, "/level1/level2", breadcrumbs[1].Path)
		assert.Equal(tb, "/level1/level2/level3", breadcrumbs[2].Path)
		assert.Equal(tb, "/level1/level2/level3/level4", breadcrumbs[3].Path)
		assert.Equal(tb, "/level1/level2/level3/level4/file.txt", breadcrumbs[4].Path)
	}, TestOptions)
}

func TestFileManagerService_GetBreadcrumbs_PathNotInDatabase(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)

		// Act - try to get breadcrumbs for a path that doesn't exist in database
		breadcrumbs, err := fileManagerSvc.GetBreadcrumbs(context.Background(), userID, "/nonexistent/file.txt")

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, breadcrumbs)
	}, TestOptions)
}

func TestFileManagerService_GetBreadcrumbs_MalformedPath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)

		// Act - try to get breadcrumbs for a malformed path
		breadcrumbs, err := fileManagerSvc.GetBreadcrumbs(context.Background(), userID, "////malformed///path//")

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, breadcrumbs)
	}, TestOptions)
}

func TestFileManagerService_ListDirectoryContents_OnlyOrphans(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		// Create orphan file paths only
		orphanPath1 := &pluginDb.FilePath{
			UserID:      userID,
			CID:         testCID1.Bytes(),
			Path:        "/" + testCID1.String(),
			Name:        testCID1.String(),
			Type:        0,
			Size:        0,
			IsDirectory: false,
			IsOrphan:    true,
			ParentPath:  pluginDb.RootPath,
			Depth:       0,
		}
		err := ctx.DB().Create(orphanPath1).Error
		require.NoError(t, err)

		orphanPath2 := &pluginDb.FilePath{
			UserID:      userID,
			CID:         testCID2.Bytes(),
			Path:        "/" + testCID2.String(),
			Name:        testCID2.String(),
			Type:        0,
			Size:        0,
			IsDirectory: false,
			IsOrphan:    true,
			ParentPath:  pluginDb.RootPath,
			Depth:       0,
		}
		err = ctx.DB().Create(orphanPath2).Error
		require.NoError(t, err)

		// Act - list files for user (should include orphans)
		files, total, err := fileManagerSvc.ListFiles(context.Background(), userID, nil, nil, queryutil.DefaultPagination)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, files, 2) // Should have both orphan files
		assert.Equal(tb, int64(2), total)

		// Verify both files are orphans
		assert.True(tb, files[0].IsOrphan)
		assert.True(tb, files[1].IsOrphan)
	}, TestOptions)
}

func TestFileManagerService_GetOrphanedPaths_ComplexScenario(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1") // orphan
		testCID2 := util.GenerateTestCID(t, "test data 2") // not orphan
		testCID3 := util.GenerateTestCID(t, "test data 3") // orphan

		// Create orphaned file paths (no corresponding pins)
		createTestFilePath(t, ctx, userID, testCID1, "/orphan1.txt", "orphan1.txt", false)
		createTestFilePath(t, ctx, userID, testCID3, "/orphan2.txt", "orphan2.txt", false)

		// Create non-orphaned file path (has corresponding pin)
		createTestPin(t, ctx, userID, testCID2)
		createTestFilePath(t, ctx, userID, testCID2, "/file2.txt", "file2.txt", false)

		// Act
		orphanedPaths, err := fileManagerSvc.GetOrphanedPaths(context.Background())

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, orphanedPaths, 2)

		// Verify the orphaned CIDs
		orphanedCIDs := [][]byte{orphanedPaths[0].CID, orphanedPaths[1].CID}
		assert.Contains(tb, orphanedCIDs, testCID1.Bytes())
		assert.Contains(tb, orphanedCIDs, testCID3.Bytes())
		assert.NotContains(tb, orphanedCIDs, testCID2.Bytes())
	}, TestOptions)
}

func TestFileManagerService_DeleteFilePathSmart_WithOrphanedPaths(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create orphaned file path (no pin reference)
		filePath := createTestFilePath(t, ctx, userID, testCID, "/orphan.txt", "orphan.txt", false)
		filePath.IsOrphan = true
		err := ctx.DB().Save(filePath).Error
		require.NoError(t, err)

		// Act
		err = fileManagerSvc.DeleteFilePathSmart(context.Background(), userID, testCID.Bytes())

		// Assert
		require.NoError(tb, err)

		// Verify the orphaned file path was deleted
		var retrievedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", filePath.ID).First(&retrievedPath)
		assert.Error(tb, result.Error) // Should not exist
	}, TestOptions)
}

func TestFileManagerService_ValidatePathCompleteness_Mixed(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1") // complete (has pin and path)
		testCID2 := util.GenerateTestCID(t, "test data 2") // incomplete (has pin, no path)

		// Create complete pin and file path
		createTestPin(t, ctx, userID, testCID1)
		createTestFilePath(t, ctx, userID, testCID1, "/file1.txt", "file1.txt", false)

		// Create incomplete pin (only pin, no file path)
		createTestPin(t, ctx, userID, testCID2)

		// Act
		isComplete, err := fileManagerSvc.ValidatePathCompleteness(context.Background())

		// Assert
		require.NoError(tb, err)
		assert.False(tb, isComplete) // Should be false because not all pins have paths
	}, TestOptions)
}

func TestFileManagerService_CreateFilePath_InvalidParentPath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create file path with invalid parent path
		filePath := &pluginDb.FilePath{
			UserID:      userID,
			CID:         testCID.Bytes(),
			Path:        "/test/file.txt",
			Name:        "file.txt",
			Type:        0,
			Size:        1024,
			IsDirectory: false,
			IsOrphan:    false,
			ParentPath:  "/nonexistent/parent", // Parent doesn't exist in DB
			Depth:       1,
		}

		// Act
		err := fileManagerSvc.CreateFilePath(context.Background(), filePath)

		// Assert - Should still create the path even if parent doesn't exist
		require.NoError(tb, err)

		// Verify the file path was created in the database
		var retrievedPath pluginDb.FilePath
		result := ctx.DB().Where("user_id = ? AND path = ?", userID, "/test/file.txt").First(&retrievedPath)
		require.NoError(tb, result.Error)
	}, TestOptions)
}

func TestFileManagerService_ListFiles_UserIDScoping(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		userID1 := uint(123)
		userID2 := uint(456)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		// Create test file paths for different users
		createTestFilePath(t, ctx, userID1, testCID1, "/file1.txt", "file1.txt", false)
		createTestFilePath(t, ctx, userID2, testCID2, "/file2.txt", "file2.txt", false)

		// Act - list files for user 1
		files1, total1, err1 := fileManagerSvc.ListFiles(context.Background(), userID1, nil, nil, queryutil.DefaultPagination)

		// Assert
		require.NoError(tb, err1)
		assert.Len(tb, files1, 1)
		assert.Equal(tb, int64(1), total1)
		assert.Equal(tb, "file1.txt", files1[0].Name)

		// Act - list files for user 2
		files2, total2, err2 := fileManagerSvc.ListFiles(context.Background(), userID2, nil, nil, queryutil.DefaultPagination)

		// Assert
		require.NoError(tb, err2)
		assert.Len(tb, files2, 1)
		assert.Equal(tb, int64(1), total2)
		assert.Equal(tb, "file2.txt", files2[0].Name)
	}, TestOptions)
}

func TestFileManagerService_DeleteFilePathsByUserID_NonExistentUser(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		// Act - delete for non-existent user
		err := fileManagerSvc.DeleteFilePathsByUserID(context.Background(), uint(999999))

		// Assert - Should not error even if user doesn't exist
		require.NoError(tb, err)
	}, TestOptions)
}
