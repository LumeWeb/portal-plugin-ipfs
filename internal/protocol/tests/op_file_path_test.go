package tests

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/block"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/file_manager"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	"go.lumeweb.com/portal/db/models"
	"gorm.io/gorm"
)

var TestOptions = coreTesting.CombineOptions(
	coreTesting.WithServiceFactory(pluginCore.FILE_MANAGER_SERVICE, func() (core.Service, []core.ContextBuilderOption, error) {
		return filemanager.NewFileManagerService()
	}),
	coreTesting.WithServiceFactory(pluginCore.BLOCK_SERVICE, func() (core.Service, []core.ContextBuilderOption, error) {
		return block.NewBlockService()
	}),
	coreTesting.WithSQLitePluginMigrations(
		internal.ProtocolName, migrations.GetSQLite(),
	),
)

func TestFilePathOperationHandler_ValidateRequest(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		// Test case 1: Valid request with hash
		testCID := util.GenerateTestCID(t, "test data")
		validReq := createTestRequest(t, testCID, uintPtr(123))
		err := handler.ValidateRequest(context.Background(), validReq)
		assert.NoError(tb, err)

		// Test case 2: Invalid request without hash
		invalidReq := createTestRequest(t, cid.Undef, uintPtr(123))
		err = handler.ValidateRequest(context.Background(), invalidReq)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "hash is required")
	}, TestOptions)
}

func TestFilePathOperationHandler_Execute_WithValidUnixFSDirectory(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")
		childCID1 := util.GenerateTestCID(t, "child1")
		childCID2 := util.GenerateTestCID(t, "child2")

		// Create test block and UnixFS directory node with children
		_, unixFSNode := util.CreateTestBlockAndNode(t, ctx, testCID, "test_dir", 1, 1024, []cid.Cid{childCID1, childCID2})

		// Create child blocks and nodes
		util.CreateTestBlockAndNode(t, ctx, childCID1, "child1.txt", 0, 512, []cid.Cid{})
		util.CreateTestBlockAndNode(t, ctx, childCID2, "child2.txt", 0, 256, []cid.Cid{})

		req := createTestRequest(t, testCID, &userID)

		// Mock the workflow service to return pin workflow data
		workflowSvc := core.GetService[*coreMocks.MockWorkflowService](ctx, core.WORKFLOW_SERVICE)

		// Create workflow data with only the root CID
		pinWorkflowData := &protocol.PinWorkflowData{
			Cids: []string{testCID.String()},
		}

		// Create a koanf instance and populate it with our test data
		k := koanf.New(".")
		err := k.Load(confmap.Provider(map[string]any{
			"cids": pinWorkflowData.Cids,
		}, "."), nil)
		require.NoError(t, err)

		workflowSvc.On("GetWorkflowMetadata", ctx, req.ID).Return(k, nil)

		// Mock the UpdateWorkflowDataStruct calls that will be made during execution
		workflowSvc.On("UpdateWorkflowDataStruct", ctx, req.ID, mock.AnythingOfType("protocol.FilePathWorkflowData"), "json").Return(nil)

		// Act
		err = handler.Execute(context.Background(), req)

		// Assert
		require.NoError(tb, err)

		// Verify file paths were created
		var filePaths []pluginDb.FilePath
		result := ctx.DB().Where("user_id = ?", userID).Find(&filePaths)
		require.NoError(tb, result.Error)

		// Should have 3 file paths: root directory and 2 children
		assert.Len(tb, filePaths, 3)

		// Verify root directory path
		var rootPath pluginDb.FilePath
		result = ctx.DB().Where("user_id = ? AND path = ?", userID, "/test_dir").First(&rootPath)
		require.NoError(tb, result.Error)
		assert.Equal(tb, unixFSNode.Name, rootPath.Name)
		assert.Equal(tb, testCID.Bytes(), rootPath.CID)
		assert.False(tb, rootPath.IsOrphan)

		// Verify child file paths
		var childPath1 pluginDb.FilePath
		result = ctx.DB().Where("user_id = ? AND path = ?", userID, "/test_dir/child1.txt").First(&childPath1)
		require.NoError(tb, result.Error)
		assert.Equal(tb, "child1.txt", childPath1.Name)
		assert.Equal(tb, childCID1.Bytes(), childPath1.CID)
		assert.Equal(tb, "/test_dir", childPath1.ParentPath)

		var childPath2 pluginDb.FilePath
		result = ctx.DB().Where("user_id = ? AND path = ?", userID, "/test_dir/child2.txt").First(&childPath2)
		require.NoError(tb, result.Error)
		assert.Equal(tb, "child2.txt", childPath2.Name)
		assert.Equal(tb, childCID2.Bytes(), childPath2.CID)
		assert.Equal(tb, "/test_dir", childPath2.ParentPath)
	}, TestOptions)
}

func TestFilePathOperationHandler_Execute_WithIncompleteMetadata(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create test block with incomplete UnixFS node (no name, no children)
		_, _ = util.CreateTestBlockAndNode(t, ctx, testCID, "", 0, 0, []cid.Cid{})

		req := createTestRequest(t, testCID, &userID)

		// Mock the workflow service to return pin workflow data with the test CID
		workflowSvc := core.GetService[*coreMocks.MockWorkflowService](ctx, core.WORKFLOW_SERVICE)

		// Create workflow data with the CID
		pinWorkflowData := &protocol.PinWorkflowData{
			Cids: []string{testCID.String()},
		}

		// Create a koanf instance and populate it with our test data
		k := koanf.New(".")
		err := k.Load(confmap.Provider(map[string]any{
			"cids": pinWorkflowData.Cids,
		}, "."), nil)
		require.NoError(t, err)

		workflowSvc.On("GetWorkflowMetadata", ctx, req.ID).Return(k, nil)
		workflowSvc.On("UpdateWorkflowDataStruct", ctx, req.ID, mock.AnythingOfType("protocol.FilePathWorkflowData"), "json").Return(nil)

		// Act
		err = handler.Execute(context.Background(), req)

		// Assert
		require.NoError(tb, err)

		// Verify orphan file path was created
		var orphanPath pluginDb.FilePath
		result := ctx.DB().Where("user_id = ? AND is_orphan = ?", userID, true).First(&orphanPath)
		require.NoError(tb, result.Error)
		assert.Equal(tb, testCID.String(), orphanPath.Name)
		assert.Equal(tb, "/"+testCID.String(), orphanPath.Path)
		assert.True(tb, orphanPath.IsOrphan)
	}, TestOptions)
}

func TestFilePathOperationHandler_Execute_WithMissingMetadata(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		req := createTestRequest(t, testCID, &userID)

		// Mock the workflow service to return pin workflow data with the test CID
		workflowSvc := core.GetService[*coreMocks.MockWorkflowService](ctx, core.WORKFLOW_SERVICE)

		// Create workflow data with the CID
		pinWorkflowData := &protocol.PinWorkflowData{
			Cids: []string{testCID.String()},
		}

		// Create a koanf instance and populate it with our test data
		k := koanf.New(".")
		err := k.Load(confmap.Provider(map[string]any{
			"cids": pinWorkflowData.Cids,
		}, "."), nil)
		require.NoError(t, err)

		workflowSvc.On("GetWorkflowMetadata", ctx, req.ID).Return(k, nil)
		workflowSvc.On("UpdateWorkflowDataStruct", ctx, req.ID, mock.AnythingOfType("protocol.FilePathWorkflowData"), "json").Return(nil)

		// Act
		err = handler.Execute(context.Background(), req)

		// Assert
		require.NoError(tb, err)

		// Verify orphan file path was created
		var orphanPath pluginDb.FilePath
		result := ctx.DB().Where("user_id = ? AND is_orphan = ?", userID, true).First(&orphanPath)
		require.NoError(tb, result.Error)
		assert.Equal(tb, testCID.String(), orphanPath.Name)
		assert.Equal(tb, "/"+testCID.String(), orphanPath.Path)
		assert.True(tb, orphanPath.IsOrphan)
	}, TestOptions)
}

func TestFilePathOperationHandler_CreateOrphanEntriesForPins(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		// Create test pins
		pins := []*pluginDb.IPFSPin{
			{
				UserID: userID,
				CID:    testCID1.Bytes(),
			},
			{
				UserID: userID,
				CID:    testCID2.Bytes(),
			},
		}

		// Act
		err := handler.CreateOrphanEntriesForPins(context.Background(), pins)

		// Assert
		require.NoError(tb, err)

		// Verify orphan entries were created
		var orphanPaths []pluginDb.FilePath
		result := ctx.DB().Where("user_id = ? AND is_orphan = ?", userID, true).Find(&orphanPaths)
		require.NoError(tb, result.Error)
		assert.Len(tb, orphanPaths, 2)

		// Verify the orphan entries
		cidStrings := []string{orphanPaths[0].Name, orphanPaths[1].Name}
		assert.Contains(tb, cidStrings, testCID1.String())
		assert.Contains(tb, cidStrings, testCID2.String())
	}, TestOptions)
}

func TestFilePathOperationHandler_computePathsRecursive_DirectoryStructure(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		rootCID := util.GenerateTestCID(t, "root data")
		childCID1 := util.GenerateTestCID(t, "child1 data")
		childCID2 := util.GenerateTestCID(t, "child2 data")

		// Create directory structure
		_, rootUnixFS := util.CreateTestBlockAndNode(t, ctx, rootCID, "test_dir", 1, 0, []cid.Cid{childCID1, childCID2})
		util.CreateTestBlockAndNode(t, ctx, childCID1, "file1.txt", 0, 512, []cid.Cid{})
		util.CreateTestBlockAndNode(t, ctx, childCID2, "file2.txt", 0, 256, []cid.Cid{})

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
		assert.Len(tb, filePaths, 3)

		// Verify directory
		assert.Equal(tb, "/test_dir", filePaths[0].Path)
		assert.Equal(tb, "test_dir", filePaths[0].Name)
		assert.True(tb, filePaths[0].IsDirectory)
		assert.Equal(tb, rootCID.Bytes(), filePaths[0].CID)

		// Verify child files
		assert.Equal(tb, "/test_dir/file1.txt", filePaths[1].Path)
		assert.Equal(tb, "file1.txt", filePaths[1].Name)
		assert.False(tb, filePaths[1].IsDirectory)
		assert.Equal(tb, childCID1.Bytes(), filePaths[1].CID)

		assert.Equal(tb, "/test_dir/file2.txt", filePaths[2].Path)
		assert.Equal(tb, "file2.txt", filePaths[2].Name)
		assert.False(tb, filePaths[2].IsDirectory)
		assert.Equal(tb, childCID2.Bytes(), filePaths[2].CID)
	}, TestOptions)
}

func TestFilePathOperationHandler_computePathsRecursive_CycleDetection(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		rootCID := util.GenerateTestCID(t, "root data")
		childCID := util.GenerateTestCID(t, "child data")

		// Create a structure where child references back to parent (potential cycle)
		_, rootUnixFS := util.CreateTestBlockAndNode(t, ctx, rootCID, "root", 1, 0, []cid.Cid{childCID})
		util.CreateTestBlockAndNode(t, ctx, childCID, "child", 0, 512, []cid.Cid{rootCID}) // Child references parent

		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		require.NotNil(tb, fileManagerSvc)

		processed := make(map[string]bool)

		// Act
		err := handler.ComputePathsRecursive(context.Background(), fileManagerSvc, rootUnixFS, userID, rootCID, "", 0, processed, false)

		// Assert
		require.NoError(tb, err)

		// Verify file paths were created without infinite recursion
		var filePaths []pluginDb.FilePath
		result := ctx.DB().Where("user_id = ?", userID).Find(&filePaths)
		require.NoError(tb, result.Error)

		// Should have both root and child paths, but no infinite recursion
		assert.True(tb, len(filePaths) >= 2)

		// Verify that the processed map prevented infinite recursion
		assert.True(tb, processed[rootCID.String()])
		assert.True(tb, processed[childCID.String()])
	}, TestOptions)
}

func TestFilePathOperationHandler_GetStatus(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		req := &models.Request{
			Model:  gorm.Model{ID: 1},
			Status: models.RequestStatusCompleted,
		}

		// Mock the workflow service to return our test data
		workflowSvc := core.GetService[*coreMocks.MockWorkflowService](ctx, core.WORKFLOW_SERVICE)

		// Create a koanf instance and populate it with our test data
		k := koanf.New(".")
		testCID := util.GenerateTestCID(t, "test data")
		err := k.Load(confmap.Provider(map[string]any{
			"request_id":       "1",
			"cids":             []string{testCID.String()},
			"user_id":          123,
			"current_phase":    protocol.FilePathPhaseCompleted,
			"completed_phases": 5,
			"total_phases":     5,
			"processed_cids":   1,
			"total_cids":       1,
		}, "."), nil)
		require.NoError(tb, err)

		workflowSvc.On("GetWorkflowMetadata", ctx, req.ID).Return(k, nil)

		// Act
		status, err := handler.GetStatus(context.Background(), req)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, status)
		assert.Equal(tb, models.RequestStatusCompleted, status.State)
		assert.EqualValues(tb, 100, status.ProgressPercent)
		assert.Equal(tb, "File paths computed and stored successfully", status.Message)
	}, TestOptions)
}

func TestFilePathOperationHandler_GetStatus_Processing(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		req := &models.Request{
			Model:  gorm.Model{ID: 1},
			Status: models.RequestStatusProcessing,
		}

		// Mock the workflow service to return our test data
		workflowSvc := core.GetService[*coreMocks.MockWorkflowService](ctx, core.WORKFLOW_SERVICE)

		// Create a koanf instance and populate it with our test data
		k := koanf.New(".")
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		err := k.Load(confmap.Provider(map[string]any{
			"request_id":       "1",
			"cids":             []string{testCID1.String(), testCID2.String()},
			"user_id":          123,
			"current_phase":    protocol.FilePathPhaseComputingPaths,
			"completed_phases": 2,
			"total_phases":     5,
			"processed_cids":   1,
			"total_cids":       2,
		}, "."), nil)
		require.NoError(tb, err)

		workflowSvc.On("GetWorkflowMetadata", ctx, req.ID).Return(k, nil)

		// Act
		status, err := handler.GetStatus(context.Background(), req)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, status)
		assert.Equal(tb, models.RequestStatusProcessing, status.State)
		assert.Equal(tb, "File path operation in progress: computing_paths", status.Message)

		// Progress should be calculated as: (2/5 * 0.7) + (1/2 * 0.3) = 0.28 + 0.15 = 0.43 = 43%
		// Due to floating point precision, we round to the nearest integer
		assert.EqualValues(tb, 43, int(status.ProgressPercent+0.5))
	}, TestOptions)
}

func TestFilePathOperationHandler_Cleanup(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		req := &models.Request{
			Model: gorm.Model{ID: 1},
		}

		// Act
		err := handler.Cleanup(context.Background(), req)

		// Assert
		require.NoError(tb, err)
	}, TestOptions)
}
