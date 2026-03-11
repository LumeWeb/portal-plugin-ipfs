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
	"go.lumeweb.com/portal/db/models"
	"gorm.io/gorm"
)

var TestOptions = GetStandardTestOptions()

func TestFilePathOperationHandler_ValidateRequest(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		// Test case 1: Valid request with hash
		testCID := util.GenerateTestCID(t, "test data")
		validReq := createTestRequest(testCID, uintPtr(123))
		err := handler.ValidateRequest(context.Background(), validReq)
		assert.NoError(tb, err)

		// Test case 2: Invalid request without hash
		invalidReq := createTestRequest(cid.Undef, uintPtr(123))
		err = handler.ValidateRequest(context.Background(), invalidReq)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "hash is required")
	}, TestOptions...)
}

func TestFilePathOperationHandler_Execute_WithValidUnixFSDirectory(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")
		childCID1 := util.GenerateTestCID(t, "child1")
		childCID2 := util.GenerateTestCID(t, "child2")

		// Create test block and UnixFS directory node with children
		_, unixFSNode := util.CreateTestBlockAndNode(t, ctx, testCID, "test_dir", 1, 1024, []cid.Cid{childCID1, childCID2})

		// Create child blocks and nodes
		util.CreateTestBlockAndNode(t, ctx, childCID1, "child1.txt", 0, 512, []cid.Cid{})
		util.CreateTestBlockAndNode(t, ctx, childCID2, "child2.txt", 0, 256, []cid.Cid{})

		// Create workflow test
		wfTest := coreTesting.NewWorkflowTest(ctx)

		// Register workflow
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-workflow", steps, false)

		// Start workflow
		req := wfTest.StartWorkflow(
			"test-workflow",
			core.WithWorkflowStructData(protocol.PinWorkflowData{
				Cids: []string{testCID.String()},
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(testCID)),
			core.WithWorkflowUserID(userID),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		// Execute the workflow step
		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// Assert operation success
		wfTest.AssertOperationSuccess(req)

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
	}, TestOptions...)
}

func TestFilePathOperationHandler_Execute_WithIncompleteMetadata(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create test block with incomplete UnixFS node (no name, no children)
		_, _ = util.CreateTestBlockAndNode(t, ctx, testCID, "", 0, 0, []cid.Cid{})

		// Create workflow test
		wfTest := coreTesting.NewWorkflowTest(ctx)

		// Register workflow
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-workflow-incomplete", steps, false)

		// Start workflow
		req := wfTest.StartWorkflow(
			"test-workflow-incomplete",
			core.WithWorkflowStructData(protocol.PinWorkflowData{
				Cids: []string{testCID.String()},
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(testCID)),
			core.WithWorkflowUserID(userID),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		// Execute the workflow step
		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// Assert operation success
		wfTest.AssertOperationSuccess(req)

		// Verify orphan file path was created
		var orphanPath pluginDb.FilePath
		result := ctx.DB().Where("user_id = ? AND is_orphan = ?", userID, true).First(&orphanPath)
		require.NoError(tb, result.Error)
		assert.Equal(tb, "/"+testCID.String(), orphanPath.Path)
		assert.True(tb, orphanPath.IsOrphan)
	}, TestOptions...)
}

func TestFilePathOperationHandler_Execute_WithMissingMetadata(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Create workflow test
		wfTest := coreTesting.NewWorkflowTest(ctx)

		// Register workflow
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-workflow-missing", steps, false)

		// Start workflow
		req := wfTest.StartWorkflow(
			"test-workflow-missing",
			core.WithWorkflowStructData(protocol.PinWorkflowData{
				Cids: []string{testCID.String()},
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(testCID)),
			core.WithWorkflowUserID(userID),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		// Execute the workflow step
		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// Assert operation success
		wfTest.AssertOperationSuccess(req)

		// Verify orphan file path was created
		var orphanPath pluginDb.FilePath
		result := ctx.DB().Where("user_id = ? AND is_orphan = ?", userID, true).First(&orphanPath)
		require.NoError(tb, result.Error)
		assert.Empty(tb, orphanPath.Name)
		assert.Equal(tb, "/"+testCID.String(), orphanPath.Path)
		assert.True(tb, orphanPath.IsOrphan)
	}, TestOptions...)
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

		// Verify the orphan entries - check paths instead of names since names are empty for orphans
		pathStrings := []string{orphanPaths[0].Path, orphanPaths[1].Path}
		assert.Contains(tb, pathStrings, "/"+testCID1.String())
		assert.Contains(tb, pathStrings, "/"+testCID2.String())
	}, TestOptions...)
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
		_, err := handler.ComputePathsRecursive(context.Background(), fileManagerSvc, rootUnixFS, userID, rootCID, "", 0, processed, false)

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
	}, TestOptions...)
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
		_, err := handler.ComputePathsRecursive(context.Background(), fileManagerSvc, rootUnixFS, userID, rootCID, "", 0, processed, false)

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
	}, TestOptions...)
}

func TestFilePathOperationHandler_GetStatus(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		testCID := util.GenerateTestCID(t, "test data")
		userID := uint(123)

		// Create test block
		_, _ = util.CreateTestBlockAndNode(t, ctx, testCID, "test.txt", 0, 1024, []cid.Cid{})

		// Create workflow test and run it to completion
		wfTest := coreTesting.NewWorkflowTest(ctx)

		// Register workflow
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-workflow-status", steps, false)

		// Start and complete workflow
		req := wfTest.StartWorkflow(
			"test-workflow-status",
			core.WithWorkflowStructData(protocol.PinWorkflowData{
				Cids: []string{testCID.String()},
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(testCID)),
			core.WithWorkflowUserID(userID),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)
		wfTest.AssertOperationSuccess(req)

		// Act
		status, err := handler.GetStatus(context.Background(), req)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, status)
	}, TestOptions...)
}

func TestFilePathOperationHandler_GetStatus_Processing(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		userID := uint(123)

		// Create test blocks
		_, _ = util.CreateTestBlockAndNode(t, ctx, testCID1, "test1.txt", 0, 1024, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, testCID2, "test2.txt", 0, 512, []cid.Cid{})

		// Create workflow test
		wfTest := coreTesting.NewWorkflowTest(ctx)

		// Register workflow
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-workflow-processing", steps, false)

		// Start workflow (don't complete it yet)
		req := wfTest.StartWorkflow(
			"test-workflow-processing",
			core.WithWorkflowStructData(protocol.PinWorkflowData{
				Cids: []string{testCID1.String(), testCID2.String()},
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(testCID1)),
			core.WithWorkflowUserID(userID),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		// Execute step but don't complete, simulating processing state
		wfTest.ExecuteWorkflowStep(req)

		// Act
		status, err := handler.GetStatus(context.Background(), req)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, status)

		// Now complete the workflow
		wfTest.CompleteWorkflowStep(req)
		wfTest.AssertOperationSuccess(req)
	}, TestOptions...)
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
	}, TestOptions...)
}
