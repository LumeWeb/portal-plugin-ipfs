package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/block"
	filemanager "go.lumeweb.com/portal-plugin-ipfs/internal/service/file_manager"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/types"
	"gorm.io/gorm"
)

var UnpinTestOptions = coreTesting.CombineOptions(
	coreTesting.WithServiceFactory(pluginCore.BLOCK_SERVICE, func() (core.Service, []core.ContextBuilderOption, error) {
		return block.NewBlockService()
	}),
	coreTesting.WithServiceFactory(pluginCore.FILE_MANAGER_SERVICE, func() (core.Service, []core.ContextBuilderOption, error) {
		return filemanager.NewFileManagerService()
	}),
	coreTesting.WithSQLitePluginMigrations(
		internal.ProtocolName, migrations.GetSQLite(),
	),
)

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

func createTestFilePath(t *testing.T, ctx coreTesting.TestContext, userID uint, testCID cid.Cid, path, name string, isDirectory bool) *pluginDb.FilePath {
	// Calculate parent path and depth
	parentPath := ""
	depth := 0
	
	// Extract parent path from the full path
	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	if len(pathParts) > 1 {
		// Parent path is all parts except the last one
		parentParts := pathParts[:len(pathParts)-1]
		if len(parentParts) > 0 {
			parentPath = "/" + strings.Join(parentParts, "/")
			depth = len(parentParts)
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
		Depth:       depth,
	}

	err := ctx.DB().Create(filePath).Error
	require.NoError(t, err)
	return filePath
}

// Test analyzeDAGDependencies method
func TestUnpinOperationHandler_AnalyzeDAGDependencies(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")
		dependentCID := util.GenerateTestCID(t, "dependent data")
		independentCID := util.GenerateTestCID(t, "independent data")

		// Create blocks and nodes
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, dependentCID, "dependent.txt", 0, 512, []cid.Cid{targetCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, independentCID, "independent.txt", 0, 256, []cid.Cid{})

		// Create pins
		createTestPin(t, ctx, userID, targetCID)
		createTestPin(t, ctx, userID, dependentCID)
		createTestPin(t, ctx, userID, independentCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeDAGDependencies(context.Background(), ctx.DB(), targetCID, userID)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, analysis)
		assert.True(tb, analysis.WouldBreakStructure)
		assert.Len(tb, analysis.DependentPins, 1)
		assert.Contains(tb, analysis.DependentPins, dependentCID.String())
		assert.Len(tb, analysis.ChildBlocks, 0)
		assert.Len(tb, analysis.ParentBlocks, 1)
		assert.Contains(tb, analysis.ParentBlocks, dependentCID.String())
	}, UnpinTestOptions)
}

// Test analyzeDAGDependencies with no dependencies
func TestUnpinOperationHandler_AnalyzeDAGDependencies_NoDependencies(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")
		independentCID := util.GenerateTestCID(t, "independent data")

		// Create blocks and nodes
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, independentCID, "independent.txt", 0, 256, []cid.Cid{})

		// Create pins
		createTestPin(t, ctx, userID, targetCID)
		createTestPin(t, ctx, userID, independentCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeDAGDependencies(context.Background(), ctx.DB(), targetCID, userID)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, analysis)
		assert.False(tb, analysis.WouldBreakStructure)
		assert.Len(tb, analysis.DependentPins, 0)
		assert.Len(tb, analysis.ChildBlocks, 0)
		assert.Len(tb, analysis.ParentBlocks, 0)
	}, UnpinTestOptions)
}

// Test getAllUserPins method
func TestUnpinOperationHandler_GetAllUserPins(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		otherUserID := uint(456)
		cid1 := util.GenerateTestCID(t, "data1")
		cid2 := util.GenerateTestCID(t, "data2")
		cid3 := util.GenerateTestCID(t, "data3")

		// Create pins for different users
		createTestPin(t, ctx, userID, cid1)
		createTestPin(t, ctx, userID, cid2)
		createTestPin(t, ctx, otherUserID, cid3)

		// Act
		pins, err := handler.GetAllUserPins(context.Background(), ctx.DB(), userID)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, pins, 2)
		cidStrings := []string{string(pins[0].CID), string(pins[1].CID)}
		assert.Contains(tb, cidStrings, string(cid1.Bytes()))
		assert.Contains(tb, cidStrings, string(cid2.Bytes()))
	}, UnpinTestOptions)
}

// Test doesPinDependOnCID method
func TestUnpinOperationHandler_DoesPinDependOnCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		pinCID := util.GenerateTestCID(t, "pin data")
		targetCID := util.GenerateTestCID(t, "target data")

		// Create blocks with dependency
		_, _ = util.CreateTestBlockAndNode(t, ctx, pinCID, "pin.txt", 0, 1024, []cid.Cid{targetCID})

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		depends, err := handler.DoesPinDependOnCID(context.Background(), blockSvc, pinCID, targetCID)

		// Assert
		require.NoError(tb, err)
		assert.True(tb, depends)
	}, UnpinTestOptions)
}

// Test doesPinDependOnCID with no dependency
func TestUnpinOperationHandler_DoesPinDependOnCID_NoDependency(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		pinCID := util.GenerateTestCID(t, "pin data")
		targetCID := util.GenerateTestCID(t, "target data")

		// Create blocks with no dependency
		_, _ = util.CreateTestBlockAndNode(t, ctx, pinCID, "pin.txt", 0, 1024, []cid.Cid{})

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		depends, err := handler.DoesPinDependOnCID(context.Background(), blockSvc, pinCID, targetCID)

		// Assert
		require.NoError(tb, err)
		assert.False(tb, depends)
	}, UnpinTestOptions)
}

// Test checkDAGForCID method
func TestUnpinOperationHandler_CheckDAGForCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		rootCID := util.GenerateTestCID(t, "root data")
		childCID1 := util.GenerateTestCID(t, "child1 data")
		childCID2 := util.GenerateTestCID(t, "child2 data")
		targetCID := util.GenerateTestCID(t, "target data")

		// Create a DAG structure: root -> child1 -> child2 -> target
		_, _ = util.CreateTestBlockAndNode(t, ctx, rootCID, "root", 1, 1024, []cid.Cid{childCID1})
		_, _ = util.CreateTestBlockAndNode(t, ctx, childCID1, "child1", 1, 512, []cid.Cid{childCID2})
		_, _ = util.CreateTestBlockAndNode(t, ctx, childCID2, "child2", 0, 256, []cid.Cid{targetCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 128, []cid.Cid{})

		visited := make(map[string]bool)
		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		found, err := handler.CheckDAGForCID(context.Background(), blockSvc, rootCID, targetCID, visited)

		// Assert
		require.NoError(tb, err)
		assert.True(tb, found)
	}, UnpinTestOptions)
}

// Test checkDAGForCID with cycle
func TestUnpinOperationHandler_CheckDAGForCID_Cycle(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		rootCID := util.GenerateTestCID(t, "root data")
		childCID := util.GenerateTestCID(t, "child data")

		// Create a cycle: root -> child -> root
		_, _ = util.CreateTestBlockAndNode(t, ctx, rootCID, "root", 1, 1024, []cid.Cid{childCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, childCID, "child", 0, 512, []cid.Cid{rootCID})

		visited := make(map[string]bool)
		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		found, err := handler.CheckDAGForCID(context.Background(), blockSvc, rootCID, childCID, visited)

		// Assert
		require.NoError(tb, err)
		assert.True(tb, found)
	}, UnpinTestOptions)
}

// Test getBlockRelationships method
func TestUnpinOperationHandler_GetBlockRelationships(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		parentCID := util.GenerateTestCID(t, "parent data")
		childCID1 := util.GenerateTestCID(t, "child1 data")
		childCID2 := util.GenerateTestCID(t, "child2 data")

		// Create blocks with relationships
		_, _ = util.CreateTestBlockAndNode(t, ctx, parentCID, "parent", 1, 1024, []cid.Cid{childCID1, childCID2})
		_, _ = util.CreateTestBlockAndNode(t, ctx, childCID1, "child1.txt", 0, 512, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, childCID2, "child2.txt", 0, 256, []cid.Cid{})

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		parents, children, err := handler.GetBlockRelationships(context.Background(), ctx.DB(), blockSvc, childCID1)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, parents, 1)
		assert.Contains(tb, parents, parentCID.String())
		assert.Len(tb, children, 0)
	}, UnpinTestOptions)
}

// Test promotePinsToOrphan method
func TestUnpinOperationHandler_PromotePinsToOrphan(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		pinCID := util.GenerateTestCID(t, "pin data")

		// Create pin and file path
		createTestPin(t, ctx, userID, pinCID)
		filePath := createTestFilePath(t, ctx, userID, pinCID, "/test/path/file.txt", "file.txt", false)

		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		require.NotNil(tb, fileManagerSvc)

		dependentPins := []string{pinCID.String()}

		// Act
		err := handler.PromotePinsToOrphan(context.Background(), ctx.DB(), dependentPins, userID)

		// Assert
		require.NoError(tb, err)

		// Verify file path was updated to orphan status
		var updatedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", filePath.ID).First(&updatedPath)
		require.NoError(tb, result.Error)
		assert.True(tb, updatedPath.IsOrphan)
		assert.Equal(tb, "/"+pinCID.String(), updatedPath.Path)
	}, UnpinTestOptions)
}

// Test analyzePathDependencies method
func TestUnpinOperationHandler_AnalyzePathDependencies(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")
		sharedCID := util.GenerateTestCID(t, "shared data")

		// Create file paths
		createTestFilePath(t, ctx, userID, targetCID, "/dir/file1.txt", "file1.txt", false)
		createTestFilePath(t, ctx, userID, sharedCID, "/dir/file2.txt", "file2.txt", false)

		// Create pins
		createTestPin(t, ctx, userID, targetCID)
		createTestPin(t, ctx, userID, sharedCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzePathDependencies(context.Background(), ctx.DB(), targetCID, userID)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, analysis)
		assert.False(tb, analysis.WouldBreakPaths)
		assert.Len(tb, analysis.AffectedPaths, 1)
		assert.Equal(tb, "/dir/file1.txt", analysis.AffectedPaths[0])
	}, UnpinTestOptions)
}

// Test isPathShared method
func TestUnpinOperationHandler_IsPathShared(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")
		sharedCID := util.GenerateTestCID(t, "shared data")

		// Create pins with same CID (shared path)
		createTestPin(t, ctx, userID, targetCID)
		createTestPin(t, ctx, userID, sharedCID)
		createTestPin(t, ctx, userID, targetCID) // Duplicate pin

		// Act
		shared, err := handler.IsPathShared(context.Background(), ctx.DB(), targetCID, userID)

		// Assert
		require.NoError(tb, err)
		assert.True(tb, shared)
	}, UnpinTestOptions)
}

// Test getAffectedPaths method
func TestUnpinOperationHandler_GetAffectedPaths(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		cid1 := util.GenerateTestCID(t, "data1")
		cid2 := util.GenerateTestCID(t, "data2")
		cid3 := util.GenerateTestCID(t, "data3")

		// Create file paths in hierarchy
		createTestFilePath(t, ctx, userID, cid1, "/dir", "dir", true)
		createTestFilePath(t, ctx, userID, cid2, "/dir/subdir", "subdir", true)
		createTestFilePath(t, ctx, userID, cid3, "/dir/subdir/file.txt", "file.txt", false)

		// Act
		affectedPaths, err := handler.GetAffectedPaths(context.Background(), ctx.DB(), "/dir/subdir", userID)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, affectedPaths, 2)
		assert.Contains(tb, affectedPaths, "/dir/subdir")
		assert.Contains(tb, affectedPaths, "/dir/subdir/file.txt")
	}, UnpinTestOptions)
}

// Test getSharedDirectories method
func TestUnpinOperationHandler_GetSharedDirectories(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		cid1 := util.GenerateTestCID(t, "data1")
		cid2 := util.GenerateTestCID(t, "data2")
		cid3 := util.GenerateTestCID(t, "data3")

		// Create file paths with shared directories
		createTestFilePath(t, ctx, userID, cid1, "/shared", "shared", true)
		createTestFilePath(t, ctx, userID, cid2, "/shared/dir1", "dir1", true)
		createTestFilePath(t, ctx, userID, cid3, "/shared/dir1/file.txt", "file.txt", false)
		
		// Create additional file paths in the same directories to make them shared
		cid4 := util.GenerateTestCID(t, "data4")
		cid5 := util.GenerateTestCID(t, "data5")
		createTestFilePath(t, ctx, userID, cid4, "/shared/another-file.txt", "another-file.txt", false)
		createTestFilePath(t, ctx, userID, cid5, "/shared/dir1/another-file.txt", "another-file.txt", false)

		// Create pins for all the file paths to make directories shared
		createTestPin(t, ctx, userID, cid1)
		createTestPin(t, ctx, userID, cid2)
		createTestPin(t, ctx, userID, cid3)
		createTestPin(t, ctx, userID, cid4)
		createTestPin(t, ctx, userID, cid5)

		// Act
		sharedDirs, err := handler.GetSharedDirectories(context.Background(), ctx.DB(), "/shared/dir1/file.txt", userID)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, sharedDirs, 2)
		assert.Contains(tb, sharedDirs, "/shared")
		assert.Contains(tb, sharedDirs, "/shared/dir1")
	}, UnpinTestOptions)
}

// Test getOrphanCandidates method
func TestUnpinOperationHandler_GetOrphanCandidates(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		parentCID := util.GenerateTestCID(t, "parent data")
		childCID1 := util.GenerateTestCID(t, "child1 data")
		childCID2 := util.GenerateTestCID(t, "child2 data")

		// Create file paths
		createTestFilePath(t, ctx, userID, parentCID, "/dir", "dir", true)
		createTestFilePath(t, ctx, userID, childCID1, "/dir/file1.txt", "file1.txt", false)
		createTestFilePath(t, ctx, userID, childCID2, "/dir/file2.txt", "file2.txt", false)

		// Create only one pin for childCID1 (not shared)
		createTestPin(t, ctx, userID, childCID1)

		// Act
		orphanCandidates, err := handler.GetOrphanCandidates(context.Background(), ctx.DB(), "/dir", userID)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, orphanCandidates, 1)
		assert.Contains(tb, orphanCandidates, childCID2.String())
	}, UnpinTestOptions)
}

// Test wouldBreakDirectoryStructure method
func TestUnpinOperationHandler_WouldBreakDirectoryStructure(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		dirCID := util.GenerateTestCID(t, "dir data")
		fileCID := util.GenerateTestCID(t, "file data")

		dirPath := createTestFilePath(t, ctx, 123, dirCID, "/dir", "dir", true)
		filePath := createTestFilePath(t, ctx, 123, fileCID, "/dir/file.txt", "file.txt", false)

		// Act & Assert
		assert.True(tb, handler.WouldBreakDirectoryStructure(*dirPath))
		assert.False(tb, handler.WouldBreakDirectoryStructure(*filePath))
	}, UnpinTestOptions)
}

// Test handlePathCascadingEffects method
func TestUnpinOperationHandler_HandlePathCascadingEffects(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")
		orphanCID := util.GenerateTestCID(t, "orphan data")

		// Create file paths
		createTestFilePath(t, ctx, userID, targetCID, "/dir/file1.txt", "file1.txt", false)
		orphanPath := createTestFilePath(t, ctx, userID, orphanCID, "/dir/subdir/file2.txt", "file2.txt", false)

		// Create pin only for targetCID (orphanCID will become orphan)
		createTestPin(t, ctx, userID, targetCID)

		analysis := &protocol.PathDependencyAnalysis{
			OrphanCandidates: []string{orphanCID.String()},
		}

		// Act
		err := handler.HandlePathCascadingEffects(context.Background(), ctx.DB(), targetCID, userID, analysis)

		// Assert
		require.NoError(tb, err)

		// Verify orphan candidate was promoted to orphan status
		var updatedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", orphanPath.ID).First(&updatedPath)
		require.NoError(tb, result.Error)
		assert.True(tb, updatedPath.IsOrphan)
		assert.Equal(tb, "/"+orphanCID.String(), updatedPath.Path)
	}, UnpinTestOptions)
}

// Test updatePathsToOrphan method
func TestUnpinOperationHandler_UpdatePathsToOrphan(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		fileCID := util.GenerateTestCID(t, "file data")

		originalPath := createTestFilePath(t, ctx, userID, fileCID, "/dir/subdir/file.txt", "file.txt", false)

		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		require.NotNil(tb, fileManagerSvc)

		// Act
		err := handler.UpdatePathsToOrphan(context.Background(), fileManagerSvc, fileCID, userID)

		// Assert
		require.NoError(tb, err)

		// Verify path was updated to orphan status
		var updatedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", originalPath.ID).First(&updatedPath)
		require.NoError(tb, result.Error)
		assert.True(tb, updatedPath.IsOrphan)
		assert.Equal(tb, "/"+fileCID.String(), updatedPath.Path)
		assert.Equal(tb, fileCID.String(), updatedPath.Name)
	}, UnpinTestOptions)
}

// Test updatePathsToOrphanWithTx method
func TestUnpinOperationHandler_UpdatePathsToOrphanWithTx(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		fileCID := util.GenerateTestCID(t, "file data")

		originalPath := createTestFilePath(t, ctx, userID, fileCID, "/dir/subdir/file.txt", "file.txt", false)

		// Act
		err := handler.UpdatePathsToOrphanWithTx(context.Background(), ctx.DB(), fileCID, userID)

		// Assert
		require.NoError(tb, err)

		// Verify path was updated to orphan status
		var updatedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", originalPath.ID).First(&updatedPath)
		require.NoError(tb, result.Error)
		assert.True(tb, updatedPath.IsOrphan)
		assert.Equal(tb, "/"+fileCID.String(), updatedPath.Path)
		assert.Equal(tb, fileCID.String(), updatedPath.Name)
	}, UnpinTestOptions)
}

// Test validateDAGIntegrityBeforeUnpin method
func TestUnpinOperationHandler_ValidateDAGIntegrityBeforeUnpin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")

		// Create block for target CID
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{})

		// Create a pin for the target CID
		createTestPin(t, ctx, userID, targetCID)

		// Act
		err := handler.ValidateDAGIntegrityBeforeUnpin(context.Background(), targetCID, userID)

		// Assert
		require.NoError(tb, err)
	}, UnpinTestOptions)
}

// Test validateDAGIntegrityAfterUnpin method
func TestUnpinOperationHandler_ValidateDAGIntegrityAfterUnpin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")
		otherCID := util.GenerateTestCID(t, "other data")

		// Create blocks
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, otherCID, "other.txt", 0, 512, []cid.Cid{})

		// Create pins (targetCID will be unpinned, otherCID remains)
		createTestPin(t, ctx, userID, otherCID)

		// Act
		err := handler.ValidateDAGIntegrityAfterUnpin(context.Background(), targetCID, userID)

		// Assert
		require.NoError(tb, err)
	}, UnpinTestOptions)
}

// Test validateUserDAGStructure method
func TestUnpinOperationHandler_ValidateUserDAGStructure(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		rootCID := util.GenerateTestCID(t, "root data")
		childCID := util.GenerateTestCID(t, "child data")

		// Create valid DAG structure
		_, _ = util.CreateTestBlockAndNode(t, ctx, rootCID, "root.txt", 0, 1024, []cid.Cid{childCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, childCID, "child.txt", 0, 512, []cid.Cid{})

		// Create pin
		createTestPin(t, ctx, userID, rootCID)

		// Act
		result, err := handler.ValidateUserDAGStructure(context.Background(), userID)

		// Assert
		require.NoError(tb, err)
		assert.True(tb, result.IsValid)
		assert.Len(tb, result.MissingBlocks, 0)
		assert.False(tb, result.CycleDetected)
	}, UnpinTestOptions)
}

// Test validateDAG method
func TestUnpinOperationHandler_ValidateDAG(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		rootCID := util.GenerateTestCID(t, "root data")
		childCID := util.GenerateTestCID(t, "child data")
		missingCID := util.GenerateTestCID(t, "missing data")

		// Create DAG with missing block
		_, _ = util.CreateTestBlockAndNode(t, ctx, rootCID, "root.txt", 1, 1024, []cid.Cid{childCID, missingCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, childCID, "child.txt", 0, 512, []cid.Cid{})

		// missingCID block is not created

		pinnedCIDs := map[string]bool{rootCID.String(): true}
		processedBlocks := make(map[string]bool)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		missingBlocks, cycleDetected, err := handler.ValidateDAG(context.Background(), blockSvc, rootCID, pinnedCIDs, processedBlocks)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, missingBlocks, 1)
		assert.Contains(tb, missingBlocks, missingCID.String())
		assert.False(tb, cycleDetected)
	}, UnpinTestOptions)
}

// Test validateOrphanPromotion method
func TestUnpinOperationHandler_ValidateOrphanPromotion(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		pinCID := util.GenerateTestCID(t, "pin data")

		// Create orphaned file path
		filePath := createTestFilePath(t, ctx, userID, pinCID, "/"+pinCID.String(), pinCID.String(), false)

		// Manually update to orphan status
		err := ctx.DB().Model(&pluginDb.FilePath{}).Where("id = ?", filePath.ID).Update("is_orphan", true).Error
		require.NoError(t, err)

		dependentPins := []string{pinCID.String()}

		// Act
		err = handler.ValidateOrphanPromotion(context.Background(), dependentPins, userID)

		// Assert
		require.NoError(tb, err)
	}, UnpinTestOptions)
}

// Test validateSystemConsistency method
func TestUnpinOperationHandler_ValidateSystemConsistency(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		pinCID := util.GenerateTestCID(t, "pin data")

		// Create orphaned file path (no pin record)
		filePath := createTestFilePath(t, ctx, userID, pinCID, "/"+pinCID.String(), pinCID.String(), false)

		// Manually update to orphan status
		err := ctx.DB().Model(&pluginDb.FilePath{}).Where("id = ?", filePath.ID).Update("is_orphan", true).Error
		require.NoError(t, err)

		// Act
		err = handler.ValidateSystemConsistency(context.Background(), pinCID, userID)

		// Assert
		require.NoError(tb, err)
	}, UnpinTestOptions)
}

// Test ValidateRequest method
func TestUnpinOperationHandler_ValidateRequest(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
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
	}, UnpinTestOptions)
}

// Test GetStatus method
func TestUnpinOperationHandler_GetStatus(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		req := &models.Request{
			Model:  gorm.Model{ID: 1},
			Status: models.RequestStatusProcessing,
		}

		// Mock workflow data
		testCID := util.GenerateTestCID(t, "test data")
		workflowData := &protocol.UnpinWorkflowData{
			PinRequestID:    "1",
			CID:             testCID.String(),
			UserID:          123,
			CurrentPhase:    protocol.UnpinPhaseAnalyzingDAGDependencies,
			CompletedPhases: 2,
			TotalPhases:     7,
		}

		// Mock the workflow service to return our test data
		workflowSvc := core.GetService[*coreMocks.MockWorkflowService](ctx, core.WORKFLOW_SERVICE)

		// Create a koanf instance and populate it with our test data
		k := koanf.New(".")
		err := k.Load(confmap.Provider(map[string]any{
			"pin_request_id":   workflowData.PinRequestID,
			"cid":              workflowData.CID,
			"user_id":          workflowData.UserID,
			"current_phase":    workflowData.CurrentPhase,
			"completed_phases": workflowData.CompletedPhases,
			"total_phases":     workflowData.TotalPhases,
		}, "."), nil)
		require.NoError(tb, err)

		workflowSvc.On("GetWorkflowMetadata", ctx, req.ID).Return(k, nil)

		// Act
		status, err := handler.GetStatus(context.Background(), req)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, status)
		assert.Equal(tb, models.RequestStatusProcessing, status.State)
		assert.Equal(tb, "Unpin operation in progress: "+string(protocol.UnpinPhaseAnalyzingDAGDependencies), status.Message)
		assert.Equal(tb, float64(28), status.ProgressPercent) // 2/7 * 100 = 28.57, rounded down
	}, UnpinTestOptions)
}

// Test Cleanup method
func TestUnpinOperationHandler_Cleanup(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		req := &models.Request{
			Model: gorm.Model{ID: 1},
		}

		// Act
		err := handler.Cleanup(context.Background(), req)

		// Assert
		require.NoError(tb, err)
	}, UnpinTestOptions)
}
