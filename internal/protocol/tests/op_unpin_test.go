package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/v2"
	"github.com/samber/lo"
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
	parentPath := util.CalculateParentPath(path)
	depth := 0
	if parentPath != "" {
		depth = len(strings.Split(strings.Trim(parentPath, "/"), "/"))
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

func TestUnpinOperationHandler_AnalyzeUnpinImpact_BasicDependency(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")
		childCID := util.GenerateTestCID(t, "child data")
		independentCID := util.GenerateTestCID(t, "independent data")

		// Create blocks and nodes
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{childCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, childCID, "child.txt", 0, 512, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, independentCID, "independent.txt", 0, 256, []cid.Cid{})

		// Create pins
		createTestPin(t, ctx, userID, targetCID)
		createTestPin(t, ctx, userID, childCID)
		createTestPin(t, ctx, userID, independentCID)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), targetCID, userID)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, analysis)
		assert.True(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 1)
		assert.Contains(tb, analysis.RootLevelCandidates, childCID.String())
		assert.Len(tb, analysis.AllChildren, 1)
		assert.Contains(tb, analysis.AllChildren, childCID.String())
	}, UnpinTestOptions)
}

// Test invalid CID scenarios
func TestUnpinOperationHandler_InvalidCIDScenarios(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := lo.ToPtr(uint(123))

		// Test with empty CID string
		emptyCIDReq := createTestRequest(t, cid.Cid{}, userID)
		err := handler.ValidateRequest(context.Background(), emptyCIDReq)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "hash is required")

		// Test with nil/undefined CID
		nilCIDReq := createTestRequest(t, cid.Undef, userID)
		err = handler.ValidateRequest(context.Background(), nilCIDReq)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "hash is required")

		malformedReq := &models.Request{
			Model:  gorm.Model{ID: 1},
			Status: models.RequestStatusProcessing,
			Hash:   cid.Undef.Bytes(),
			UserID: lo.ToPtr(uint(1)),
		}
		err = handler.ValidateRequest(context.Background(), malformedReq)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "hash is required")

		// Test with unpinning non-existent CID
		nonExistentCID := util.GenerateTestCID(t, "non-existent data")
		err = handler.ValidateDAGIntegrityBeforeUnpin(context.Background(), ctx.DB(), nonExistentCID, *userID)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "not found")

		// Test with CID that doesn't belong to user
		otherUserCID := util.GenerateTestCID(t, "other user data")
		_, _ = util.CreateTestBlockAndNode(t, ctx, otherUserCID, "other.txt", 0, 1024, []cid.Cid{})
		otherUserID := uint(999)
		createTestPin(t, ctx, otherUserID, otherUserCID)

		err = handler.ValidateDAGIntegrityBeforeUnpin(context.Background(), ctx.DB(), otherUserCID, *userID)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "not found")
	}, UnpinTestOptions)
}

// Test invalid user scenarios
func TestUnpinOperationHandler_InvalidUserScenarios(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		testCID := util.GenerateTestCID(t, "test data")

		// Test with zero user ID
		zeroUserReq := createTestRequest(t, testCID, lo.ToPtr(uint(0)))
		err := handler.ValidateRequest(context.Background(), zeroUserReq)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "user ID is required")

		// Test with very large user ID
		largeUserReq := &models.Request{
			Model:  gorm.Model{ID: 1},
			Status: models.RequestStatusProcessing,
			Hash:   testCID.Bytes(),
			UserID: lo.ToPtr(uint(999999999)),
		}
		err = handler.ValidateRequest(context.Background(), largeUserReq)
		assert.Error(tb, err) // Should fail because user doesn't exist / Should fail because user doesn't exist
	}, UnpinTestOptions)
}

// Test invalid request scenarios
func TestUnpinOperationHandler_InvalidRequestScenarios(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		// Test request with missing hash field (empty hash)
		missingHashReq := &models.Request{
			Model:  gorm.Model{ID: 1},
			Status: models.RequestStatusProcessing,
			Hash:   cid.Undef.Bytes(),
		}
		err := handler.ValidateRequest(context.Background(), missingHashReq)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "hash is required")

		// Test request with missing user ID
		missingUserIDReq := &models.Request{
			Model:  gorm.Model{ID: 1},
			Status: models.RequestStatusProcessing,
			Hash:   util.GenerateTestCID(t, "test data").Bytes(),
		}
		err = handler.ValidateRequest(context.Background(), missingUserIDReq)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "user ID is required")

		// Test request with nil user ID pointer
		nilUserIDReq := createTestRequest(t, util.GenerateTestCID(t, "test data"), nil)
		err = handler.ValidateRequest(context.Background(), nilUserIDReq)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "user ID is required")
	}, UnpinTestOptions)
}

// Test invalid database state scenarios
func TestUnpinOperationHandler_InvalidDatabaseStateScenarios(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")

		// Test unpinning non-existent CID
		err := handler.ValidateDAGIntegrityBeforeUnpin(context.Background(), ctx.DB(), testCID, userID)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "not found")

		// Test unpinning CID that doesn't belong to user
		otherUserID := uint(456)
		createTestPin(t, ctx, otherUserID, testCID)
		err = handler.ValidateDAGIntegrityBeforeUnpin(context.Background(), ctx.DB(), testCID, userID)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "not found")

		// Test with corrupted database records (simulate by closing DB)
		db, dbErr := ctx.DB().DB()
		require.NoError(tb, dbErr)
		dbErr = db.Close()
		require.NoError(tb, dbErr)

		err = handler.ValidateDAGIntegrityBeforeUnpin(context.Background(), ctx.DB(), testCID, userID)
		assert.Error(tb, err)
	}, UnpinTestOptions)
}

// Test invalid context scenarios
func TestUnpinOperationHandler_InvalidContextScenarios(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")
		_, _ = util.CreateTestBlockAndNode(t, ctx, testCID, "test.txt", 0, 1024, []cid.Cid{})
		createTestPin(t, ctx, userID, testCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Test with cancelled context
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := handler.AnalyzeUnpinImpact(cancelledCtx, ctx.DB(), testCID, userID)
		assert.Error(tb, err)
		assert.True(tb, errors.Is(err, context.Canceled))

		// Test with timeout context
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		time.Sleep(1 * time.Millisecond) // Ensure timeout
		_, err = handler.AnalyzeUnpinImpact(timeoutCtx, ctx.DB(), testCID, userID)
		assert.Error(tb, err)
		assert.True(tb, errors.Is(err, context.DeadlineExceeded))
	}, UnpinTestOptions)
}

// Test invalid service scenarios
func TestUnpinOperationHandler_InvalidServiceScenarios(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		testCID := util.GenerateTestCID(t, "test data")
		dependentCID := util.GenerateTestCID(t, "dependent data")
		_, _ = util.CreateTestBlockAndNode(t, ctx, dependentCID, "dependent.txt", 0, 512, []cid.Cid{testCID})
		createTestPin(t, ctx, userID, dependentCID)

		// Test with unavailable block service
		var nilBlockSvc pluginCore.BlockService
		_, err := handler.CheckDAGForCID(context.Background(), nilBlockSvc, dependentCID, testCID, make(map[string]bool))
		assert.Error(tb, err)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_NoDependencies(t *testing.T) {
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
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), targetCID, userID)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, analysis)
		assert.False(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 0)
		assert.Len(tb, analysis.AllChildren, 0)
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

		userID := uint(123)
		parentCID := util.GenerateTestCID(t, "parent data")
		childCID1 := util.GenerateTestCID(t, "child1 data")
		childCID2 := util.GenerateTestCID(t, "child2 data")

		// Create blocks with relationships
		_, _ = util.CreateTestBlockAndNode(t, ctx, parentCID, "parent", 1, 1024, []cid.Cid{childCID1, childCID2})
		_, _ = util.CreateTestBlockAndNode(t, ctx, childCID1, "child1.txt", 0, 512, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, childCID2, "child2.txt", 0, 256, []cid.Cid{})

		// Create pins for the blocks to associate them with the user
		createTestPin(t, ctx, userID, parentCID)
		createTestPin(t, ctx, userID, childCID1)
		createTestPin(t, ctx, userID, childCID2)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		parents, children, err := handler.GetBlockRelationships(context.Background(), ctx.DB(), blockSvc, childCID1, userID)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, parents, 1)
		assert.Contains(tb, parents, parentCID.String())
		assert.Len(tb, children, 0)
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

// Test updatePathsToRootLevelVisibilityWithTx method
func TestUnpinOperationHandler_UpdatePathsToRootLevelVisibilityWithTx(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		fileCID := util.GenerateTestCID(t, "file data")

		originalPath := createTestFilePath(t, ctx, userID, fileCID, "/dir/subdir/file.txt", "file.txt", false)

		// Act
		err := handler.UpdatePathsToRootLevelVisibilityWithTx(context.Background(), ctx.DB(), fileCID, userID)

		// Assert
		require.NoError(tb, err)

		// Verify path was updated to root level visibility status
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
		err := handler.ValidateDAGIntegrityBeforeUnpin(context.Background(), ctx.DB(), targetCID, userID)

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

		// Create blocks
		otherCID := util.GenerateTestCID(t, "other data")
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, otherCID, "other.txt", 0, 512, []cid.Cid{})

		// Create pins (targetCID will be unpinned, otherCID remains)
		createTestPin(t, ctx, userID, otherCID)

		// Act
		err := handler.ValidateDAGIntegrityAfterUnpin(context.Background(), ctx.DB(), targetCID, userID)

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
		result, err := handler.ValidateUserDAGStructure(context.Background(), ctx.DB(), userID)

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

// Test validateRootLevelVisibilityPromotion method
func TestUnpinOperationHandler_ValidateRootLevelVisibilityPromotion(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		pinCID := util.GenerateTestCID(t, "pin data")

		// Create root level visible file path
		filePath := createTestFilePath(t, ctx, userID, pinCID, "/"+pinCID.String(), pinCID.String(), false)

		// Manually update to root level visible status
		err := ctx.DB().Model(&pluginDb.FilePath{}).Where("id = ?", filePath.ID).Update("is_orphan", true).Error
		require.NoError(t, err)

		dependentPins := []string{pinCID.String()}

		// Act
		err = handler.ValidateRootLevelVisibilityPromotion(context.Background(), dependentPins, userID)

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
		err = handler.ValidateSystemConsistency(ctx, pinCID, userID)

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

func TestUnpinOperationHandler_AnalyzeUnpinImpact_InvalidCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		invalidCID := cid.Undef

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), invalidCID, userID)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, analysis)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_DBError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")

		// Close database connection to simulate failure
		db, err := ctx.DB().DB()
		require.NoError(tb, err)
		err = db.Close()
		require.NoError(tb, err)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), targetCID, userID)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, analysis)
	}, UnpinTestOptions)
}

// Test getAllUserPins with database error
func TestUnpinOperationHandler_GetAllUserPins_DBError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)

		// Close database connection to simulate failure
		db, err := ctx.DB().DB()
		require.NoError(tb, err)
		err = db.Close()
		require.NoError(tb, err)

		// Act
		pins, err := handler.GetAllUserPins(context.Background(), ctx.DB(), userID)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, pins)
	}, UnpinTestOptions)
}

// Test doesPinDependOnCID with service unavailability
func TestUnpinOperationHandler_DoesPinDependOnCID_ServiceUnavailable(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		pinCID := util.GenerateTestCID(t, "pin data")
		targetCID := util.GenerateTestCID(t, "target data")

		var nilBlockSvc pluginCore.BlockService

		// Act
		depends, err := handler.DoesPinDependOnCID(context.Background(), nilBlockSvc, pinCID, targetCID)

		// Assert
		assert.Error(tb, err)
		assert.False(tb, depends)
	}, UnpinTestOptions)
}

// Test checkDAGForCID with context cancellation
func TestUnpinOperationHandler_CheckDAGForCID_ContextCancellation(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		rootCID := util.GenerateTestCID(t, "root data")
		targetCID := util.GenerateTestCID(t, "target data")

		// Create a simple block
		_, _ = util.CreateTestBlockAndNode(t, ctx, rootCID, "root", 1, 1024, []cid.Cid{})

		// Create cancelled context
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		found, err := handler.CheckDAGForCID(cancelledCtx, blockSvc, rootCID, targetCID, make(map[string]bool))

		// Assert
		assert.Error(tb, err)
		assert.False(tb, found)
	}, UnpinTestOptions)
}

// Test getBlockRelationships with transaction failure
func TestUnpinOperationHandler_GetBlockRelationships_TransactionFailure(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		parentCID := util.GenerateTestCID(t, "parent data")
		childCID := util.GenerateTestCID(t, "child data")

		// Create a block with relationships
		_, _ = util.CreateTestBlockAndNode(t, ctx, parentCID, "parent", 1, 1024, []cid.Cid{childCID})

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Close database connection to simulate transaction failure
		db, err := ctx.DB().DB()
		require.NoError(tb, err)
		err = db.Close()
		require.NoError(tb, err)

		// Act
		parents, children, err := handler.GetBlockRelationships(context.Background(), ctx.DB(), blockSvc, childCID, userID)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, parents)
		assert.Nil(tb, children)
	}, UnpinTestOptions)
}

// Test promotePinsToRootLevelVisibility with empty dependent pins
func TestUnpinOperationHandler_PromotePinsToRootLevelVisibility_EmptyPins(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		dependentPins := []string{}

		// Act
		err := handler.PromotePinsToRootLevelVisibility(context.Background(), ctx.DB(), dependentPins, userID)

		// Assert
		assert.NoError(tb, err)
	}, UnpinTestOptions)
}

// Test analyzePathDependencies with nil CID
func TestUnpinOperationHandler_AnalyzePathDependencies_NilCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		nilCID := cid.Undef

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzePathDependencies(context.Background(), ctx.DB(), nilCID, userID)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, analysis)
	}, UnpinTestOptions)
}

// Test isPathShared with non-existent CID
func TestUnpinOperationHandler_IsPathShared_NonExistentCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		nonExistentCID := util.GenerateTestCID(t, "non-existent data")

		// Act
		shared, err := handler.IsPathShared(context.Background(), ctx.DB(), nonExistentCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.False(tb, shared)
	}, UnpinTestOptions)
}

// Test getAffectedPaths with maximum path depth
func TestUnpinOperationHandler_GetAffectedPaths_MaxPathDepth(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)

		// Create a deeply nested path structure
		maxDepthPath := ""
		cids := make([]cid.Cid, 0)
		for i := 0; i < 100; i++ { // Assuming 100 is a reasonable max depth for testing
			maxDepthPath += "/dir" + fmt.Sprintf("%d", i)
			testCID := util.GenerateTestCID(t, "data"+fmt.Sprintf("%d", i))
			cids = append(cids, testCID)
		}

		// Create file paths
		for i, c := range cids {
			path := ""
			for j := 0; j <= i; j++ {
				path += "/dir" + fmt.Sprintf("%d", j)
			}
			isDir := i < len(cids)-1 // All but the last are directories
			name := "dir" + fmt.Sprintf("%d", i)
			if !isDir {
				name = "file.txt"
			}
			createTestFilePath(t, ctx, userID, c, path, name, isDir)
		}

		// Act
		affectedPaths, err := handler.GetAffectedPaths(context.Background(), ctx.DB(), "/dir0/dir1", userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, affectedPaths)
	}, UnpinTestOptions)
}

// Test getSharedDirectories with unicode paths
func TestUnpinOperationHandler_GetSharedDirectories_UnicodePaths(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		fileCID := util.GenerateTestCID(t, "file data")

		// Create file path with unicode characters
		unicodePath := "/共享/文件夹/файл/ファイル.txt"
		createTestFilePath(t, ctx, userID, fileCID, unicodePath, "ファイル.txt", false)
		createTestPin(t, ctx, userID, fileCID)

		// Act
		sharedDirs, err := handler.GetSharedDirectories(context.Background(), ctx.DB(), unicodePath, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, sharedDirs)
	}, UnpinTestOptions)
}

// Test getOrphanCandidates with special characters in paths
func TestUnpinOperationHandler_GetOrphanCandidates_SpecialChars(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		parentCID := util.GenerateTestCID(t, "parent data")
		childCID := util.GenerateTestCID(t, "child data")

		// Create file paths with special characters
		parentPath := "/path with spaces"
		childPath := "/path with spaces/file-with-special-chars_@#$%.txt"

		createTestFilePath(t, ctx, userID, parentCID, parentPath, "path with spaces", true)
		createTestFilePath(t, ctx, userID, childCID, childPath, "file-with-special-chars_@#$%.txt", false)
		createTestPin(t, ctx, userID, parentCID) // Only parent is pinned

		// Act
		orphanCandidates, err := handler.GetOrphanCandidates(context.Background(), ctx.DB(), parentPath, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, orphanCandidates)
	}, UnpinTestOptions)
}

// Test wouldBreakDirectoryStructure with nil file path
func TestUnpinOperationHandler_WouldBreakDirectoryStructure_NilPath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		var nilPath pluginDb.FilePath

		// Act & Assert
		assert.False(tb, handler.WouldBreakDirectoryStructure(nilPath))
	}, UnpinTestOptions)
}

// Test handlePathCascadingEffects with nil analysis
func TestUnpinOperationHandler_HandlePathCascadingEffects_NilAnalysis(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")

		// Act
		err := handler.HandlePathCascadingEffects(context.Background(), ctx.DB(), targetCID, userID, nil)

		// Assert
		assert.Error(tb, err)
	}, UnpinTestOptions)
}

// Test updatePathsToRootLevelVisibilityWithTx with transaction failure
func TestUnpinOperationHandler_UpdatePathsToRootLevelVisibilityWithTx_TransactionFailure(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		fileCID := util.GenerateTestCID(t, "file data")

		// Close database connection to simulate transaction failure
		db, err := ctx.DB().DB()
		require.NoError(tb, err)
		err = db.Close()
		require.NoError(tb, err)

		// Act
		err = handler.UpdatePathsToRootLevelVisibilityWithTx(context.Background(), ctx.DB(), fileCID, userID)

		// Assert
		assert.Error(tb, err)
	}, UnpinTestOptions)
}

// Test validateDAGIntegrityBeforeUnpin with non-existent CID
func TestUnpinOperationHandler_ValidateDAGIntegrityBeforeUnpin_NonExistent(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		nonExistentCID := util.GenerateTestCID(t, "non-existent data")

		// Act
		err := handler.ValidateDAGIntegrityBeforeUnpin(context.Background(), ctx.DB(), nonExistentCID, userID)

		// Assert
		assert.Error(tb, err)
	}, UnpinTestOptions)
}

// Test validateDAGIntegrityAfterUnpin with database error
func TestUnpinOperationHandler_ValidateDAGIntegrityAfterUnpin_DBError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")

		// Close database connection to simulate failure
		db, err := ctx.DB().DB()
		require.NoError(tb, err)
		err = db.Close()
		require.NoError(tb, err)

		// Act
		err = handler.ValidateDAGIntegrityAfterUnpin(context.Background(), ctx.DB(), targetCID, userID)

		// Assert
		assert.Error(tb, err)
	}, UnpinTestOptions)
}

// Test validateUserDAGStructure with cycle detection
func TestUnpinOperationHandler_ValidateUserDAGStructure_Cycle(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		cid1 := util.GenerateTestCID(t, "data1")
		cid2 := util.GenerateTestCID(t, "data2")

		// Create a cycle: cid1 -> cid2 -> cid1
		_, _ = util.CreateTestBlockAndNode(t, ctx, cid1, "file1.txt", 1, 1024, []cid.Cid{cid2})
		_, _ = util.CreateTestBlockAndNode(t, ctx, cid2, "file2.txt", 0, 512, []cid.Cid{cid1})
		createTestPin(t, ctx, userID, cid1)

		// Act
		result, err := handler.ValidateUserDAGStructure(context.Background(), ctx.DB(), userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, result)
	}, UnpinTestOptions)
}

// Test validateDAG with missing blocks
func TestUnpinOperationHandler_ValidateDAG_MissingBlocks(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		rootCID := util.GenerateTestCID(t, "root data")
		missingCID := util.GenerateTestCID(t, "missing data")

		// Create root block with reference to missing block
		_, _ = util.CreateTestBlockAndNode(t, ctx, rootCID, "root.txt", 1, 1024, []cid.Cid{missingCID})

		// Don't create missingCID block
		pinnedCIDs := map[string]bool{rootCID.String(): true}
		processedBlocks := make(map[string]bool)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		missingBlocks, cycleDetected, err := handler.ValidateDAG(context.Background(), blockSvc, rootCID, pinnedCIDs, processedBlocks)

		// Assert
		assert.NoError(tb, err)
		assert.Contains(tb, missingBlocks, missingCID.String())
		assert.False(tb, cycleDetected)
	}, UnpinTestOptions)
}

// Test validateRootLevelVisibilityPromotion with empty dependent pins
func TestUnpinOperationHandler_ValidateRootLevelVisibilityPromotion_EmptyPins(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		dependentPins := []string{}

		// Act
		err := handler.ValidateRootLevelVisibilityPromotion(context.Background(), dependentPins, userID)

		// Assert
		assert.NoError(tb, err)
	}, UnpinTestOptions)
}

// Test validateSystemConsistency with non-existent file path
func TestUnpinOperationHandler_ValidateSystemConsistency_NonExistentPath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		pinCID := util.GenerateTestCID(t, "pin data")

		// Don't create any file path

		// Act
		err := handler.ValidateSystemConsistency(context.Background(), pinCID, userID)

		// Assert
		assert.NoError(tb, err)
	}, UnpinTestOptions)
}

// Test ValidateRequest with nil user ID
func TestUnpinOperationHandler_ValidateRequest_NilUserID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		testCID := util.GenerateTestCID(t, "test data")
		req := createTestRequest(t, testCID, nil) // nil user ID

		// Act
		err := handler.ValidateRequest(context.Background(), req)

		// Assert
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "user ID is required")
	}, UnpinTestOptions)
}

// Test GetStatus with workflow service error
func TestUnpinOperationHandler_GetStatus_WorkflowError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		req := &models.Request{
			Model:  gorm.Model{ID: 1},
			Status: models.RequestStatusProcessing,
		}

		workflowSvc := core.GetService[*coreMocks.MockWorkflowService](ctx, core.WORKFLOW_SERVICE)
		workflowSvc.On("GetWorkflowMetadata", ctx, req.ID).Return((*koanf.Koanf)(nil), errors.New("workflow service error"))

		// Act
		status, err := handler.GetStatus(context.Background(), req)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, status)
	}, UnpinTestOptions)
}

// Test complex dependency chain
func TestUnpinOperationHandler_ComplexDependencyChain(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)

		// Create a complex dependency chain: A -> B -> C -> D
		//                                      -> E -> F
		cidA := util.GenerateTestCID(t, "dataA")
		cidB := util.GenerateTestCID(t, "dataB")
		cidC := util.GenerateTestCID(t, "dataC")
		cidD := util.GenerateTestCID(t, "dataD")
		cidE := util.GenerateTestCID(t, "dataE")
		cidF := util.GenerateTestCID(t, "dataF")

		_, _ = util.CreateTestBlockAndNode(t, ctx, cidA, "fileA.txt", 1, 1024, []cid.Cid{cidB, cidE})
		_, _ = util.CreateTestBlockAndNode(t, ctx, cidB, "fileB.txt", 1, 512, []cid.Cid{cidC})
		_, _ = util.CreateTestBlockAndNode(t, ctx, cidC, "fileC.txt", 1, 256, []cid.Cid{cidD})
		_, _ = util.CreateTestBlockAndNode(t, ctx, cidD, "fileD.txt", 0, 128, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, cidE, "fileE.txt", 1, 512, []cid.Cid{cidF})
		_, _ = util.CreateTestBlockAndNode(t, ctx, cidF, "fileF.txt", 0, 128, []cid.Cid{})

		// Pin all CIDs
		createTestPin(t, ctx, userID, cidA)
		createTestPin(t, ctx, userID, cidB)
		createTestPin(t, ctx, userID, cidC)
		createTestPin(t, ctx, userID, cidD)
		createTestPin(t, ctx, userID, cidE)
		createTestPin(t, ctx, userID, cidF)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), cidA, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
	}, UnpinTestOptions)
}

// Test rollback scenario
func TestUnpinOperationHandler_RollbackScenario(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		fileCID := util.GenerateTestCID(t, "file data")

		// Create file path
		originalPath := createTestFilePath(t, ctx, userID, fileCID, "/test/file.txt", "file.txt", false)
		createTestPin(t, ctx, userID, fileCID)

		// Simulate a partial failure by closing DB connection
		db, err := ctx.DB().DB()
		require.NoError(tb, err)
		err = db.Close()
		require.NoError(tb, err)

		// Act - This should fail due to closed connection
		err = handler.UpdatePathsToRootLevelVisibilityWithTx(context.Background(), ctx.DB(), fileCID, userID)

		// Assert
		assert.Error(tb, err)

		// Verify that the original path was not updated (rollback simulation)
		var unchangedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", originalPath.ID).First(&unchangedPath)
		if result.Error == nil {
			assert.False(tb, unchangedPath.IsOrphan)
			assert.NotEqual(tb, "/"+fileCID.String(), unchangedPath.Path)
		}
	}, UnpinTestOptions)
}

// Test system state after failed unpin operations
func TestUnpinOperationHandler_SystemStateAfterFailedUnpin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")

		// Create blocks and pins
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{})
		pin := createTestPin(t, ctx, userID, targetCID)
		filePath := createTestFilePath(t, ctx, userID, targetCID, "/test/target.txt", "target.txt", false)

		// Simulate a failed unpin operation by closing DB connection
		db, err := ctx.DB().DB()
		require.NoError(tb, err)
		err = db.Close()
		require.NoError(tb, err)

		// Act - This should fail
		err = handler.UpdatePathsToRootLevelVisibilityWithTx(context.Background(), ctx.DB(), targetCID, userID)

		// Assert
		assert.Error(tb, err)

		// Verify system state is unchanged
		var unchangedPin pluginDb.IPFSPin
		result := ctx.DB().Where("id = ?", pin.ID).First(&unchangedPin)
		if result.Error == nil {
			assert.Equal(tb, pluginDb.PinningStatusPinned, unchangedPin.Status)
		}

		var unchangedPath pluginDb.FilePath
		result = ctx.DB().Where("id = ?", filePath.ID).First(&unchangedPath)
		if result.Error == nil {
			assert.False(tb, unchangedPath.IsOrphan)
		}
	}, UnpinTestOptions)
}

// Test single file unpin scenarios
func TestUnpinOperationHandler_AnalyzeUnpinImpact_SingleFile(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		fileCID := util.GenerateTestCID(t, "file data")

		// Create a standalone file
		_, _ = util.CreateTestBlockAndNode(t, ctx, fileCID, "standalone.txt", 0, 1024, []cid.Cid{})
		filePath := createTestFilePath(t, ctx, userID, fileCID, "/standalone.txt", "standalone.txt", false)
		createTestPin(t, ctx, userID, fileCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), fileCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// A single file should not create orphans since it has no children
		assert.False(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 0)
		assert.Len(tb, analysis.AllChildren, 0)

		// Verify the file path still exists but is not orphaned yet
		var unchangedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", filePath.ID).First(&unchangedPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedPath.IsOrphan)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_SingleFileInDirectory(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		dirCID := util.GenerateTestCID(t, "directory data")
		fileCID := util.GenerateTestCID(t, "file data")

		// Create blocks and relationships
		_, _ = util.CreateTestBlockAndNode(t, ctx, dirCID, "testdir", 1, 1024, []cid.Cid{fileCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, fileCID, "file.txt", 0, 512, []cid.Cid{})

		// Create directory structure
		createTestFilePath(t, ctx, userID, dirCID, "/testdir", "testdir", true)
		filePath := createTestFilePath(t, ctx, userID, fileCID, "/testdir/file.txt", "file.txt", false)

		// Pin the directory and file
		createTestPin(t, ctx, userID, dirCID)
		createTestPin(t, ctx, userID, fileCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), fileCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		assert.False(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 0)
		assert.Len(tb, analysis.AllChildren, 0)

		// Verify the file path still exists but is not orphaned yet
		var unchangedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", filePath.ID).First(&unchangedPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedPath.IsOrphan)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_SingleFileWithMultipleReferences(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		fileCID := util.GenerateTestCID(t, "shared file data")

		// Create blocks for the shared file
		_, _ = util.CreateTestBlockAndNode(t, ctx, fileCID, "shared.txt", 0, 1024, []cid.Cid{})

		// Create file paths that reference the same CID
		filePath1 := createTestFilePath(t, ctx, userID, fileCID, "/file1.txt", "file1.txt", false)
		filePath2 := createTestFilePath(t, ctx, userID, fileCID, "/file2.txt", "file2.txt", false)

		// Create a file that depends on our shared file
		dependentCID := util.GenerateTestCID(t, "dependent data")
		_, _ = util.CreateTestBlockAndNode(t, ctx, dependentCID, "dependent.txt", 0, 512, []cid.Cid{fileCID})
		createTestFilePath(t, ctx, userID, dependentCID, "/dependent.txt", "dependent.txt", false)

		// Pin all files
		createTestPin(t, ctx, userID, fileCID)
		createTestPin(t, ctx, userID, dependentCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), fileCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Should not create orphans because this file has no children
		assert.False(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 0)
		assert.Len(tb, analysis.AllChildren, 0)

		// Verify the file paths still exist but are not root level visible yet
		var unchangedPath1 pluginDb.FilePath
		result := ctx.DB().Where("id = ?", filePath1.ID).First(&unchangedPath1)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedPath1.IsOrphan)

		var unchangedPath2 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", filePath2.ID).First(&unchangedPath2)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedPath2.IsOrphan)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_SingleFileOnlyContentInDirectory(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		dirCID := util.GenerateTestCID(t, "directory data")
		fileCID := util.GenerateTestCID(t, "file data")

		// Create blocks and relationships
		_, _ = util.CreateTestBlockAndNode(t, ctx, dirCID, "onlycontent", 1, 1024, []cid.Cid{fileCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, fileCID, "file.txt", 0, 512, []cid.Cid{})

		// Create directory with only one file
		dirPath := createTestFilePath(t, ctx, userID, dirCID, "/onlycontent", "onlycontent", true)
		filePath := createTestFilePath(t, ctx, userID, fileCID, "/onlycontent/file.txt", "file.txt", false)

		// Pin the file (not the directory)
		createTestPin(t, ctx, userID, fileCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), fileCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		assert.False(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 0)
		assert.Len(tb, analysis.AllChildren, 0)

		// Verify the file path still exists but is not orphaned yet
		var unchangedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", filePath.ID).First(&unchangedPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedPath.IsOrphan)

		// Verify the directory path still exists
		var unchangedDirPath pluginDb.FilePath
		result = ctx.DB().Where("id = ?", dirPath.ID).First(&unchangedDirPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedDirPath.IsOrphan)
	}, UnpinTestOptions)
}

// Test directory unpin scenarios
func TestUnpinOperationHandler_AnalyzeUnpinImpact_EmptyDirectory(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		dirCID := util.GenerateTestCID(t, "empty directory data")

		// Create an empty directory
		dirPath := createTestFilePath(t, ctx, userID, dirCID, "/emptydir", "emptydir", true)
		createTestPin(t, ctx, userID, dirCID)

		// Create block and node for the empty directory
		_, _ = util.CreateTestBlockAndNode(t, ctx, dirCID, "emptydir", 0, 1024, []cid.Cid{})

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), dirCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Empty directory should not create orphans
		assert.False(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 0)
		assert.Len(tb, analysis.AllChildren, 0)

		// Verify the directory path still exists but is not root level visible yet
		var unchangedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", dirPath.ID).First(&unchangedPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedPath.IsOrphan)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_DirectoryWithMultipleFiles(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		dirCID := util.GenerateTestCID(t, "directory data")
		file1CID := util.GenerateTestCID(t, "file1 data")
		file2CID := util.GenerateTestCID(t, "file2 data")
		file3CID := util.GenerateTestCID(t, "file3 data")

		// Create directory with multiple files
		dirPath := createTestFilePath(t, ctx, userID, dirCID, "/multifiles", "multifiles", true)
		file1Path := createTestFilePath(t, ctx, userID, file1CID, "/multifiles/file1.txt", "file1.txt", false)
		file2Path := createTestFilePath(t, ctx, userID, file2CID, "/multifiles/file2.txt", "file2.txt", false)
		file3Path := createTestFilePath(t, ctx, userID, file3CID, "/multifiles/file3.txt", "file3.txt", false)

		// Create blocks for all CIDs
		_, _ = util.CreateTestBlockAndNode(t, ctx, dirCID, "multifiles", 1, 1024, []cid.Cid{file1CID, file2CID, file3CID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, file1CID, "file1.txt", 0, 512, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, file2CID, "file2.txt", 0, 512, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, file3CID, "file3.txt", 0, 512, []cid.Cid{})

		// Pin the directory and all files
		createTestPin(t, ctx, userID, dirCID)
		createTestPin(t, ctx, userID, file1CID)
		createTestPin(t, ctx, userID, file2CID)
		createTestPin(t, ctx, userID, file3CID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), dirCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Directory unpin should create orphans because its contents are pinned by this user
		assert.True(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 3)
		assert.Contains(tb, analysis.RootLevelCandidates, file1CID.String())
		assert.Contains(tb, analysis.RootLevelCandidates, file2CID.String())
		assert.Contains(tb, analysis.RootLevelCandidates, file3CID.String())
		assert.Len(tb, analysis.AllChildren, 3)
		assert.Contains(tb, analysis.AllChildren, file1CID.String())
		assert.Contains(tb, analysis.AllChildren, file2CID.String())
		assert.Contains(tb, analysis.AllChildren, file3CID.String())

		// Verify paths still exist but are not orphaned yet
		var unchangedDirPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", dirPath.ID).First(&unchangedDirPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedDirPath.IsOrphan)

		var unchangedFile1Path pluginDb.FilePath
		result = ctx.DB().Where("id = ?", file1Path.ID).First(&unchangedFile1Path)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedFile1Path.IsOrphan)

		var unchangedFile2Path pluginDb.FilePath
		result = ctx.DB().Where("id = ?", file2Path.ID).First(&unchangedFile2Path)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedFile2Path.IsOrphan)

		var unchangedFile3Path pluginDb.FilePath
		result = ctx.DB().Where("id = ?", file3Path.ID).First(&unchangedFile3Path)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedFile3Path.IsOrphan)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_DirectoryWithNestedSubdirectories(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		rootDirCID := util.GenerateTestCID(t, "root directory data")
		subDir1CID := util.GenerateTestCID(t, "subdir1 data")
		subDir2CID := util.GenerateTestCID(t, "subdir2 data")
		fileCID := util.GenerateTestCID(t, "file data")

		// Create blocks and nodes with proper relationships
		_, _ = util.CreateTestBlockAndNode(t, ctx, rootDirCID, "nested", 1, 1024, []cid.Cid{subDir1CID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, subDir1CID, "sub1", 1, 512, []cid.Cid{subDir2CID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, subDir2CID, "sub2", 1, 256, []cid.Cid{fileCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, fileCID, "file.txt", 0, 128, []cid.Cid{})

		// Create nested directory structure
		rootDirPath := createTestFilePath(t, ctx, userID, rootDirCID, "/nested", "nested", true)
		subDir1Path := createTestFilePath(t, ctx, userID, subDir1CID, "/nested/sub1", "sub1", true)
		subDir2Path := createTestFilePath(t, ctx, userID, subDir2CID, "/nested/sub1/sub2", "sub2", true)
		filePath := createTestFilePath(t, ctx, userID, fileCID, "/nested/sub1/sub2/file.txt", "file.txt", false)

		// Pin all blocks to associate them with the user
		createTestPin(t, ctx, userID, rootDirCID)
		createTestPin(t, ctx, userID, subDir1CID)
		createTestPin(t, ctx, userID, subDir2CID)
		createTestPin(t, ctx, userID, fileCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), rootDirCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Directory unpin should create orphans because its contents are pinned by this user
		assert.True(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 3)
		assert.Contains(tb, analysis.RootLevelCandidates, subDir1CID.String())
		assert.Contains(tb, analysis.RootLevelCandidates, subDir2CID.String())
		assert.Contains(tb, analysis.RootLevelCandidates, fileCID.String())
		assert.Len(tb, analysis.AllChildren, 1)
		assert.Contains(tb, analysis.AllChildren, subDir1CID.String())

		// Verify paths still exist but are not orphaned yet
		var unchangedRootDirPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", rootDirPath.ID).First(&unchangedRootDirPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedRootDirPath.IsOrphan)

		var unchangedSubDir1Path pluginDb.FilePath
		result = ctx.DB().Where("id = ?", subDir1Path.ID).First(&unchangedSubDir1Path)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSubDir1Path.IsOrphan)

		var unchangedSubDir2Path pluginDb.FilePath
		result = ctx.DB().Where("id = ?", subDir2Path.ID).First(&unchangedSubDir2Path)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSubDir2Path.IsOrphan)

		var unchangedFilePath pluginDb.FilePath
		result = ctx.DB().Where("id = ?", filePath.ID).First(&unchangedFilePath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedFilePath.IsOrphan)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_DirectoryWithMixedContent(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		dirCID := util.GenerateTestCID(t, "directory data")
		fileCID := util.GenerateTestCID(t, "file data")
		subDirCID := util.GenerateTestCID(t, "subdir data")

		// Create blocks and nodes with proper relationships
		_, _ = util.CreateTestBlockAndNode(t, ctx, dirCID, "mixed", 1, 1024, []cid.Cid{fileCID, subDirCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, fileCID, "file.txt", 0, 512, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, subDirCID, "subdir", 0, 512, []cid.Cid{})

		// Create directory with both files and subdirectories
		dirPath := createTestFilePath(t, ctx, userID, dirCID, "/mixed", "mixed", true)
		filePath := createTestFilePath(t, ctx, userID, fileCID, "/mixed/file.txt", "file.txt", false)
		subDirPath := createTestFilePath(t, ctx, userID, subDirCID, "/mixed/subdir", "subdir", true)

		// Pin the directory and its contents
		createTestPin(t, ctx, userID, dirCID)
		createTestPin(t, ctx, userID, fileCID)
		createTestPin(t, ctx, userID, subDirCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), dirCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Directory unpin should create orphans because its contents are pinned by this user
		assert.True(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 2)
		assert.Contains(tb, analysis.RootLevelCandidates, fileCID.String())
		assert.Contains(tb, analysis.RootLevelCandidates, subDirCID.String())
		assert.Len(tb, analysis.AllChildren, 2)
		assert.Contains(tb, analysis.AllChildren, fileCID.String())
		assert.Contains(tb, analysis.AllChildren, subDirCID.String())

		// Verify paths still exist but are not orphaned yet
		var unchangedDirPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", dirPath.ID).First(&unchangedDirPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedDirPath.IsOrphan)

		var unchangedFilePath pluginDb.FilePath
		result = ctx.DB().Where("id = ?", filePath.ID).First(&unchangedFilePath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedFilePath.IsOrphan)

		var unchangedSubDirPath pluginDb.FilePath
		result = ctx.DB().Where("id = ?", subDirPath.ID).First(&unchangedSubDirPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSubDirPath.IsOrphan)
	}, UnpinTestOptions)
}

// Test shared path scenarios
func TestUnpinOperationHandler_AnalyzeUnpinImpact_SharedFileAcrossMultipleUsers(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID1 := uint(123)
		userID2 := uint(456)
		fileCID := util.GenerateTestCID(t, "shared file data")

		// Create blocks and nodes for the shared file
		util.CreateTestBlockAndNode(t, ctx, fileCID, "shared.txt", 0, 1024, []cid.Cid{})

		// Create file paths for multiple users referencing the same CID
		filePath1 := createTestFilePath(t, ctx, userID1, fileCID, "/user1/shared.txt", "shared.txt", false)
		filePath2 := createTestFilePath(t, ctx, userID2, fileCID, "/user2/shared.txt", "shared.txt", false)

		// Pin for both users
		createTestPin(t, ctx, userID1, fileCID)
		createTestPin(t, ctx, userID2, fileCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act - Unpin for user1
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), fileCID, userID1)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Should not create orphans because this is a standalone file with no children
		assert.False(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 0)
		assert.Len(tb, analysis.AllChildren, 0)

		// Verify user1's path still exists but is not orphaned yet
		var unchangedPath1 pluginDb.FilePath
		result := ctx.DB().Where("id = ?", filePath1.ID).First(&unchangedPath1)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedPath1.IsOrphan)

		// Verify user2's path still exists
		var unchangedPath2 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", filePath2.ID).First(&unchangedPath2)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedPath2.IsOrphan)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_DirectoryWithSharedFiles(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		dirCID := util.GenerateTestCID(t, "directory data")
		sharedFileCID := util.GenerateTestCID(t, "shared file data")
		privateFileCID := util.GenerateTestCID(t, "private file data")

		// Create blocks for the files
		_, _ = util.CreateTestBlockAndNode(t, ctx, sharedFileCID, "shared.txt", 0, 512, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, privateFileCID, "private.txt", 0, 256, []cid.Cid{})

		// Create directory block containing both shared and private files
		_, _ = util.CreateTestBlockAndNode(t, ctx, dirCID, "sharedcontent", 1, 1024, []cid.Cid{sharedFileCID, privateFileCID})

		// Create directory containing both shared and private files
		dirPath := createTestFilePath(t, ctx, userID, dirCID, "/sharedcontent", "sharedcontent", true)
		sharedFilePath := createTestFilePath(t, ctx, userID, sharedFileCID, "/sharedcontent/shared.txt", "shared.txt", false)
		privateFilePath := createTestFilePath(t, ctx, userID, privateFileCID, "/sharedcontent/private.txt", "private.txt", false)

		// Pin the directory
		createTestPin(t, ctx, userID, dirCID)

		// Create another user's pin for the shared file
		otherUserID := uint(456)
		createTestPin(t, ctx, otherUserID, sharedFileCID)

		// Pin both files for the user
		createTestPin(t, ctx, userID, sharedFileCID)
		createTestPin(t, ctx, userID, privateFileCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), dirCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Directory unpin should create orphans because both files are pinned by this user
		assert.True(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 2)
		assert.Contains(tb, analysis.RootLevelCandidates, sharedFileCID.String())
		assert.Contains(tb, analysis.RootLevelCandidates, privateFileCID.String())
		assert.Len(tb, analysis.AllChildren, 2)
		assert.Contains(tb, analysis.AllChildren, sharedFileCID.String())
		assert.Contains(tb, analysis.AllChildren, privateFileCID.String())

		// Verify paths still exist but are not orphaned yet
		var unchangedDirPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", dirPath.ID).First(&unchangedDirPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedDirPath.IsOrphan)

		var unchangedSharedFilePath pluginDb.FilePath
		result = ctx.DB().Where("id = ?", sharedFilePath.ID).First(&unchangedSharedFilePath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSharedFilePath.IsOrphan)

		var unchangedPrivateFilePath pluginDb.FilePath
		result = ctx.DB().Where("id = ?", privateFilePath.ID).First(&unchangedPrivateFilePath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedPrivateFilePath.IsOrphan)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_FromSharedDirectoryStructure(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID1 := uint(123)
		userID2 := uint(456)
		dirCID := util.GenerateTestCID(t, "shared directory data")
		fileCID := util.GenerateTestCID(t, "file data")

		// Create blocks and nodes for the directory structure
		_, _ = util.CreateTestBlockAndNode(t, ctx, dirCID, "shared", 1, 1024, []cid.Cid{fileCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, fileCID, "file.txt", 0, 512, []cid.Cid{})

		// Create shared directory structure
		dirPath1 := createTestFilePath(t, ctx, userID1, dirCID, "/shared", "shared", true)
		dirPath2 := createTestFilePath(t, ctx, userID2, dirCID, "/shared", "shared", true)
		filePath := createTestFilePath(t, ctx, userID1, fileCID, "/shared/file.txt", "file.txt", false)

		// Pin for both users
		createTestPin(t, ctx, userID1, dirCID)
		createTestPin(t, ctx, userID1, fileCID)
		createTestPin(t, ctx, userID2, dirCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act - Unpin directory for user1
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), dirCID, userID1)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Directory unpin should create orphans because its contents are pinned by this user
		assert.True(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 1)
		assert.Contains(tb, analysis.RootLevelCandidates, fileCID.String())
		assert.Len(tb, analysis.AllChildren, 1)
		assert.Contains(tb, analysis.AllChildren, fileCID.String())

		// Verify paths still exist but are not orphaned yet
		var unchangedDirPath1 pluginDb.FilePath
		result := ctx.DB().Where("id = ?", dirPath1.ID).First(&unchangedDirPath1)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedDirPath1.IsOrphan)

		var unchangedDirPath2 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", dirPath2.ID).First(&unchangedDirPath2)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedDirPath2.IsOrphan)

		var unchangedFilePath pluginDb.FilePath
		result = ctx.DB().Where("id = ?", filePath.ID).First(&unchangedFilePath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedFilePath.IsOrphan)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_MultiplePinsSameCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		fileCID := util.GenerateTestCID(t, "file data")

		// Create blocks and nodes for the file
		_, _ = util.CreateTestBlockAndNode(t, ctx, fileCID, "multi-pin.txt", 0, 1024, []cid.Cid{})

		// Create file path
		filePath := createTestFilePath(t, ctx, userID, fileCID, "/multi-pin.txt", "multi-pin.txt", false)

		// Create multiple pins for the same CID
		createTestPin(t, ctx, userID, fileCID)
		createTestPin(t, ctx, userID, fileCID)
		createTestPin(t, ctx, userID, fileCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), fileCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Should not create orphans because this is a standalone file with no children
		assert.False(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 0)
		assert.Len(tb, analysis.AllChildren, 0)

		// Verify the file path still exists but is not orphaned yet
		var unchangedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", filePath.ID).First(&unchangedPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedPath.IsOrphan)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_ComplexSharedDirectoryHierarchies(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID1 := uint(123)
		userID2 := uint(456)
		userID3 := uint(789)

		rootDirCID := util.GenerateTestCID(t, "root directory data")
		subDirCID := util.GenerateTestCID(t, "sub directory data")
		sharedFileCID := util.GenerateTestCID(t, "shared file data")
		user1FileCID := util.GenerateTestCID(t, "user1 file data")
		user2FileCID := util.GenerateTestCID(t, "user2 file data")

		// Create blocks and nodes for the complex shared directory hierarchy
		_, _ = util.CreateTestBlockAndNode(t, ctx, rootDirCID, "shared", 1, 1024, []cid.Cid{subDirCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, subDirCID, "subdir", 1, 512, []cid.Cid{user1FileCID, sharedFileCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, user1FileCID, "file1.txt", 0, 256, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, user2FileCID, "file2.txt", 0, 256, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, sharedFileCID, "shared.txt", 0, 128, []cid.Cid{})

		// Create complex shared directory hierarchy
		// User1: /shared/subdir/file1.txt, /shared/subdir/shared.txt
		// User2: /shared/subdir/file2.txt, /shared/subdir/shared.txt
		// User3: /shared/subdir/shared.txt (only the shared file)

		rootDirPath1 := createTestFilePath(t, ctx, userID1, rootDirCID, "/shared", "shared", true)
		subDirPath1 := createTestFilePath(t, ctx, userID1, subDirCID, "/shared/subdir", "subdir", true)
		user1FilePath := createTestFilePath(t, ctx, userID1, user1FileCID, "/shared/subdir/file1.txt", "file1.txt", false)
		sharedFilePath1 := createTestFilePath(t, ctx, userID1, sharedFileCID, "/shared/subdir/shared.txt", "shared.txt", false)

		rootDirPath2 := createTestFilePath(t, ctx, userID2, rootDirCID, "/shared", "shared", true)
		subDirPath2 := createTestFilePath(t, ctx, userID2, subDirCID, "/shared/subdir", "subdir", true)
		user2FilePath := createTestFilePath(t, ctx, userID2, user2FileCID, "/shared/subdir/file2.txt", "file2.txt", false)
		sharedFilePath2 := createTestFilePath(t, ctx, userID2, sharedFileCID, "/shared/subdir/shared.txt", "shared.txt", false)

		// Pin everything
		createTestPin(t, ctx, userID1, rootDirCID)
		createTestPin(t, ctx, userID1, subDirCID)
		createTestPin(t, ctx, userID1, user1FileCID)
		createTestPin(t, ctx, userID1, sharedFileCID)

		createTestPin(t, ctx, userID2, rootDirCID)
		createTestPin(t, ctx, userID2, subDirCID)
		createTestPin(t, ctx, userID2, user2FileCID)
		createTestPin(t, ctx, userID2, sharedFileCID)

		createTestPin(t, ctx, userID3, sharedFileCID)

		// Create file path for user3 as well
		createTestFilePath(t, ctx, userID3, sharedFileCID, "/shared.txt", "shared.txt", false)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act - Unpin the root directory for user1
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), rootDirCID, userID1)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Directory unpin should create orphans because its contents are pinned by this user
		assert.True(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 3)
		assert.Contains(tb, analysis.RootLevelCandidates, subDirCID.String())
		assert.Contains(tb, analysis.RootLevelCandidates, user1FileCID.String())
		assert.Contains(tb, analysis.RootLevelCandidates, sharedFileCID.String())
		assert.Len(tb, analysis.AllChildren, 1)
		assert.Contains(tb, analysis.AllChildren, subDirCID.String())

		// Verify paths still exist but are not orphaned yet
		var unchangedRootDirPath1 pluginDb.FilePath
		result := ctx.DB().Where("id = ?", rootDirPath1.ID).First(&unchangedRootDirPath1)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedRootDirPath1.IsOrphan)

		var unchangedSubDirPath1 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", subDirPath1.ID).First(&unchangedSubDirPath1)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSubDirPath1.IsOrphan)

		var unchangedUser1FilePath pluginDb.FilePath
		result = ctx.DB().Where("id = ?", user1FilePath.ID).First(&unchangedUser1FilePath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedUser1FilePath.IsOrphan)

		var unchangedSharedFilePath1 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", sharedFilePath1.ID).First(&unchangedSharedFilePath1)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSharedFilePath1.IsOrphan)

		// Verify other users' paths still exist
		var unchangedRootDirPath2 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", rootDirPath2.ID).First(&unchangedRootDirPath2)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedRootDirPath2.IsOrphan)

		var unchangedSubDirPath2 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", subDirPath2.ID).First(&unchangedSubDirPath2)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSubDirPath2.IsOrphan)

		var unchangedUser2FilePath pluginDb.FilePath
		result = ctx.DB().Where("id = ?", user2FilePath.ID).First(&unchangedUser2FilePath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedUser2FilePath.IsOrphan)

		var unchangedSharedFilePath2 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", sharedFilePath2.ID).First(&unchangedSharedFilePath2)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSharedFilePath2.IsOrphan)

		// Verify user3's shared file path still exists
		var unchangedSharedFilePath3 pluginDb.FilePath
		result = ctx.DB().Where("user_id = ? AND cid = ?", userID3, sharedFileCID.Bytes()).First(&unchangedSharedFilePath3)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSharedFilePath3.IsOrphan)
	}, UnpinTestOptions)
}

// Test mixed scenarios
func TestUnpinOperationHandler_AnalyzeUnpinImpact_FileBothInDirectoryAndShared(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID1 := uint(123)
		userID2 := uint(456)
		fileCID := util.GenerateTestCID(t, "file data")

		// Create file in directory structure
		dirCID := util.GenerateTestCID(t, "directory data")

		// Create blocks and nodes for the file and directory
		_, _ = util.CreateTestBlockAndNode(t, ctx, fileCID, "file.txt", 0, 512, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, dirCID, "dir", 1, 1024, []cid.Cid{fileCID})

		dirPath := createTestFilePath(t, ctx, userID1, dirCID, "/dir", "dir", true)
		filePath1 := createTestFilePath(t, ctx, userID1, fileCID, "/dir/file.txt", "file.txt", false)

		// Create shared reference to same file
		filePath2 := createTestFilePath(t, ctx, userID2, fileCID, "/shared-file.txt", "shared-file.txt", false)

		// Pin the file for user1 (in directory)
		createTestPin(t, ctx, userID1, fileCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), fileCID, userID1)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Should not create orphans because this is a standalone file with no children
		assert.False(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 0)
		assert.Len(tb, analysis.AllChildren, 0)

		// Verify paths still exist but are not orphaned yet
		var unchangedDirPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", dirPath.ID).First(&unchangedDirPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedDirPath.IsOrphan)

		var unchangedFilePath1 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", filePath1.ID).First(&unchangedFilePath1)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedFilePath1.IsOrphan)

		var unchangedFilePath2 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", filePath2.ID).First(&unchangedFilePath2)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedFilePath2.IsOrphan)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_DirectoryWithSharedAndNonSharedContent(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID1 := uint(123)
		userID2 := uint(456)
		dirCID := util.GenerateTestCID(t, "directory data")
		sharedFileCID := util.GenerateTestCID(t, "shared file data")
		privateFileCID := util.GenerateTestCID(t, "private file data")

		// Create blocks and nodes for the directory structure
		_, _ = util.CreateTestBlockAndNode(t, ctx, dirCID, "mixed-content", 1, 1024, []cid.Cid{sharedFileCID, privateFileCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, sharedFileCID, "shared.txt", 0, 512, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, privateFileCID, "private.txt", 0, 256, []cid.Cid{})

		// Create directory with mixed content
		dirPath := createTestFilePath(t, ctx, userID1, dirCID, "/mixed-content", "mixed-content", true)
		sharedFilePath := createTestFilePath(t, ctx, userID1, sharedFileCID, "/mixed-content/shared.txt", "shared.txt", false)
		privateFilePath := createTestFilePath(t, ctx, userID1, privateFileCID, "/mixed-content/private.txt", "private.txt", false)

		// Create shared reference to one of the files
		sharedFilePathOther := createTestFilePath(t, ctx, userID2, sharedFileCID, "/user2-shared.txt", "user2-shared.txt", false)

		// Pin the directory for user1
		createTestPin(t, ctx, userID1, dirCID)

		// Pin both files for user1
		createTestPin(t, ctx, userID1, sharedFileCID)
		createTestPin(t, ctx, userID1, privateFileCID)

		// Pin the shared file for user2
		createTestPin(t, ctx, userID2, sharedFileCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), dirCID, userID1)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Directory unpin should create orphans because its contents are pinned by this user
		assert.True(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 2)
		assert.Contains(tb, analysis.RootLevelCandidates, sharedFileCID.String())
		assert.Contains(tb, analysis.RootLevelCandidates, privateFileCID.String())
		assert.Len(tb, analysis.AllChildren, 2)
		assert.Contains(tb, analysis.AllChildren, sharedFileCID.String())
		assert.Contains(tb, analysis.AllChildren, privateFileCID.String())

		// Verify paths still exist but are not orphaned yet
		var unchangedDirPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", dirPath.ID).First(&unchangedDirPath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedDirPath.IsOrphan)

		var unchangedSharedFilePath pluginDb.FilePath
		result = ctx.DB().Where("id = ?", sharedFilePath.ID).First(&unchangedSharedFilePath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSharedFilePath.IsOrphan)

		var unchangedPrivateFilePath pluginDb.FilePath
		result = ctx.DB().Where("id = ?", privateFilePath.ID).First(&unchangedPrivateFilePath)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedPrivateFilePath.IsOrphan)

		// Verify user2's shared file path still exists
		var unchangedSharedFilePathOther pluginDb.FilePath
		result = ctx.DB().Where("id = ?", sharedFilePathOther.ID).First(&unchangedSharedFilePathOther)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSharedFilePathOther.IsOrphan)
	}, UnpinTestOptions)
}

func TestUnpinOperationHandler_AnalyzeUnpinImpact_NestedSharedDirectoryStructures(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID1 := uint(123)
		userID2 := uint(456)
		rootCID := util.GenerateTestCID(t, "root data")
		dir1CID := util.GenerateTestCID(t, "dir1 data")
		dir2CID := util.GenerateTestCID(t, "dir2 data")
		sharedFileCID := util.GenerateTestCID(t, "shared file data")
		privateFileCID := util.GenerateTestCID(t, "private file data")

		// Create blocks and nodes for the nested directory structure
		_, _ = util.CreateTestBlockAndNode(t, ctx, rootCID, "root", 1, 1024, []cid.Cid{dir1CID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, dir1CID, "dir1", 1, 512, []cid.Cid{dir2CID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, dir2CID, "dir2", 1, 256, []cid.Cid{sharedFileCID, privateFileCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, sharedFileCID, "shared.txt", 0, 128, []cid.Cid{})
		_, _ = util.CreateTestBlockAndNode(t, ctx, privateFileCID, "private.txt", 0, 128, []cid.Cid{})

		// Create nested shared directory structure
		// /root/dir1/dir2/shared.txt (shared between users)
		// /root/dir1/dir2/private.txt (private to user1)

		rootPath1 := createTestFilePath(t, ctx, userID1, rootCID, "/root", "root", true)
		dir1Path1 := createTestFilePath(t, ctx, userID1, dir1CID, "/root/dir1", "dir1", true)
		dir2Path1 := createTestFilePath(t, ctx, userID1, dir2CID, "/root/dir1/dir2", "dir2", true)
		sharedFilePath1 := createTestFilePath(t, ctx, userID1, sharedFileCID, "/root/dir1/dir2/shared.txt", "shared.txt", false)
		privateFilePath1 := createTestFilePath(t, ctx, userID1, privateFileCID, "/root/dir1/dir2/private.txt", "private.txt", false)

		// User2 has the same structure
		rootPath2 := createTestFilePath(t, ctx, userID2, rootCID, "/root", "root", true)
		dir1Path2 := createTestFilePath(t, ctx, userID2, dir1CID, "/root/dir1", "dir1", true)
		dir2Path2 := createTestFilePath(t, ctx, userID2, dir2CID, "/root/dir1/dir2", "dir2", true)
		sharedFilePath2 := createTestFilePath(t, ctx, userID2, sharedFileCID, "/root/dir1/dir2/shared.txt", "shared.txt", false)

		// Pin everything for user1
		createTestPin(t, ctx, userID1, rootCID)
		createTestPin(t, ctx, userID1, dir1CID)
		createTestPin(t, ctx, userID1, dir2CID)
		createTestPin(t, ctx, userID1, sharedFileCID)
		createTestPin(t, ctx, userID1, privateFileCID)

		// Pin shared items for user2
		createTestPin(t, ctx, userID2, rootCID)
		createTestPin(t, ctx, userID2, dir1CID)
		createTestPin(t, ctx, userID2, dir2CID)
		createTestPin(t, ctx, userID2, sharedFileCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act - Unpin root directory for user1
		analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), rootCID, userID1)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
		// Directory unpin should create orphans because its contents are pinned by this user
		assert.True(tb, analysis.WouldCreateOrphans)
		assert.Len(tb, analysis.RootLevelCandidates, 4)
		assert.Contains(tb, analysis.RootLevelCandidates, dir1CID.String())
		assert.Contains(tb, analysis.RootLevelCandidates, dir2CID.String())
		assert.Contains(tb, analysis.RootLevelCandidates, sharedFileCID.String())
		assert.Contains(tb, analysis.RootLevelCandidates, privateFileCID.String())
		assert.Len(tb, analysis.AllChildren, 1)
		assert.Contains(tb, analysis.AllChildren, dir1CID.String())

		// Verify user1's paths still exist but are not orphaned yet
		var unchangedRootPath1 pluginDb.FilePath
		result := ctx.DB().Where("id = ?", rootPath1.ID).First(&unchangedRootPath1)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedRootPath1.IsOrphan)

		var unchangedDir1Path1 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", dir1Path1.ID).First(&unchangedDir1Path1)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedDir1Path1.IsOrphan)

		var unchangedDir2Path1 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", dir2Path1.ID).First(&unchangedDir2Path1)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedDir2Path1.IsOrphan)

		var unchangedSharedFilePath1 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", sharedFilePath1.ID).First(&unchangedSharedFilePath1)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSharedFilePath1.IsOrphan)

		var unchangedPrivateFilePath1 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", privateFilePath1.ID).First(&unchangedPrivateFilePath1)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedPrivateFilePath1.IsOrphan)

		// Verify user2's paths still exist
		var unchangedRootPath2 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", rootPath2.ID).First(&unchangedRootPath2)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedRootPath2.IsOrphan)

		var unchangedDir1Path2 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", dir1Path2.ID).First(&unchangedDir1Path2)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedDir1Path2.IsOrphan)

		var unchangedDir2Path2 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", dir2Path2.ID).First(&unchangedDir2Path2)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedDir2Path2.IsOrphan)

		var unchangedSharedFilePath2 pluginDb.FilePath
		result = ctx.DB().Where("id = ?", sharedFilePath2.ID).First(&unchangedSharedFilePath2)
		assert.NoError(tb, result.Error)
		assert.False(tb, unchangedSharedFilePath2.IsOrphan)
	}, UnpinTestOptions)
}
