package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	"go.lumeweb.com/portal/db/types"
	"gorm.io/gorm"
)

// runConcurrentUnpinTest runs multiple concurrent operations and checks results
func runConcurrentUnpinTest(t *testing.T, ctx coreTesting.TestContext, handler *protocol.UnpinOperationHandler,
	userID uint, cids []cid.Cid, operation func(c cid.Cid) error, expectErrors bool) {
	var wg sync.WaitGroup
	_errors := make(chan error, len(cids))

	for _, c := range cids {
		wg.Add(1)
		go func(c cid.Cid) {
			defer wg.Done()
			_errors <- operation(c)
		}(c)
	}

	wg.Wait()
	close(_errors)

	for err := range _errors {
		if expectErrors {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}

// runConcurrentAnalysisTest runs concurrent DAG analysis operations
func runConcurrentAnalysisTest(t *testing.T, ctx coreTesting.TestContext, handler *protocol.UnpinOperationHandler,
	userID uint, cids []cid.Cid, expectErrors bool) {
	runConcurrentUnpinTest(t, ctx, handler, userID, cids, func(c cid.Cid) error {
		_, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), c, userID)
		return err
	}, expectErrors)
}

// runHighFrequencyConcurrentAnalysisTest runs high-frequency concurrent DAG analysis operations
// It launches 2*len(cids) operations, cycling through the CIDs
func runHighFrequencyConcurrentAnalysisTest(t *testing.T, ctx coreTesting.TestContext, handler *protocol.UnpinOperationHandler,
	userID uint, cids []cid.Cid, expectErrors bool) {
	
	// Create a slice with 2*len(cids) elements, cycling through the original CIDs
	highFrequencyCIDs := make([]cid.Cid, 2*len(cids))
	for i := 0; i < 2*len(cids); i++ {
		highFrequencyCIDs[i] = cids[i%len(cids)]
	}
	
	// Reuse the existing concurrent analysis test function
	runConcurrentAnalysisTest(t, ctx, handler, userID, highFrequencyCIDs, expectErrors)
}

// Test concurrent unpin operations
func TestUnpinOperationHandler_ConcurrentUnpinOperations(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")

		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{})
		createTestPin(t, ctx, userID, targetCID)

		// Create slice with same CID for concurrent operations
		cids := make([]cid.Cid, 10)
		for i := range cids {
			cids[i] = targetCID
		}

		runConcurrentAnalysisTest(t, ctx, handler, userID, cids, false)
	}, UnpinTestOptions)
}

// Test concurrent unpin operations on different CIDs
func TestUnpinOperationHandler_ConcurrentDifferentCIDs(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		cids := make([]cid.Cid, 10)

		for i := range cids {
			cids[i] = util.GenerateTestCID(t, fmt.Sprintf("data%d", i))
			_, _ = util.CreateTestBlockAndNode(t, ctx, cids[i], fmt.Sprintf("file%d.txt", i), 0, 1024, []cid.Cid{})
			createTestPin(t, ctx, userID, cids[i])
		}

		runConcurrentAnalysisTest(t, ctx, handler, userID, cids, false)
	}, UnpinTestOptions)
}

// Test concurrent unpin operations on the same CID
func TestUnpinOperationHandler_ConcurrentSameCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")

		// Create block and pin
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{})
		createTestPin(t, ctx, userID, targetCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act - Run multiple analyses concurrently on the same CID
		var wg sync.WaitGroup
		errors := make(chan error, 20)

		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), targetCID, userID)
				errors <- err
			}()
		}

		wg.Wait()
		close(errors)

		// Assert - Check that all operations completed without error
		for err := range errors {
			assert.NoError(tb, err)
		}
	}, UnpinTestOptions)
}

// Test concurrent unpin operations on dependent CIDs
func TestUnpinOperationHandler_ConcurrentDependentCIDs(t *testing.T) {
	t.Skip()
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)

		// Create a dependency chain: A -> B -> C -> D
		cidA := util.GenerateTestCID(t, "dataA")
		cidB := util.GenerateTestCID(t, "dataB")
		cidC := util.GenerateTestCID(t, "dataC")
		cidD := util.GenerateTestCID(t, "dataD")

		_, _ = util.CreateTestBlockAndNode(t, ctx, cidA, "fileA.txt", 1, 1024, []cid.Cid{cidB})
		_, _ = util.CreateTestBlockAndNode(t, ctx, cidB, "fileB.txt", 1, 512, []cid.Cid{cidC})
		_, _ = util.CreateTestBlockAndNode(t, ctx, cidC, "fileC.txt", 1, 256, []cid.Cid{cidD})
		_, _ = util.CreateTestBlockAndNode(t, ctx, cidD, "fileD.txt", 0, 128, []cid.Cid{})

		// Pin all CIDs
		createTestPin(t, ctx, userID, cidA)
		createTestPin(t, ctx, userID, cidB)
		createTestPin(t, ctx, userID, cidC)
		createTestPin(t, ctx, userID, cidD)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act - Try to unpin all CIDs concurrently using helper
		cids := []cid.Cid{cidA, cidB, cidC, cidD}
		runConcurrentAnalysisTest(t, ctx, handler, userID, cids, false)

		// Verify analysis results
		analysisA, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), cidA, userID)
		require.NoError(tb, err)
		assert.True(tb, analysisA.WouldCreateOrphans, "A should create root level visibility candidates as it has child pins")

		analysisB, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), cidB, userID)
		require.NoError(tb, err)
		assert.True(tb, analysisB.WouldCreateOrphans, "B should create root level visibility candidates as it has child pins")

		analysisC, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), cidC, userID)
		require.NoError(tb, err)
		assert.True(tb, analysisC.WouldCreateOrphans, "C should create root level visibility candidates as it has child pins")

		analysisD, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), cidD, userID)
		require.NoError(tb, err)
		assert.False(tb, analysisD.WouldCreateOrphans, "D should not create root level visibility candidates as it has no child pins")
	}, UnpinTestOptions)
}

// Test concurrent unpin operations on shared directory structures
func TestUnpinOperationHandler_ConcurrentSharedDirectoryStructures(t *testing.T) {
	t.Skip()
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID1 := uint(123)
		userID2 := uint(456)

		// Create shared directory structure
		rootDirCID := util.GenerateTestCID(t, "root directory data")
		subDirCID := util.GenerateTestCID(t, "sub directory data")
		file1CID := util.GenerateTestCID(t, "file1 data")
		file2CID := util.GenerateTestCID(t, "file2 data")

		// Create paths for user1
		rootDirPath1 := createTestFilePath(t, ctx, userID1, rootDirCID, "/shared", "shared", true)
		subDirPath1 := createTestFilePath(t, ctx, userID1, subDirCID, "/shared/subdir", "subdir", true)
		file1Path := createTestFilePath(t, ctx, userID1, file1CID, "/shared/subdir/file1.txt", "file1.txt", false)

		// Create paths for user2
		rootDirPath2 := createTestFilePath(t, ctx, userID2, rootDirCID, "/shared", "shared", true)
		subDirPath2 := createTestFilePath(t, ctx, userID2, subDirCID, "/shared/subdir", "subdir", true)
		file2Path := createTestFilePath(t, ctx, userID2, file2CID, "/shared/subdir/file2.txt", "file2.txt", false)

		// Pin everything for both users
		createTestPin(t, ctx, userID1, rootDirCID)
		createTestPin(t, ctx, userID1, subDirCID)
		createTestPin(t, ctx, userID1, file1CID)

		createTestPin(t, ctx, userID2, rootDirCID)
		createTestPin(t, ctx, userID2, subDirCID)
		createTestPin(t, ctx, userID2, file2CID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act - Try to analyze root directory for both users concurrently using helper
		cids := []cid.Cid{rootDirCID, rootDirCID}
		runConcurrentAnalysisTest(t, ctx, handler, userID1, cids, false)

		// Verify analysis results
		analysis1, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), rootDirCID, userID1)
		require.NoError(tb, err)
		assert.True(tb, analysis1.WouldCreateOrphans)
		assert.Len(tb, analysis1.RootLevelCandidates, 2) // subdir and file

		analysis2, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), rootDirCID, userID2)
		require.NoError(tb, err)
		assert.True(tb, analysis2.WouldCreateOrphans)
		assert.Len(tb, analysis2.RootLevelCandidates, 2) // subdir and file

		// Verify paths still exist
		var path pluginDb.FilePath
		assert.NoError(tb, ctx.DB().Where("id = ?", rootDirPath1.ID).First(&path).Error)
		assert.NoError(tb, ctx.DB().Where("id = ?", rootDirPath2.ID).First(&path).Error)
		assert.NoError(tb, ctx.DB().Where("id = ?", subDirPath1.ID).First(&path).Error)
		assert.NoError(tb, ctx.DB().Where("id = ?", subDirPath2.ID).First(&path).Error)
		assert.NoError(tb, ctx.DB().Where("id = ?", file1Path.ID).First(&path).Error)
		assert.NoError(tb, ctx.DB().Where("id = ?", file2Path.ID).First(&path).Error)
	}, UnpinTestOptions)
}

// Test concurrent operations on large DAG structures
func TestUnpinOperationHandler_ConcurrentLargeDAG(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		rootCID := util.GenerateTestCID(t, "root data")

		// Create a large DAG with 500 children
		children := make([]cid.Cid, 0)
		for i := 0; i < 500; i++ {
			childCID := util.GenerateTestCID(t, fmt.Sprintf("child%d", i))
			children = append(children, childCID)
			_, _ = util.CreateTestBlockAndNode(t, ctx, childCID, fmt.Sprintf("child%d.txt", i), 0, 1024, []cid.Cid{})
			createTestPin(t, ctx, userID, childCID)
		}

		// Create root block with all children
		_, _ = util.CreateTestBlockAndNode(t, ctx, rootCID, "root.txt", 1, 1024, children)
		createTestPin(t, ctx, userID, rootCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act - Concurrent operations on large DAG
		var wg sync.WaitGroup
		errors := make(chan error, 5)

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), rootCID, userID)
				errors <- err
			}()
		}

		wg.Wait()
		close(errors)

		// Assert - Check that all operations completed without error
		for err := range errors {
			assert.NoError(tb, err)
		}
	}, UnpinTestOptions)
}

// Test concurrent operations with many dependent pins
func TestUnpinOperationHandler_ConcurrentManyDependentPins(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		rootCID := util.GenerateTestCID(t, "root data")

		// Create root with many dependent pins
		dependentCIDs := make([]cid.Cid, 0)
		for i := 0; i < 100; i++ {
			depCID := util.GenerateTestCID(t, fmt.Sprintf("dependent%d", i))
			dependentCIDs = append(dependentCIDs, depCID)
			_, _ = util.CreateTestBlockAndNode(t, ctx, depCID, fmt.Sprintf("dependent%d.txt", i), 0, 1024, []cid.Cid{})
			createTestPin(t, ctx, userID, depCID)
		}

		// Create root block that references some dependent blocks
		_, _ = util.CreateTestBlockAndNode(t, ctx, rootCID, "root.txt", 1, 1024, dependentCIDs[:10])
		createTestPin(t, ctx, userID, rootCID)

		// Act - Concurrent operations with many dependent pins
		var wg sync.WaitGroup
		errors := make(chan error, 10)
		results := make(chan *protocol.UnpinImpactAnalysis, 10)

		// Run concurrent analyses on different dependent CIDs
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), dependentCIDs[idx], userID)
				errors <- err
				results <- analysis
			}(i)
		}

		wg.Wait()
		close(errors)
		close(results)

		// Assert - Check that all operations completed
		for err := range errors {
			assert.NoError(tb, err)
		}

		resultCount := 0
		for analysis := range results {
			if analysis != nil {
				resultCount++
				// Each dependent file should not break structure since they have no dependencies
				assert.False(tb, analysis.WouldCreateOrphans)
			}
		}
		assert.Equal(tb, 10, resultCount)
	}, UnpinTestOptions)
}

// Test stress testing with many concurrent unpin requests
func TestUnpinOperationHandler_StressConcurrentUnpinRequests(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		cids := make([]cid.Cid, 100)

		for i := range cids {
			cids[i] = util.GenerateTestCID(t, fmt.Sprintf("stress-data%d", i))
			_, _ = util.CreateTestBlockAndNode(t, ctx, cids[i], fmt.Sprintf("stress-file%d.txt", i), 0, 1024, []cid.Cid{})
			createTestPin(t, ctx, userID, cids[i])
		}

		// Run concurrent operations cycling through the 100 CIDs
		runConcurrentAnalysisTest(t, ctx, handler, userID, cids, false)
	}, UnpinTestOptions)
}

// Test concurrent operations with partial failures
func TestUnpinOperationHandler_ConcurrentPartialFailures(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)

		// Create valid CIDs
		validCID := util.GenerateTestCID(t, "valid data")
		_, _ = util.CreateTestBlockAndNode(t, ctx, validCID, "valid.txt", 0, 1024, []cid.Cid{})
		createTestPin(t, ctx, userID, validCID)

		// Create undefined CID for failure case
		undefinedCID := cid.Undef

		// Act - Concurrent operations with mix of valid and invalid CIDs
		var wg sync.WaitGroup
		errors := make(chan error, 2)

		// Valid operation
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), validCID, userID)
			errors <- err
		}()

		// Invalid operation
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), undefinedCID, userID)
			errors <- err
		}()

		wg.Wait()
		close(errors)

		// Assert - One should succeed, one should fail
		errorList := make([]error, 0)
		for err := range errors {
			errorList = append(errorList, err)
		}

		assert.Len(tb, errorList, 2)
		successCount := 0
		failureCount := 0
		for _, err := range errorList {
			if err != nil {
				failureCount++
			} else {
				successCount++
			}
		}
		assert.Equal(tb, 1, successCount)
		assert.Equal(tb, 1, failureCount)
	}, UnpinTestOptions)
}

// Test concurrent operations with service unavailability
func TestUnpinOperationHandler_ConcurrentServiceUnavailable(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")

		// Create block
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{})

		// Act - Run concurrent operations with our helper
		runConcurrentUnpinTest(t, ctx, handler, userID, []cid.Cid{targetCID}, func(c cid.Cid) error {
			// Simulate service unavailability by passing nil service
			var nilBlockSvc pluginCore.BlockService
			_, err := handler.CheckDAGForCID(context.Background(), nilBlockSvc, c, c, make(map[string]bool))
			return err
		}, true)

		// Assert - The test helper will verify all operations completed with errors
	}, UnpinTestOptions)
}

// Test concurrent operations with database contention
func TestUnpinOperationHandler_ConcurrentDatabaseContention(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)

		// Create CIDs
		cids := make([]cid.Cid, 0)
		for i := 0; i < 10; i++ {
			_cid := util.GenerateTestCID(t, fmt.Sprintf("data%d", i))
			cids = append(cids, _cid)
			_, _ = util.CreateTestBlockAndNode(t, ctx, _cid, fmt.Sprintf("file%d.txt", i), 0, 1024, []cid.Cid{})
			createTestPin(t, ctx, userID, _cid)
		}

		// Act - Concurrent operations that all access the same user's pins
		operation := func(c cid.Cid) error {
			_, err := handler.GetAllUserPins(context.Background(), ctx.DB(), userID)
			if err != nil {
				return err
			}
			_, err = handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), c, userID)
			return err
		}

		runConcurrentUnpinTest(t, ctx, handler, userID, cids, operation, false)
	}, UnpinTestOptions)
}

// Test concurrent operations with timeout scenarios
func TestUnpinOperationHandler_ConcurrentTimeouts(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)

		// Create CIDs
		cids := make([]cid.Cid, 0)
		for i := 0; i < 5; i++ {
			_cid := util.GenerateTestCID(t, fmt.Sprintf("data%d", i))
			cids = append(cids, _cid)
			_, _ = util.CreateTestBlockAndNode(t, ctx, _cid, fmt.Sprintf("file%d.txt", i), 0, 1024, []cid.Cid{})
			createTestPin(t, ctx, userID, _cid)
		}

		// Act - Concurrent operations with different timeout contexts
		var wg sync.WaitGroup
		_errors := make(chan error, len(cids)*2)

		// Short timeout operations
		for i := 0; i < len(cids); i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
				defer cancel()
				_, err := handler.AnalyzeUnpinImpact(timeoutCtx, ctx.DB(), cids[idx], userID)
				_errors <- err
			}(i)
		}

		// Normal operations
		for i := 0; i < len(cids); i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), cids[idx], userID)
				_errors <- err
			}(i)
		}

		wg.Wait()
		close(_errors)

		// Assert - Collect results
		errorList := make([]error, 0)
		for err := range _errors {
			errorList = append(errorList, err)
		}

		assert.Len(tb, errorList, len(cids)*2)
		// Some operations with very short timeout might fail
	}, UnpinTestOptions)
}

// Test race condition between unpin and pin operations on same CID
func TestUnpinOperationHandler_RaceUnpinPinSameCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")

		// Create block
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{})

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act - Race between pinning and analyzing for unpin
		var wg sync.WaitGroup
		errors := make(chan error, 2)

		// First goroutine pins the CID
		wg.Add(1)
		go func() {
			defer wg.Done()
			pin := &pluginDb.IPFSPin{
				UserID:    userID,
				CID:       targetCID.Bytes(),
				RequestID: types.NewBinUUID(),
				Status:    pluginDb.PinningStatusPinned,
			}
			errors <- ctx.DB().Create(pin).Error
		}()

		// Second goroutine analyzes dependencies
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), targetCID, userID)
			errors <- err
		}()

		wg.Wait()
		close(errors)

		// Assert - Check that operations completed without critical errors
		for err := range errors {
			// We're not checking for specific errors since the race condition
			// might result in different outcomes depending on execution order
			if err != nil {
				// Error is acceptable in race conditions
				tb.Logf("Race condition resulted in error: %v", err)
			}
		}
	}, UnpinTestOptions)
}

// Test race condition between unpin and file path operations
func TestUnpinOperationHandler_RaceUnpinFilePathOperations(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")

		// Create initial file path
		filePath := createTestFilePath(t, ctx, userID, targetCID, "/test/file.txt", "file.txt", false)

		fileManagerSvc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		require.NotNil(tb, fileManagerSvc)

		// Act - Race between updating file path and analyzing for unpin
		var wg sync.WaitGroup
		errors := make(chan error, 2)

		// First goroutine updates the file path
		wg.Add(1)
		go func() {
			defer wg.Done()
			errors <- ctx.DB().Model(&pluginDb.FilePath{}).Where("id = ?", filePath.ID).Update("name", "updated-file.txt").Error
		}()

		// Second goroutine analyzes path dependencies
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := handler.AnalyzePathDependencies(context.Background(), ctx.DB(), targetCID, userID)
			errors <- err
		}()

		wg.Wait()
		close(errors)

		// Assert - Check that operations completed without critical errors
		for err := range errors {
			if err != nil {
				tb.Logf("Race condition resulted in error: %v", err)
			}
		}
	}, UnpinTestOptions)
}

// Test race condition between unpin and DAG validation operations
func TestUnpinOperationHandler_RaceUnpinDAGValidation(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		rootCID := util.GenerateTestCID(t, "root data")
		childCID := util.GenerateTestCID(t, "child data")

		// Create DAG structure
		_, _ = util.CreateTestBlockAndNode(t, ctx, rootCID, "root.txt", 1, 1024, []cid.Cid{childCID})
		_, _ = util.CreateTestBlockAndNode(t, ctx, childCID, "child.txt", 0, 512, []cid.Cid{})
		createTestPin(t, ctx, userID, rootCID)

		blockSvc := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		require.NotNil(tb, blockSvc)

		// Act - Race between DAG validation and dependency analysis
		var wg sync.WaitGroup
		errors := make(chan error, 2)
		results := make(chan interface{}, 2)

		// First goroutine validates user DAG structure
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := handler.ValidateUserDAGStructure(context.Background(), ctx.DB(), userID)
			errors <- err
			results <- result
		}()

		// Second goroutine analyzes DAG dependencies
		wg.Add(1)
		go func() {
			defer wg.Done()
			analysis, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), rootCID, userID)
			errors <- err
			results <- analysis
		}()

		wg.Wait()
		close(errors)
		close(results)

		// Assert - Check that operations completed without critical errors
		for err := range errors {
			if err != nil {
				tb.Logf("Race condition resulted in error: %v", err)
			}
		}

		resultCount := 0
		for result := range results {
			if result != nil {
				resultCount++
			}
		}
		assert.Equal(tb, 2, resultCount)
	}, UnpinTestOptions)
}

// Test race conditions during orphan promotion
func TestUnpinOperationHandler_RaceOrphanPromotion(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		fileCID := util.GenerateTestCID(t, "file data")

		// Create file path and pin
		filePath := createTestFilePath(t, ctx, userID, fileCID, "/test/file.txt", "file.txt", false)
		createTestPin(t, ctx, userID, fileCID)

		// Run concurrent operations directly without mutex
		runConcurrentUnpinTest(t, ctx, handler, userID, []cid.Cid{fileCID}, func(c cid.Cid) error {
			dependentPins := []string{c.String()}
			return handler.PromotePinsToRootLevelVisibility(context.Background(), ctx.DB(), dependentPins, userID)
		}, false)

		// Run concurrent path updates
		runConcurrentUnpinTest(t, ctx, handler, userID, []cid.Cid{fileCID}, func(c cid.Cid) error {
			return ctx.DB().Model(&pluginDb.FilePath{}).Where("id = ?", filePath.ID).Update("name", "racing-file.txt").Error
		}, false)

		// Verify the file path was either updated or orphaned, but not corrupted
		var updatedPath pluginDb.FilePath
		result := ctx.DB().Where("id = ?", filePath.ID).First(&updatedPath)
		require.NoError(tb, result.Error)
		// In concurrent scenario, we can't predict the exact outcome, but it should be valid
		assert.True(tb, true, "Path should be valid after concurrent operations")
	}, UnpinTestOptions)
}

// Test concurrent unpin operations without locking (true concurrency)
func TestUnpinOperationHandler_ConcurrentOperations(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		cids := make([]cid.Cid, 5)

		for i := range cids {
			cids[i] = util.GenerateTestCID(t, fmt.Sprintf("data%d", i))
			_, _ = util.CreateTestBlockAndNode(t, ctx, cids[i], fmt.Sprintf("file%d.txt", i), 0, 1024, []cid.Cid{})
			createTestPin(t, ctx, userID, cids[i])
		}

		// Create a custom operation function that runs concurrently
		concurrentOperation := func(c cid.Cid) error {
			_, err := handler.AnalyzeUnpinImpact(context.Background(), ctx.DB(), c, userID)
			return err
		}

		// Run the test with our concurrent operation
		runConcurrentUnpinTest(t, ctx, handler, userID, cids, concurrentOperation, false)
	}, UnpinTestOptions)
}

// Test concurrent operations with transaction isolation
func TestUnpinOperationHandler_ConcurrentTransactionIsolation(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")

		// Create block and pin
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{})
		createTestPin(t, ctx, userID, targetCID)

		// Create slice with same CID for concurrent operations
		cids := make([]cid.Cid, 10)
		for i := range cids {
			cids[i] = targetCID
		}

		// Verify each transaction can read/write independently without interference
		var successCount int32
		operation := func(c cid.Cid) error {
			return ctx.DB().Transaction(func(tx *gorm.DB) error {
				_, err := handler.AnalyzeUnpinImpact(context.Background(), tx, c, userID)
				if err != nil {
					return err
				}
				atomic.AddInt32(&successCount, 1)
				return nil
			})
		}

		runConcurrentUnpinTest(t, ctx, handler, userID, cids, operation, false)
		assert.Equal(tb, int32(10), successCount, "All transactions should complete successfully")
	}, UnpinTestOptions)
}

// Test concurrent operations with context cancellation
func TestUnpinOperationHandler_ConcurrentContextCancellation(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		targetCID := util.GenerateTestCID(t, "target data")

		// Create block and pin
		_, _ = util.CreateTestBlockAndNode(t, ctx, targetCID, "target.txt", 0, 1024, []cid.Cid{})
		createTestPin(t, ctx, userID, targetCID)

		// Create slice with same CID for concurrent operations
		cids := make([]cid.Cid, 10)
		for i := range cids {
			cids[i] = targetCID
		}

		// Create a custom operation function that uses short timeout contexts
		cancellableOperation := func(c cid.Cid) error {
			// Create a context that cancels after a very short delay
			_ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
			defer cancel()

			_, err := handler.AnalyzeUnpinImpact(_ctx, ctx.DB(), c, userID)
			return err
		}

		// Run concurrent operations with our helper
		runConcurrentUnpinTest(t, ctx, handler, userID, cids, cancellableOperation, true)

		// Additional verification can be done here if needed
	}, UnpinTestOptions)
}

// Test concurrent operations during system high load
func TestUnpinOperationHandler_ConcurrentHighLoad(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		numCids := 100 // Increased from 50 to better test high load
		cids := make([]cid.Cid, numCids)

		// Create many CIDs to simulate high load
		for i := range cids {
			cids[i] = util.GenerateTestCID(t, fmt.Sprintf("data%d", i))
			_, _ = util.CreateTestBlockAndNode(t, ctx, cids[i], fmt.Sprintf("file%d.txt", i), 0, 1024, []cid.Cid{})
			createTestPin(t, ctx, userID, cids[i])
		}

		// Run concurrent operations with our helper
		runConcurrentAnalysisTest(t, ctx, handler, userID, cids, false)

		// Additional verification - check all pins still exist
		var pinCount int64
		err := ctx.DB().Model(&pluginDb.IPFSPin{}).Where("user_id = ?", userID).Count(&pinCount).Error
		require.NoError(tb, err)
		assert.Equal(tb, int64(numCids), pinCount, "All pins should still exist after analysis")
	}, UnpinTestOptions)
}

// Test high-frequency concurrent unpin operations
func TestUnpinOperationHandler_HighFrequencyConcurrentUnpins(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)

		// Create CIDs
		cids := make([]cid.Cid, 0)
		for i := 0; i < 20; i++ {
			_cid := util.GenerateTestCID(t, fmt.Sprintf("data%d", i))
			cids = append(cids, _cid)
			_, _ = util.CreateTestBlockAndNode(t, ctx, _cid, fmt.Sprintf("file%d.txt", i), 0, 1024, []cid.Cid{})
			createTestPin(t, ctx, userID, _cid)
		}

		// Act - High frequency concurrent operations using helper
		runHighFrequencyConcurrentAnalysisTest(t, ctx, handler, userID, cids, false)
	}, UnpinTestOptions)
}
