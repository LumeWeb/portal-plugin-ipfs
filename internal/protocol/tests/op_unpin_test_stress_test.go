package tests

import (
	"context"
	"fmt"
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

// Test large DAG structure
func TestUnpinOperationHandler_LargeDAGStructure(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		rootCID := util.GenerateTestCID(t, "root data")

		// Create a large number of child CIDs
		children := make([]cid.Cid, 0)
		for i := 0; i < 1000; i++ { // Large DAG with 1000 children
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

		// Act
		analysis, err := handler.AnalyzeDAGDependencies(context.Background(), ctx.DB(), rootCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
	}, UnpinTestOptions)
}

// Test very large file sizes
func TestUnpinOperationHandler_VeryLargeFileSizes(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		handler := &protocol.UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		}

		userID := uint(123)
		largeCID := util.GenerateTestCID(t, "large data")

		// Create file path with very large size
		filePath := createTestFilePath(t, ctx, userID, largeCID, "/large-file.dat", "large-file.dat", false)

		// Update the file size to a very large value
		err := ctx.DB().Model(&pluginDb.FilePath{}).Where("id = ?", filePath.ID).Update("size", int64(1000000000000)).Error // 1TB
		require.NoError(tb, err)

		createTestPin(t, ctx, userID, largeCID)

		// Act
		analysis, err := handler.AnalyzePathDependencies(context.Background(), ctx.DB(), largeCID, userID)

		// Assert
		assert.NoError(tb, err)
		assert.NotNil(tb, analysis)
	}, UnpinTestOptions)
}
