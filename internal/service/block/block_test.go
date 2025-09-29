package block

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
	"testing"
)



var TestOptions = coreTesting.CombineOptions(
	coreTesting.WithServiceFactory(pluginCore.BLOCK_SERVICE, NewBlockService),
	coreTesting.WithSQLitePluginMigrations(
		internal.ProtocolName, migrations.GetSQLite(),
	),
)



func TestBlockService_GetBlockMeta(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		blockService := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		name := "test_file.txt"
		nodeType := uint8(1)
		blockSize := int64(1024)
		childCIDs := []cid.Cid{util.GenerateTestCID(t, "child1"), util.GenerateTestCID(t, "child2")}

		_, expectedNode := util.CreateTestBlockAndNode(t, ctx, testCID, name, nodeType, blockSize, childCIDs)

		// Act
		meta, err := blockService.GetBlockMeta(context.Background(), testCID)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, meta)
		assert.Equal(tb, expectedNode.Name, meta.Name)
		assert.Equal(tb, expectedNode.Type, meta.Type)
		assert.Equal(tb, expectedNode.BlockSize, meta.BlockSize)

		// Convert childCIDs to string representation for comparison
		expectedChildCIDStrings := make([]string, len(childCIDs))
		for i, c := range childCIDs {
			expectedChildCIDStrings[i] = c.String()
		}
		assert.ElementsMatch(tb, expectedChildCIDStrings, meta.ChildCID)
	}, TestOptions)
}

func TestBlockService_GetBlockMeta_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		blockService := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
		nonExistentCID := util.GenerateTestCID(t, "non existent")

		// Act
		meta, err := blockService.GetBlockMeta(context.Background(), nonExistentCID)

		// Assert
		require.Error(tb, err)
		assert.Nil(tb, meta)
		assert.Equal(tb, gorm.ErrRecordNotFound, err)
	}, TestOptions)
}

func TestBlockService_GetBlockMetaBatch(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		blockService := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)

		testCID1 := util.GenerateTestCID(t, "test data 1")
		name1 := "test_file1.txt"
		nodeType1 := uint8(1)
		blockSize1 := int64(1024)
		childCIDs1 := []cid.Cid{util.GenerateTestCID(t, "child1"), util.GenerateTestCID(t, "child2")}
		_, expectedNode1 := util.CreateTestBlockAndNode(t, ctx, testCID1, name1, nodeType1, blockSize1, childCIDs1)

		testCID2 := util.GenerateTestCID(t, "test data 2")
		name2 := "test_file2.txt"
		nodeType2 := uint8(2)
		blockSize2 := int64(2048)
		childCIDs2 := []cid.Cid{util.GenerateTestCID(t, "child3"), util.GenerateTestCID(t, "child4")}
		_, expectedNode2 := util.CreateTestBlockAndNode(t, ctx, testCID2, name2, nodeType2, blockSize2, childCIDs2)

		cids := []cid.Cid{testCID1, testCID2}

		// Act
		metas, err := blockService.GetBlockMetaBatch(context.Background(), cids)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, metas)
		assert.Len(tb, metas, 2)

		// Assert meta for testCID1
		meta1 := metas[testCID1.String()]
		assert.NotNil(tb, meta1)
		assert.Equal(tb, expectedNode1.Name, meta1.Name)
		assert.Equal(tb, expectedNode1.Type, meta1.Type)
		assert.Equal(tb, expectedNode1.BlockSize, meta1.BlockSize)
		expectedChildCIDStrings1 := make([]string, len(childCIDs1))
		for i, c := range childCIDs1 {
			expectedChildCIDStrings1[i] = c.String()
		}
		assert.ElementsMatch(tb, expectedChildCIDStrings1, meta1.ChildCID)

		// Assert meta for testCID2
		meta2 := metas[testCID2.String()]
		assert.NotNil(tb, meta2)
		assert.Equal(tb, expectedNode2.Name, meta2.Name)
		assert.Equal(tb, expectedNode2.Type, meta2.Type)
		assert.Equal(tb, expectedNode2.BlockSize, meta2.BlockSize)
		expectedChildCIDStrings2 := make([]string, len(childCIDs2))
		for i, c := range childCIDs2 {
			expectedChildCIDStrings2[i] = c.String()
		}
		assert.ElementsMatch(tb, expectedChildCIDStrings2, meta2.ChildCID)
	}, TestOptions)
}


func TestBlockService_GetBlockMetaBatch_Mixed(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		blockService := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)

		// Create two test CIDs with nodes
		testCID1 := util.GenerateTestCID(t, "test data 1")
		name1 := "test_file1.txt"
		nodeType1 := uint8(1)
		blockSize1 := int64(1024)
		childCIDs1 := []cid.Cid{util.GenerateTestCID(t, "child1"), util.GenerateTestCID(t, "child2")}
		_, expectedNode1 := util.CreateTestBlockAndNode(t, ctx, testCID1, name1, nodeType1, blockSize1, childCIDs1)

		testCID2 := util.GenerateTestCID(t, "test data 2")
		name2 := "test_file2.txt"
		nodeType2 := uint8(2)
		blockSize2 := int64(2048)
		childCIDs2 := []cid.Cid{util.GenerateTestCID(t, "child3"), util.GenerateTestCID(t, "child4")}
		_, expectedNode2 := util.CreateTestBlockAndNode(t, ctx, testCID2, name2, nodeType2, blockSize2, childCIDs2)

		nonExistentCID := util.GenerateTestCID(t, "non existent")

		cids := []cid.Cid{testCID1, testCID2, nonExistentCID}

		// Act
		metas, err := blockService.GetBlockMetaBatch(context.Background(), cids)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, metas)
		assert.Len(tb, metas, 2) // Should return 2 existing CIDs

		// Assert meta for testCID1
		meta1 := metas[testCID1.String()]
		assert.NotNil(tb, meta1)
		assert.Equal(tb, expectedNode1.Name, meta1.Name)
		assert.Equal(tb, expectedNode1.Type, meta1.Type)
		assert.Equal(tb, expectedNode1.BlockSize, meta1.BlockSize)
		expectedChildCIDStrings1 := make([]string, len(childCIDs1))
		for i, c := range childCIDs1 {
			expectedChildCIDStrings1[i] = c.String()
		}
		assert.ElementsMatch(tb, expectedChildCIDStrings1, meta1.ChildCID)

		// Assert meta for testCID2
		meta2 := metas[testCID2.String()]
		assert.NotNil(tb, meta2)
		assert.Equal(tb, expectedNode2.Name, meta2.Name)
		assert.Equal(tb, expectedNode2.Type, meta2.Type)
		assert.Equal(tb, expectedNode2.BlockSize, meta2.BlockSize)
		expectedChildCIDStrings2 := make([]string, len(childCIDs2))
		for i, c := range childCIDs2 {
			expectedChildCIDStrings2[i] = c.String()
		}
		assert.ElementsMatch(tb, expectedChildCIDStrings2, meta2.ChildCID)

		// Assert that nonExistentCID is not in the result
		_, ok := metas[nonExistentCID.String()]
		assert.False(tb, ok, "Non-existent CID should not be in the result")
	}, TestOptions)
}

func TestBlockService_EmptyBatch(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		blockService := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)

		var cids []cid.Cid

		// Act
		metas, err := blockService.GetBlockMetaBatch(context.Background(), cids)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, metas)
		assert.Len(tb, metas, 0)
	}, TestOptions)
}

func TestBlockService_MultipleNotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		blockService := core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)

		nonExistentCID1 := util.GenerateTestCID(t, "non existent 1")
		nonExistentCID2 := util.GenerateTestCID(t, "non existent 2")

		cids := []cid.Cid{nonExistentCID1, nonExistentCID2}

		// Act
		metas, err := blockService.GetBlockMetaBatch(context.Background(), cids)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, metas)
		assert.Len(tb, metas, 0)

		// Assert that nonExistentCIDs are not in the result
		_, ok1 := metas[nonExistentCID1.String()]
		assert.False(tb, ok1, "Non-existent CID1 should not be in the result")
		_, ok2 := metas[nonExistentCID2.String()]
		assert.False(tb, ok2, "Non-existent CID2 should not be in the result")
	}, TestOptions)
}
