package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	format "github.com/ipfs/go-ipld-format"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal/core"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

func createPinnedBlock(tb testing.TB, ctx coreTesting.TestContext, data string) pluginCore.PinnedBlock {
	testCid := generateCid(tb.(*testing.T), data)
	block, err := blocks.NewBlockWithCid([]byte(data), testCid)
	require.NoError(tb, err)

	node, err := encoding.DecodeBlock(ctx, block)
	require.NoError(tb, err)

	return pluginCore.PinnedBlock{
		Cid:  testCid,
		Size: uint64(len(data)),
		Node: node,
	}
}

func TestMetadataStore_PinUnpinBlockExists(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		testData := "test data"
		pinnedBlock := createPinnedBlock(tb, ctx, testData)

		// Act & Assert - Pin
		err := metadataStore.Pin(context.Background(), pinnedBlock)
		require.NoError(tb, err)

		// Act & Assert - BlockExists
		err = metadataStore.BlockExists(context.Background(), pinnedBlock.Cid)
		require.NoError(tb, err)

		// Act & Assert - Unpin
		err = metadataStore.Unpin(context.Background(), pinnedBlock.Cid)
		require.NoError(tb, err)

		// Act & Assert - BlockExists (should return error after unpin)
		err = metadataStore.BlockExists(context.Background(), pinnedBlock.Cid)
		assert.Error(tb, err)
		assert.True(tb, errors.Is(err, format.ErrNotFound{}))
	}, ipfsTestConfig)
}

func TestMetadataStore_BlockChildren(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))

		// Generate test data and CIDs first
		parentData := "parent data"
		child1Data := "child1 data"
		child2Data := "child2 data"

		// Generate CIDs up front and store them
		expectedChild1CID := generateCid(t, child1Data)
		expectedChild2CID := generateCid(t, child2Data)
		expectedChildren := []cid.Cid{expectedChild1CID, expectedChild2CID}

		// Create parent block with links to children
		parentBlock := createPinnedBlock(tb, ctx, parentData)
		parentBlock.Links = expectedChildren

		// Pin all blocks
		require.NoError(tb, metadataStore.Pin(context.Background(), parentBlock))
		require.NoError(tb, metadataStore.Pin(context.Background(), createPinnedBlock(tb, ctx, child1Data)))
		require.NoError(tb, metadataStore.Pin(context.Background(), createPinnedBlock(tb, ctx, child2Data)))

		// Debug: Verify what's in the database
		var blocks []pluginDb.IPFSBlock
		require.NoError(tb, ctx.DB().Find(&blocks).Error)
		// Act
		actualChildren, err := metadataStore.BlockChildren(context.Background(), parentBlock.Cid, nil)
		require.NoError(tb, err)
		// Assert
		require.Len(tb, actualChildren, len(expectedChildren), "wrong number of children returned")

		// Convert to string sets for comparison
		expected := make(map[string]struct{})
		for _, c := range expectedChildren {
			expected[c.String()] = struct{}{}
		}

		actual := make(map[string]struct{})
		for _, c := range actualChildren {
			actual[c.String()] = struct{}{}
		}

		assert.Equal(tb, expected, actual, "child CIDs don't match expected")
	}, ipfsTestConfig)
}

func TestMetadataStore_BlockSiblings(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		parentData := "parent data"
		child1Data := "child1 data"
		child1Cid := generateCid(t, child1Data)
		child2Data := "child2 data"
		child2Cid := generateCid(t, child2Data)
		child3Data := "child3 data"
		child3Cid := generateCid(t, child3Data)

		// Pin parent block with links to child1 and child2
		parentBlock := createPinnedBlock(tb, ctx, parentData)
		parentBlock.Links = []cid.Cid{
			child1Cid,
			child2Cid,
			child3Cid,
		}
		err := metadataStore.Pin(context.Background(), parentBlock)
		require.NoError(tb, err)

		// Pin child blocks
		child1Block := createPinnedBlock(tb, ctx, child1Data)
		err = metadataStore.Pin(context.Background(), child1Block)
		require.NoError(tb, err)

		child2Block := createPinnedBlock(tb, ctx, child2Data)
		err = metadataStore.Pin(context.Background(), child2Block)
		require.NoError(tb, err)

		child3Block := createPinnedBlock(tb, ctx, child3Data)
		err = metadataStore.Pin(context.Background(), child3Block)
		require.NoError(tb, err)

		// Act
		siblings, err := metadataStore.BlockSiblings(context.Background(), child1Cid, 2)

		// Assert
		require.NoError(tb, err)
		assert.ElementsMatch(tb, []cid.Cid{child2Cid, child3Cid}, siblings)
	}, ipfsTestConfig)
}

func TestMetadataStore_ProvideCIDs(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		testData1 := "test data 1"
		testCid1 := generateCid(t, testData1)
		testData2 := "test data 2"
		testCid2 := generateCid(t, testData2)

		// Pin blocks
		block1 := createPinnedBlock(tb, ctx, testData1)
		err := metadataStore.Pin(context.Background(), block1)
		require.NoError(tb, err)

		block2 := createPinnedBlock(tb, ctx, testData2)
		err = metadataStore.Pin(context.Background(), block2)
		require.NoError(tb, err)

		// Act
		cids, err := metadataStore.ProvideCIDs(context.Background(), 2)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, cids, 2)
		assert.ElementsMatch(tb, []cid.Cid{testCid1, testCid2}, []cid.Cid{cids[0].CID, cids[1].CID})
	}, ipfsTestConfig)
}

func TestMetadataStore_SetLastAnnouncement(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		testData := "test data"
		pinnedBlock := createPinnedBlock(tb, ctx, testData)
		err := metadataStore.Pin(context.Background(), pinnedBlock)
		require.NoError(tb, err)

		announcementTime := time.Now()

		// Act
		err = metadataStore.SetLastAnnouncement(context.Background(), []cid.Cid{pinnedBlock.Cid}, announcementTime)

		// Assert
		require.NoError(tb, err)

		// Verify last announcement time in DB
		var block pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", pinnedBlock.Cid.Bytes()).First(&block).Error
		require.NoError(tb, err)
		assert.WithinDuration(tb, announcementTime, *block.LastAnnouncement, time.Second)
	}, ipfsTestConfig)
}

func TestMetadataStore_Pinned(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		testData1 := "test data 1"
		testCid1 := generateCid(t, testData1)
		testData2 := "test data 2"
		testCid2 := generateCid(t, testData2)

		// Pin blocks
		block1 := createPinnedBlock(tb, ctx, testData1)
		err := metadataStore.Pin(context.Background(), block1)
		require.NoError(tb, err)

		block2 := createPinnedBlock(tb, ctx, testData2)
		err = metadataStore.Pin(context.Background(), block2)
		require.NoError(tb, err)

		// Act
		pinned, err := metadataStore.Pinned(context.Background(), 0, 10)

		// Assert
		require.NoError(tb, err)
		assert.ElementsMatch(tb, []cid.Cid{testCid1, testCid2}, pinned)
	}, ipfsTestConfig)
}

func TestMetadataStore_Size(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		testData := "test data"
		pinnedBlock := createPinnedBlock(tb, ctx, testData)
		err := metadataStore.Pin(context.Background(), pinnedBlock)
		require.NoError(tb, err)

		// Act
		size, err := metadataStore.Size(context.Background(), pinnedBlock.Cid)

		// Assert
		require.NoError(tb, err)
		assert.Equal(tb, uint64(len(testData)), size)
	}, ipfsTestConfig)
}

func TestMetadataStore_UpdateUnixFSMetadata(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		testData := "test data"
		pinnedBlock := createPinnedBlock(tb, ctx, testData)
		err := metadataStore.Pin(context.Background(), pinnedBlock)
		require.NoError(tb, err)

		// Create initial metadata
		initialMetadata := &pluginDb.UnixFSNode{
			Type:      1,
			BlockSize: 1024,
		}

		// Act - Update metadata
		err = metadataStore.UpdateUnixFSMetadata(pinnedBlock.Cid, initialMetadata)
		require.NoError(tb, err)

		// Assert - Verify updated metadata
		metadata, err := metadataStore.GetUnixFSMetadata(pinnedBlock.Cid)
		require.NoError(tb, err)
		assert.Equal(tb, initialMetadata.Type, metadata.Type)
		assert.Equal(tb, initialMetadata.BlockSize, metadata.BlockSize)
	}, ipfsTestConfig)
}

func TestMetadataStore_GetUnixFSMetadata(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		testData := "test data"
		pinnedBlock := createPinnedBlock(tb, ctx, testData)

		// Create initial metadata
		initialMetadata := &pluginDb.UnixFSNode{
			Type:      1,
			BlockSize: 1024,
		}

		err := metadataStore.Pin(context.Background(), pinnedBlock)
		require.NoError(tb, err)

		err = metadataStore.UpdateUnixFSMetadata(pinnedBlock.Cid, initialMetadata)
		require.NoError(tb, err)

		// Act
		metadata, err := metadataStore.GetUnixFSMetadata(pinnedBlock.Cid)

		// Assert
		require.NoError(tb, err)
		assert.Equal(tb, initialMetadata.Type, metadata.Type)
		assert.Equal(tb, initialMetadata.BlockSize, metadata.BlockSize)
	}, ipfsTestConfig)
}

func TestMetadataStore_ReaddBlockAfterDeletion(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		testData := "test data for re-add"
		testCid := generateCid(t, testData)
		pinnedBlock := createPinnedBlock(tb, ctx, testData)

		// Act & Assert - First Pin
		err := metadataStore.Pin(context.Background(), pinnedBlock)
		require.NoError(tb, err)

		// Verify block exists
		err = metadataStore.BlockExists(context.Background(), pinnedBlock.Cid)
		require.NoError(tb, err)

		// Act & Assert - Unpin (delete)
		err = metadataStore.Unpin(context.Background(), pinnedBlock.Cid)
		require.NoError(tb, err)

		// Verify block is deleted
		err = metadataStore.BlockExists(context.Background(), pinnedBlock.Cid)
		assert.Error(tb, err)
		assert.True(tb, errors.Is(err, format.ErrNotFound{}))

		// Act & Assert - Re-Pin same CID (this tests that unique constraints don't block re-add)
		err = metadataStore.Pin(context.Background(), pinnedBlock)
		require.NoError(tb, err, "Should be able to re-add block after deletion")

		// Verify block exists again
		err = metadataStore.BlockExists(context.Background(), pinnedBlock.Cid)
		require.NoError(tb, err)

		// Verify we can retrieve the block
		var block pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", testCid.Bytes()).First(&block).Error
		require.NoError(tb, err)
		assert.Equal(tb, testCid.Bytes(), block.CID)
		assert.Equal(tb, uint64(len(testData)), block.Size)
	}, ipfsTestConfig)
}

func TestMetadataStore_ReaddLinkedBlockAfterDeletion(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		
		parentData := "parent data"
		childData := "child data"
		parentCid := generateCid(t, parentData)
		childCid := generateCid(t, childData)

		// Create parent block with a link to the child
		parentBlock := createPinnedBlock(tb, ctx, parentData)
		parentBlock.Links = []cid.Cid{childCid}
		childBlock := createPinnedBlock(tb, ctx, childData)

		// Act & Assert - First Pin with linked block
		err := metadataStore.Pin(context.Background(), parentBlock)
		require.NoError(tb, err)
		err = metadataStore.Pin(context.Background(), childBlock)
		require.NoError(tb, err)

		// Verify linked block exists
		var linkedBlocks []pluginDb.IPFSLinkedBlock
		err = ctx.DB().Find(&linkedBlocks).Error
		require.NoError(tb, err)
		require.Len(tb, linkedBlocks, 1, "Should have one linked block")

		firstLinkedID := linkedBlocks[0].ID

		// Get parent and child block IDs
		var parentBlockRecord pluginDb.IPFSBlock
		var childBlockRecord pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", parentCid.Bytes()).First(&parentBlockRecord).Error
		require.NoError(tb, err)
		err = ctx.DB().Where("cid = ?", childCid.Bytes()).First(&childBlockRecord).Error
		require.NoError(tb, err)

		// Act & Assert - Unpin parent (should delete linked blocks)
		err = metadataStore.Unpin(context.Background(), parentBlock.Cid)
		require.NoError(tb, err)

		// Verify linked block is deleted
		linkedBlocks = []pluginDb.IPFSLinkedBlock{}
		err = ctx.DB().Find(&linkedBlocks).Error
		require.NoError(tb, err)
		require.Len(tb, linkedBlocks, 0, "Linked blocks should be deleted")

		// Verify parent block is deleted
		err = metadataStore.BlockExists(context.Background(), parentBlock.Cid)
		assert.Error(tb, err)
		assert.True(tb, errors.Is(err, format.ErrNotFound{}))

		// Act & Assert - Re-Pin parent with same link structure
		// This tests that unique constraints on (parent_id, child_id, link_index) don't block re-add
		err = metadataStore.Pin(context.Background(), parentBlock)
		require.NoError(tb, err, "Should be able to re-add parent with same linking after deletion")

		// Get the new parent block record (re-pinned parent gets new ID)
		var newParentBlockRecord pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", parentCid.Bytes()).First(&newParentBlockRecord).Error
		require.NoError(tb, err)

		// Verify new linked block created
		linkedBlocks = []pluginDb.IPFSLinkedBlock{}
		err = ctx.DB().Find(&linkedBlocks).Error
		require.NoError(tb, err)
		require.Len(tb, linkedBlocks, 1, "Should have one new linked block")

		// Verify it's a new record (different ID from before deletion)
		assert.NotEqual(tb, firstLinkedID, linkedBlocks[0].ID, "Should create a new linked block record")

		// Verify the link structure is correct
		assert.Equal(tb, newParentBlockRecord.ID, linkedBlocks[0].ParentID, "ParentID should be new parent's ID")
		assert.Equal(tb, childBlockRecord.ID, linkedBlocks[0].ChildID, "ChildID should be the child's ID (unchanged)")
		assert.Equal(tb, 0, linkedBlocks[0].LinkIndex, "LinkIndex should be 0")
	}, ipfsTestConfig)
}

func TestMetadataStore_MarkBlockReady(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		testData := "test data"
		pinnedBlock := createPinnedBlock(tb, ctx, testData)
		err := metadataStore.Pin(context.Background(), pinnedBlock)
		require.NoError(tb, err)

		// Act
		err = metadataStore.MarkBlockReady(pinnedBlock.Cid, false)
		require.NoError(tb, err)

		// Assert
		var block pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", pinnedBlock.Cid.Bytes()).First(&block).Error
		require.NoError(tb, err)
		assert.False(tb, block.Ready)

		// Act
		err = metadataStore.MarkBlockReady(pinnedBlock.Cid, true)
		require.NoError(tb, err)

		// Assert
		err = ctx.DB().Where("cid = ?", pinnedBlock.Cid.Bytes()).First(&block).Error
		require.NoError(tb, err)
		assert.True(tb, block.Ready)
	}, ipfsTestConfig)
}

func TestMetadataStore_SetLastAnnouncement_NoBlockFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		testData := "test data"
		testCid := generateCid(t, testData)

		announcementTime := time.Now()

		// Act
		err := metadataStore.SetLastAnnouncement(context.Background(), []cid.Cid{testCid}, announcementTime)

		// Assert
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), fmt.Sprintf("no block found with CID %q", testCid))
	}, ipfsTestConfig)
}

func TestMetadataStore_Pin_DuplicateLinks(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		parentData := "parent data"
		parentCid := generateCid(t, parentData)
		childData := "child data"
		childCid := generateCid(t, childData)

		// Pin parent block with duplicate links to the same child
		parentBlock := createPinnedBlock(tb, ctx, parentData)
		parentBlock.Links = []cid.Cid{
			childCid,
			childCid, // Duplicate link
		}
		err := metadataStore.Pin(context.Background(), parentBlock)
		require.NoError(tb, err)

		// Pin child block
		childBlock := createPinnedBlock(tb, ctx, childData)
		err = metadataStore.Pin(context.Background(), childBlock)
		require.NoError(tb, err)

		// Act
		children, err := metadataStore.BlockChildren(context.Background(), parentCid, nil)

		// Assert
		require.NoError(tb, err)
		assert.ElementsMatch(tb, []cid.Cid{childCid}, children)
	}, ipfsTestConfig)
}

func TestMetadataStore_Pin_ExistingLinkedBlockWithoutParent(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		parentData := "parent data"
		parentCid := generateCid(t, parentData)
		childData := "child data"
		childCid := generateCid(t, childData)

		// Pin child block first
		childBlock := createPinnedBlock(tb, ctx, childData)
		err := metadataStore.Pin(context.Background(), childBlock)
		require.NoError(tb, err)

		// Pin parent block, linking to the existing child
		parentBlock := createPinnedBlock(tb, ctx, parentData)
		parentBlock.Links = []cid.Cid{childCid}
		err = metadataStore.Pin(context.Background(), parentBlock)
		require.NoError(tb, err)

		// Act
		children, err := metadataStore.BlockChildren(context.Background(), parentCid, nil)

		// Assert
		require.NoError(tb, err)
		assert.ElementsMatch(tb, []cid.Cid{childCid}, children)

		// Verify that the linked block now has the correct parent ID
		var linkedBlock pluginDb.IPFSLinkedBlock
		var parentBlockDb pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", parentCid.Bytes()).First(&parentBlockDb).Error
		require.NoError(tb, err)
		var childBlockDb pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", childCid.Bytes()).First(&childBlockDb).Error
		require.NoError(tb, err)

		err = ctx.DB().Where("child_id = ? AND parent_id = ?", childBlockDb.ID, parentBlockDb.ID).First(&linkedBlock).Error
		require.NoError(tb, err)
	}, ipfsTestConfig)
}

func TestMetadataStore_Unpin_NonExistentBlock(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		testData := "test data"
		testCid := generateCid(t, testData)

		// Act
		err := metadataStore.Unpin(context.Background(), testCid)

		// Assert
		require.NoError(tb, err) // Unpinning a non-existent block should not return an error
	}, ipfsTestConfig)
}

func TestMetadataStore_Unpin_WithLinkedBlocks(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
		parentData := "parent data"
		parentBlock := createPinnedBlock(tb, ctx, parentData)
		childData := "child data"
		childCid := generateCid(t, childData)

		// Pin parent block with a link to the child
		parentBlock.Links = []cid.Cid{childCid}
		err := metadataStore.Pin(context.Background(), parentBlock)
		require.NoError(tb, err)

		// Pin child block
		childBlock := createPinnedBlock(tb, ctx, childData)
		err = metadataStore.Pin(context.Background(), childBlock)
		require.NoError(tb, err)

		// Act
		err = metadataStore.Unpin(context.Background(), parentBlock.Cid)
		require.NoError(tb, err)

		// Assert - Parent block should be deleted
		err = metadataStore.BlockExists(context.Background(), parentBlock.Cid)
		assert.Error(tb, err)

		// Assert - Linked block entries should be deleted
		var linkedBlock pluginDb.IPFSLinkedBlock
		var parentBlockDb pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", parentBlock.Cid.Bytes()).First(&parentBlockDb).Error
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			require.NoError(tb, err)
		}

		var childBlockDb pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", childCid.Bytes()).First(&childBlockDb).Error
		require.NoError(tb, err)

		err = ctx.DB().Where("parent_id = ? OR child_id = ?", parentBlockDb.ID, parentBlockDb.ID).First(&linkedBlock).Error
		assert.Error(tb, err)
		assert.True(tb, errors.Is(err, gorm.ErrRecordNotFound))
	}, ipfsTestConfig)
}
