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

// TestMetadataStore_BatchPin_SharedChildBlock tests that two different parent
// blocks linking to the same child CID result in a single ipfs_blocks row for
// the child and two IPFSLinkedBlock entries (one per parent).
func TestMetadataStore_BatchPin_SharedChildBlock(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))

		childData := "shared child"
		childCid := generateCid(t, childData)

		parent1Data := "parent 1"
		parent1Cid := generateCid(t, parent1Data)
		parent1 := createPinnedBlock(tb, ctx, parent1Data)
		parent1.Links = []cid.Cid{childCid}

		parent2Data := "parent 2"
		parent2Cid := generateCid(t, parent2Data)
		parent2 := createPinnedBlock(tb, ctx, parent2Data)
		parent2.Links = []cid.Cid{childCid}

		// Batch pin both parents — they share the same child
		err := metadataStore.BatchPin(context.Background(), []pluginCore.PinnedBlock{parent1, parent2})
		require.NoError(tb, err)

		// Pin the child so it's Ready=true (BlockChildren only returns ready children)
		childPinned := createPinnedBlock(tb, ctx, childData)
		err = metadataStore.Pin(context.Background(), childPinned)
		require.NoError(tb, err)

		// Verify: only one ipfs_blocks row for the child
		var childBlocks []pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", childCid.Bytes()).Find(&childBlocks).Error
		require.NoError(tb, err)
		assert.Len(tb, childBlocks, 1, "shared child should have exactly one ipfs_blocks row")

		// Verify: both parents can retrieve the child via BlockChildren
		children1, err := metadataStore.BlockChildren(context.Background(), parent1Cid, nil)
		require.NoError(tb, err)
		assert.ElementsMatch(tb, []cid.Cid{childCid}, children1)

		children2, err := metadataStore.BlockChildren(context.Background(), parent2Cid, nil)
		require.NoError(tb, err)
		assert.ElementsMatch(tb, []cid.Cid{childCid}, children2)

		// Verify: two IPFSLinkedBlock rows (one per parent)
		var linkedBlocks []pluginDb.IPFSLinkedBlock
		err = ctx.DB().Find(&linkedBlocks).Error
		require.NoError(tb, err)
		assert.Len(tb, linkedBlocks, 2, "should have two linked block entries (one per parent)")
	}, ipfsTestConfig)
}

// TestMetadataStore_BatchPin_ChildPromotedToParent tests that a block first
// seen as a child (created with Ready=false) is correctly promoted to
// Ready=true when later pinned as a parent.
func TestMetadataStore_BatchPin_ChildPromotedToParent(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))

		childData := "future parent"
		childCid := generateCid(t, childData)

		// Pin a parent that links to the child — child is created as placeholder (Ready=false)
		parentData := "parent data"
		parent := createPinnedBlock(tb, ctx, parentData)
		parent.Links = []cid.Cid{childCid}
		err := metadataStore.Pin(context.Background(), parent)
		require.NoError(tb, err)

		// Verify: child exists but is not ready
		var childBlock pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", childCid.Bytes()).First(&childBlock).Error
		require.NoError(tb, err)
		assert.False(tb, childBlock.Ready, "child should be a placeholder (Ready=false)")

		// Now pin the child as a parent — should be promoted to Ready=true
		childPinned := createPinnedBlock(tb, ctx, childData)
		err = metadataStore.Pin(context.Background(), childPinned)
		require.NoError(tb, err)

		// Verify: child is now ready
		err = ctx.DB().Where("cid = ?", childCid.Bytes()).First(&childBlock).Error
		require.NoError(tb, err)
		assert.True(tb, childBlock.Ready, "child should be promoted to Ready=true")
	}, ipfsTestConfig)
}

// TestMetadataStore_BatchPin_DuplicateChildInBatch tests that when two blocks
// in the same batch share a child CID, the child is only inserted once.
func TestMetadataStore_BatchPin_DuplicateChildInBatch(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))

		childData := "shared child"
		childCid := generateCid(t, childData)

		parent1Data := "parent 1"
		parent1 := createPinnedBlock(tb, ctx, parent1Data)
		parent1.Links = []cid.Cid{childCid}

		parent2Data := "parent 2"
		parent2 := createPinnedBlock(tb, ctx, parent2Data)
		parent2.Links = []cid.Cid{childCid}

		// Batch pin — both reference the same child
		err := metadataStore.BatchPin(context.Background(), []pluginCore.PinnedBlock{parent1, parent2})
		require.NoError(tb, err)

		// Verify: exactly one ipfs_blocks row for the child
		var childBlocks []pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", childCid.Bytes()).Find(&childBlocks).Error
		require.NoError(tb, err)
		assert.Len(tb, childBlocks, 1, "duplicate child in batch should result in single row")
	}, ipfsTestConfig)
}

// TestMetadataStore_BatchPin_ExistingChildFromAnotherUpload tests that when a
// child block already exists (Ready=true from a previous Pin), BatchPin's bulk
// INSERT ON CONFLICT DO NOTHING preserves the existing row and doesn't downgrade
// Ready to false.
func TestMetadataStore_BatchPin_ExistingChildFromAnotherUpload(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))

		childData := "already pinned child"
		childCid := generateCid(t, childData)

		// First: pin the child as a parent (Ready=true)
		childBlock := createPinnedBlock(tb, ctx, childData)
		err := metadataStore.Pin(context.Background(), childBlock)
		require.NoError(tb, err)

		// Verify: child is ready
		var existingChild pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", childCid.Bytes()).First(&existingChild).Error
		require.NoError(tb, err)
		assert.True(tb, existingChild.Ready, "child should be Ready=true after being pinned")

		// Now: pin a different parent that links to this already-pinned child
		parentData := "new parent"
		parent := createPinnedBlock(tb, ctx, parentData)
		parent.Links = []cid.Cid{childCid}
		err = metadataStore.Pin(context.Background(), parent)
		require.NoError(tb, err)

		// Verify: child is still Ready=true (not downgraded to false)
		err = ctx.DB().Where("cid = ?", childCid.Bytes()).First(&existingChild).Error
		require.NoError(tb, err)
		assert.True(tb, existingChild.Ready, "existing child should remain Ready=true")

		// Verify: parent can see the child
		parentCid := generateCid(t, parentData)
		children, err := metadataStore.BlockChildren(context.Background(), parentCid, nil)
		require.NoError(tb, err)
		assert.ElementsMatch(tb, []cid.Cid{childCid}, children)
	}, ipfsTestConfig)
}

// TestMetadataStore_Pin_PlaceholderPromotedWithUnixFSMetadata tests that when
// a block is first created as a placeholder by ensureChildBlocksFromSet (Ready=false)
// and then later pinned as a parent with links, the linked blocks get the correct
// parent_id (not 0).
//
// On MySQL, GORM's ON DUPLICATE KEY UPDATE does not populate the auto-increment ID
// in the model struct, so parentBlock.ID remains 0 after the upsert. Without the
// fallback query, linked blocks would have parent_id = 0, and UnixFS nodes would
// have block_id = 0 (failing FK constraint). This test guards against that regression.
func TestMetadataStore_Pin_PlaceholderPromotedWithUnixFSMetadata(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))

		grandchildData := "grandchild block"
		grandchildCid := generateCid(t, grandchildData)

		// Pin a top-level parent that links to "middle" — this creates
		// "middle" as a placeholder (Ready=false) in ipfs_blocks.
		topData := "top parent"
		middleData := "middle block"
		middleCid := generateCid(t, middleData)

		topBlock := createPinnedBlock(tb, ctx, topData)
		topBlock.Links = []cid.Cid{middleCid}
		err := metadataStore.Pin(context.Background(), topBlock)
		require.NoError(tb, err)

		// Verify: middle exists as a placeholder
		var middleBlock pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", middleCid.Bytes()).First(&middleBlock).Error
		require.NoError(tb, err)
		require.False(tb, middleBlock.Ready, "middle should be a placeholder")
		middleBlockID := middleBlock.ID
		require.NotZero(tb, middleBlockID, "placeholder should have a valid ID")

		// Pin the middle block as a parent — it links to grandchild.
		// This hits the ON CONFLICT DO UPDATE path in pinPreparedBlockInTx
		// because the row already exists. On MySQL, parentBlock.ID would be 0
		// without the fallback query, causing FK violations and wrong parent_ids.
		middlePinned := createPinnedBlock(tb, ctx, middleData)
		middlePinned.Links = []cid.Cid{grandchildCid}
		err = metadataStore.Pin(context.Background(), middlePinned)
		require.NoError(tb, err)

		// Verify: middle is now Ready=true
		err = ctx.DB().Where("cid = ?", middleCid.Bytes()).First(&middleBlock).Error
		require.NoError(tb, err)
		assert.True(tb, middleBlock.Ready, "middle should be promoted to Ready=true")

		// Verify: linked block from middle → grandchild has correct parent_id (not 0)
		grandchildPinned := createPinnedBlock(tb, ctx, grandchildData)
		err = metadataStore.Pin(context.Background(), grandchildPinned)
		require.NoError(tb, err)

		var grandchildBlockDb pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", grandchildCid.Bytes()).First(&grandchildBlockDb).Error
		require.NoError(tb, err)

		var middleToGrandchild pluginDb.IPFSLinkedBlock
		err = ctx.DB().Where("parent_id = ? AND child_id = ?", middleBlock.ID, grandchildBlockDb.ID).First(&middleToGrandchild).Error
		require.NoError(tb, err, "middle→grandchild linked block should exist")
		assert.NotZero(tb, middleToGrandchild.ParentID, "parent_id must not be 0")
		assert.Equal(tb, middleBlockID, middleToGrandchild.ParentID, "parent_id must match middle block's original ID")
		assert.Equal(tb, grandchildBlockDb.ID, middleToGrandchild.ChildID, "child_id must match grandchild block")
	}, ipfsTestConfig)
}

// TestMetadataStore_BatchPin_OrphanAdoption tests that when a child block has
// an IPFSLinkedBlock with parent_id IS NULL, pinning a parent that links to it
// updates the parent_id (orphan adoption).
func TestMetadataStore_BatchPin_OrphanAdoption(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		metadataStore := store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))

		childData := "orphan child"
		childCid := generateCid(t, childData)

		// Manually create an orphan linked block (parent_id IS NULL)
		childBlock := createPinnedBlock(tb, ctx, childData)
		err := metadataStore.Pin(context.Background(), childBlock)
		require.NoError(tb, err)

		var childDb pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", childCid.Bytes()).First(&childDb).Error
		require.NoError(tb, err)

		// Use raw SQL to create a truly NULL parent_id (GORM's zero uint = 0, not NULL)
		err = ctx.DB().Exec(
			"INSERT INTO ipfs_linked_blocks (child_id, link_index, created_at, updated_at) VALUES (?, 0, datetime('now'), datetime('now'))",
			childDb.ID,
		).Error
		require.NoError(tb, err)

		// Verify: orphan exists with NULL parent
		var link pluginDb.IPFSLinkedBlock
		err = ctx.DB().Where("child_id = ? AND parent_id IS NULL", childDb.ID).First(&link).Error
		require.NoError(tb, err)

		// Now pin a parent that links to this child — should adopt the orphan
		parentData := "adopting parent"
		parent := createPinnedBlock(tb, ctx, parentData)
		parent.Links = []cid.Cid{childCid}
		err = metadataStore.Pin(context.Background(), parent)
		require.NoError(tb, err)

		// Verify: orphan is now adopted (parent_id is set)
		var parentDb pluginDb.IPFSBlock
		parentCid := generateCid(t, parentData)
		err = ctx.DB().Where("cid = ?", parentCid.Bytes()).First(&parentDb).Error
		require.NoError(tb, err)

		var adoptedLink pluginDb.IPFSLinkedBlock
		err = ctx.DB().Where("child_id = ? AND parent_id = ?", childDb.ID, parentDb.ID).First(&adoptedLink).Error
		require.NoError(tb, err, "orphan should be adopted by the new parent")
	}, ipfsTestConfig)
}
