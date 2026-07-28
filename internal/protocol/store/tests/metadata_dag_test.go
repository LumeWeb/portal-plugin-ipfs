package tests

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
)

// newTestMetadataStore creates a MetadataStore bound to the test protocol.
func newTestMetadataStore(tb coreTesting.TB, ctx coreTesting.TestContext) pluginCore.MetadataStore {
	tb.Helper()
	return store.NewMetadataStore(ctx, core.GetProtocol(internal.ProtocolName).(protocol.ProtoNode))
}

// pinBlocks pins all blocks in order, failing the test on the first error.
func pinBlocks(tb coreTesting.TB, store pluginCore.MetadataStore, blocks ...pluginCore.PinnedBlock) {
	tb.Helper()
	for _, blk := range blocks {
		require.NoError(tb, store.Pin(context.Background(), blk))
	}
}

// nodesToMap converts a DAG node slice into a map keyed by CID string.
func nodesToMap(nodes []core.DAGBlockNode) map[string]core.DAGBlockNode {
	m := make(map[string]core.DAGBlockNode, len(nodes))
	for _, node := range nodes {
		m[node.CID.String()] = node
	}
	return m
}

// childCIDs extracts a node's children as a string slice for order-sensitive assertion.
func childCIDs(node core.DAGBlockNode) []string {
	cids := make([]string, len(node.Children))
	for i, c := range node.Children {
		cids[i] = c.String()
	}
	return cids
}

// resolveDAG pins blocks, resolves the DAG from rootCid, and returns a node map.
// Use this for tests that don't need to manipulate store state between pin and resolve.
func resolveDAG(tb coreTesting.TB, ctx coreTesting.TestContext, rootCid cid.Cid, blocks ...pluginCore.PinnedBlock) map[string]core.DAGBlockNode {
	tb.Helper()
	ms := newTestMetadataStore(tb, ctx)
	pinBlocks(tb, ms, blocks...)
	nodes, err := ms.ResolveDAG(context.Background(), rootCid)
	require.NoError(tb, err)
	return nodesToMap(nodes)
}

// TestMetadataStore_ResolveDAG_SimpleTree verifies ResolveDAG returns all blocks
// in a simple parent → [child1, child2] tree with correct sizes and children.
func TestMetadataStore_ResolveDAG_SimpleTree(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		child1Data := "child1 data"
		child2Data := "child2 data"
		child1CID := generateCid(t, child1Data)
		child2CID := generateCid(t, child2Data)

		parentBlock := createPinnedBlock(tb, ctx, "parent data")
		parentBlock.Links = []cid.Cid{child1CID, child2CID}

		nodeMap := resolveDAG(tb, ctx, parentBlock.Cid,
			parentBlock,
			createPinnedBlock(tb, ctx, child1Data),
			createPinnedBlock(tb, ctx, child2Data),
		)

		require.Len(tb, nodeMap, 3, "should return root + 2 children")

		// Root: correct size, 2 children in link_index order
		rootNode := nodeMap[parentBlock.Cid.String()]
		assert.Equal(tb, uint64(len("parent data")), rootNode.Size)
		assert.Equal(tb, []string{child1CID.String(), child2CID.String()}, childCIDs(rootNode))

		// Children: no children of their own
		for _, childCID := range []cid.Cid{child1CID, child2CID} {
			assert.Empty(tb, nodeMap[childCID.String()].Children)
		}
	}, ipfsTestConfig)
}

// TestMetadataStore_ResolveDAG_DeepChain verifies ResolveDAG traverses a multi-level
// chain: root → middle → leaf.
func TestMetadataStore_ResolveDAG_DeepChain(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		leafData := "leaf data"
		middleData := "middle data"
		leafCID := generateCid(t, leafData)
		middleCID := generateCid(t, middleData)

		leafBlock := createPinnedBlock(tb, ctx, leafData)

		middleBlock := createPinnedBlock(tb, ctx, middleData)
		middleBlock.Links = []cid.Cid{leafCID}

		rootBlock := createPinnedBlock(tb, ctx, "root data")
		rootBlock.Links = []cid.Cid{middleCID}

		nodeMap := resolveDAG(tb, ctx, rootBlock.Cid, leafBlock, middleBlock, rootBlock)

		require.Len(tb, nodeMap, 3, "should return root + middle + leaf")

		assert.Equal(tb, []string{middleCID.String()}, childCIDs(nodeMap[rootBlock.Cid.String()]))
		assert.Equal(tb, []string{leafCID.String()}, childCIDs(nodeMap[middleCID.String()]))
		assert.Empty(tb, nodeMap[leafCID.String()].Children)
	}, ipfsTestConfig)
}

// TestMetadataStore_ResolveDAG_Diamond verifies ResolveDAG deduplicates blocks
// that appear in multiple paths. Diamond structure:
//
//	root → childA → shared
//	     → childB → shared
//
// The shared block should appear only once in the result.
func TestMetadataStore_ResolveDAG_Diamond(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		sharedData := "shared data"
		childAData := "childA data"
		childBData := "childB data"
		sharedCID := generateCid(t, sharedData)
		childACID := generateCid(t, childAData)
		childBCID := generateCid(t, childBData)

		sharedBlock := createPinnedBlock(tb, ctx, sharedData)

		childABlock := createPinnedBlock(tb, ctx, childAData)
		childABlock.Links = []cid.Cid{sharedCID}

		childBBlock := createPinnedBlock(tb, ctx, childBData)
		childBBlock.Links = []cid.Cid{sharedCID}

		rootBlock := createPinnedBlock(tb, ctx, "root data")
		rootBlock.Links = []cid.Cid{childACID, childBCID}

		nodeMap := resolveDAG(tb, ctx, rootBlock.Cid, sharedBlock, childABlock, childBBlock, rootBlock)

		require.Len(tb, nodeMap, 4, "shared block should appear only once despite multiple paths")

		_, exists := nodeMap[sharedCID.String()]
		assert.True(tb, exists, "shared block should be present")

		assert.Equal(tb, []string{sharedCID.String()}, childCIDs(nodeMap[childACID.String()]))
		assert.Equal(tb, []string{sharedCID.String()}, childCIDs(nodeMap[childBCID.String()]))
	}, ipfsTestConfig)
}

// TestMetadataStore_ResolveDAG_NotFound verifies ResolveDAG returns empty result
// for a CID that doesn't exist in the store.
func TestMetadataStore_ResolveDAG_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ms := newTestMetadataStore(tb, ctx)
		pinBlocks(tb, ms, createPinnedBlock(tb, ctx, "filler"))

		nodes, err := ms.ResolveDAG(context.Background(), generateCid(t, "nonexistent root"))
		require.NoError(tb, err)
		assert.Empty(tb, nodes, "should return empty slice for nonexistent root")
	}, ipfsTestConfig)
}

// TestMetadataStore_ResolveDAG_SingleBlock verifies ResolveDAG returns just the root
// when it has no children.
func TestMetadataStore_ResolveDAG_SingleBlock(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		testData := "lone block data"
		block := createPinnedBlock(tb, ctx, testData)

		nodeMap := resolveDAG(tb, ctx, block.Cid, block)

		require.Len(tb, nodeMap, 1)
		node := nodeMap[block.Cid.String()]
		assert.Equal(tb, uint64(len(testData)), node.Size)
		assert.Empty(tb, node.Children)
	}, ipfsTestConfig)
}

// TestMetadataStore_ResolveDAG_NotReadyRoot verifies ResolveDAG excludes the DAG
// entirely when the root block is not ready.
func TestMetadataStore_ResolveDAG_NotReadyRoot(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ms := newTestMetadataStore(tb, ctx)

		childCID := generateCid(t, "child data")
		rootBlock := createPinnedBlock(tb, ctx, "root data")
		rootBlock.Links = []cid.Cid{childCID}

		pinBlocks(tb, ms, createPinnedBlock(tb, ctx, "child data"), rootBlock)
		require.NoError(tb, ms.MarkBlockReady(rootBlock.Cid, false))

		nodes, err := ms.ResolveDAG(context.Background(), rootBlock.Cid)
		require.NoError(tb, err)
		assert.Empty(tb, nodes, "should return empty when root is not ready")
	}, ipfsTestConfig)
}

// TestMetadataStore_ResolveDAG_NotReadyChild verifies ResolveDAG excludes children
// that are not ready from the traversal. The root is ready, but a child is not —
// the not-ready child should be absent from the result and its descendants
// should not be traversed.
func TestMetadataStore_ResolveDAG_NotReadyChild(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ms := newTestMetadataStore(tb, ctx)

		leafData := "leaf data"
		leafCID := generateCid(t, leafData)

		middleData := "middle data"
		middleCID := generateCid(t, middleData)
		middleBlock := createPinnedBlock(tb, ctx, middleData)
		middleBlock.Links = []cid.Cid{leafCID}

		rootBlock := createPinnedBlock(tb, ctx, "root data")
		rootBlock.Links = []cid.Cid{middleCID}

		pinBlocks(tb, ms, createPinnedBlock(tb, ctx, leafData), middleBlock, rootBlock)
		require.NoError(tb, ms.MarkBlockReady(middleCID, false))

		nodeMap := nodesToMap(mustResolveDAG(tb, ms, rootBlock.Cid))

		assert.Contains(tb, nodeMap, rootBlock.Cid.String(), "root should be present")

		_, ok := nodeMap[middleCID.String()]
		assert.False(tb, ok, "not-ready child should be excluded from DAG")

		_, ok = nodeMap[leafCID.String()]
		assert.False(tb, ok, "descendant of not-ready block should not be traversed")
	}, ipfsTestConfig)
}

// TestMetadataStore_ResolveDAG_ChildOrder verifies ResolveDAG returns children
// in link_index order, not insertion or CID order.
func TestMetadataStore_ResolveDAG_ChildOrder(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		child1Data := "alpha"
		child2Data := "beta"
		child3Data := "gamma"
		child1CID := generateCid(t, child1Data)
		child2CID := generateCid(t, child2Data)
		child3CID := generateCid(t, child3Data)

		// Link children in a non-sequential order to verify link_index ordering
		parentBlock := createPinnedBlock(tb, ctx, "parent")
		parentBlock.Links = []cid.Cid{child3CID, child1CID, child2CID}

		nodeMap := resolveDAG(tb, ctx, parentBlock.Cid,
			parentBlock,
			createPinnedBlock(tb, ctx, child1Data),
			createPinnedBlock(tb, ctx, child2Data),
			createPinnedBlock(tb, ctx, child3Data),
		)

		expected := []string{child3CID.String(), child1CID.String(), child2CID.String()}
		assert.Equal(tb, expected, childCIDs(nodeMap[parentBlock.Cid.String()]), "children must be in link_index order")
	}, ipfsTestConfig)
}

// TestMetadataStore_ResolveDAG_UnpinnedChild verifies ResolveDAG silently excludes
// children that have a link record but no ipfs_blocks row (never pinned). The
// recursive CTE inner-joins on ipfs_blocks, so orphaned links are skipped.
func TestMetadataStore_ResolveDAG_UnpinnedChild(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ms := newTestMetadataStore(tb, ctx)

		// Pin only the parent with a link to a child that is never pinned
		ghostChildCID := generateCid(t, "never pinned")
		parentBlock := createPinnedBlock(tb, ctx, "parent data")
		parentBlock.Links = []cid.Cid{ghostChildCID}
		pinBlocks(tb, ms, parentBlock)

		nodes, err := ms.ResolveDAG(context.Background(), parentBlock.Cid)
		require.NoError(tb, err)

		require.Len(tb, nodes, 1, "only the parent should be in the DAG; unpinned child excluded")
		assert.Equal(tb, parentBlock.Cid.String(), nodes[0].CID.String())
		assert.Empty(tb, nodes[0].Children, "unpinned child should not appear in children list")
	}, ipfsTestConfig)
}

// TestMetadataStore_ResolveDAG_NodeOrder verifies that the returned node slice
// has deterministic, stable ordering across repeated calls. This is a
// regression test for map-iteration nondeterminism in the assembly step.
func TestMetadataStore_ResolveDAG_NodeOrder(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		leafData := "leaf data"
		middleData := "middle data"
		leafCID := generateCid(t, leafData)
		middleCID := generateCid(t, middleData)

		leafBlock := createPinnedBlock(tb, ctx, leafData)

		middleBlock := createPinnedBlock(tb, ctx, middleData)
		middleBlock.Links = []cid.Cid{leafCID}

		rootBlock := createPinnedBlock(tb, ctx, "root data")
		rootBlock.Links = []cid.Cid{middleCID}

		ms := newTestMetadataStore(tb, ctx)
		pinBlocks(tb, ms, leafBlock, middleBlock, rootBlock)

		// Run multiple times and verify the ordering is identical every time
		var firstOrder []string
		for i := 0; i < 10; i++ {
			nodes, err := ms.ResolveDAG(context.Background(), rootBlock.Cid)
			require.NoError(tb, err)
			require.Len(tb, nodes, 3)

			order := make([]string, 3)
			for j, n := range nodes {
				order[j] = n.CID.String()
			}

			if firstOrder == nil {
				firstOrder = order
			} else {
				assert.Equal(tb, firstOrder, order,
					"node ordering must be deterministic across calls (iteration %d)", i)
			}
		}

		// Verify all expected nodes are present
		require.Len(tb, firstOrder, 3)
		assert.Contains(tb, firstOrder, rootBlock.Cid.String())
		assert.Contains(tb, firstOrder, middleCID.String())
		assert.Contains(tb, firstOrder, leafCID.String())
	}, ipfsTestConfig)
}

// mustResolveDAG resolves the DAG and fails the test on error.
func mustResolveDAG(tb coreTesting.TB, ms pluginCore.MetadataStore, rootCid cid.Cid) []core.DAGBlockNode {
	tb.Helper()
	nodes, err := ms.ResolveDAG(context.Background(), rootCid)
	require.NoError(tb, err)
	return nodes
}
