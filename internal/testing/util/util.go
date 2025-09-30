package util

import (
	"strings"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"testing"
)

// CalculateParentPath computes the parent directory path for a given file path
func CalculateParentPath(path string) string {
	if path == "/" || path == "" {
		return ""
	}
	lastSlash := strings.LastIndex(path, "/")
	if lastSlash > 0 {
		return path[:lastSlash]
	}
	if lastSlash == 0 {
		return "/"
	}
	return ""
}

func GenerateTestCID(t *testing.T, data string) cid.Cid {
	hash, err := multihash.Sum([]byte(data), multihash.SHA2_256, -1)
	require.NoError(t, err)
	testCID := cid.NewCidV1(cid.DagCBOR, hash)
	return testCID
}

func CreateTestBlockAndNode(t *testing.T, ctx coreTesting.TestContext, cid cid.Cid, name string, nodeType uint8, blockSize int64, childCIDs []cid.Cid) (*pluginDb.IPFSBlock, *pluginDb.UnixFSNode) {
	// Create or get the parent block
	var block *pluginDb.IPFSBlock
	err := ctx.DB().Where("cid = ?", cid.Bytes()).First(&block).Error
	if err != nil {
		block = &pluginDb.IPFSBlock{
			CID:  cid.Bytes(),
			Size: uint64(blockSize),
		}
		err = ctx.DB().Create(block).Error
		require.NoError(t, err)
	}

	// Create the UnixFS node
	node := &pluginDb.UnixFSNode{
		BlockID:   block.ID,
		Name:      name,
		Type:      nodeType,
		BlockSize: blockSize,
		ChildCID:  childCIDs,
	}
	err = ctx.DB().Create(node).Error
	require.NoError(t, err)

	// Create IPFSLinkedBlock records for each child
	for i, childCID := range childCIDs {
		// Create or get the child block
		var childBlock *pluginDb.IPFSBlock
		err = ctx.DB().Where("cid = ?", childCID.Bytes()).First(&childBlock).Error
		if err != nil {
			childBlock = &pluginDb.IPFSBlock{
				CID:  childCID.Bytes(),
				Size: 0, // Size will be updated when child block is properly created
			}
			err = ctx.DB().Create(childBlock).Error
			require.NoError(t, err)
		}

		// Create the link record
		link := &pluginDb.IPFSLinkedBlock{
			ParentID:  block.ID,
			ChildID:   childBlock.ID,
			LinkIndex: i,
		}
		err = ctx.DB().Create(link).Error
		require.NoError(t, err)
	}

	return block, node
}
