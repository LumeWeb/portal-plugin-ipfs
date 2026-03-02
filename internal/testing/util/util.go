package util

import (
	"errors"
	"strings"

	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/ipfs"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/db"
	"gorm.io/gorm"

	protocol "go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
)

// CalculateParentPath computes the parent directory path for a given file path
func CalculateParentPath(path string) string {
	if path == "/" || path == "" {
		return "/" // Root path should return "/", not empty string
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
	err := db.RetryableTransaction(ctx, ctx.DB(), func(g *gorm.DB) *gorm.DB {
		return g.Where("cid = ?", cid.Bytes()).First(&block)
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			block = &pluginDb.IPFSBlock{
				CID:  cid.Bytes(),
				Size: uint64(blockSize),
			}

			err = db.RetryableTransaction(ctx, ctx.DB(), func(g *gorm.DB) *gorm.DB {
				return g.Create(block)
			})

			require.NoError(t, err)
		} else {
			require.NoError(t, err)
		}
	}

	// Create the UnixFS node
	node := &pluginDb.UnixFSNode{
		BlockID:   block.ID,
		Name:      name,
		Type:      nodeType,
		BlockSize: blockSize,
		ChildCID:  childCIDs,
	}
	err = db.RetryableTransaction(ctx, ctx.DB(), func(g *gorm.DB) *gorm.DB {
		return g.Create(node)
	})
	require.NoError(t, err)

	// Create IPFSLinkedBlock records for each child
	for i, childCID := range childCIDs {
		// Create or get the child block
		var childBlock *pluginDb.IPFSBlock
		err = db.RetryableTransaction(ctx, ctx.DB(), func(g *gorm.DB) *gorm.DB {
			return g.Where("cid = ?", childCID.Bytes()).First(&childBlock)
		})
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				childBlock = &pluginDb.IPFSBlock{
					CID:  childCID.Bytes(),
					Size: 0, // Size will be updated when child block is properly created
				}
				err = db.RetryableTransaction(ctx, ctx.DB(), func(g *gorm.DB) *gorm.DB {
					return g.Create(childBlock)
				})
				require.NoError(t, err)
			} else {
				require.NoError(t, err)
			}
		}

		// Create the link record
		link := &pluginDb.IPFSLinkedBlock{
			ParentID:  block.ID,
			ChildID:   childBlock.ID,
			LinkIndex: i,
		}
		err = db.RetryableTransaction(ctx, ctx.DB(), func(g *gorm.DB) *gorm.DB {
			return g.Create(link)
		})
		require.NoError(t, err)
	}

	return block, node
}

func GetProtocolMock() coreTesting.TestContextBuilderOption {
	return coreTesting.WithCustomMockProtocol(internal.ProtocolName, func(ctx coreTesting.TestContext) core.Protocol {
		protoMock := protocol.NewMockProtoNode(ctx.T())
		protoMock.EXPECT().GetConfig().Return(&config.ProtocolConfig{}).Maybe()
		protoMock.EXPECT().Workflows().Return(nil).Maybe()
		ipfsNode := mocks.NewMockIPFSNode(ctx.T())
		mockPeer := config.BootstrapPeers[0].ToAddrInfo()
		ipfsNode.EXPECT().PeerID().Return(mockPeer.ID).Maybe()
		ipfsNode.EXPECT().DelegateAddresses().Return(ipfs.ConnectionAddresses(ipfsNode)).Maybe()
		protoMock.EXPECT().GetNode().Return(ipfsNode).Maybe()

		ipfsNode.EXPECT().ConnectionAddresses().RunAndReturn(func() ([]multiaddr.Multiaddr, error) {
			return ipfs.ConnectionAddresses(ipfsNode)
		}).Maybe()

		return protoMock
	})
}
