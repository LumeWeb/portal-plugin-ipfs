package util

import (
	"errors"
	"strings"

	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	protocol "go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/db"
	"gorm.io/gorm"

	protomock "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/mock_tests"
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
		protoMock := protomock.NewMockProtoNode(ctx.T())
		protoMock.EXPECT().Name().Return(internal.ProtocolName).Maybe()
		protoMock.EXPECT().GetConfig().Return(&config.ProtocolConfig{}).Maybe()
		protoMock.EXPECT().Workflows().Return(nil).Maybe()

		// Create mocks for IPNS components
		pluginCoreMockPublisher := mocks.NewMockIPNSPublisher(ctx.T())
		mockKeystore := mocks.NewMockKeystore(ctx.T())
		mockDatastore := mocks.NewMockDatastore(ctx.T())
		
		// Set up keystore to handle Has() calls (needed for SyncToBoxoKeystore during startup)
		mockKeystore.EXPECT().Has(mock.Anything).Return(false, nil).Maybe()
		mockKeystore.EXPECT().Put(mock.Anything, mock.Anything).Return(nil).Maybe()
		mockKeystore.EXPECT().Get(mock.Anything).Return(nil, errors.New("key not found")).Maybe()
		mockKeystore.EXPECT().List().Return([]string{}, nil).Maybe()

		ipfsNode := mocks.NewMockIPFSNode(ctx.T())
		mockPeer := config.BootstrapPeers[0].ToAddrInfo()
		ipfsNode.EXPECT().PeerID().Return(mockPeer.ID).Maybe()
		ipfsNode.EXPECT().DelegateAddresses().Return(nil, nil).Maybe()
		protoMock.EXPECT().GetNode().Return(ipfsNode).Maybe()
		protoMock.EXPECT().GetIPNSNode().Return(ipfsNode).Maybe()
		ipfsNode.EXPECT().GetKeystore().Return(mockKeystore).Maybe()
		ipfsNode.EXPECT().GetPublisher().Return(pluginCoreMockPublisher).Maybe()
		ipfsNode.EXPECT().GetDatastore().Return(mockDatastore).Maybe()
		
		// Generate a valid private key for the republisher
		privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 2048)
		require.NoError(ctx.T(), err, "Failed to generate test private key")
		ipfsNode.EXPECT().GetPrivateKey().Return(privKey).Maybe()

		ipfsNode.EXPECT().ConnectionAddresses().Return(nil, nil).Maybe()
		ipfsNode.EXPECT().HostAddrs().Return(nil).Maybe()
		ipfsNode.EXPECT().AnnounceWeb().Return(false).Maybe()
		ipfsNode.EXPECT().AnnounceDomain().Return("").Maybe()

		// Mock AddBlock to return nil (success)
		ipfsNode.EXPECT().AddBlock(mock.Anything, mock.Anything).Return(nil).Maybe()

		// Mock GetMetadataStore to return a mock metadata store
		mockMetadataStore := mocks.NewMockMetadataStore(ctx.T())
		mockMetadataStore.EXPECT().Size(mock.Anything, mock.Anything).Return(uint64(0), nil).Maybe()
		protoMock.EXPECT().GetMetadataStore().Return(mockMetadataStore).Maybe()

		// Set up operations expectation - this will call Context() on the proto
		protoMock.EXPECT().Operations().RunAndReturn(func() []core.Operation {
			// Return real operations via NewProtocolOperations
			// Set up expectations that will be called when operations are created
			dummyProto := protomock.NewMockProtoNode(ctx.T())
			dummyProto.EXPECT().Name().Return(internal.ProtocolName).Maybe()
			dummyProto.EXPECT().Context().Return(ctx).Maybe()
			return protocol.NewProtocolOperations(dummyProto)
		}).Maybe()

		return protoMock
	})
}
