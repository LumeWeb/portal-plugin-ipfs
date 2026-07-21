package tests

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/ipfs/boxo/bitswap"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/dag"
	pluginUpload "go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"

	// Import required packages for UnixFS generation
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	contentUnixFS "go.lumeweb.com/ipfs-content/unixfs"
)

// TestSecondNodeInfo holds information about test data representing a second IPFS node
type TestSecondNodeInfo struct {
	FileCID  cid.Cid
	Blocks   []blocks.Block // All blocks in the DAG
	NodeData []byte         // Raw data for the root block
	PeerID   peer.ID        // The peer ID of the second node
	Addrs    []string       // Listen addresses of the second node
	Cleanup  func()
}

// setupTestSecondNode creates a libp2p peer with test data representing a second IPFS node
func setupTestSecondNode(t *testing.T) *TestSecondNodeInfo {
	// Create large content to ensure multi-block file (more than typical 256KB chunk)
	fileContent := strings.Repeat("Multi-block UnixFS test data for retrieve operation. ", 10000)

	// Generate a new Ed25519 private key for the second node (same as protocol node)
	privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 2048)
	require.NoError(t, err, "Failed to generate private key")

	// Create libp2p host for the second peer
	secondHost, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	require.NoError(t, err, "Failed to create libp2p host")

	// Collect listen addresses
	var addrs []string
	for _, addr := range secondHost.Addrs() {
		addrs = append(addrs, addr.String())
	}

	// Create in-memory datastore and blockstore for the second peer
	secondDstore := dssync.MutexWrap(ds.NewMapDatastore())
	secondBstore := blockstore.NewBlockstore(secondDstore)

	// Create LAN mode DHT for the test node (matching production configuration)
	dhtOpts := []dht.Option{
		dht.Mode(dht.ModeServer),
		dht.Datastore(secondDstore),
		dht.ProtocolExtension("/lan"),
	}
	testDHT, err := dht.New(secondHost, dhtOpts...)
	require.NoError(t, err, "Failed to create test DHT")

	// Create bitswap exchange using bsnet.NewFromIpfsHost for network functionality
	bitswapNet := bsnet.NewFromIpfsHost(secondHost)
	secondBitswap := bitswap.New(context.Background(), bitswapNet, testDHT, secondBstore)

	secondBserv := blockservice.New(secondBstore, secondBitswap)
	secondDserv := merkledag.NewDAGService(secondBserv)

	// Use UnixFSNodeGenerator to create a multi-block UnixFS file
	nodeGenerator := contentUnixFS.NewUnixFSNodeGenerator(
		contentUnixFS.WithUnixFSNodeDAGService(secondDserv),
		contentUnixFS.WithUnixFSNodeBlockstore(secondBstore),
	)

	// Create the UnixFS node
	seekableFile := pluginUpload.NewUniversalReader(bytes.NewReader([]byte(fileContent)))
	rootNode, err := nodeGenerator.CreateNode(context.Background(), seekableFile)
	require.NoError(t, err, "Failed to create UnixFS node")

	rootCID := rootNode.Cid()

	// Collect all blocks from the DAG
	ctx := context.Background()
	var collectedBlocks []blocks.Block
	var mu sync.Mutex

	opts := &dag.WalkDAGOptions{
		NormalizeCID: false,
		Concurrent:   true,
		IgnoreErrors: false,
		Logger:       nil,
	}

	err = dag.WalkDAG(ctx, secondDserv, rootCID, func(_ context.Context, c cid.Cid, node *merkledag.ProtoNode) error {
		block, err := blocks.NewBlockWithCid(node.RawData(), c)
		if err != nil {
			return err
		}
		mu.Lock()
		collectedBlocks = append(collectedBlocks, block)
		mu.Unlock()
		return nil
	}, opts)
	require.NoError(t, err, "Failed to walk DAG")

	// Verify all blocks are in the blockstore after DAG walk
	for _, block := range collectedBlocks {
		hasBlock, err := secondBstore.Has(ctx, block.Cid())
		require.NoError(t, err, "Failed to check if block %s exists in blockstore", block.Cid())
		require.True(t, hasBlock, "Block %s should be in blockstore after DAG walk", block.Cid())
	}

	return &TestSecondNodeInfo{
		FileCID: rootCID,
		Blocks:  collectedBlocks,
		PeerID:  secondHost.ID(),
		Addrs:   addrs,
		Cleanup: func() {
			testDHT.Close()
			secondHost.Close()
			secondDstore.Close()
		},
	}
}

func TestRetrieveOperationHandler_Execute_Integration(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange - Setup test data from "second node"
		secondNodeData := setupTestSecondNode(t)
		defer secondNodeData.Cleanup()

		// Get the IPFS protocol
		proto := core.GetProtocol(internal.ProtocolName)
		require.NotNil(tb, proto)

		nodeProto, ok := proto.(protocol.ProtoNode)
		require.True(tb, ok)

		ipfsNode := nodeProto.GetNode()
		require.NotNil(tb, ipfsNode)

		// Add the second peer as a bootstrap peer using the node factory
		// This allows the node to bootstrap to the second node for retrieval
		nodeFactory := nodeProto.GetNodeFactory()
		require.NotNil(tb, nodeFactory)

		// Clear existing bootstrap peers to use only our test node
		nodeFactory.ClearBootstrapPeers()

		peerInfo := peer.AddrInfo{
			ID: secondNodeData.PeerID,
		}
		for _, addrStr := range secondNodeData.Addrs {
			// Parse the base address and encapsulate with the peer ID
			baseAddr, addrErr := multiaddr.NewMultiaddr(addrStr)
			require.NoError(tb, addrErr, "Failed to parse base address %s", addrStr)

			// Build the P2P multiaddr protocol component
			peerIDAddr, addrErr := multiaddr.NewMultiaddr("/p2p/" + secondNodeData.PeerID.String())
			require.NoError(tb, addrErr, "Failed to create P2P multiaddr component")

			// Encapsulate the base address with the peer ID
			fullAddr := baseAddr.Encapsulate(peerIDAddr)
			peerInfo.Addrs = append(peerInfo.Addrs, fullAddr)
		}
		nodeFactory.AddBootstrapPeer(peerInfo)

		// Restart the node to pick up the new bootstrap peer
		err := nodeProto.RestartNode()
		require.NoError(tb, err, "Failed to restart node with new bootstrap peer")

		// Get the updated node reference after restart
		ipfsNode = nodeProto.GetNode()

		// Ensure blocks are NOT in the main blockstore before retrieval
		// This is critical for the regression test - we want to verify blocks ARE stored
		for _, block := range secondNodeData.Blocks {
			hasBlock, _ := ipfsNode.HasBlock(context.Background(), block.Cid())
			if hasBlock {
				// Block exists - this shouldn't happen in a proper test setup
				tb.Fatalf("Block %s already in blockstore, test setup is invalid", block.Cid())
			}
		}

		// Create test user
		userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
		testUser, err := userSvc.CreateAccount(context.Background(), "test-retrieve@example.com", "testpassword123", false)
		require.NoError(tb, err)

		// Create pin request with the CID from "second node"
		model, err := dto.PinRequest{
			CID: secondNodeData.FileCID.String(),
		}.ToModel()
		require.NoError(tb, err)

		pinService := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
		require.NotNil(tb, pinService)

		// Create a WorkflowTest instance
		wfTest := coreTesting.NewWorkflowTest(ctx)
		wfTest.DisableWorkflow(protocol.PIN_WORKFLOW)

		// Act - Add the pin
		_pin, err := pinService.AddPin(context.Background(), model)
		require.NoError(tb, err)

		// Get the operation name
		operationName := core.RetrieveOperationName(internal.ProtocolName)

		// Start the workflow
		req := wfTest.StartOperationWorkflow(operationName,
			core.WithWorkflowStructData(protocol.PinWorkflowData{
				PinRequestID: _pin.RequestID.ToUUID(),
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(secondNodeData.FileCID)),
			core.WithWorkflowUserID(testUser.ID),
			core.WithWorkflowSourceIP("127.0.0.1"))

		// Execute the workflow step
		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// Assertions
		wfTest.AssertOperationSuccess(req)
		wfTest.AssertOperationStatusMessageContains(req, "Finalizing Retrieve")
		wfTest.AssertOperationStatusProgress(req, 100)

		// REGRESSION CHECK: Verify all blocks are actually stored in blockstore
		// This test ensures the bug where virtual collection happened but blocks weren't stored
		// does not reoccur. After retrieve operation, all DAG blocks must be in blockstore.
		for _, block := range secondNodeData.Blocks {
			hasBlock, err := ipfsNode.HasBlock(context.Background(), block.Cid())
			require.NoError(tb, err, "Failed to check if block exists in blockstore")
			require.True(tb, hasBlock, "Block %s should be stored in blockstore after retrieve operation", block.Cid())
		}

	}, coreTesting.CombineOptions(
		GetStandardTestOptions(),
		coreTesting.WithConfig("plugin.ipfs.protocol.dht_mode", "basic"),
	))
}
