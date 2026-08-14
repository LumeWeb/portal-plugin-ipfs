package tests

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/ipfs/boxo/bitswap"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	dht "github.com/libp2p/go-libp2p-kad-dht"
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
	contentUnixFS "go.lumeweb.com/ipfs-content/unixfs"
)

// TestRetrieveSingleBlockPopulatesWorkflowCids is a regression test for the
// single-block DAG bug. Before the fix, op_retrieve.go only persisted
// workflowData.Cids inside the `if len(childCids) > 0` branch, so a root-only
// DAG (no child CIDs) left Cids empty. ConfirmOperationHandler then failed
// with "no CIDs to confirm" and the workflow step retried forever.
//
// This test pins a single-block CID via the retrieve workflow, then asserts
// the workflow data Cids field is populated with the root CID so confirm can
// proceed.
func TestRetrieveSingleBlockPopulatesWorkflowCids(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange - second node hosting a small (single-block) file
		secondNode := setupSingleBlockTestNode(t, "single block content")
		defer secondNode.Cleanup()

		proto := core.GetProtocol(internal.ProtocolName)
		require.NotNil(tb, proto)

		nodeProto, ok := proto.(protocol.ProtoNode)
		require.True(tb, ok)

		ipfsNode := nodeProto.GetNode()
		require.NotNil(tb, ipfsNode)

		// Point the node's bootstrap peers at the second node for retrieval
		nodeFactory := nodeProto.GetNodeFactory()
		require.NotNil(tb, nodeFactory)

		nodeFactory.ClearBootstrapPeers()
		peerInfo := peer.AddrInfo{ID: secondNode.PeerID}
		for _, addrStr := range secondNode.Addrs {
			baseAddr, addrErr := multiaddr.NewMultiaddr(addrStr)
			require.NoError(tb, addrErr)
			peerIDAddr, addrErr := multiaddr.NewMultiaddr("/p2p/" + secondNode.PeerID.String())
			require.NoError(tb, addrErr)
			peerInfo.Addrs = append(peerInfo.Addrs, baseAddr.Encapsulate(peerIDAddr))
		}
		nodeFactory.AddBootstrapPeer(peerInfo)
		require.NoError(tb, nodeProto.RestartNode())

		// Ensure the block is NOT already in the local blockstore
		has, _ := ipfsNode.HasBlock(context.Background(), secondNode.RootCID)
		require.False(tb, has, "root block should not already be in blockstore")

		// Create a test user
		userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
		testUser, err := userSvc.CreateAccount(context.Background(), "single-block@example.com", "testpassword123", false)
		require.NoError(tb, err)

		// Create pin request with the single-block CID from the second node
		model, err := dto.PinRequest{
			CID: secondNode.RootCID.String(),
		}.ToModel()
		require.NoError(tb, err)

		pinService := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
		require.NotNil(tb, pinService)

		// Create a WorkflowTest instance, disabling PIN_WORKFLOW so we can drive
		// the retrieve operation step in isolation (mirrors existing retrieve test).
		wfTest := coreTesting.NewWorkflowTest(ctx)
		wfTest.DisableWorkflow(protocol.PIN_WORKFLOW)

		_pin, err := pinService.AddPin(context.Background(), model)
		require.NoError(tb, err)

		operationName := core.RetrieveOperationName(internal.ProtocolName)

		// Start the retrieve operation workflow
		req := wfTest.StartOperationWorkflow(operationName,
			core.WithWorkflowStructData(protocol.PinWorkflowData{
				PinRequestID: _pin.RequestID.ToUUID(),
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(secondNode.RootCID)),
			core.WithWorkflowUserID(testUser.ID),
			core.WithWorkflowSourceIP("127.0.0.1"))

		// Execute the retrieve step
		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		wfTest.AssertOperationSuccess(req)

		// Act - read back the workflow data to verify Cids was populated.
		var wd protocol.PinWorkflowData
		wdErr := workflowDataStruct(ctx, req.ID, &wd)
		require.NoError(tb, wdErr, "failed to read workflow data")

		// Assert - the root CID must be present so confirm can proceed.
		require.NotEmpty(tb, wd.Cids, "single-block DAG must populate workflow Cids, got empty (confirm would fail with 'no CIDs to confirm')")
		require.ElementsMatch(tb, []string{secondNode.RootCID.String()}, wd.Cids,
			"single-block DAG should persist exactly the root CID in workflow data")
	}, coreTesting.CombineOptions(
		GetStandardTestOptions(),
		coreTesting.WithConfig("plugin.ipfs.protocol.dht_mode", "basic"),
	))
}

// workflowDataStruct reads the workflow metadata for a request and unmarshals
// it into out (mirrors OperationHelperDefault.StructuredWorkflowData).
func workflowDataStruct(ctx coreTesting.TestContext, requestID uint, out any) error {
	wfSvc := core.GetService[core.WorkflowService](ctx, core.WORKFLOW_SERVICE)
	k, err := wfSvc.GetWorkflowMetadata(ctx, requestID)
	if err != nil {
		return err
	}
	return k.Unmarshal("", out)
}

// singleBlockNodeInfo holds the minimal info needed to expose a single-block
// DAG on a second (remote) IPFS node.
type singleBlockNodeInfo struct {
	RootCID cid.Cid
	PeerID  peer.ID
	Addrs   []string
	Cleanup func()
}

// setupSingleBlockTestNode creates a libp2p peer hosting a single-block file
// (small content that produces a DAG with no child CIDs).
func setupSingleBlockTestNode(t *testing.T, content string) *singleBlockNodeInfo {
	privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 2048)
	require.NoError(t, err, "Failed to generate private key")

	host, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	require.NoError(t, err, "Failed to create libp2p host")

	var addrs []string
	for _, addr := range host.Addrs() {
		addrs = append(addrs, addr.String())
	}

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)

	dhtOpts := []dht.Option{
		dht.Mode(dht.ModeServer),
		dht.Datastore(dstore),
		dht.ProtocolExtension("/lan"),
	}
	testDHT, err := dht.New(host, dhtOpts...)
	require.NoError(t, err, "Failed to create test DHT")

	bitswapNet := bsnet.NewFromIpfsHost(host)
	bitswap := bitswap.New(context.Background(), bitswapNet, testDHT, bstore)
	bserv := blockservice.New(bstore, bitswap)
	dserv := merkledag.NewDAGService(bserv)

	// Small content -> single block (no child CIDs).
	nodeGenerator := contentUnixFS.NewUnixFSNodeGenerator(
		contentUnixFS.WithUnixFSNodeDAGService(dserv),
		contentUnixFS.WithUnixFSNodeBlockstore(bstore),
	)
	rootNode, err := nodeGenerator.CreateNode(context.Background(), pluginUpload.NewUniversalReader(bytes.NewReader([]byte(content))))
	require.NoError(t, err, "Failed to create UnixFS node")

	rootCID := rootNode.Cid()

	// Collect and verify exactly one block (single-block DAG).
	ctx := context.Background()
	var collected []cid.Cid
	var mu sync.Mutex

	opts := &dag.WalkDAGOptions{
		NormalizeCID: false,
		Concurrent:   true,
		IgnoreErrors: false,
		Logger:       nil,
	}
	err = dag.WalkDAG(ctx, dserv, rootCID, func(_ context.Context, c cid.Cid, node *merkledag.ProtoNode) error {
		mu.Lock()
		collected = append(collected, c)
		mu.Unlock()
		return nil
	}, opts)
	require.NoError(t, err, "Failed to walk DAG")
	require.Len(t, collected, 1, "single-block DAG should have exactly one block, got %d", len(collected))
	if len(collected) == 0 {
		t.Fatal("single-block DAG produced no blocks")
	}

	return &singleBlockNodeInfo{
		RootCID: rootCID,
		PeerID:  host.ID(),
		Addrs:   addrs,
		Cleanup: func() {
			testDHT.Close()
			host.Close()
			dstore.Close()
		},
	}
}
