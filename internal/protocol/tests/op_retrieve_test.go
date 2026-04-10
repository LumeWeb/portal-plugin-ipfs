package tests

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/exchange/offline"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
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
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	contentUnixFS "go.lumeweb.com/ipfs-content/unixfs"
)

// TestSecondNodeInfo holds information about test data representing a second IPFS node
type TestSecondNodeInfo struct {
	FileCID  cid.Cid
	Blocks   []blocks.Block // All blocks in the DAG
	NodeData []byte         // Raw data for the root block
	Cleanup  func()
}

// setupTestSecondNode creates test data that simulates a multi-block UnixFS file from a "second node"
func setupTestSecondNode(t *testing.T) *TestSecondNodeInfo {
	// Create large content to ensure multi-block file (more than typical 256KB chunk)
	fileContent := strings.Repeat("Multi-block UnixFS test data for retrieve operation. ", 10000)

	// Create in-memory components
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := blockservice.New(bstore, offline.Exchange(bstore))
	dserv := merkledag.NewDAGService(bserv)

	// Use UnixFSNodeGenerator to create a multi-block UnixFS file
	nodeGenerator := contentUnixFS.NewUnixFSNodeGenerator(
		contentUnixFS.WithUnixFSNodeDAGService(dserv),
		contentUnixFS.WithUnixFSNodeBlockstore(bstore),
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

	err = dag.WalkDAG(ctx, dserv, rootCID, func(_ context.Context, c cid.Cid, node *merkledag.ProtoNode) error {
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

	return &TestSecondNodeInfo{
		FileCID: rootCID,
		Blocks:  collectedBlocks,
		Cleanup: func() { dstore.Close() },
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

		// Add all blocks from the "second node" to the primary node's blockstore
		// This simulates fetching from a peer
		for _, block := range secondNodeData.Blocks {
			err := ipfsNode.AddBlock(context.Background(), block)
			require.NoError(tb, err, "Failed to add block to IPFS node")
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

	}, GetStandardTestOptions()...)
}
