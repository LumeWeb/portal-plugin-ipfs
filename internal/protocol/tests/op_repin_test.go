package tests

import (
	"bytes"
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	contentArchive "go.lumeweb.com/ipfs-content/archive"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
	pluginTusUtils "go.lumeweb.com/portal-plugin-ipfs/internal/testing/tus"
	pluginUpload "go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

type repinUploadFunc func(t *testing.T, ctx coreTesting.TestContext) cid.Cid

type repinIterationFunc func(t *testing.T, ctx coreTesting.TestContext, root cid.Cid, i int)

func testRepinCycle(t *testing.T, ctx coreTesting.TestContext, upload repinUploadFunc, iterationFuncs ...repinIterationFunc) {
	var root cid.Cid

	for i := range 2 {
		reRoot := upload(t, ctx)

		if i == 0 {
			root = reRoot
		} else {
			require.True(t, encoding.NormalizeCid(root).Equals(encoding.NormalizeCid(reRoot)), "Re-uploaded root CID should match original: expected %s, got %s", root, reRoot)
		}

		for _, fn := range iterationFuncs {
			fn(t, ctx, root, i)
		}
	}
}

func assertRepinBlockState(t *testing.T, ctx coreTesting.TestContext, root cid.Cid, _ int) {
	assertAllBlocksFetchable(t, ctx, root)
	assertAllBlocksReady(t, root)
}

func deletePinAfterFirst(t *testing.T, ctx coreTesting.TestContext, root cid.Cid, i int) {
	deletePinByCID(t, ctx, root)
}

func collectAllBlocks(t *testing.T, root cid.Cid) []cid.Cid {
	metadataStore := getMetadataStore(t)

	visited := map[cid.Cid]bool{}
	var all []cid.Cid
	queue := []cid.Cid{root}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		normalized := encoding.NormalizeCid(cur)
		if visited[normalized] {
			continue
		}
		visited[normalized] = true
		all = append(all, normalized)

		children, err := metadataStore.BlockChildren(context.Background(), normalized, nil)
		if err != nil {
			continue
		}
		queue = append(queue, children...)
	}

	return all
}

func assertAllBlocksFetchable(t *testing.T, ctx coreTesting.TestContext, root cid.Cid) {
	t.Helper()

	proto := core.GetProtocol(internal.ProtocolName)
	require.NotNil(t, proto, "IPFS protocol not found")

	nodeProto, ok := proto.(protocol.ProtoNode)
	require.True(t, ok, "protocol does not implement ProtoNode")

	ipfsNode := nodeProto.GetNode()
	require.NotNil(t, ipfsNode, "IPFS node not available")

	allBlocks := collectAllBlocks(t, root)
	require.NotEmpty(t, allBlocks, "No blocks found for root CID %s", root)

	fetchCtx := pc.SkipQuotaCheckOption(ctx, true)
	for _, c := range allBlocks {
		node, err := ipfsNode.GetBlock(fetchCtx, c)
		require.NoError(t, err, "Block %s should be fetchable", c)
		require.NotNil(t, node, "Fetched block %s node is nil", c)
	}
}

func assertAllBlocksReady(t *testing.T, root cid.Cid) {
	t.Helper()

	metadataStore := getMetadataStore(t)
	allBlocks := collectAllBlocks(t, root)
	require.NotEmpty(t, allBlocks, "No blocks found for root CID %s", root)

	for _, c := range allBlocks {
		err := metadataStore.BlockExists(context.Background(), c)
		require.NoError(t, err, "Block %s should exist and be Ready=true", c)
	}
}

func getMetadataStore(t *testing.T) pluginCore.MetadataStore {
	t.Helper()

	proto := core.GetProtocol(internal.ProtocolName)
	require.NotNil(t, proto, "IPFS protocol not found")

	nodeProto, ok := proto.(protocol.ProtoNode)
	require.True(t, ok, "protocol does not implement ProtoNode")

	metadataStore := nodeProto.GetMetadataStore()
	require.NotNil(t, metadataStore, "Metadata store not available")

	return metadataStore
}

func buildReader(t *testing.T, ctx coreTesting.TestContext, tc repinFormatCase) *pluginUpload.UniversalReader {
	if tc.creator != nil {
		testFiles := pluginUpload.GetDefaultTestFiles()
		archiveData := tc.creator(t, ctx, testFiles)
		return pluginUpload.NewUniversalReader(bytes.NewReader(archiveData))
	}
	return pluginUpload.NewUniversalReader(bytes.NewReader([]byte("Test content for repin after delete verification")))
}

type repinFormatCase struct {
	name    string
	format  contentArchive.Format
	creator pluginUpload.ArchiveCreator
}

func TestPostUploadOperation_RepinAfterDelete(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		formats := []repinFormatCase{
			{"Plain file", contentArchive.FormatFile, nil},
			{"CAR file", contentArchive.FormatCAR, pluginUpload.CreateCARArchive},
			{"ZIP archive", contentArchive.FormatZIP, pluginUpload.CreateZIPArchive},
		}

		for _, tc := range formats {
			t.Run(tc.name, func(t *testing.T) {
				var user *models.User
				var wfTest *coreTesting.WorkflowTest

				upload := func(t *testing.T, ctx coreTesting.TestContext) cid.Cid {
					reader := buildReader(t, ctx, tc)
					if user == nil {
						root, u, w := testPostUploadWithUser(t, ctx, reader, tc.format, pluginUpload.ArchiveConvert)
						user = u
						wfTest = w
						return root
					}
					root, _ := testPostUploadWorkflow(t, ctx, reader, tc.format, pluginUpload.ArchiveConvert,
						withExistingUser(user),
						withExistingWorkflow(wfTest),
					)
					return root
				}

				testRepinCycle(t, ctx, upload, assertRepinBlockState, deletePinAfterFirst)
			})
		}
	}, GetStandardTestOptions()...)
}

func TestTUSUploadOperation_RepinAfterDelete(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		var userID uint

		upload := func(t *testing.T, ctx coreTesting.TestContext) cid.Cid {
			var opts []pluginTusUtils.TusUploadOption
			if userID != 0 {
				u := &models.User{}
				u.ID = userID
				opts = append(opts, pluginTusUtils.WithExistingUser(u))
			}
			root, reUserID := runTUSFileUploadInternal(t, ctx, "Test content for TUS repin after delete verification", opts...)
			userID = reUserID
			return root
		}

		testRepinCycle(t, ctx, upload, assertRepinBlockState, deletePinAfterFirst)
	}, GetStandardTestOptions()...)
}

func TestPostUploadOperation_PinDeletePinStatusConsistency(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		pinSvc := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
		require.NotNil(t, pinSvc, "Pin service not available")

		sort := []filter.Sort{
			{Field: "created_at", Order: filter.OrderDesc},
		}

		var user *models.User
		var wfTest *coreTesting.WorkflowTest

		upload := func(t *testing.T, ctx coreTesting.TestContext) cid.Cid {
			reader := pluginUpload.NewUniversalReader(bytes.NewReader([]byte("Test content for pin/delete/pin status consistency")))
			if user == nil {
				root, u, w := testPostUploadWithUser(t, ctx, reader, contentArchive.FormatFile, pluginUpload.ArchiveConvert)
				user = u
				wfTest = w
				return root
			}
			root, _ := testPostUploadWorkflow(t, ctx, reader, contentArchive.FormatFile, pluginUpload.ArchiveConvert,
				withExistingUser(user),
				withExistingWorkflow(wfTest),
			)
			return root
		}

		assertPinStatus := func(t *testing.T, ctx coreTesting.TestContext, _ cid.Cid, _ int) {
			pins, _, err := pinSvc.ListPins(ctx, nil, sort, queryutil.DefaultPagination)
			require.NoError(t, err)
			require.NotEmpty(t, pins)
			require.Equal(t, db.PinningStatusPinned, pins[0].Status, "Pin status should be pinned")
		}

		testRepinCycle(t, ctx, upload, assertPinStatus, deletePinAfterFirst)
	}, GetStandardTestOptions()...)
}

func testPostUploadWithUser(t *testing.T, ctx coreTesting.TestContext, universalReader *pluginUpload.UniversalReader, format contentArchive.Format, mode pluginUpload.ArchiveMode) (cid.Cid, *models.User, *coreTesting.WorkflowTest) {
	testUser := setupTestUser(t, ctx, format, mode)
	root, wfTest := testPostUploadWorkflow(t, ctx, universalReader, format, mode, withExistingUser(testUser))
	return root, testUser, wfTest
}

func deletePinByCID(t *testing.T, ctx coreTesting.TestContext, rootCID cid.Cid) {
	pinSvc := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
	require.NotNil(t, pinSvc, "Pin service not available")

	sort := []filter.Sort{
		{Field: "created_at", Order: filter.OrderDesc},
	}
	pins, total, err := pinSvc.ListPins(ctx, nil, sort, queryutil.DefaultPagination)
	require.NoError(t, err, "Failed to list pins for deletion")
	require.NotZero(t, total, "No pins found to delete")
	require.NotEmpty(t, pins, "No pins found to delete")

	normalizedRoot := encoding.NormalizeCid(rootCID)
	var targetPin *db.IPFSPin
	for _, p := range pins {
		pCID, castErr := cid.Cast(p.CID)
		if castErr == nil && encoding.NormalizeCid(pCID).Equals(normalizedRoot) {
			targetPin = p
			break
		}
	}
	require.NotNil(t, targetPin, "No pin found for root CID %s", rootCID)

	err = pinSvc.DeletePin(ctx, targetPin.RequestID)
	require.NoError(t, err, "Failed to delete pin for CID %s", rootCID)
}
