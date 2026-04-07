package api

// Note: These tests use helper.makeAuthenticatedRequest() or helper.makeRequest() which internally
// uses ctx.Router() and httpSvc.APISubdomain() as required by the specification.
// This maintains consistency with api_pins_test.go, api_files_test.go, and
// api_upload_test.go which all use the same helper pattern.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	protocol "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/mock_tests"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/db/models"
	"gorm.io/gorm"
)

func TestAPI_handleGetBlockMeta(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _, testCID, _ := helper.SetupAuthenticatedTest()

		rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/api/block/meta/%s", testCID.String()), token, nil)

		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.BlockMetaResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)

		assert.NotNil(t, response)
		assert.IsType(t, "", response.Name)
		assert.IsType(t, dto.UnixFSType(0), response.Type)
		assert.IsType(t, uint64(0), response.BlockSize)
		assert.IsType(t, int64(0), response.UnixFSSize)
		assert.IsType(t, []string{}, response.ChildCID)
		assert.True(t, len(response.ChildCID) > 0, "ChildCID should not be empty")
	}, TestOptions)
}

func TestAPI_handleGetBlockMetaBatch(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _, testCID, _ := helper.SetupAuthenticatedTest()

		reqBody := fmt.Sprintf(`{"cid":["%s"]}`, testCID.String())
		rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/block/meta/batch", token, []byte(reqBody))

		assert.Equal(t, http.StatusOK, rec.Code)
		var response map[string]*dto.BlockMetaResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)

		assert.NotEmpty(t, response)
		for cidKey, meta := range response {
			assert.NotEmpty(t, cidKey)
			assert.NotNil(t, meta)
			assert.IsType(t, "", meta.Name)
			assert.IsType(t, dto.UnixFSType(0), meta.Type)
			assert.IsType(t, uint64(0), meta.BlockSize)
			assert.IsType(t, int64(0), meta.UnixFSSize)
			assert.IsType(t, []string{}, meta.ChildCID)
			assert.True(t, len(meta.ChildCID) > 0, "ChildCID should not be empty")
		}
	}, TestOptions)
}

func TestAPI_handleGetInfo(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)

		rec := helper.makeRequest(http.MethodGet, "/api/info", nil)

		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.InfoResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)

		assert.NotEmpty(t, response.PeerID)

		// AnnouncementAddresses may be empty in test environments if no suitable network
		// addresses are available (e.g., only loopback addresses which get filtered out)
		if len(response.AnnouncementAddresses) > 0 {
			for _, addr := range response.AnnouncementAddresses {
				assert.Contains(t, addr, "/ip6/")
			}
		}

		// ConnectionAddresses may be empty in test environments for the same reason
		if len(response.ConnectionAddresses) > 0 {
			for _, addr := range response.ConnectionAddresses {
				assert.Contains(t, addr, "/ip6/")
				assert.Contains(t, addr, "/p2p/")
				assert.Contains(t, addr, response.PeerID)
			}
		}
	}, TestOptions)
}

func TestAPI_handleIPFSGet(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _, testCID, _ := helper.SetupAuthenticatedTest()

		// Setup IPFS node mock expectations for HasBlock
		protoMock := core.GetProtocol(internal.ProtocolName).(*protocol.MockProtoNode)
		mockIPFSNode := protoMock.GetNode().(*mocks.MockIPFSNode)

		// Mock HasBlock to return true for the test CID
		mockIPFSNode.EXPECT().HasBlock(mock.Anything, testCID).Return(true, nil)

		uploadSvc := core.GetService[*coreMocks.MockUploadService](ctx, core.UPLOAD_SERVICE)
		testUpload := &models.Upload{
			Model:    gorm.Model{ID: 1},
			UserID:   1,
			Hash:     testCID.Hash(),
			CIDType:  1, // CIDv1
			MimeType: "application/octet-stream",
			Protocol: "ipfs",
			Size:     1024,
		}
		uploadSvc.EXPECT().GetUpload(mock.Anything, internal.NewIPFSHash(testCID)).Return(testUpload, nil)

		// Mock GetBlock to return a mock node for the test CID
		testData := []byte("tornadocash")
		mockNode := merkledag.NewRawNode(testData)
		mockIPFSNode.EXPECT().GetBlock(mock.Anything, testCID).Return(mockNode, nil)

		rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/ipfs/%s", testCID.String()), token, nil)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "tornadocash")
	}, TestOptions)
}
