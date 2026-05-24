package api

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	protocol "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/mock_tests"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestAPI_RoutingGetIPNS(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			protoMock := core.GetProtocol(internal.ProtocolName).(*protocol.MockProtoNode)
			mockIPFSNode := protoMock.GetNode().(*mocks.MockIPFSNode)
			mockPublisher := mockIPFSNode.GetPublisher().(*mocks.MockIPNSPublisher)

			mockRecord := createMockIPNSRecord(t, TestCID)
			mockPublisher.EXPECT().GetPublished(mock.Anything, mock.AnythingOfType("ipns.Name"), false).Return(mockRecord, nil)

			rec := helper.makeRequest(http.MethodGet, fmt.Sprintf("/routing/v1/ipns/%s", TestIPNSName), nil)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), "ipns-record")
			assert.NotEmpty(t, rec.Body.Bytes())
		}, TestOptions)
	})

	t.Run("not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			protoMock := core.GetProtocol(internal.ProtocolName).(*protocol.MockProtoNode)
			mockIPFSNode := protoMock.GetNode().(*mocks.MockIPFSNode)
			mockPublisher := mockIPFSNode.GetPublisher().(*mocks.MockIPNSPublisher)

			mockPublisher.EXPECT().GetPublished(mock.Anything, mock.AnythingOfType("ipns.Name"), false).Return(nil, nil)

			rec := helper.makeRequest(http.MethodGet, fmt.Sprintf("/routing/v1/ipns/%s", TestIPNSName), nil)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "not found")
		}, TestOptions)
	})

	t.Run("invalid_peer_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			rec := helper.makeRequest(http.MethodGet, "/routing/v1/ipns/invalid", nil)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})

	t.Run("raw_peer_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			protoMock := core.GetProtocol(internal.ProtocolName).(*protocol.MockProtoNode)
			mockIPFSNode := protoMock.GetNode().(*mocks.MockIPFSNode)
			mockPublisher := mockIPFSNode.GetPublisher().(*mocks.MockIPNSPublisher)

			mockRecord := createMockIPNSRecord(t, TestCID)
			mockPublisher.EXPECT().GetPublished(mock.Anything, mock.AnythingOfType("ipns.Name"), false).Return(mockRecord, nil)

			rec := helper.makeRequest(http.MethodGet, fmt.Sprintf("/routing/v1/ipns/%s", TestPeerID), nil)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), "ipns-record")
			assert.NotEmpty(t, rec.Body.Bytes())
		}, TestOptions)
	})
}

func TestAPI_RoutingFindProviders(t *testing.T) {
	t.Run("has_block", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			protoMock := core.GetProtocol(internal.ProtocolName).(*protocol.MockProtoNode)
			mockIPFSNode := protoMock.GetNode().(*mocks.MockIPFSNode)

			testCID := cid.MustParse(TestCID)
			mockIPFSNode.EXPECT().HasBlock(mock.Anything, testCID).Return(true, nil)

			defaultPeerID := mockIPFSNode.PeerID()

			rec := helper.makeRequest(http.MethodGet, fmt.Sprintf("/routing/v1/providers/%s", TestCID), nil)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.True(t,
				strings.Contains(rec.Header().Get("Content-Type"), "ndjson") ||
					strings.Contains(rec.Header().Get("Content-Type"), "json"),
				"Expected ndjson or json content-type, got: %s", rec.Header().Get("Content-Type"))
			assert.Contains(t, rec.Body.String(), defaultPeerID.String())
		}, TestOptions)
	})

	t.Run("no_block", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			protoMock := core.GetProtocol(internal.ProtocolName).(*protocol.MockProtoNode)
			mockIPFSNode := protoMock.GetNode().(*mocks.MockIPFSNode)

			testCID := cid.MustParse(TestCID)
			mockIPFSNode.EXPECT().HasBlock(mock.Anything, testCID).Return(false, nil)

			rec := helper.makeRequest(http.MethodGet, fmt.Sprintf("/routing/v1/providers/%s", TestCID), nil)

			assert.Equal(t, http.StatusOK, rec.Code)
		}, TestOptions)
	})

	t.Run("invalid_cid", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			rec := helper.makeRequest(http.MethodGet, "/routing/v1/providers/notacid", nil)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})
}
