package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ipfs/boxo/ipns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCoreCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// AdminTestOptions provides test configuration for admin extension tests
var AdminTestOptions = coreTesting.CombineOptions(
	testPluginOptions,
	coreTesting.WithAPIExtension(NewAdminExtension()),
	coreTesting.WithAPIID("admin"),
)

func TestAdminExtension_TargetAPI(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		extFactory := NewAdminExtension()
		ext, _, err := extFactory()
		assert.NoError(t, err)

		adminExt := ext.(*AdminExtension)

		assert.Equal(t, "admin", adminExt.TargetAPI())
	}, AdminTestOptions)
}

func TestAdminExtension_BlockWebsite_Success(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCoreCore.WEBSITE_SERVICE)
		websiteID := uint(123)

		websiteService.EXPECT().BlockWebsite(mock.Anything, websiteID).Return(nil)

		req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/123/block", nil)
		rec := httptest.NewRecorder()

		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	}, AdminTestOptions)
}

func TestAdminExtension_BlockWebsite_InvalidID(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/invalid/block", nil)
		rec := httptest.NewRecorder()

		ctx.Router().ServeHTTP(rec, req)

		assert.NotEqual(t, http.StatusOK, rec.Code)
	}, AdminTestOptions)
}

func TestAdminExtension_BlockWebsite_ServiceError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCoreCore.WEBSITE_SERVICE)
		websiteID := uint(123)

		websiteService.EXPECT().BlockWebsite(mock.Anything, websiteID).Return(assert.AnError)

		req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/123/block", nil)
		rec := httptest.NewRecorder()

		ctx.Router().ServeHTTP(rec, req)

		assert.NotEqual(t, http.StatusOK, rec.Code)
	}, AdminTestOptions)
}

func TestAdminExtension_UnblockWebsite_Success(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCoreCore.WEBSITE_SERVICE)
		websiteID := uint(123)

		websiteService.EXPECT().UnblockWebsite(mock.Anything, websiteID).Return(nil)

		req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/123/unblock", nil)
		rec := httptest.NewRecorder()

		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	}, AdminTestOptions)
}

func TestAdminExtension_UnblockWebsite_InvalidID(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/invalid/unblock", nil)
		rec := httptest.NewRecorder()

		ctx.Router().ServeHTTP(rec, req)

		assert.NotEqual(t, http.StatusOK, rec.Code)
	}, AdminTestOptions)
}

func TestAdminExtension_UnblockWebsite_ServiceError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCoreCore.WEBSITE_SERVICE)
		websiteID := uint(123)

		websiteService.EXPECT().UnblockWebsite(mock.Anything, websiteID).Return(assert.AnError)

		req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/123/unblock", nil)
		rec := httptest.NewRecorder()

		ctx.Router().ServeHTTP(rec, req)

		assert.NotEqual(t, http.StatusOK, rec.Code)
	}, AdminTestOptions)
}

func TestAdminExtension_RepublishIPNS(t *testing.T) {
	t.Run("success_all_keys", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			mockIPNSKeyService := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCoreCore.IPNS_KEY_SERVICE)

			mockRecord := createMockIPNSRecord(t, TestCID)
			ipnsName, _ := ipns.NameFromString(TestIPNSName)
			records := map[ipns.Name]*ipns.Record{
				ipnsName: mockRecord,
			}

			mockIPNSKeyService.EXPECT().ListPublished(mock.Anything).Return(records, nil)
			mockIPNSKeyService.EXPECT().GetPrivateKeyByPeerID(mock.Anything, ipnsName.Peer().String()).Return(nil, uint(1), nil).Times(1)
			mockIPNSKeyService.EXPECT().PublishWithKey(mock.Anything, nil, TestCID, mock.AnythingOfType("time.Duration")).Return(nil)

			req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/ipns/republish", nil)
			rec := httptest.NewRecorder()

			ctx.Router().ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.IPNSRepublishResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, 1, response.Count)
		}, AdminTestOptions)
	})

	t.Run("error_list_published_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			mockIPNSKeyService := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCoreCore.IPNS_KEY_SERVICE)

			mockIPNSKeyService.EXPECT().ListPublished(mock.Anything).Return(nil, errors.New("republish failed"))

			req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/ipns/republish", nil)
			rec := httptest.NewRecorder()

			ctx.Router().ServeHTTP(rec, req)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, AdminTestOptions)
	})

	t.Run("regression_path_extraction_bulk_keys", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			mockIPNSKeyService := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCoreCore.IPNS_KEY_SERVICE)

			mockRecord := createMockIPNSRecord(t, TestCID)
			ipnsName, _ := ipns.NameFromString(TestIPNSName)
			records := map[ipns.Name]*ipns.Record{
				ipnsName: mockRecord,
			}

			mockIPNSKeyService.EXPECT().ListPublished(mock.Anything).Return(records, nil)
			mockIPNSKeyService.EXPECT().GetPrivateKeyByPeerID(mock.Anything, ipnsName.Peer().String()).Return(nil, uint(1), nil).Times(1)
			mockIPNSKeyService.EXPECT().PublishWithKey(mock.Anything, nil, TestCID, mock.AnythingOfType("time.Duration")).Return(nil)

			req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/ipns/republish", nil)
			rec := httptest.NewRecorder()

			ctx.Router().ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.IPNSRepublishResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, 1, response.Count)
		}, AdminTestOptions)
	})
}
