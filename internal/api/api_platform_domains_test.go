package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

// TestAPI_PlatformDomainAvailability exercises the auth-only availability
// endpoint. It must list only enabled platform roots (never user-managed zones)
// and report per-root claimability.
func TestAPI_PlatformDomainAvailability(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _, _, _ := helper.SetupAuthenticatedTest()

		// Enabled ICANN root and a disabled HNS root (must be excluded).
		require.NoError(tb, ctx.DB().Create(&pluginDb.PlatformDomain{
			Domain: "platform.test", Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 1, Enabled: true,
		}).Error)
		require.NoError(tb, ctx.DB().Create(&pluginDb.PlatformDomain{
			Domain: "altroot", Namespace: pluginDb.DomainNamespaceHNS, ZoneID: 2, Enabled: false,
		}).Error)

		rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/platform-domains/availability?label=myblog", token, nil)
		require.Equal(tb, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var resp dto.PlatformAvailabilityResponse
		require.NoError(tb, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(tb, "myblog", resp.Label)
		require.Len(tb, resp.Results, 1)
		assert.Equal(tb, "platform.test", resp.Results[0].PlatformDomain)
		assert.True(tb, resp.Results[0].Available)
	}, TestOptions)
}

// TestAPI_ListSupportedPlatformDomains exercises the user-facing list endpoint.
// It must return only enabled (supported) platform roots, ordered by domain, in
// the standard paginated {data, total} shape.
func TestAPI_ListSupportedPlatformDomains(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _, _, _ := helper.SetupAuthenticatedTest()

		require.NoError(tb, ctx.DB().Create(&pluginDb.PlatformDomain{
			Domain: "platform.test", Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 1, Enabled: true,
		}).Error)
		require.NoError(tb, ctx.DB().Create(&pluginDb.PlatformDomain{
			Domain: "aplatform.test", Namespace: pluginDb.DomainNamespaceHNS, ZoneID: 2, Enabled: true,
		}).Error)
		require.NoError(tb, ctx.DB().Create(&pluginDb.PlatformDomain{
			Domain: "disabled.test", Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 3, Enabled: false,
		}).Error)

		rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/platform-domains", token, nil)
		require.Equal(tb, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var resp dto.PlatformDomainListResponse
		require.NoError(tb, json.Unmarshal(rec.Body.Bytes(), &resp))
		// Only the two enabled roots are returned, ordered by domain.
		require.Equal(tb, int64(2), resp.Total)
		require.Len(tb, resp.Data, 2)
		assert.Equal(tb, "aplatform.test", resp.Data[0].Domain)
		assert.Equal(tb, "platform.test", resp.Data[1].Domain)
		assert.NotEqual(tb, "disabled.test", resp.Data[0].Domain)
	}, TestOptions)
}

// TestAPI_ListSupportedPlatformDomains_Unauthenticated ensures the user-facing
// list endpoint is auth-only, mirroring the availability endpoint.
func TestAPI_ListSupportedPlatformDomains_Unauthenticated(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		require.NoError(tb, ctx.DB().Create(&pluginDb.PlatformDomain{
			Domain: "platform.test", Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 1, Enabled: true,
		}).Error)

		req := ctx.NewAPIRequest(http.MethodGet, "/api/websites/platform-domains", nil)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)
		assert.NotEqual(tb, http.StatusOK, rec.Code)
	}, TestOptions)
}

// TestAPI_PlatformDomainAvailability_Unauthenticated ensures the availability
// endpoint is auth-only (leaks nothing about the root registry to anonymous
// callers).
func TestAPI_PlatformDomainAvailability_Unauthenticated(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		require.NoError(tb, ctx.DB().Create(&pluginDb.PlatformDomain{
			Domain: "platform.test", Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 1, Enabled: true,
		}).Error)

		req := ctx.NewAPIRequest(http.MethodGet, "/api/websites/platform-domains/availability?label=myblog", nil)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)
		// Unauthenticated access is rejected, never a 200.
		assert.NotEqual(tb, http.StatusOK, rec.Code)
	}, TestOptions)
}

// TestAdminPlatformDomainCRUD covers the operator CRUD surface: create, list,
// enable/disable, and soft-delete of a platform root.
func TestAdminPlatformDomainCRUD(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, mockDNS)

		t.Run("create_and_list", func(t *testing.T) {
			helper := newMockHelper(t, ctx)
			token, userID, _, _ := helper.SetupAuthenticatedTest()

			// CreatePlatformDomain auto-creates (idempotently) the operator's DNS
			// zone for the root from the authenticated admin user; the operator is
			// derived from the request context, never trusted from client input.
			mockDNS.EXPECT().CreateZone(mock.Anything, "platform.test", userID).
				Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "platform.test"}, nil).Once()

			req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/platform-domains",
				[]byte(`{"domain":"platform.test","namespace":"icann","enabled":true}`))
			setAuthHeader(req, token)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

			req = ctx.NewAPIRequest(http.MethodGet, "/api/ipfs/platform-domains", nil)
			setAuthHeader(req, token)
			rec = httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			var listResp dto.PlatformDomainListResponse
			require.NoError(tb, json.Unmarshal(rec.Body.Bytes(), &listResp))
			require.Len(tb, listResp.Data, 1)
			assert.Equal(tb, int64(1), listResp.Total)
			assert.Equal(tb, "platform.test", listResp.Data[0].Domain)
			assert.True(tb, listResp.Data[0].Enabled)
		})

		t.Run("update_disable_and_soft_delete", func(t *testing.T) {
			helper := newMockHelper(t, ctx)
			token, _, _, _ := helper.SetupAuthenticatedTest()

			require.NoError(tb, ctx.DB().Create(&pluginDb.PlatformDomain{
				Domain: "update.test", Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 7, Enabled: true,
			}).Error)
			var pd pluginDb.PlatformDomain
			require.NoError(tb, ctx.DB().Where("domain = ?", "update.test").First(&pd).Error)
			path := fmt.Sprintf("/api/ipfs/platform-domains/%d", pd.ID)

			// Disable: future claims blocked, but the row remains queryable.
			req := ctx.NewAPIRequest(http.MethodPatch, path, []byte(`{"enabled":false}`))
			setAuthHeader(req, token)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			var updated dto.PlatformDomainResponse
			require.NoError(tb, json.Unmarshal(rec.Body.Bytes(), &updated))
			assert.False(tb, updated.Enabled)

			// Delete -> soft delete (tombstone present via Unscoped).
			req = ctx.NewAPIRequest(http.MethodDelete, path, nil)
			setAuthHeader(req, token)
			rec = httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			require.Equal(t, http.StatusNoContent, rec.Code)

			var after pluginDb.PlatformDomain
			require.NoError(tb, ctx.DB().Unscoped().First(&after, pd.ID).Error)
			assert.True(tb, after.DeletedAt.Valid, "platform domain should be soft-deleted")
		})
	}, AdminTestOptions)
}

// TestAPI_CreateDomain_PlatformClaim exercises the user-facing createDomain
// handler's platform branch: claiming a free subdomain under an operator root
// via {platform_domain, generate}. It asserts the binding is created active,
// DNS-hosted, under the platform root, and references it.
func TestAPI_CreateDomain_PlatformClaim(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, userID, testCID, _ := helper.SetupAuthenticatedTest()

		website := createTestIPFSGatewayWebsite(1, userID, "example.com", testCID, pluginDb.WebsiteStatusActive)
		require.NoError(tb, ctx.DB().Create(website).Error)

		require.NoError(tb, ctx.DB().Create(&pluginDb.PlatformDomain{
			Domain: "platform.test", Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 7, Enabled: true,
		}).Error)

		mockDNS := helper.SetupDNSServiceMocks()
		// resolveManagedZone under the platform root resolves the operator zone.
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "platform.test").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "platform.test"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(7), mock.Anything, mock.Anything).Return(nil).Maybe()

		rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites/1/domains",
			token, []byte(`{"platform_domain":"platform.test","generate":true}`))
		require.Equal(tb, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

		var resp dto.DomainResponse
		require.NoError(tb, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Contains(tb, resp.Domain, ".platform.test")

		// The binding is recorded with the platform reference and active.
		var wd pluginDb.WebsiteDomain
		require.NoError(tb, ctx.DB().Where("website_id = ?", website.ID).First(&wd).Error)
		require.NotNil(tb, wd.PlatformDomainID)
		assert.Equal(tb, pluginDb.DomainStatusActive, wd.Status)
		assert.True(tb, wd.DNSHostingEnabled)
	}, TestOptions)
}
