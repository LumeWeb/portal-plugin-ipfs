package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/datatypes"
)

func TestAPI_DeleteDomain(t *testing.T) {
	t.Run("hard_delete_allows_recreate_same_domain_namespace", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			// Create a website to attach the domain to.
			website := createTestIPFSGatewayWebsite(1, userID, "example.com", testCID, pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			// Create a domain binding.
			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1,
				UserID:    userID,
				Domain:    "example.com",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusDraft,
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			// Delete it via the API.
			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/1/domains/1", token, nil)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// The record should be gone (hard-deleted, not soft-deleted).
			var count int64
			ctx.DB().Unscoped().Model(&pluginDb.WebsiteDomain{}).Where("domain = ? AND namespace = ?", "example.com", pluginDb.DomainNamespaceICANN).Count(&count)
			assert.Zero(t, count, "domain should be hard-deleted, not soft-deleted")

			// Re-create the same domain+namespace — should succeed (no unique collision).
			wd2 := &pluginDb.WebsiteDomain{
				WebsiteID: 1,
				UserID:    userID,
				Domain:    "example.com",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusDraft,
			}
			err := ctx.DB().Create(wd2).Error
			assert.NoError(t, err, "re-creating same domain+namespace after hard delete should succeed")
		}, TestOptions)
	})

	t.Run("delete_other_users_domain_returns_404", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			// Create a website owned by the authenticated user.
			website := createTestIPFSGatewayWebsite(1, userID, "other.com", testCID, pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			// Create a domain binding owned by a different user.
			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1,
				UserID:    userID + 100, // different user
				Domain:    "other.com",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusDraft,
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			// Authenticated user tries to delete the other user's domain.
			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/1/domains/1", token, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_DomainDNSRequirements(t *testing.T) {
	t.Run("returns_delegation_for_bound_domain", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			website := createTestIPFSGatewayWebsite(1, userID, "example.com", testCID, pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			wd := &pluginDb.WebsiteDomain{
				WebsiteID:   1,
				UserID:      userID,
				Domain:      "lumeweb",
				Namespace:   pluginDb.DomainNamespaceHNS,
				Status:      pluginDb.DomainStatusRecordsGenerated,
				ZoneName:    "lumeweb.",
				GatewayHost: "gateway.lumeweb.com",
				DelegationData: datatypes.JSONMap{
					"mode": "delegated",
					"parent_records": []map[string]any{
						{"type": "NS", "value": "ns1.lumeweb,ns2.lumeweb"},
						{"type": "DS", "value": "lumeweb. 3600 IN DS 12345 13 2 <digest>"},
					},
					"authoritative_records": []map[string]any{
						{"type": "NS", "value": "ns1.lumeweb\nns2.lumeweb"},
					},
				},
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/1/domains/1/dns-requirements", token, nil)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			var resp dto.DomainResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "lumeweb", resp.Domain)
			assert.Equal(t, "hns", resp.Namespace)
			require.NotNil(t, resp.Delegation)
			assert.Equal(t, "delegated", resp.Delegation.Mode)
			assert.Equal(t, "lumeweb. 3600 IN DS 12345 13 2 <digest>", resp.Delegation.DS)
			require.Len(t, resp.Delegation.ParentRecords, 2)
			assert.Equal(t, "NS", resp.Delegation.ParentRecords[0].Type)
		}, TestOptions)
	})

	t.Run("missing_domain_returns_404", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _, _, _ := helper.SetupAuthenticatedTest()

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/1/domains/999/dns-requirements", token, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})
}
