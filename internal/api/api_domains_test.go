package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestAPI_DeleteDomain(t *testing.T) {
	t.Run("hard_delete_allows_recreate_same_domain_namespace", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			// Create a website to attach the domain to.
			website := createTestIPFSGatewayWebsite(1, userID, "example.com", testCID, db.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			// Create a domain binding.
			wd := &db.WebsiteDomain{
				WebsiteID: 1,
				UserID:    userID,
				Domain:    "example.com",
				Namespace: db.DomainNamespaceICANN,
				Status:    db.DomainStatusDraft,
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			// Delete it via the API.
			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/1/domains/1", token, nil)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// The record should be gone (hard-deleted, not soft-deleted).
			var count int64
			ctx.DB().Unscoped().Model(&db.WebsiteDomain{}).Where("domain = ? AND namespace = ?", "example.com", db.DomainNamespaceICANN).Count(&count)
			assert.Zero(t, count, "domain should be hard-deleted, not soft-deleted")

			// Re-create the same domain+namespace — should succeed (no unique collision).
			wd2 := &db.WebsiteDomain{
				WebsiteID: 1,
				UserID:    userID,
				Domain:    "example.com",
				Namespace: db.DomainNamespaceICANN,
				Status:    db.DomainStatusDraft,
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
			website := createTestIPFSGatewayWebsite(1, userID, "other.com", testCID, db.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			// Create a domain binding owned by a different user.
			wd := &db.WebsiteDomain{
				WebsiteID: 1,
				UserID:    userID + 100, // different user
				Domain:    "other.com",
				Namespace: db.DomainNamespaceICANN,
				Status:    db.DomainStatusDraft,
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			// Authenticated user tries to delete the other user's domain.
			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/1/domains/1", token, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})
}
