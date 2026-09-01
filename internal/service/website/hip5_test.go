package website

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestShouldPerformTokenCheck(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)
		svc, ok := ws.(*WebsiteServiceDefault)
		require.True(tb, ok, "expected WebsiteServiceDefault")

		pending := &pluginDb.Website{Status: string(pluginDb.WebsiteStatusPendingValidation)}

		// A delegated-domain service that considers .hns-suffixed names
		// delegation-owned (mimicking the real HNS provider), so only the
		// on-chain-managed guard should make the token check run for HNS.
		setMockDelegatedDomainSvc(ws, &testDelegatedDomainService{
			uses: func(domain string) bool {
				return strings.HasSuffix(domain, ".hns")
			},
		})

		t.Run("onchain managed HNS performs TXT token check", func(t *testing.T) {
			wd := &pluginDb.WebsiteDomain{
				Domain:    "my.hns",
				Namespace: pluginDb.DomainNamespaceHNS,
				Status:    pluginDb.DomainStatusOnchainManaged,
			}
			assert.True(tb, svc.shouldPerformTokenCheck(pending, wd),
				"on-chain managed (HIP-5) must prove ownership via TXT token")
		})

		t.Run("native HNS skips TXT token check (delegation)", func(t *testing.T) {
			wd := &pluginDb.WebsiteDomain{
				Domain:    "example.hns",
				Namespace: pluginDb.DomainNamespaceHNS,
				Status:    pluginDb.DomainStatusWaitingDelegation,
			}
			assert.False(tb, svc.shouldPerformTokenCheck(pending, wd),
				"native HNS proves ownership via delegation, no TXT token")
		})

		t.Run("platform subdomain skips TXT token check", func(t *testing.T) {
			platformID := uint(1)
			wd := &pluginDb.WebsiteDomain{
				Domain:           "sub.hns",
				Namespace:        pluginDb.DomainNamespaceHNS,
				Status:           pluginDb.DomainStatusActive,
				PlatformDomainID: &platformID,
			}
			assert.False(tb, svc.shouldPerformTokenCheck(pending, wd),
				"platform subdomain is operator-controlled, no TXT token")
		})

		t.Run("ICANN performs TXT token check", func(t *testing.T) {
			wd := &pluginDb.WebsiteDomain{
				Domain:    "example.com",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusRecordsGenerated,
			}
			assert.True(tb, svc.shouldPerformTokenCheck(pending, wd),
				"ICANN proves ownership via TXT token")
		})
	}, TestOptions)
}

func TestSetDomainDNSEnabled_OnchainManagedRefusesEnable(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "onchain-test")
		domain := "onchain.hns"
		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Status = string(pluginDb.WebsiteStatusPendingValidation)
		require.NoError(tb, ctx.DB().Create(website).Error)

		wd := prebindPrimaryDomain(tb, ctx, website, domain, false)
		// Mark the binding on-chain managed (HIP-5): no portal zone, hosting off.
		wd.Namespace = pluginDb.DomainNamespaceHNS
		wd.Status = pluginDb.DomainStatusOnchainManaged
		require.NoError(tb, ctx.DB().Model(wd).Updates(map[string]interface{}{
			"namespace": string(pluginDb.DomainNamespaceHNS),
			"status":    string(pluginDb.DomainStatusOnchainManaged),
		}).Error)

		// Enabling portal DNS hosting is refused with a clear error — the
		// external contract serves the name's DNS, so a portal zone would be
		// unreachable.
		_, err := websiteService.SetDomainDNSEnabled(context.Background(), testUserID1, website.ID, wd.ID, true)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "on-chain managed")

		// Disabling is a reconciled no-op (flag=false and zone=0 already agree);
		// the binding comes back unchanged without DNS churn.
		updated, err := websiteService.SetDomainDNSEnabled(context.Background(), testUserID1, website.ID, wd.ID, false)
		require.NoError(tb, err)
		require.NotNil(tb, updated)
		assert.Equal(tb, pluginDb.DomainStatusOnchainManaged, updated.Status)
		assert.False(tb, updated.DNSHostingEnabled)

		// The flag was never flipped on and no zone appeared.
		var persisted pluginDb.WebsiteDomain
		require.NoError(tb, ctx.DB().First(&persisted, wd.ID).Error)
		assert.False(tb, persisted.DNSHostingEnabled)
		assert.Equal(tb, uint(0), persisted.ZoneID)
	}, TestOptions)
}

// TestSetDomainDNSEnabled_OnchainManagedStrayZone_RefusesEnableNoZoneDelete is
// the stray-zone variant of the on-chain enable refusal: even when an on-chain
// binding incoherently carries a zone reference, enabling portal DNS must be
// refused and the website/DNS flows must never create, delete, or write to any
// portal zone.
func TestSetDomainDNSEnabled_OnchainManagedStrayZone_RefusesEnableNoZoneDelete(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, mockDNS)

		testCID := util.GenerateTestCID(t, "onchain-stray-enable")
		domain := "onchain-stray-enable.hns"
		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Status = string(pluginDb.WebsiteStatusPendingValidation)
		require.NoError(tb, ctx.DB().Create(website).Error)

		wd := prebindPrimaryDomain(tb, ctx, website, domain, false)
		wd.Namespace = pluginDb.DomainNamespaceHNS
		wd.Status = pluginDb.DomainStatusOnchainManaged
		require.NoError(tb, ctx.DB().Model(wd).Updates(map[string]interface{}{
			"namespace": string(pluginDb.DomainNamespaceHNS),
			"status":    string(pluginDb.DomainStatusOnchainManaged),
			"zone_id":   uint(77), // stray, incoherent zone reference
		}).Error)

		_, err := websiteService.SetDomainDNSEnabled(context.Background(), testUserID1, website.ID, wd.ID, true)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "on-chain managed")

		// No portal DNS operation may fire from the enable refusal.
		mockDNS.AssertNotCalled(tb, "CreateZone")
		mockDNS.AssertNotCalled(tb, "GetZone")
		mockDNS.AssertNotCalled(tb, "GetZoneByDomain")
		mockDNS.AssertNotCalled(tb, "DeleteZone")
		mockDNS.AssertNotCalled(tb, "CreateWebsiteDNSRecords")
		mockDNS.AssertNotCalled(tb, "CreateWebsiteValidationRecord")

		// Disabling stays a no-op (flag already false); the stray zone reference
		// is left untouched — cleanup is out of scope for website flows.
		updated, err := websiteService.SetDomainDNSEnabled(context.Background(), testUserID1, website.ID, wd.ID, false)
		require.NoError(tb, err)
		require.NotNil(tb, updated)
		assert.Equal(tb, pluginDb.DomainStatusOnchainManaged, updated.Status)
		assert.False(tb, updated.DNSHostingEnabled)
		mockDNS.AssertNotCalled(tb, "DeleteZone")
		mockDNS.AssertNotCalled(tb, "DeleteWebsiteDNSRecords")
	}, TestOptions)
}
