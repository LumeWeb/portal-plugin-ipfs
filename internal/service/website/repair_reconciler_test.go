package website

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// TestWebsiteService_PlanRepairReconciler_ChallengeRotation verifies the
// flagged expired-challenge rotation: the rotation is an explicit
// application command — the plan drives the record reconciliation (DNSLink
// re-assert through the executor adapter; the fresh token's validation
// TXT through the rotate-challenge command) and NO legacy
// CreateWebsiteDNSRecords/UpdateWebsiteDNSRecords writer fires.
func TestWebsiteService_PlanRepairReconciler_ChallengeRotation(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		enableRepairReconciler(t, websiteService)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		testCID := util.GenerateTestCID(t, "repair-rotation")
		domain := "repair-rotation-test.com"
		testZoneID := uint(9200)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Status = string(pluginDb.WebsiteStatusPendingValidation)
		oldToken := "old-expired-token"
		website.ValidationToken = oldToken
		past := time.Now().Add(-1 * time.Hour)
		website.ValidationExpiresAt = &past
		require.NoError(tb, ctx.DB().Create(website).Error)

		wd := bindPrimaryDomain(tb, ctx, website.ID, domain, true)
		wd.ZoneID = testZoneID
		require.NoError(tb, ctx.DB().Model(&pluginDb.WebsiteDomain{}).Where("id = ?", wd.ID).Update("zone_id", testZoneID).Error)
		wd.ZoneID = testZoneID

		// Flagged-path writes only: the DNSLink re-assert (Diff's unobserved
		// portal-zone intent) and the rotated validation record (the
		// rotate-challenge command carrying the freshly persisted token). No
		// legacy CreateWebsiteDNSRecords / UpdateWebsiteDNSRecords call.
		writtenToken := ""
		mockDNS.EXPECT().CreateDNSLinkRecord(
			mock.Anything, testZoneID, domain, "/ipfs/"+testCID.String(),
		).Return(nil).Once()
		mockDNS.EXPECT().CreateWebsiteValidationRecord(
			mock.Anything, testZoneID, domain, mock.Anything,
		).RunAndReturn(func(_ context.Context, _ uint, _ string, token string) error {
			writtenToken = token
			return nil
		}).Once()

		svc, ok := websiteService.(*WebsiteServiceDefault)
		require.True(tb, ok, "service is not *WebsiteServiceDefault")
		handled, err := svc.rotateExpiredTokenPlanDriven(context.Background(), website, wd)
		require.NoError(tb, err)
		assert.True(tb, handled, "the flagged repair reconciler must own this rotation")

		// The token was rotated and persisted: the website's validation token
		// changed and the expiry moved into the future.
		require.NoError(tb, ctx.DB().First(website, website.ID).Error)
		assert.NotEqual(tb, oldToken, website.ValidationToken)
		require.NotNil(tb, website.ValidationExpiresAt)
		assert.True(tb, website.ValidationExpiresAt.After(time.Now()))
		assert.Equal(tb, website.ValidationToken, writtenToken,
			"the reconciled validation record must carry the persisted token")
	}, dnsLinkReconcilerTestOptions)
}

// TestWebsiteService_RepairFlagOff_ChallengeRotationLegacy verifies flag-off
// (default) parity: the rotation keeps the legacy writer shape (the
// all-in-one CreateWebsiteDNSRecords writer with the fresh token) and no
// plan-driven repair executor call fires.
func TestWebsiteService_RepairFlagOff_ChallengeRotationLegacy(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		svc, ok := websiteService.(*WebsiteServiceDefault)
		require.True(tb, ok, "service is not *WebsiteServiceDefault")
		assert.False(tb, svc.repairReconcilerEnabled(),
			"the repair flag must default to OFF (legacy repair paths)")
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		testCID := util.GenerateTestCID(t, "legacy-rotation")
		domain := "legacy-rotation-test.com"
		testZoneID := uint(9210)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Status = string(pluginDb.WebsiteStatusPendingValidation)
		website.ValidationToken = "old-expired-token"
		past := time.Now().Add(-1 * time.Hour)
		website.ValidationExpiresAt = &past
		require.NoError(tb, ctx.DB().Create(website).Error)

		wd := bindPrimaryDomain(tb, ctx, website.ID, domain, true)
		wd.ZoneID = testZoneID

		// Legacy rotation: the all-in-one records writer with the new token.
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything, testZoneID, domain, mock.Anything,
			pluginDb.WebsiteTargetTypeIPFS, mock.Anything,
		).Return(nil).Once()

		err := svc.regenerateExpiredToken(context.Background(), website, wd)
		require.NoError(tb, err)

		require.NoError(tb, ctx.DB().First(website, website.ID).Error)
		assert.NotEqual(tb, "old-expired-token", website.ValidationToken)

		// No plan-driven repair executor call.
		mockDNS.AssertNotCalled(tb, "CreateDNSLinkRecord")
		mockDNS.AssertNotCalled(tb, "CreateWebsiteValidationRecord")
	}, TestOptions)
}
