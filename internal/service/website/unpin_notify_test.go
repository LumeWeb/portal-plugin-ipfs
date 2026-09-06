package website

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/db/models"
	mh "github.com/multiformats/go-multihash"
)

// stubOwnerEmail primes the mock user service so resolveUserEmail returns the
// given address for the (test) account the websites are owned by.
func stubOwnerEmail(ctx coreTesting.TestContext, userID uint, email string) {
	userSvc := coreTesting.GetMockUserService(ctx)
	if userSvc == nil {
		return
	}
	userSvc.EXPECT().
		AccountExists(mock.Anything, userID).
		Return(true, &models.User{Email: email}, nil).
		Maybe()
}

// unpinNotifyTestOptions adds a mock user service on top of the
// notification-enabled website test options so owner email resolution has a
// stubbable user service.
var unpinNotifyTestOptions = coreTesting.CombineOptions(
	notifyEnabledTestOptions,
	coreTesting.WithMockUserService(),
)

// ipnsBackedPeerMultihash is a synthetic peer-id multihash used to bind an
// IPNS-key record and an IPNS-target website together in unpin notification
// tests.
var ipnsBackedPeerMultihash, _ = mh.Sum([]byte("ipns-unpin-notify-peer"), mh.SHA2_256, -1)

// mustNormalizeCID converts a test CID to its normalized (CIDv1) form, the
// string format IPNS publish stores in last_published_cid.
func mustNormalizeCID(tb coreTesting.TB, c cid.Cid) cid.Cid {
	normalized := encoding.NormalizeCid(c)
	require.NotNil(tb, normalized)
	return normalized
}

// TestWebsiteService_NotifyOwnerCIDUnpinned_DirectIPFSTarget verifies an
// active website whose IPFS target multihash matches the unpinned CID gets an
// owner email; a non-active website with the same target must not be emailed.
func TestWebsiteService_NotifyOwnerCIDUnpinned_DirectIPFSTarget(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Owner account so resolveUserEmail finds an address.
		stubOwnerEmail(ctx, testUserID1, "owner@test")

		testCID := util.GenerateTestCID(t, "site content")

		active := createTestIPFSWebsite(testUserID1, "example.com", testCID.String())
		active.Status = string(pluginDb.WebsiteStatusActive)
		active.ValidationToken = "test-token"
		require.NoError(tb, ctx.DB().Create(active).Error)
		bindPrimaryDomain(tb, ctx, active.ID, "example.com", false)

		// Same CID, but the site is pending_validation — no email expected.
		pending := createTestIPFSWebsite(testUserID1, "pending.example.com", testCID.String())
		pending.Status = string(pluginDb.WebsiteStatusPendingValidation)
		pending.ValidationToken = "test-token"
		require.NoError(tb, ctx.DB().Create(pending).Error)

		mailer := coreTesting.GetMockMailerService(ctx)
		require.NotNil(tb, mailer)
		mailer.EXPECT().TemplateSend(
			"website_cid_unpinned_user",
			mock.Anything, mock.Anything,
			"owner@test",
		).Return(nil).Once()

		require.NoError(tb, websiteService.NotifyOwnerCIDUnpinned(context.Background(), testCID.String()))
	}, unpinNotifyTestOptions)
}

// TestWebsiteService_NotifyOwnerCIDUnpinned_IPNSResolution verifies the owner
// email reaches the owner of an active website backing an IPNS key whose last
// published CID is the unpinned one (resolution through IPNS).
func TestWebsiteService_NotifyOwnerCIDUnpinned_IPNSResolution(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		stubOwnerEmail(ctx, testUserID1, "owner@test")

		testCID := util.GenerateTestCID(t, "ipns site content")

		active := createTestIPFSWebsite(testUserID1, "example.com", testCID.String())
		active.Status = string(pluginDb.WebsiteStatusActive)
		active.ValidationToken = "test-token"
		active.TargetType = string(pluginDb.WebsiteTargetTypeIPNS)
		active.TargetMultihash = ipnsBackedPeerMultihash
		active.CIDVersion = nil
		require.NoError(tb, ctx.DB().Create(active).Error)
		bindPrimaryDomain(tb, ctx, active.ID, "example.com", false)

		key := &pluginDb.IPFSIPNSKey{
			UserID:              testUserID1,
			Name:                "site-key",
			PeerIDMultihash:     ipnsBackedPeerMultihash,
			LastPublishedCID:    mustNormalizeCID(tb, testCID).String(),
			PrivateKeyEncrypted: []byte("test"),
		}
		require.NoError(tb, ctx.DB().Create(key).Error)

		mailer := coreTesting.GetMockMailerService(ctx)
		require.NotNil(tb, mailer)
		mailer.EXPECT().TemplateSend(
			"website_cid_unpinned_user",
			mock.Anything, mock.Anything,
			"owner@test",
		).Return(nil).Once()

		require.NoError(tb, websiteService.NotifyOwnerCIDUnpinned(context.Background(), testCID.String()))
	}, unpinNotifyTestOptions)
}

// TestWebsiteService_NotifyOwnerCIDUnpinned_NotificationDisabled verifies the
// lookup path is skipped entirely when notifications are disabled.
func TestWebsiteService_NotifyOwnerCIDUnpinned_NotificationDisabled(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "no notifications")
		require.NoError(tb, websiteService.NotifyOwnerCIDUnpinned(context.Background(), testCID.String()))
	}, TestOptions)
}

// TestWebsiteService_NotifyAdminWebsiteBroken verifies the janitor warn-only
// admin warning: the lookup resolves the domain binding and the email goes to
// the configured admin address.
func TestWebsiteService_NotifyAdminWebsiteBroken(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		stubOwnerEmail(ctx, testUserID1, "owner@test")

		testCID := util.GenerateTestCID(t, "broken site content")
		website := createTestIPFSWebsite(testUserID1, "example.com", testCID.String())
		website.Status = string(pluginDb.WebsiteStatusActive)
		website.ValidationToken = "test-token"
		website.CreatedAt = time.Now().Add(-2 * time.Hour)
		require.NoError(tb, ctx.DB().Create(website).Error)
		bindPrimaryDomain(tb, ctx, website.ID, "example.com", false)

		mailer := coreTesting.GetMockMailerService(ctx)
		require.NotNil(tb, mailer)
		mailer.EXPECT().TemplateSend(
			"website_broken_admin",
			mock.Anything, mock.Anything,
			"admin@test",
		).Return(nil).Once()

		require.NoError(tb, websiteService.NotifyAdminWebsiteBroken(context.Background(), website.ID))
	}, unpinNotifyTestOptions)
}
