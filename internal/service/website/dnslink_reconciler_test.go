package website

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"

	dnslink "github.com/dnslink-std/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	domsvc "go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/testopts"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// dnsLinkReconcilerTestOptions is TestOptions plus the real
// DelegatedDomainService: the flagged reconciler derives its desired state
// from the binding plan, which only the domain service can map. The DNS
// service stays a mock so the plan-driven write family is directly
// observable (CreateDNSLinkRecord instead of UpdateWebsiteDNSRecords).
var dnsLinkReconcilerTestOptions = coreTesting.CombineOptions(
	coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
	testopts.NewBaseMockPluginBuilder().
		WithService(pluginCore.WEBSITE_SERVICE, NewWebsiteService).
		WithServiceConfig(pluginCore.WEBSITE_SERVICE, &pluginConfig.WebsiteConfig{
			NotificationsEnabled: false,
			AdminEmail:           "",
			ValidationTokenTTL:   24 * time.Hour,
		}).
		WithMockServiceFactory(pluginCore.DNS_SERVICE, mocks.NewMockDNSService).
		WithServiceConfig(pluginCore.DNS_SERVICE, &pluginConfig.DnsConfig{
			Enabled:                      true,
			Nameservers:                  []string{"ns1.localhost", "ns2.localhost"},
			NameserverValidationInterval: 5 * time.Minute,
			VerificationTokenKey:         "lumeweb-verify",
		}).
		WithService(pluginCore.DELEGATED_DOMAIN_SERVICE, domsvc.NewDelegatedDomainServiceFactory).
		WithServiceConfig(pluginCore.DELEGATED_DOMAIN_SERVICE, &pluginConfig.DelegatedDomainConfig{}).
		WithMigrations(map[core.DBType]fs.FS{
			core.DB_TYPE_SQLITE: migrations.GetSQLite(),
		}).BuilderOption(),
	coreTesting.WithMockMailerService(),
	util.GetProtocolMock(),
)

// enableDNSLinkReconciler flips the DNSLink reconciler feature flag on the website
// service's DNS config for one test, preserving the rest of the config.
func enableDNSLinkReconciler(t *testing.T, ws pluginCore.WebsiteService) {
	t.Helper()
	svc, ok := ws.(*WebsiteServiceDefault)
	require.True(t, ok, "service is not *WebsiteServiceDefault")
	cfg := pluginConfig.DnsConfig{}
	if svc.dnsConfig != nil {
		cfg = *svc.dnsConfig
	}
	cfg.DomainPolicyDNSLinkReconcilerEnabled = true
	svc.dnsConfig = &cfg
}

// TestWebsiteService_PlanDNSLinkReconciler_KeySwitch_WritesDNSLinkRecord
// verifies the flagged path for the IPNS→IPNS key switch: the plan
// drives the DNSLink write through the reconciler's create/update executor
// (CreateDNSLinkRecord) and the legacy UpdateWebsiteDNSRecords writer is NOT
// called for the DNSLink family.
func TestWebsiteService_PlanDNSLinkReconciler_KeySwitch_WritesDNSLinkRecord(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		enableDNSLinkReconciler(t, websiteService)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "flagon-keyswitch-test.com"
		testZoneID := uint(9102)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 9101
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		testIPNSKey := setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything, testZoneID, mock.Anything, mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS, mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)

		// Act: switch to a different IPNS peer ID (IPNS→IPNS).
		testPeerID := "12D3KooWCqvCZqaG6LmG4mtoWZZwrvYB911DK8qqwE9gc25s4Hft"

		mockIPNSKey.EXPECT().GetPrivateKeyByPeerID(mock.Anything, testPeerID).
			Return(nil, testUserID1, nil).Once()
		mockIPNSKey.EXPECT().GetKeyByID(mock.Anything, testUserID1, *createdWebsite.IPNSKeyID).Return(testIPNSKey, nil).Once()
		mockIPNSKey.EXPECT().PublishCID(mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("time.Duration")).Return(nil).Once()

		// The live dnslink carries a stale value: the reconciler must write
		// through its executor (CreateDNSLinkRecord with the target path).
		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink(domain).Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{},
		}, nil)
		setMockResolver(websiteService, mockResolver)

		mockDNS.EXPECT().CreateDNSLinkRecord(
			mock.Anything, testZoneID, domain, "/ipns/"+testPeerID,
		).Return(nil).Once()

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, map[string]interface{}{
			"target_hash": testPeerID,
		})
		websiteService.WaitForPublishes()

		// Assert: update succeeded and the record write was plan-driven.
		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), updatedWebsite.TargetType)
		assert.Equal(tb, testPeerID, updatedWebsite.TargetHash())
		// No legacy UpdateWebsiteDNSRecords call is expected: the mock fails
		// on unexpected calls.
	}, dnsLinkReconcilerTestOptions)
}

// TestWebsiteService_PlanDNSLinkReconciler_MatchingRecord_SkipsWrite verifies
// the flagged path's idempotency: a no-op update whose live dnslink already
// carries the current target performs zero DNS writes.
func TestWebsiteService_PlanDNSLinkReconciler_MatchingRecord_SkipsWrite(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		enableDNSLinkReconciler(t, websiteService)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		mailer := coreTesting.GetMockMailerService(ctx)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "flagon-noop-match-test.com"
		testZoneID := uint(9103)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 9102
		prebindPrimaryDomain(tb, ctx, website, domain, true)

		testIPNSKey := setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		mailer.EXPECT().TemplateSend(
			"website_created_admin",
			mock.Anything, mock.Anything, mock.Anything,
		).Return(nil).Maybe()

		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything, testZoneID, mock.Anything, mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS, mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)

		peerID := testIPNSKey.PeerID().String()

		// The live dnslink already carries the current IPNS target: the
		// flagged reconcile must be a strict no-op — neither the reconciler's
		// CreateDNSLinkRecord nor the legacy UpdateWebsiteDNSRecords runs.
		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink(domain).Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipns": {{Identifier: peerID}},
			},
		}, nil)
		setMockResolver(websiteService, mockResolver)

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, map[string]interface{}{
			"target_type": string(pluginDb.WebsiteTargetTypeIPNS),
		})
		websiteService.WaitForPublishes()

		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), updatedWebsite.TargetType)
	}, dnsLinkReconcilerTestOptions)
}

// TestWebsiteService_PlanDNSLinkReconciler_DelegationOwned_ReconcilesDNSLink
// verifies that a delegation-owned DNSLink (binding-content record inside a
// delegation-protected zone) is still reconciled on the flagged path: the
// tort doctrine is preserved — DNSLink remains binding-content-owned.
func TestWebsiteService_PlanDNSLinkReconciler_DelegationOwned_ReconcilesDNSLink(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		enableDNSLinkReconciler(t, websiteService)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "flagon-delegation-convert-test.com"
		testZoneID := uint(9104)

		newDelegationOwnedIPFSWebsite(t, tb, ctx, 9105, domain, testZoneID, testCID.String())
		testIPNSKey := setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)
		peerID := testIPNSKey.PeerID().String()

		// The live dnslink is stale (no links): the plan drives the write
		// through the executor's create/update port.
		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink(domain).Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{},
		}, nil)
		setMockResolver(websiteService, mockResolver)

		mockDNS.EXPECT().CreateDNSLinkRecord(
			mock.Anything, testZoneID, domain, "/ipns/"+peerID,
		).Return(nil).Once()

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, 9105, map[string]interface{}{
			"target_type": string(pluginDb.WebsiteTargetTypeIPNS),
		})
		websiteService.WaitForPublishes()

		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), updatedWebsite.TargetType)
		assert.Equal(tb, peerID, updatedWebsite.TargetHash())
	}, dnsLinkReconcilerTestOptions)
}

// TestWebsiteService_PlanDNSLinkReconciler_NoPortalZone_NoWriteCounterpart
// verifies the flagged path leaves owner-hosted bindings alone: with DNS
// hosting disabled (no zone reference) no DNS write — plan-driven or legacy —
// ever runs, exactly like the legacy path.
func TestWebsiteService_PlanDNSLinkReconciler_NoPortalZone_NoWrites(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		enableDNSLinkReconciler(t, websiteService)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "flagon-ownerhosted-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 9106
		prebindPrimaryDomain(tb, ctx, website, domain, false) // DNS hosting disabled

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)

		testIPNSKey := setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		mockPinSvc.EXPECT().GetPinByCIDAndUser(mock.Anything, testCID, testUserID1).
			Return(&pluginDb.IPFSPin{UserID: testUserID1, CID: testCID.Bytes(), Status: pluginDb.PinningStatusPinned}, nil).Maybe()

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, map[string]interface{}{
			"target_type": string(pluginDb.WebsiteTargetTypeIPNS),
		})
		websiteService.WaitForPublishes()

		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), updatedWebsite.TargetType)
		assert.Equal(tb, testIPNSKey.PeerID().String(), updatedWebsite.TargetHash())
		// No zone exists: no DNS call of any kind is expected — the mock
		// fails on unexpected calls.
	}, dnsLinkReconcilerTestOptions)
}

// TestWebsiteService_PlanDNSLinkReconciler_HostingReEnable_WriteErrorRunsLegacyRollback
// verifies the hosting re-enable path (handleDNSEnabledTransition →
// createWebsiteDNSRecords) under the flag: when the plan-driven reconciler's
// executor write fails with a non-ErrDNSLinkNotReconciled error, the failure
// must propagate exactly like the legacy UpdateWebsiteDNSRecords `return err`
// so handleDNSEnabledTransition's legacy DNS-setup rollback runs
// (DeleteWebsiteValidationRecord for delegation-owned bindings; zone detaches
// stay delegation-safe), the dns_hosting_enabled flag is NOT persisted, and
// the binding is re-enabled cleanly on a retry rather than stranded with a
// stale dnslink + validation TXT behind a "successful" re-enable.
func TestWebsiteService_PlanDNSLinkReconciler_HostingReEnable_WriteErrorRunsLegacyRollback(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "flagon-reenable-rollback-test.com"
		testZoneID := uint(9107)

		newDelegationOwnedIPFSWebsite(t, tb, ctx, 9107, domain, testZoneID, testCID.String())
		apex, err := websiteService.GetApexDomainBinding(context.Background(), 9107)
		require.NoError(tb, err)
		require.NotNil(tb, apex)

		// Disable hosting first: delegation-owned bindings keep their zone,
		// so the later enable is a genuine re-enable (zone survives, flag
		// flips). Flag still OFF here — this leg is materially the same on
		// both sides of the flag and needs no DNS writes.
		disabled, err := websiteService.SetDomainDNSEnabled(context.Background(), testUserID1, 9107, apex.ID, false)
		require.NoError(tb, err)
		require.NotNil(tb, disabled)
		assert.False(tb, disabled.DNSHostingEnabled)
		assert.Equal(tb, testZoneID, disabled.ZoneID, "delegation-owned zone must survive the disable")

		// Now enable the reconciler flag and re-enable hosting. The live
		// dnslink is stale, so the plan drives the write; the executor fails.
		enableDNSLinkReconciler(t, websiteService)
		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink(domain).Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{},
		}, nil)
		setMockResolver(websiteService, mockResolver)

		mockDNS.EXPECT().CreateDNSLinkRecord(
			mock.Anything, testZoneID, domain, mock.Anything,
		).Return(errors.New("powerdns write failed")).Once()
		// Rollback for a delegation-owned binding: only the website
		// validation record may be removed.
		mockDNS.EXPECT().DeleteWebsiteValidationRecord(
			mock.Anything, testZoneID, domain,
		).Return(nil).Once()

		_, err = websiteService.SetDomainDNSEnabled(context.Background(), testUserID1, 9107, apex.ID, true)

		// The error must propagate (legacy `return err` parity) instead of
		// being swallowed behind a "handled" re-enable.
		require.Error(tb, err)
		assert.ErrorContains(tb, err, "failed to create DNS records")

		// The validation TXT write never happened (strict mock would fail on
		// the unexpected CreateWebsiteValidationRecord call), and the enable
		// flag was not persisted — the transition failed before the flag
		// write, so the binding is still disabled and retryable.
		reApex, gerr := websiteService.GetApexDomainBinding(context.Background(), 9107)
		require.NoError(tb, gerr)
		assert.False(tb, reApex.DNSHostingEnabled, "dns_hosting_enabled must not persist when the transition failed")
		assert.Equal(tb, testZoneID, reApex.ZoneID, "delegation-owned zone must survive the failed re-enable")
	}, dnsLinkReconcilerTestOptions)
}

// TestWebsiteService_PlanDNSLinkReconciler_NoOpRepair_WriteErrorFailSoftIsLegacyParity
// pins the fail-soft contract of the no-op repair path (reconcileManagedDNSLink)
// under the flag: an executor write failure is a loud Warn only — the update
// still reports success and no legacy fallback write fires, exactly like the
// legacy writer's Warn-only failure handling on that path.
func TestWebsiteService_PlanDNSLinkReconciler_NoOpRepair_WriteErrorFailSoftIsLegacyParity(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "flagon-noop-failsoft-test.com"
		testZoneID := uint(9108)

		newDelegationOwnedIPFSWebsite(t, tb, ctx, 9108, domain, testZoneID, testCID.String())
		testIPNSKey := setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)
		peerID := testIPNSKey.PeerID().String()

		// First update (target change IPFS→IPNS) succeeds with the flagged
		// reconciler writing the dnslink.
		enableDNSLinkReconciler(t, websiteService)
		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink(domain).Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{},
		}, nil)
		setMockResolver(websiteService, mockResolver)

		mockDNS.EXPECT().CreateDNSLinkRecord(
			mock.Anything, testZoneID, domain, "/ipns/"+peerID,
		).Return(nil).Once()

		converted, err := websiteService.UpdateWebsite(context.Background(), testUserID1, 9108, map[string]interface{}{
			"target_type": string(pluginDb.WebsiteTargetTypeIPNS),
		})
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		require.NotNil(tb, converted)

		// Second update is a no-op. The live dnslink mismatches the target,
		// so the plan drives the write — and the executor fails this time.
		// No additional DNS expectations: a legacy fallback write
		// (UpdateWebsiteDNSRecords) or any other repair write would exhaust
		// the mock loudly, and the update must still succeed (fail-soft).
		mockFailResolver := mocks.NewMockDNSResolver(t)
		mockFailResolver.EXPECT().ResolveDNSLink(domain).Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{},
		}, nil)
		setMockResolver(websiteService, mockFailResolver)

		mockDNS.EXPECT().CreateDNSLinkRecord(
			mock.Anything, testZoneID, domain, "/ipns/"+peerID,
		).Return(errors.New("powerdns write failed")).Once()

		updated, err := websiteService.UpdateWebsite(context.Background(), testUserID1, 9108, map[string]interface{}{
			"target_type": string(pluginDb.WebsiteTargetTypeIPNS),
		})
		websiteService.WaitForPublishes()

		// Fail-soft legacy parity: no error escapes the no-op repair path.
		require.NoError(tb, err)
		require.NotNil(tb, updated)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), updated.TargetType)
	}, dnsLinkReconcilerTestOptions)
}
