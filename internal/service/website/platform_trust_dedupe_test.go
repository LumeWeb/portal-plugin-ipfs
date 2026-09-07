package website

import (
	"context"
	"io/fs"
	"sync/atomic"
	"testing"
	"time"

	dnslink "github.com/dnslink-std/go"
	"github.com/stretchr/testify/assert"
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
	"gorm.io/gorm"
)

// platformTrustTestOptions mirrors TestOptions but additionally wires the REAL
// delegated-domain service alongside the real website service, so a
// platform-managed ValidateDNS run exercises every ValidatePlatformBinding
// call site for real: plan derivation (CurrentBindingPlan's mapper read) AND
// VerifyDomain's platform-trust gate.
var platformTrustTestOptions = coreTesting.CombineOptions(
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

// countPlatformDomainSelects registers a GORM query callback that counts
// SELECT statements against the platform_domains table — the exact query
// ValidatePlatformBinding performs. It acts as the counting spy for the
// platform-trust dedupe assertions.
func countPlatformDomainSelects(tb coreTesting.TB, db *gorm.DB) *atomic.Int64 {
	tb.Helper()
	count := &atomic.Int64{}
	require.NoError(tb, db.Callback().Query().Before("gorm:query").Register("test_count_platform_domains", func(tx *gorm.DB) {
		if tx.Statement != nil && tx.Statement.Table == "platform_domains" {
			count.Add(1)
		}
	}))
	return count
}

// bindPlatformManagedPrimaryDomain creates a platform root (owning zone) and a
// portal-managed platform binding under it, wired as the website's primary.
func bindPlatformManagedPrimaryDomain(tb coreTesting.TB, ctx coreTesting.TestContext, websiteID uint, root, subdomain string) *pluginDb.WebsiteDomain {
	tb.Helper()
	zone := &pluginDb.DNSZone{
		Domain: root,
		UserID: testUserID1,
		Status: string(pluginDb.DNSZoneStatusActive),
	}
	require.NoError(tb, ctx.DB().Create(zone).Error)
	pd := &pluginDb.PlatformDomain{
		Domain:    root,
		Namespace: pluginDb.DomainNamespaceICANN,
		ZoneID:    zone.ID,
		Enabled:   true,
	}
	require.NoError(tb, ctx.DB().Create(pd).Error)
	wd := bindPrimaryDomain(tb, ctx, websiteID, subdomain, true)
	require.NoError(tb, ctx.DB().Model(wd).Updates(map[string]any{
		"zone_id":            zone.ID,
		"platform_domain_id": pd.ID,
	}).Error)
	wd.ZoneID = zone.ID
	wd.PlatformDomainID = &pd.ID
	return wd
}

// TestValidateDNS_PlatformManagedDedupe_PlatformBindingValidatedOnce asserts
// that a platform-managed ValidateDNS pass performs exactly one
// ValidatePlatformBinding DB query. Plan derivation (CurrentBindingPlan) runs
// the shared platform-trust validator fail-closed before the delegation gate;
// VerifyDomain must reuse that result instead of re-querying (previously the
// same SELECT ran twice per call). The end result is unchanged: the binding
// auto-activates and validation passes.
func TestValidateDNS_PlatformManagedDedupe_PlatformBindingValidatedOnce(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "platform-dedupe")
		website := createTestIPFSWebsite(testUserID1, "site.platform-dedupe.test", testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		wd := bindPlatformManagedPrimaryDomain(tb, ctx, created.ID, "platform-dedupe.test", "site.platform-dedupe.test")

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("site.platform-dedupe.test").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		setMockResolver(ws, mockResolver)

		platformSelects := countPlatformDomainSelects(tb, ctx.DB())

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonValidated, result.Reason)

		// Exactly one platform-trust DB validation for the whole pass: the
		// unconditional plan-derivation read. The delegation gate reuses it.
		assert.Equal(t, int64(1), platformSelects.Load(),
			"platform-managed ValidateDNS must run ValidatePlatformBinding exactly once")

		// Behavior preserved: the binding auto-activated through VerifyDomain.
		reloaded := &pluginDb.WebsiteDomain{}
		require.NoError(tb, ctx.DB().First(reloaded, wd.ID).Error)
		assert.Equal(t, pluginDb.DomainStatusActive, reloaded.Status)
	}, platformTrustTestOptions)
}

// TestValidateDNS_PlatformTrustFailure_TypedRejection preserves the legacy
// fail-closed behavior on a broken platform trust relation: when the shared
// validator rejects the binding during plan derivation (no plan available),
// VerifyDomain still performs its own platform-trust validation and the run
// fails with the typed delegation-pending rejection — never a silent skip.
func TestValidateDNS_PlatformTrustFailure_TypedRejection(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "platform-trust-fail")
		website := createTestIPFSWebsite(testUserID1, "site.platform-trust-fail.test", testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		wd := bindPlatformManagedPrimaryDomain(tb, ctx, created.ID, "platform-trust-fail.test", "site.platform-trust-fail.test")

		// Corrupt the trust relation: the binding's zone no longer matches the
		// operator root's zone (data-integrity corruption the validator must
		// reject).
		require.NoError(tb, ctx.DB().Model(wd).Update("zone_id", wd.ZoneID+1).Error)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("site.platform-trust-fail.test").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		setMockResolver(ws, mockResolver)

		platformSelects := countPlatformDomainSelects(tb, ctx.DB())

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.False(t, result.Valid, "broken platform trust must fail validation")
		assert.Equal(t, pluginCore.ValidationReasonDelegationPending, result.Reason)

		// The rejection was produced by an actual trust check, not a silent
		// skip: VerifyDomain ran the shared validator on the legacy path.
		assert.GreaterOrEqual(tb, platformSelects.Load(), int64(1),
			"trust failure must be detected by a real platform-trust validation")

		// Nothing was auto-activated or status-mutated on the rejected binding.
		reloaded := &pluginDb.WebsiteDomain{}
		require.NoError(tb, ctx.DB().First(reloaded, wd.ID).Error)
		assert.Equal(t, pluginDb.DomainStatusDraft, reloaded.Status)
	}, platformTrustTestOptions)
}
