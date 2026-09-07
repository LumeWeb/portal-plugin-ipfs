package domain

// Bounded application backfill + persisted-axis dual-read tests. The fixture matrix covers every
// coherent current-behavior profile plus the required incoherent rows: each
// coherent fixture must map to its exact profile axes, every incoherent
// fixture must end with the persisted ERROR reconciliation status and NO
// axis values (never guessed), soft-deleted bindings are never processed,
// and dual-read prefers complete mapped axes, falling back to the legacy
// mapper for unmapped and stale rows.

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

type coherentFixture struct {
	name      string
	domain    string
	namespace string
	seed      func(*pluginDb.WebsiteDomain, uint, uint, *uint) // (binding, zoneID, platformZoneID, platformDomainID)

	lifecycle domainpolicy.Lifecycle
	authority domainpolicy.AuthorityLocus
	route     domainpolicy.ResolutionRoute
	backend   domainpolicy.BackendID
	hosting   domainpolicy.HostingRequest
	policyID  domainpolicy.ProfileID
	ver       domainpolicy.ProfileVersion
}

// seedPlatformRoot creates the operator zone + PlatformDomain row shared by
// the platform fixtures, returning (zoneID, platformDomainID).
func seedPlatformRoot(tb coreTesting.TB, gormDB *gorm.DB) (uint, uint) {
	tb.Helper()
	zone := &pluginDb.DNSZone{Domain: "roots.example", UserID: 42, Status: "active"}
	require.NoError(tb, gormDB.Create(zone).Error)
	pd := &pluginDb.PlatformDomain{Domain: "roots.example", Namespace: pluginDb.DomainNamespaceICANN, ZoneID: zone.ID, Enabled: true}
	require.NoError(tb, gormDB.Create(pd).Error)
	return zone.ID, pd.ID
}

func TestPolicyAxesBackfill_CoherentMatrix(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		platformZoneID, platformDomainID := seedPlatformRoot(tb, gormDB)

		fixtures := []coherentFixture{
			{
				name: "icann.portal", domain: "icann-managed.example.com", namespace: "icann",
				seed: func(w *pluginDb.WebsiteDomain, _ uint, _ uint, _ *uint) {
					w.ZoneID, w.Status, w.DNSHostingEnabled = 11, pluginDb.DomainStatusRecordsGenerated, true
				},
				lifecycle: domainpolicy.LifecycleProvisioning, authority: domainpolicy.AuthorityLocusPortalZone,
				route: domainpolicy.ResolutionRouteStandardDNS, backend: domainpolicy.BackendSystemDNS,
				hosting: domainpolicy.HostingRequestPortal, policyID: domainpolicy.ProfileIDICANNPortal, ver: domainpolicy.ProfileVersion1,
			},
			{
				name: "icann.owner", domain: "icann-owner.example.com", namespace: "icann",
				seed: func(w *pluginDb.WebsiteDomain, _ uint, _ uint, _ *uint) {
					w.Status, w.DNSHostingEnabled = pluginDb.DomainStatusSelfHosted, false
				},
				lifecycle: domainpolicy.LifecycleAwaitingProof, authority: domainpolicy.AuthorityLocusOwnerDNS,
				route: domainpolicy.ResolutionRouteStandardDNS, backend: domainpolicy.BackendSystemDNS,
				hosting: domainpolicy.HostingRequestOwner, policyID: domainpolicy.ProfileIDICANNOwner, ver: domainpolicy.ProfileVersion1,
			},
			{
				name: "hns.portal.native", domain: "native", namespace: "hns",
				seed: func(w *pluginDb.WebsiteDomain, _ uint, _ uint, _ *uint) {
					w.ZoneID, w.Status, w.DNSHostingEnabled = 12, pluginDb.DomainStatusActive, true
				},
				lifecycle: domainpolicy.LifecycleActive, authority: domainpolicy.AuthorityLocusPortalZone,
				route: domainpolicy.ResolutionRouteHNSRoot, backend: domainpolicy.BackendHNSRoot,
				hosting: domainpolicy.HostingRequestPortal, policyID: domainpolicy.ProfileIDHNSPortalNative, ver: domainpolicy.ProfileVersion1,
			},
			{
				name: "hns.portal.namebase-child", domain: "a.hns", namespace: "hns",
				seed: func(w *pluginDb.WebsiteDomain, _ uint, _ uint, _ *uint) {
					w.ZoneID, w.Status, w.DNSHostingEnabled = 13, pluginDb.DomainStatusWaitingDelegation, true
				},
				lifecycle: domainpolicy.LifecycleAwaitingProof, authority: domainpolicy.AuthorityLocusPortalZone,
				route: domainpolicy.ResolutionRouteHNSRoot, backend: domainpolicy.BackendHNSRoot,
				hosting: domainpolicy.HostingRequestPortal, policyID: domainpolicy.ProfileIDHNSPortalNamebaseChild, ver: domainpolicy.ProfileVersion1,
			},
			{
				name: "hns.owner.native", domain: "mine", namespace: "hns",
				seed: func(w *pluginDb.WebsiteDomain, _ uint, _ uint, _ *uint) {
					w.Status, w.DNSHostingEnabled = pluginDb.DomainStatusSelfHosted, false
				},
				lifecycle: domainpolicy.LifecycleAwaitingProof, authority: domainpolicy.AuthorityLocusOwnerDNS,
				route: domainpolicy.ResolutionRouteHNSRoot, backend: domainpolicy.BackendHNSRoot,
				hosting: domainpolicy.HostingRequestOwner, policyID: domainpolicy.ProfileIDHNSOwnerNative, ver: domainpolicy.ProfileVersion1,
			},
			{
				name: "hns.chain.ethereum", domain: "chainy", namespace: "hns",
				seed: func(w *pluginDb.WebsiteDomain, _ uint, _ uint, _ *uint) {
					w.Status, w.DNSHostingEnabled = pluginDb.DomainStatusOnchainManaged, false
				},
				lifecycle: domainpolicy.LifecycleAwaitingProof, authority: domainpolicy.AuthorityLocusChain,
				route: domainpolicy.ResolutionRouteCrossChain, backend: domainpolicy.BackendEthereum,
				hosting: domainpolicy.HostingRequestOwner, policyID: domainpolicy.ProfileIDHNSChainEthereum, ver: domainpolicy.ProfileVersion1,
			},
			{
				name: "platform root", domain: "roots.example", namespace: "icann",
				seed: func(w *pluginDb.WebsiteDomain, zoneID uint, _ uint, platformID *uint) {
					w.ZoneID, w.Status, w.DNSHostingEnabled, w.PlatformDomainID = zoneID, pluginDb.DomainStatusActive, true, platformID
				},
				lifecycle: domainpolicy.LifecycleActive, authority: domainpolicy.AuthorityLocusOperatorZone,
				route: domainpolicy.ResolutionRouteStandardDNS, backend: domainpolicy.BackendPowerDNS,
				hosting: domainpolicy.HostingRequestPortal, policyID: domainpolicy.ProfileIDPlatformICANN, ver: domainpolicy.ProfileVersion1,
			},
			{
				name: "platform subdomain", domain: "sub.roots.example", namespace: "icann",
				seed: func(w *pluginDb.WebsiteDomain, zoneID uint, pZone uint, platformID *uint) {
					w.ZoneID, w.Status, w.DNSHostingEnabled, w.PlatformDomainID = pZone, pluginDb.DomainStatusActive, true, platformID
					_ = zoneID
				},
				lifecycle: domainpolicy.LifecycleActive, authority: domainpolicy.AuthorityLocusOperatorZone,
				route: domainpolicy.ResolutionRouteStandardDNS, backend: domainpolicy.BackendPowerDNS,
				hosting: domainpolicy.HostingRequestPortal, policyID: domainpolicy.ProfileIDPlatformICANN, ver: domainpolicy.ProfileVersion1,
			},
			{
				name: "shared zone", domain: "app.icann-managed.example.com", namespace: "icann",
				seed: func(w *pluginDb.WebsiteDomain, _ uint, _ uint, _ *uint) {
					w.ZoneID, w.Status, w.DNSHostingEnabled = 11, pluginDb.DomainStatusRecordsGenerated, true
				},
				lifecycle: domainpolicy.LifecycleProvisioning, authority: domainpolicy.AuthorityLocusPortalZone,
				route: domainpolicy.ResolutionRouteStandardDNS, backend: domainpolicy.BackendSystemDNS,
				hosting: domainpolicy.HostingRequestPortal, policyID: domainpolicy.ProfileIDICANNPortal, ver: domainpolicy.ProfileVersion1,
			},
		}

		ids := map[string]uint{}
		for _, fx := range fixtures {
			website := createTestWebsite(tb, gormDB, 1, fx.domain)
			require.NotNil(tb, website)
			wd := &pluginDb.WebsiteDomain{
				WebsiteID: website.ID,
				UserID:    1,
				Domain:    fx.domain,
				Namespace: pluginDb.DomainNamespace(fx.namespace),
				Status:    pluginDb.DomainStatusDraft,
			}
			fx.seed(wd, platformZoneID, platformZoneID, &platformDomainID)
			require.NoError(tb, gormDB.Create(wd).Error, "fixture %s", fx.name)
			ids[fx.name] = wd.ID
		}

		summary, err := svc.BackfillPolicyAxes(context.Background(), 0)
		require.NoError(tb, err)
		assert.Equal(tb, len(fixtures), summary.Mapped)
		assert.Equal(tb, 0, summary.Inconsistent)
		assert.Equal(tb, 0, summary.Remaining)

		for _, fx := range fixtures {
			reloaded := reloadBindingTB(tb, gormDB, ids[fx.name])
			require.True(tb, reloaded.PolicyAxesMapped(), "fixture %s must map", fx.name)
			lc, err := reloaded.GetLifecycleStatus()
			require.NoError(tb, err, "fixture %s", fx.name)
			au, err := reloaded.GetAuthorityLocus()
			require.NoError(tb, err)
			rt, err := reloaded.GetResolutionRoute()
			require.NoError(tb, err)
			bk, err := reloaded.GetResolutionBackend()
			require.NoError(tb, err)
			ho, err := reloaded.GetHostingRequest()
			require.NoError(tb, err)
			pid, ver, err := reloaded.GetPolicy()
			require.NoError(tb, err)
			assert.Equal(tb, fx.lifecycle, lc, "fixture %s", fx.name)
			assert.Equal(tb, fx.authority, au, "fixture %s", fx.name)
			assert.Equal(tb, fx.route, rt, "fixture %s", fx.name)
			assert.Equal(tb, fx.backend, bk, "fixture %s", fx.name)
			assert.Equal(tb, fx.hosting, ho, "fixture %s", fx.name)
			assert.Equal(tb, fx.policyID, pid, "fixture %s", fx.name)
			assert.Equal(tb, fx.ver, ver, "fixture %s", fx.name)

			// Dual-write parity: rederiving now yields the same columns that
			// were persisted, and the new-representation class equals the
			// legacy derived class for every coherent row.
			website := fetchBackfillWebsite(tb, gormDB, reloaded.WebsiteID)
			cols := svc.DerivePolicyAxisColumns(context.Background(), &reloaded, website)
			assert.Equal(tb, pluginDb.PolicyReconciliationMapped, cols["reconciliation_status"], "fixture %s", fx.name)
			cls, err := reloaded.PersistedDomainClass()
			require.NoError(tb, err)
			assert.Equal(tb, reloaded.Class(), cls, "fixture %s", fx.name)
		}
	}, TestOptions)
}

// incoherentFixture seeds a row that the legacy mapper must REJECT; the
// backfill must persist the error reconciliation status and nothing else.
func TestPolicyAxesBackfill_IncoherentFixtures(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		platformZoneID, platformDomainID := seedPlatformRoot(tb, gormDB)

		seeds := []struct {
			name   string
			domain string
			ns     string
			seed   func(*pluginDb.WebsiteDomain)
		}{
			{
				"on-chain plus stray zone", "chainsg", "hns",
				func(w *pluginDb.WebsiteDomain) { w.Status, w.ZoneID = pluginDb.DomainStatusOnchainManaged, 21 },
			},
			{
				"self-hosted plus zone", "sh-zone", "icann",
				func(w *pluginDb.WebsiteDomain) { w.Status, w.ZoneID = pluginDb.DomainStatusSelfHosted, 22 },
			},
			{
				"portal lifecycle status without zone", "portal-orphan", "icann",
				func(w *pluginDb.WebsiteDomain) {
					w.Status, w.DNSHostingEnabled = pluginDb.DomainStatusWaitingDelegation, true
				},
			},
			{
				"platform binding with wrong zone", "wrong.roots.example", "icann",
				func(w *pluginDb.WebsiteDomain) {
					w.ZoneID = platformZoneID + 100
					w.PlatformDomainID = &platformDomainID
					w.Status, w.DNSHostingEnabled = pluginDb.DomainStatusActive, true
				},
			},
			{
				"unknown namespace", "weird.name", "fido",
				func(w *pluginDb.WebsiteDomain) { w.Status = pluginDb.DomainStatusActive },
			},
			{
				"ICANN on-chain (missing/unknown route backend)", "no-route-here", "icann",
				func(w *pluginDb.WebsiteDomain) { w.Status = pluginDb.DomainStatusOnchainManaged },
			},
		}

		ids := make([]uint, 0, len(seeds))
		for _, s := range seeds {
			website := createTestWebsite(tb, gormDB, 1, s.domain)
			wd := &pluginDb.WebsiteDomain{
				WebsiteID: website.ID,
				UserID:    1,
				Domain:    s.domain,
				Namespace: pluginDb.DomainNamespace(s.ns),
				Status:    pluginDb.DomainStatusDraft,
			}
			s.seed(wd)
			require.NoError(tb, gormDB.Create(wd).Error, s.name)
			ids = append(ids, wd.ID)
		}

		// A binding with no website row at all: the mapper cannot validate the
		// target facts; it must land on the error reconciliation status.
		ghost := &pluginDb.WebsiteDomain{WebsiteID: 999999, UserID: 1, Domain: "ghost.name", Namespace: pluginDb.DomainNamespaceICANN, Status: pluginDb.DomainStatusDraft}
		require.NoError(tb, gormDB.Create(ghost).Error)
		ids = append(ids, ghost.ID)

		summary, err := svc.BackfillPolicyAxes(context.Background(), 0)
		require.NoError(tb, err)
		assert.Equal(tb, len(seeds)+1, summary.Inconsistent)
		assert.Equal(tb, 0, summary.Mapped)
		assert.Equal(tb, 0, summary.Remaining)

		for _, id := range ids {
			reloaded := reloadBindingTB(tb, gormDB, id)
			status, err := reloaded.GetReconciliationStatus()
			require.NoError(tb, err, "binding %d", id)
			assert.Equal(tb, pluginDb.PolicyReconciliationError, status, "binding %d must carry the persisted error reconciliation status", id)
			// No axis value was guessed.
			assert.Nil(tb, reloaded.LifecycleStatus, "binding %d", id)
			assert.Nil(tb, reloaded.AuthorityLocus, "binding %d", id)
			assert.Nil(tb, reloaded.ResolutionRoute, "binding %d", id)
			assert.Nil(tb, reloaded.ResolutionBackend, "binding %d", id)
			assert.Nil(tb, reloaded.HostingRequest, "binding %d", id)
			assert.Nil(tb, reloaded.PolicyID, "binding %d", id)
			assert.Nil(tb, reloaded.PolicyVersion, "binding %d", id)
		}
		_ = platformZoneID
	}, TestOptions)
}

func TestPolicyAxesBackfill_SoftDeletedIgnored(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		website := createTestWebsite(tb, gormDB, 1, "tombstone.example.com")
		wd := &pluginDb.WebsiteDomain{
			WebsiteID: website.ID, UserID: 1, Domain: "tombstone.example.com",
			Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 44, Status: pluginDb.DomainStatusRecordsGenerated, DNSHostingEnabled: true,
		}
		require.NoError(tb, gormDB.Create(wd).Error)
		require.NoError(tb, gormDB.Delete(wd).Error) // soft delete

		summary, err := svc.BackfillPolicyAxes(context.Background(), 0)
		require.NoError(tb, err)

		var tombstone pluginDb.WebsiteDomain
		require.NoError(tb, gormDB.Unscoped().Where("domain = ?", "tombstone.example.com").First(&tombstone).Error)
		assert.False(tb, tombstone.PolicyAxesMapped())
		assert.Nil(tb, tombstone.ReconciliationStatus, "soft-deleted bindings are never processed")
		assert.Equal(tb, 0, summary.Mapped)
		assert.Equal(tb, 0, summary.Remaining)
	}, TestOptions)
}

// TestPolicyAxesBackfill_AlreadyMappedIdempotent runs the backfill twice: the
// second pass must report the rows as already mapped and map nothing new.
func TestPolicyAxesBackfill_AlreadyMappedIdempotent(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		website := createTestWebsite(tb, gormDB, 1, "twice.example.com")
		wd := &pluginDb.WebsiteDomain{
			WebsiteID: website.ID, UserID: 1, Domain: "twice.example.com",
			Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 51, Status: pluginDb.DomainStatusRecordsGenerated, DNSHostingEnabled: true,
		}
		require.NoError(tb, gormDB.Create(wd).Error)

		first, err := svc.BackfillPolicyAxes(context.Background(), 0)
		require.NoError(tb, err)
		assert.Equal(tb, 1, first.Mapped)

		second, err := svc.BackfillPolicyAxes(context.Background(), 0)
		require.NoError(tb, err)
		assert.Equal(tb, 1, second.AlreadyMapped)
		assert.Equal(tb, 0, second.Mapped)
		assert.Equal(tb, 0, second.Remaining)
	}, TestOptions)
}

// TestPolicyAxes_DualRead_PreferPersistedAxes verifies the dual-read
// precedence: chosen axes when complete and current; legacy fallback for
// unmapped rows; legacy fallback when axes are stale relative to the legacy
// fields; and parity between the two paths for coherent rows.
func TestPolicyAxes_DualRead_PreferPersistedAxes(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		website := createTestWebsite(tb, gormDB, 1, "dualread.example.com")
		legacy := &pluginDb.WebsiteDomain{
			WebsiteID: website.ID, UserID: 1, Domain: "dualread.example.com",
			Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 61, Status: pluginDb.DomainStatusRecordsGenerated, DNSHostingEnabled: true,
		}
		require.NoError(tb, gormDB.Create(legacy).Error)

		// 1. Before mapping: the legacy mapper answers (fallback path).
		legacyPlan, err := svc.CurrentBindingPlan(legacy, website)
		require.NoError(tb, err)

		summary, err := svc.BackfillPolicyAxes(context.Background(), 0)
		require.NoError(tb, err)
		require.Equal(tb, 1, summary.Mapped)

		reloaded := reloadBindingTB(tb, gormDB, legacy.ID)
		website = fetchBackfillWebsite(tb, gormDB, reloaded.WebsiteID)

		// 2. Parity: the mapped row's axes path produces the same plan the
		// legacy mapper produces for the coherent row.
		axesPlan, ok, err := svc.planFromPersistedAxes(&reloaded, website)
		require.NoError(tb, err)
		require.True(tb, ok, "mapped row must take the persisted-axes path")
		assert.True(tb, reflect.DeepEqual(legacyPlan, axesPlan))
		plan, err := svc.CurrentBindingPlan(&reloaded, website)
		require.NoError(tb, err)
		assert.True(tb, reflect.DeepEqual(legacyPlan, plan))

		// 3. A legacy-only write that skipped the dual-write leaves the axes
		// stale: the reader falls back to the legacy mapper (same-behavior
		// guarantee) instead of authorizing the stale axes.
		require.NoError(tb, gormDB.Model(&pluginDb.WebsiteDomain{}).Where("id = ?", reloaded.ID).
			Update("status", pluginDb.DomainStatusActive).Error)
		stale := reloadBindingTB(tb, gormDB, legacy.ID)
		website = fetchBackfillWebsite(tb, gormDB, stale.WebsiteID)
		stalePlan, err := svc.CurrentBindingPlan(&stale, website)
		require.NoError(tb, err)
		masked := axesMasked(&stale)
		expectedPlan, err := svc.CurrentBindingPlan(masked, website)
		require.NoError(tb, err)
		assert.True(tb, reflect.DeepEqual(expectedPlan, stalePlan))

		// 4. Corrupt axis values on a mapped row fail closed instead of
		// silently authorizing the legacy mapper.
		require.NoError(tb, gormDB.Model(&pluginDb.WebsiteDomain{}).Where("id = ?", legacy.ID).
			Update("lifecycle_status", "corrupt-garbage").Error)
		corrupt := reloadBindingTB(tb, gormDB, legacy.ID)
		_, _, err = svc.planFromPersistedAxes(&corrupt, website)
		require.Error(tb, err)
	}, TestOptions)
}

// axesMasked returns a copy of wd with the axis columns suppressed, forcing
// the LEGACY read path in CurrentBindingPlan.
func axesMasked(wd *pluginDb.WebsiteDomain) *pluginDb.WebsiteDomain {
	masked := *wd
	masked.LifecycleStatus = nil
	masked.AuthorityLocus = nil
	masked.ResolutionRoute = nil
	masked.ResolutionBackend = nil
	masked.HostingRequest = nil
	masked.PolicyID = nil
	masked.PolicyVersion = nil
	masked.ReconciliationStatus = nil
	return &masked
}

// reloadBindingTB reloads one binding fresh from the database.
func reloadBindingTB(tb coreTesting.TB, gormDB *gorm.DB, id uint) pluginDb.WebsiteDomain {
	tb.Helper()
	var wd pluginDb.WebsiteDomain
	require.NoError(tb, gormDB.Where("id = ?", id).First(&wd).Error)
	return wd
}

func fetchBackfillWebsite(tb coreTesting.TB, gormDB *gorm.DB, id uint) *pluginDb.Website {
	tb.Helper()
	var w pluginDb.Website
	require.NoError(tb, gormDB.First(&w, id).Error)
	return &w
}
func TestPolicyAxes_DualWrite_CreateDomainFlow(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		website := createTestWebsite(tb, gormDB, 1, "dualwritetest.com")

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, mockDNS)
		// The zone's gorm Model ID must be set: the flow assigns
		// wd.ZoneID from the returned zone, and a zero id would leave the
		// binding unresolved (not portal-managed).
		mockDNS.EXPECT().CreateZone(mock.Anything, "dualwritetest.com", uint(1)).
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 1}, Domain: "dualwritetest.com"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		mockWebsite := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockWebsite.EXPECT().NotifyAdminWebsiteCreated(mock.Anything, website.ID).Return(nil).Once()

		wd, err := svc.CreateDomain(context.Background(), "icann", "dualwritetest.com", website.ID, 1, true, true, nil, nil)
		require.NoError(tb, err)

		reloaded := reloadBindingTB(tb, gormDB, wd.ID)
		assert.True(tb, reloaded.PolicyAxesMapped())
		au, err := reloaded.GetAuthorityLocus()
		require.NoError(tb, err)
		assert.Equal(tb, domainpolicy.AuthorityLocusPortalZone, au)
		lc, err := reloaded.GetLifecycleStatus()
		require.NoError(tb, err)
		assert.Equal(tb, domainpolicy.LifecycleProvisioning, lc)
		pid, ver, err := reloaded.GetPolicy()
		require.NoError(tb, err)
		assert.Equal(tb, domainpolicy.ProfileIDICANNPortal, pid)
		assert.Equal(tb, domainpolicy.ProfileVersion1, ver)
		ho, err := reloaded.GetHostingRequest()
		require.NoError(tb, err)
		assert.Equal(tb, domainpolicy.HostingRequestPortal, ho)
	}, TestOptions)
}
