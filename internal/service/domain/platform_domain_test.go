package domain

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/queryutil"
	"gorm.io/gorm"
)

// createPlatformRoot inserts a PlatformDomain directly (as the admin flow would)
// and returns it.
func createPlatformRoot(tb testing.TB, ctx coreTesting.TestContext, domain string, namespace pluginDb.DomainNamespace, zoneID uint, enabled bool) *pluginDb.PlatformDomain {
	tb.Helper()
	pd := &pluginDb.PlatformDomain{
		Domain:    domain,
		Namespace: namespace,
		ZoneID:    zoneID,
		Enabled:   enabled,
	}
	require.NoError(tb, ctx.DB().Create(pd).Error)
	return pd
}

func TestCreatePlatformDomain_AutoCreatesZone(t *testing.T) {
	// The operator's zone is auto-created (idempotently) from the operator
	// user; the resulting zone ID is stored on the PlatformDomain row. A root
	// is registered only when zone creation succeeds.
	t.Run("creates_zone_and_registers_root", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			mockDNS.EXPECT().CreateZone(mock.Anything, "platform.com", uint(1)).
				Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "platform.com", UserID: 1}, nil).Once()

			pd, err := svc.CreatePlatformDomain(context.Background(), "platform.com", pluginDb.DomainNamespaceICANN, 1, true)
			require.NoError(tb, err)
			assert.Equal(tb, uint(7), pd.ZoneID)
			assert.Equal(tb, "platform.com", pd.Domain)
		}, TestOptions)
	})
	t.Run("reuses_existing_zone_idempotently", func(t *testing.T) {
		// CreateZone is idempotent: when a zone already exists for the domain it
		// returns the existing zone. CreatePlatformDomain must store THAT zone's
		// ID, never assume a fresh one.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			mockDNS.EXPECT().CreateZone(mock.Anything, "platform.com", uint(1)).
				Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 9}, Domain: "platform.com", UserID: 1}, nil).Once()

			pd, err := svc.CreatePlatformDomain(context.Background(), "platform.com", pluginDb.DomainNamespaceICANN, 1, true)
			require.NoError(tb, err)
			assert.Equal(tb, uint(9), pd.ZoneID)
		}, TestOptions)
	})
	t.Run("rejects_when_zone_creation_fails", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			mockDNS.EXPECT().CreateZone(mock.Anything, "platform.com", uint(1)).
				Return(nil, errors.New("boom")).Once()

			_, err := svc.CreatePlatformDomain(context.Background(), "platform.com", pluginDb.DomainNamespaceICANN, 1, true)
			require.Error(tb, err)
			assert.Contains(tb, err.Error(), "provision platform zone")
		}, TestOptions)
	})
	t.Run("rejects_zero_operator_user", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			_, err := svc.CreatePlatformDomain(context.Background(), "platform.com", pluginDb.DomainNamespaceICANN, 0, true)
			require.Error(tb, err)
			assert.Contains(tb, err.Error(), "operator user")
		}, TestOptions)
	})
}

func TestCreatePlatformDomain_DuplicateLiveRootRejected(t *testing.T) {
	// Kody review: the old (domain, namespace, deleted_at) widening let two live
	// rows with the same (domain, namespace) coexist (NULL deleted_at values are
	// distinct), so re-registering a root created duplicates that made
	// GetEnabledPlatformDomain report a spurious "multiple namespaces" ambiguity.
	// With the strict (domain, namespace) unique key + tombstone purge, a second
	// live registration must be rejected, not silently duplicated.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockDNS.EXPECT().CreateZone(mock.Anything, "platform.com", uint(1)).
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "platform.com", UserID: 1}, nil).Times(2)

		_, err := svc.CreatePlatformDomain(context.Background(), "platform.com", pluginDb.DomainNamespaceICANN, 1, true)
		require.NoError(tb, err)

		_, err = svc.CreatePlatformDomain(context.Background(), "platform.com", pluginDb.DomainNamespaceICANN, 1, true)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "already registered")
	}, TestOptions)
}

func TestDeletePlatformDomain_SoftDelete(t *testing.T) {
	// C5: deleting a platform root is a soft delete — it disappears from
	// lookups but can be re-registered (the unique key includes deleted_at).
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		pd := createPlatformRoot(tb, ctx, "platform.com", pluginDb.DomainNamespaceICANN, 7, true)

		// Still resolvable before deletion.
		got, err := svc.GetEnabledPlatformDomainByDomain(context.Background(), "platform.com")
		require.NoError(tb, err)
		require.NotNil(tb, got)

		require.NoError(tb, svc.DeletePlatformDomain(context.Background(), pd.ID))

		// Soft-deleted root is filtered from lookups.
		got, err = svc.GetEnabledPlatformDomainByDomain(context.Background(), "platform.com")
		require.NoError(tb, err)
		assert.Nil(tb, got)

		// Re-registration of the same domain works: CreatePlatformDomain purges the
		// soft-delete tombstone before inserting against the strict
		// (domain, namespace) unique key.
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		// Re-registration auto-creates (idempotently) the operator zone again.
		mockDNS.EXPECT().CreateZone(mock.Anything, "platform.com", uint(1)).
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "platform.com"}, nil).Once()
		recreated, err := svc.CreatePlatformDomain(context.Background(), "platform.com", pluginDb.DomainNamespaceICANN, 1, true)
		require.NoError(tb, err)
		assert.Equal(tb, "platform.com", recreated.Domain)
		_ = db
	}, TestOptions)
}

func TestGetEnabledPlatformDomain_NamespaceAmbiguity(t *testing.T) {
	// C4: when the same root is registered under multiple namespaces, the
	// resolver must not silently pick one; it either disambiguates by namespace
	// or returns an ambiguity error.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		createPlatformRoot(tb, ctx, "altroot", pluginDb.DomainNamespaceHNS, 1, true)
		createPlatformRoot(tb, ctx, "altroot", pluginDb.DomainNamespaceICANN, 2, true)

		// Ambiguous without a namespace.
		_, err := svc.GetEnabledPlatformDomain(context.Background(), "altroot", "")
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "multiple namespaces")

		// Disambiguating by ICANN picks the ICANN root.
		pd, err := svc.GetEnabledPlatformDomain(context.Background(), "altroot", pluginDb.DomainNamespaceICANN)
		require.NoError(tb, err)
		require.NotNil(tb, pd)
		assert.Equal(tb, pluginDb.DomainNamespaceICANN, pd.Namespace)

		// Disambiguating by HNS picks the HNS root.
		pd, err = svc.GetEnabledPlatformDomain(context.Background(), "altroot", pluginDb.DomainNamespaceHNS)
		require.NoError(tb, err)
		require.NotNil(tb, pd)
		assert.Equal(tb, pluginDb.DomainNamespaceHNS, pd.Namespace)
	}, TestOptions)
}

func TestIsPlatformRootDomain(t *testing.T) {
	// The apex of a platform root is operator-owned and may not be claimed by
	// end users as a custom domain. IsPlatformRootDomain must return true for
	// an enabled root apex (in any normalized form) and false for a subdomain,
	// a disabled/soft-deleted root, or a non-platform domain.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		createPlatformRoot(tb, ctx, "platform.test", pluginDb.DomainNamespaceICANN, 1, true)
		createPlatformRoot(tb, ctx, "disabled.test", pluginDb.DomainNamespaceICANN, 1, false)
		pd := createPlatformRoot(tb, ctx, "deleted.test", pluginDb.DomainNamespaceICANN, 1, true)
		require.NoError(tb, svc.DeletePlatformDomain(context.Background(), pd.ID))

		// Enabled apex is detected in its canonical form.
		isRoot, err := svc.IsPlatformRootDomain(context.Background(), "platform.test")
		require.NoError(tb, err)
		assert.True(tb, isRoot)

		// Normalization (www. prefix, case, whitespace) still detects the apex.
		isRoot, err = svc.IsPlatformRootDomain(context.Background(), "  WWW.PLATFORM.TEST  ")
		require.NoError(tb, err)
		assert.True(tb, isRoot)

		// Disabled and soft-deleted roots are not claimable apexes.
		isRoot, err = svc.IsPlatformRootDomain(context.Background(), "disabled.test")
		require.NoError(tb, err)
		assert.False(tb, isRoot)

		isRoot, err = svc.IsPlatformRootDomain(context.Background(), "deleted.test")
		require.NoError(tb, err)
		assert.False(tb, isRoot)

		// A subdomain of a platform root is NOT the apex — it must be allowed
		// through to the platform-subdomain claim flow.
		isRoot, err = svc.IsPlatformRootDomain(context.Background(), "blog.platform.test")
		require.NoError(tb, err)
		assert.False(tb, isRoot)

		// A completely unrelated domain is not a platform root.
		isRoot, err = svc.IsPlatformRootDomain(context.Background(), "example.com")
		require.NoError(tb, err)
		assert.False(tb, isRoot)

		// Empty domain (auto-generate path) is not a platform root.
		isRoot, err = svc.IsPlatformRootDomain(context.Background(), "")
		require.NoError(tb, err)
		assert.False(tb, isRoot)
	}, TestOptions)
}

func TestGetEnabledPlatformDomain_DisabledAndDeletedExcluded(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		createPlatformRoot(tb, ctx, "disabled.site", pluginDb.DomainNamespaceICANN, 1, false)
		pd := createPlatformRoot(tb, ctx, "deleted.site", pluginDb.DomainNamespaceICANN, 1, true)
		require.NoError(tb, svc.DeletePlatformDomain(context.Background(), pd.ID))

		pd2, err := svc.GetEnabledPlatformDomainByDomain(context.Background(), "disabled.site")
		require.NoError(tb, err)
		assert.Nil(tb, pd2)

		pd3, err := svc.GetEnabledPlatformDomainByDomain(context.Background(), "deleted.site")
		require.NoError(tb, err)
		assert.Nil(tb, pd3)
	}, TestOptions)
}

func TestListEnabledPlatformDomains_FiltersDisabledAndPaginates(t *testing.T) {
	// The user-facing list must expose only enabled (supported) roots, ordered
	// by domain, and honor pagination.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		createPlatformRoot(tb, ctx, "b.site", pluginDb.DomainNamespaceICANN, 1, true)
		createPlatformRoot(tb, ctx, "a.site", pluginDb.DomainNamespaceICANN, 2, true)
		createPlatformRoot(tb, ctx, "disabled.site", pluginDb.DomainNamespaceICANN, 3, false)

		// Lists only enabled roots, ordered by domain asc.
		domains, total, err := svc.ListEnabledPlatformDomains(context.Background(), queryutil.Pagination{})
		require.NoError(tb, err)
		require.Equal(tb, int64(2), total)
		require.Len(tb, domains, 2)
		assert.Equal(tb, "a.site", domains[0].Domain)
		assert.Equal(tb, "b.site", domains[1].Domain)

		// Honors pagination (total is pre-pagination).
		paginated, err := queryutil.NewPagination(0, 1)
		require.NoError(tb, err)
		domains, total, err = svc.ListEnabledPlatformDomains(context.Background(), paginated)
		require.NoError(tb, err)
		require.Equal(tb, int64(2), total)
		require.Len(tb, domains, 1)
		assert.Equal(tb, "a.site", domains[0].Domain)
	}, TestOptions)
}

// TestCreatePlatformSubdomain_NestedRoot_UsesGrantedRootZone is the regression
// test for the fix-4 concern: a claim is granted under a specific PlatformDomain
// and must resolve its authoritative zone from THAT root — never by re-deriving
// via longest suffix-match across registered roots. If a longer nested root
// ("api.platform.com") is registered alongside the granted root
// ("platform.com"), a subdomain under the granted root must still land in the
// granted root's zone (zone for "platform.com"), not the nested root's zone.
func TestCreatePlatformSubdomain_NestedRoot_UsesGrantedRootZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		// Two operator zones exist: the granted parent root and a nested root.
		// Only the granted root's zone is ever consulted by the claim flow.
		require.NoError(tb, db.Create(&pluginDb.DNSZone{
			UserID: 10, Domain: "platform.com", Status: string(pluginDb.DNSZoneStatusActive),
			PowerDNSZoneID: "pdns-parent",
		}).Error)
		require.NoError(tb, db.Create(&pluginDb.DNSZone{
			UserID: 10, Domain: "api.platform.com", Status: string(pluginDb.DNSZoneStatusActive),
			PowerDNSZoneID: "pdns-nested",
		}).Error)
		var parentZone, nestedZone pluginDb.DNSZone
		require.NoError(tb, db.Where("domain = ?", "platform.com").First(&parentZone).Error)
		require.NoError(tb, db.Where("domain = ?", "api.platform.com").First(&nestedZone).Error)

		// Register the granted root and a longer nested root (both enabled).
		pd := createPlatformRoot(tb, ctx, "platform.com", pluginDb.DomainNamespaceICANN, parentZone.ID, true)
		createPlatformRoot(tb, ctx, "api.platform.com", pluginDb.DomainNamespaceICANN, nestedZone.ID, true)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		// The subdomain must resolve to the GRANTED root's zone (parentZone),
		// not the nested root's. The mock only allows the parent zone lookup.
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "platform.com").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: parentZone.ID}, Domain: "platform.com"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(parentZone.ID), mock.Anything, mock.Anything).Return(nil).Once()
		mockDNS.EXPECT().CreateApexRecord(mock.Anything, uint(parentZone.ID), mock.Anything, pluginCore.RecordTypeALIAS, "gw.example.com").Return(nil).Once()

		wd, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "blog", false)
		require.NoError(tb, err)
		require.NotNil(tb, wd)
		assert.Equal(t, "blog.platform.com", wd.Domain)
		assert.Equal(t, parentZone.ID, wd.ZoneID)
		require.NotNil(t, wd.PlatformDomainID)
		assert.Equal(t, pd.ID, *wd.PlatformDomainID)
		// The nested root's zone must never be consulted or created.
		mockDNS.AssertNotCalled(tb, "GetZoneByDomain", mock.Anything, "api.platform.com")
		mockDNS.AssertNotCalled(tb, "CreateZone", mock.Anything, mock.Anything, mock.Anything)
	}, platformApexTestOptions)
}

func TestCheckAvailability(t *testing.T) {
	// Availability must be multi-root, exclude disabled roots, and never probe
	// user-managed zones — it only reads the platform label table.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		createPlatformRoot(tb, ctx, "platform.com", pluginDb.DomainNamespaceICANN, 1, true)
		createPlatformRoot(tb, ctx, "altroot", pluginDb.DomainNamespaceHNS, 2, true)
		createPlatformRoot(tb, ctx, "off.site", pluginDb.DomainNamespaceICANN, 3, false)

		// Occupy one label under root "altroot" (HNS) to prove availability is
		// namespace-scoped: the same label under "platform.com" stays available.
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "taken.altroot", Namespace: pluginDb.DomainNamespaceHNS,
		}).Error)

		results, err := svc.CheckAvailability(context.Background(), "taken")
		require.NoError(tb, err)
		require.Len(tb, results, 2) // disabled root excluded

		byDomain := map[string]PlatformDomainAvailability{}
		for _, r := range results {
			byDomain[r.PlatformDomain] = r
		}
		assert.True(tb, byDomain["platform.com"].Available, "icann label should be free")
		assert.False(tb, byDomain["altroot"].Available, "hns label is taken")
	}, TestOptions)
}

func TestCreatePlatformSubdomain_ExplicitLabelAlreadyTaken(t *testing.T) {
	// C3-ish: claiming an explicit label already held by another binding must
	// surface a clean "already taken" error (unique-key collision on insert),
	// not a 500.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")
		pd := createPlatformRoot(tb, ctx, "platform.com", pluginDb.DomainNamespaceICANN, 7, true)

		// A live binding already occupying "taken.platform.com".
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: website.ID, UserID: 1, Domain: "taken.platform.com", Namespace: pluginDb.DomainNamespaceICANN,
		}).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		// resolveManagedZone resolves the operator zone before the insert
		// attempt hits the unique-key collision.
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "platform.com").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "platform.com"}, nil).Maybe()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(7), mock.Anything, mock.Anything).Return(nil).Maybe()

		_, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "taken", false)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "already taken")
	}, TestOptions)
}

func TestCreatePlatformSubdomain_LabelWWWRejected(t *testing.T) {
	// "www" is deliberately reserved: composing a label whose FQDN starts with a
	// leading "www." would be mangled by NormalizeDomain (and collapse to the
	// apex for the bare "www" label), so the claim flow rejects it up front with
	// a clear error rather than producing a broken binding.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")
		pd := createPlatformRoot(tb, ctx, "platform.com", pluginDb.DomainNamespaceICANN, 7, true)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		_, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "www", false)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "reserved")

		// Any www.-prefixed label is likewise rejected.
		_, err = svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "www.blog", false)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "reserved")
	}, TestOptions)
}

func TestCreatePlatformSubdomain_Generate_HappyPath(t *testing.T) {
	// Claiming a generated label creates a binding under the operator's zone,
	// marks it PlatformDomainID, and sets it active (platform controls both ends
	// of the DNS check, so no user TXT verification is needed).
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")
		pd := createPlatformRoot(tb, ctx, "platform.com", pluginDb.DomainNamespaceICANN, 7, true)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		websiteMock := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		// resolveManagedZone (platform-managed) resolves the operator zone.
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "platform.com").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "platform.com"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(7), mock.Anything, mock.Anything).Return(nil).Once()
		// Claiming the subdomain must trigger the website activation hook (the
		// FSM transition itself is exercised by the website-service tests).
		websiteMock.EXPECT().ActivatePlatformSubdomainWebsite(mock.Anything, website.ID).Return(nil).Once()

		wd, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "", true)
		require.NoError(tb, err)
		require.NotNil(tb, wd)
		require.NotNil(tb, wd.PlatformDomainID)
		assert.Equal(tb, pd.ID, *wd.PlatformDomainID)
		assert.Equal(tb, pluginDb.DomainStatusActive, wd.Status)
		assert.True(tb, wd.DNSHostingEnabled)
		assert.Contains(tb, wd.Domain, ".platform.com")

		// The activation hook must fire as part of the claim — no external
		// websites_validate call should be required for a platform subdomain.
		websiteMock.AssertExpectations(tb)
	}, TestOptions)
}

func TestCreatePlatformSubdomain_RequiresLabelWhenNotGenerate(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")
		pd := createPlatformRoot(tb, ctx, "platform.com", pluginDb.DomainNamespaceICANN, 7, true)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		_, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "", false)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "required")
	}, TestOptions)
}

func TestCreatePlatformSubdomain_DisabledRootRejected(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")
		pd := createPlatformRoot(tb, ctx, "platform.com", pluginDb.DomainNamespaceICANN, 7, false)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		_, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "foo", false)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "disabled")
	}, TestOptions)
}

// TestCreateDomain_NonPlatformSubdomainOfRoot_Rejected proves the S1 security
// gate: only a genuine platform claim may relax into the operator's zone. A
// normal (non-platform) binding for "x.platform.com" must go through the normal
// parent-zone path and be rejected because the operator's zone belongs to a
// different user — it must NOT silently reuse the platform zone.
func TestCreateDomain_NonPlatformSubdomainOfRoot_Rejected(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")

		// Operator-owned zone for the root, owned by user 10 (not the actor).
		require.NoError(tb, db.Create(&pluginDb.DNSZone{
			UserID: 10, Domain: "platform.com", Status: string(pluginDb.DNSZoneStatusActive),
			PowerDNSZoneID: "pdns-1",
		}).Error)
		var opZone pluginDb.DNSZone
		require.NoError(tb, db.Where("domain = ?", "platform.com").First(&opZone).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		// platform-managed=false: the subdomain must NOT resolve to the operator
		// zone; instead it hits the ordinary parent lookup and is rejected as
		// foreign-owned. The platform zone must never be consulted as a relax.
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "platform.com").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: opZone.ID}, Domain: "platform.com", UserID: 10}, nil).Once()
		mockDNS.AssertNotCalled(tb, "CreateZone", mock.Anything, mock.Anything, mock.Anything)

		_, err := svc.CreateDomain(context.Background(), "icann", "x.platform.com", website.ID, 1, true, false, nil, nil)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "owned by another user")
	}, TestOptions)
}

// platformApexTestOptions configures a gateway domain so the managed-DNS flow
// publishes an ALIAS apex record for a binding. TestOptions deliberately leaves
// the gateway unset, so default tests omit the apex-record call and this
// variant is needed only where the apex publish path is under test.
var platformApexTestOptions = coreTesting.CombineOptions(
	TestOptions,
	coreTesting.WithConfig("plugin.ipfs.service.dns.gateway_domain", "gw.example.com"),
)

// platformHNSTestOptions adds the gateway IP needed by HNS, which uses real A
// records at the apex (not synthetic ALIAS like ICANN).
var platformHNSTestOptions = coreTesting.CombineOptions(
	TestOptions,
	coreTesting.WithConfig("plugin.ipfs.service.dns.gateway_ip", "127.0.0.1"),
)

// TestCreatePlatformSubdomain_Generate_RetriesOnCollision proves the C3
// regenerate path: when a generated candidate label is already taken, the
// service must roll to a fresh slug instead of failing. An injectable slugGen
// makes the sequence deterministic.
func TestCreatePlatformSubdomain_Generate_RetriesOnCollision(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")
		pd := createPlatformRoot(tb, ctx, "platform.com", pluginDb.DomainNamespaceICANN, 7, true)

		// Occupy the first generated label so the loop must roll to the next.
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: website.ID, UserID: 1, Domain: "alpha.platform.com", Namespace: pluginDb.DomainNamespaceICANN,
		}).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		slugs := []string{"alpha", "beta"}
		i := 0
		svc.slugGen = func() string {
			s := slugs[i]
			if i < len(slugs)-1 {
				i++
			}
			return s
		}

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "platform.com").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "platform.com"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(7), mock.Anything, mock.Anything).Return(nil).Once()

		wd, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "", true)
		require.NoError(tb, err)
		require.NotNil(tb, wd)
		assert.Equal(tb, "beta.platform.com", wd.Domain)
		require.NotNil(tb, wd.PlatformDomainID)
		assert.Equal(tb, pd.ID, *wd.PlatformDomainID)
	}, TestOptions)
}

// TestCreatePlatformSubdomain_Generate_WritesApexToOperatorZone proves that a
// platform claim publishes its apex record into the operator-owned platform
// zone (ID 7 here), not a freshly created user zone.
func TestCreatePlatformSubdomain_Generate_WritesApexToOperatorZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")
		pd := createPlatformRoot(tb, ctx, "platform.com", pluginDb.DomainNamespaceICANN, 7, true)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "platform.com").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "platform.com"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(7), mock.Anything, mock.Anything).Return(nil).Once()
		mockDNS.EXPECT().CreateApexRecord(mock.Anything, uint(7), mock.Anything, pluginCore.RecordTypeALIAS, "gw.example.com").Return(nil).Once()

		wd, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "blog", false)
		require.NoError(tb, err)
		require.NotNil(tb, wd)
		assert.Equal(tb, "blog.platform.com", wd.Domain)
		assert.Equal(tb, uint(7), wd.ZoneID)
		assert.Equal(tb, "gw.example.com", wd.GatewayHost)
		// The operator's zone is reused — no new zone is ever created.
		mockDNS.AssertNotCalled(tb, "CreateZone", mock.Anything, mock.Anything, mock.Anything)
	}, platformApexTestOptions)
}

// TestLabelFor_WWWNotNormalized proves that labelFor does NOT strip a leading
// "www." — the label "www" on root "platform.com" must yield "www.platform.com",
// not the bare root apex "platform.com".
func TestLabelFor_WWWNotNormalized(t *testing.T) {
	assert.Equal(t, "www.platform.com", labelFor("www", "platform.com"))
	assert.Equal(t, "blog.platform.com", labelFor("blog", "platform.com"))
	assert.Equal(t, "my-label.platform.com", labelFor("MY-LABEL", "platform.com"))
}

// TestLabelFor_HNSSingleLabelRoot proves that labelFor correctly composes a
// subdomain under a single-label HNS root (no dot in the root domain).
func TestLabelFor_HNSSingleLabelRoot(t *testing.T) {
	assert.Equal(t, "blog.altroot", labelFor("blog", "altroot"))
	assert.Equal(t, "www.altroot", labelFor("www", "altroot"))
	assert.Equal(t, "my-label.altroot", labelFor("MY-LABEL", "altroot"))
}

// TestCreatePlatformSubdomain_HNS_HappyPath proves the full create flow works
// for an HNS (single-label) platform root: the binding is created with the
// correct FQDN, the platform zone is reused (not a new zone), and the binding
// is marked active with the PlatformDomain reference.
func TestCreatePlatformSubdomain_HNS_HappyPath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")
		pd := createPlatformRoot(tb, ctx, "altroot", pluginDb.DomainNamespaceHNS, 7, true)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "altroot").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "altroot"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(7), mock.Anything, mock.Anything).Return(nil).Once()
		mockDNS.EXPECT().CreateApexRecord(mock.Anything, uint(7), mock.Anything, pluginCore.RecordTypeA, "127.0.0.1").Return(nil).Once()
		mockDNS.EXPECT().EnableDNSSEC(mock.Anything, uint(7)).Return("", nil).Maybe()

		wd, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "blog", false)
		require.NoError(tb, err)
		require.NotNil(tb, wd)
		assert.Equal(tb, "blog.altroot", wd.Domain)
		require.NotNil(tb, wd.PlatformDomainID)
		assert.Equal(tb, pd.ID, *wd.PlatformDomainID)
		assert.Equal(tb, pluginDb.DomainStatusActive, wd.Status)
		assert.True(tb, wd.DNSHostingEnabled)
		assert.Equal(tb, pluginDb.DomainNamespaceHNS, wd.Namespace)
		// No new zone created — the operator's HNS zone is reused.
		mockDNS.AssertNotCalled(tb, "CreateZone", mock.Anything, mock.Anything, mock.Anything)
	}, platformHNSTestOptions)
}

// TestCreatePlatformSubdomain_HNS_Generate proves that the generate path
// works under an HNS root and produces a subdomain with the correct suffix.
func TestCreatePlatformSubdomain_HNS_Generate(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")
		pd := createPlatformRoot(tb, ctx, "altroot", pluginDb.DomainNamespaceHNS, 7, true)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "altroot").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "altroot"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(7), mock.Anything, mock.Anything).Return(nil).Once()
		mockDNS.EXPECT().CreateApexRecord(mock.Anything, uint(7), mock.Anything, pluginCore.RecordTypeA, "127.0.0.1").Return(nil).Once()
		mockDNS.EXPECT().EnableDNSSEC(mock.Anything, uint(7)).Return("", nil).Maybe()

		wd, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "", true)
		require.NoError(tb, err)
		require.NotNil(tb, wd)
		assert.Contains(tb, wd.Domain, ".altroot")
		require.NotNil(tb, wd.PlatformDomainID)
		assert.Equal(tb, pd.ID, *wd.PlatformDomainID)
		assert.Equal(tb, pluginDb.DomainNamespaceHNS, wd.Namespace)
	}, platformHNSTestOptions)
}

// TestCheckAvailability_HNS tests availability checking under an HNS root
// specifically, verifying that the single-label root is handled and the
// namespace scoping works with HNS.
func TestCheckAvailability_HNS(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		createPlatformRoot(tb, ctx, "altroot", pluginDb.DomainNamespaceHNS, 1, true)

		// Occupy one label.
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "taken.altroot", Namespace: pluginDb.DomainNamespaceHNS,
		}).Error)

		// Taken label is not available.
		results, err := svc.CheckAvailability(context.Background(), "taken")
		require.NoError(tb, err)
		require.Len(tb, results, 1)
		assert.False(tb, results[0].Available)
		assert.Equal(tb, "altroot", results[0].PlatformDomain)

		// Free label is available.
		results, err = svc.CheckAvailability(context.Background(), "free")
		require.NoError(tb, err)
		require.Len(tb, results, 1)
		assert.True(tb, results[0].Available)
	}, TestOptions)
}

// TestResolveManagedZone_PlatformGuard_HNSSingleLabel exercises the guards in
// resolveManagedZone's platform branch for a single-label (HNS) root: the root
// apex itself must be rejected (a claim is never the root), and an unrelated
// name must be rejected as not a subdomain of the granted root, while a proper
// subdomain resolves to the granted root's zone.
func TestResolveManagedZone_PlatformGuard_HNSSingleLabel(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		pd := createPlatformRoot(tb, ctx, "altroot", pluginDb.DomainNamespaceHNS, 7, true)

		// Apex of the root resolves to the operator's platform zone (the
		// root-apex binding path via BindPlatformRootApex), never a new zone.
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "altroot").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "altroot"}, nil).Once()
		z, created, err := svc.resolveManagedZone(context.Background(), "altroot", 1, &pd.ID)
		require.NoError(tb, err)
		require.NotNil(tb, z)
		assert.Equal(t, uint(7), z.ID)
		assert.False(t, created, "a platform apex must never create a new zone")

		// An unrelated name is not a subdomain of the granted root.
		_, _, err = svc.resolveManagedZone(context.Background(), "other.xyz", 1, &pd.ID)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "not a subdomain")

		// A proper subdomain resolves to the granted root's zone.
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "altroot").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "altroot"}, nil).Once()
		z, created, err = svc.resolveManagedZone(context.Background(), "blog.altroot", 1, &pd.ID)
		require.NoError(tb, err)
		require.NotNil(tb, z)
		assert.Equal(t, uint(7), z.ID)
		assert.False(t, created, "a platform claim must never create a new zone")
	}, TestOptions)
}

// TestBindPlatformRootApex_WritesApexIntoOperatorZone proves the root-apex
// binding path (BindPlatformRootApex): an operator-owned website is bound
// directly to the platform root's apex, and the DNSLink + apex records are
// written into the operator's platform zone (ID 7), never a freshly created
// zone.
func TestBindPlatformRootApex_WritesApexIntoOperatorZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		// Operator owns the website and the platform root.
		website := createTestWebsite(tb, db, 1, "pinned.site")
		pd := createPlatformRoot(tb, ctx, "pinned.site", pluginDb.DomainNamespaceICANN, 7, true)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		// Apex match resolves the operator's zone; records are written into it.
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "pinned.site").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "pinned.site"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(7), "pinned.site", mock.Anything).Return(nil).Once()
		mockDNS.EXPECT().CreateApexRecord(mock.Anything, uint(7), "pinned.site", pluginCore.RecordTypeALIAS, "gw.example.com").Return(nil).Once()

		wd, err := svc.BindPlatformRootApex(context.Background(), website.ID, 1, pd.ID)
		require.NoError(tb, err)
		require.NotNil(tb, wd)
		assert.Equal(tb, "pinned.site", wd.Domain)
		assert.Equal(tb, uint(7), wd.ZoneID)
		assert.Equal(tb, "gw.example.com", wd.GatewayHost)
		require.NotNil(tb, wd.PlatformDomainID)
		assert.Equal(tb, pd.ID, *wd.PlatformDomainID)
		assert.Equal(tb, pluginDb.DomainStatusActive, wd.Status)
		// No new zone is ever created for a platform apex binding.
		mockDNS.AssertNotCalled(tb, "CreateZone", mock.Anything, mock.Anything, mock.Anything)
	}, platformApexTestOptions)
}

// TestBindPlatformRootApex_DisabledRootRejected ensures a disabled platform
// root cannot be apex-bound.
func TestBindPlatformRootApex_DisabledRootRejected(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "pinned.site")
		pd := createPlatformRoot(tb, ctx, "pinned.site", pluginDb.DomainNamespaceICANN, 7, false)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		_, err := svc.BindPlatformRootApex(context.Background(), website.ID, 1, pd.ID)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "disabled")
	}, TestOptions)
}

// TestBindPlatformRootApex_MultipleRootsAsAdditionalDomains proves that two
// distinct platform roots can both be apex-bound to one operator-owned website
// as additional domains: each root's apex lands in its own operator zone and
// carries its own PlatformDomain reference. The first binding becomes the
// website primary; the second is an additional domain (primary stays the
// first).
func TestBindPlatformRootApex_MultipleRootsAsAdditionalDomains(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "pinned.site")
		pdSite := createPlatformRoot(tb, ctx, "pinned.site", pluginDb.DomainNamespaceICANN, 11, true)
		pdXyz := createPlatformRoot(tb, ctx, "pinner.xyz", pluginDb.DomainNamespaceICANN, 22, true)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "pinned.site").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 11}, Domain: "pinned.site"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(11), "pinned.site", mock.Anything).Return(nil).Once()
		mockDNS.EXPECT().CreateApexRecord(mock.Anything, uint(11), "pinned.site", pluginCore.RecordTypeALIAS, "gw.example.com").Return(nil).Once()
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "pinner.xyz").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 22}, Domain: "pinner.xyz"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(22), "pinner.xyz", mock.Anything).Return(nil).Once()
		mockDNS.EXPECT().CreateApexRecord(mock.Anything, uint(22), "pinner.xyz", pluginCore.RecordTypeALIAS, "gw.example.com").Return(nil).Once()

		wdSite, err := svc.BindPlatformRootApex(context.Background(), website.ID, 1, pdSite.ID)
		require.NoError(tb, err)
		require.NotNil(tb, wdSite)
		assert.Equal(tb, "pinned.site", wdSite.Domain)
		assert.Equal(tb, uint(11), wdSite.ZoneID)
		require.NotNil(tb, wdSite.PlatformDomainID)
		assert.Equal(tb, pdSite.ID, *wdSite.PlatformDomainID)

		wdXyz, err := svc.BindPlatformRootApex(context.Background(), website.ID, 1, pdXyz.ID)
		require.NoError(tb, err)
		require.NotNil(tb, wdXyz)
		assert.Equal(tb, "pinner.xyz", wdXyz.Domain)
		assert.Equal(tb, uint(22), wdXyz.ZoneID)
		require.NotNil(tb, wdXyz.PlatformDomainID)
		assert.Equal(tb, pdXyz.ID, *wdXyz.PlatformDomainID)

		// Both bindings belong to the same website, and the first remains primary.
		var siteDomain, xyzDomain pluginDb.WebsiteDomain
		require.NoError(tb, db.Where("id = ?", wdSite.ID).First(&siteDomain).Error)
		require.NoError(tb, db.Where("id = ?", wdXyz.ID).First(&xyzDomain).Error)
		assert.Equal(tb, website.ID, siteDomain.WebsiteID)
		assert.Equal(tb, website.ID, xyzDomain.WebsiteID)

		var websiteAfter pluginDb.Website
		require.NoError(tb, db.First(&websiteAfter, website.ID).Error)
		require.NotNil(tb, websiteAfter.PrimaryDomainID)
		assert.Equal(tb, wdSite.ID, *websiteAfter.PrimaryDomainID)

		mockDNS.AssertNotCalled(tb, "CreateZone", mock.Anything, mock.Anything, mock.Anything)
	}, platformApexTestOptions)
}
