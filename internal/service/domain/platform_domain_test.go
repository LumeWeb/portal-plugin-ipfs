package domain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
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

func TestCreatePlatformDomain_ZoneValidation(t *testing.T) {
	// S2: a platform root may only be registered against a zone that actually
	// exists and whose ID matches the root's domain.
	t.Run("registers_when_zone_exists", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "pinner.site").
				Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "pinner.site"}, nil).Once()

			pd, err := svc.CreatePlatformDomain(context.Background(), "pinner.site", pluginDb.DomainNamespaceICANN, 7, true)
			require.NoError(tb, err)
			assert.Equal(tb, uint(7), pd.ZoneID)
			assert.Equal(tb, "pinner.site", pd.Domain)
		}, TestOptions)
	})
	t.Run("rejects_when_zone_missing", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "pinner.site").Return(nil, nil).Once()
			_, err := svc.CreatePlatformDomain(context.Background(), "pinner.site", pluginDb.DomainNamespaceICANN, 7, true)
			require.Error(tb, err)
			assert.Contains(tb, err.Error(), "no provisioned zone")
		}, TestOptions)
	})
	t.Run("rejects_when_zone_id_mismatch", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "pinner.site").
				Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "pinner.site"}, nil).Once()

			_, err := svc.CreatePlatformDomain(context.Background(), "pinner.site", pluginDb.DomainNamespaceICANN, 99, true)
			require.Error(tb, err)
			assert.Contains(tb, err.Error(), "does not match")
		}, TestOptions)
	})
	t.Run("rejects_when_zone_id_zero", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			_, err := svc.CreatePlatformDomain(context.Background(), "pinner.site", pluginDb.DomainNamespaceICANN, 0, true)
			require.Error(tb, err)
			assert.Contains(tb, err.Error(), "requires a provisioned zone")
		}, TestOptions)
	})
	t.Run("rejects_soft_deleted_zone", func(t *testing.T) {
		// N2 regression: GetZoneByDomain intentionally returns soft-deleted
		// zones, but a platform root must never be registered against a
		// logically-removed zone.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "retired.site").
				Return(&pluginDb.DNSZone{
					Model:    gorm.Model{ID: 7, DeletedAt: gorm.DeletedAt{Valid: true}},
					Domain:   "retired.site",
					UserID:   1,
					Status:   string(pluginDb.DNSZoneStatusActive),
				}, nil).Once()

			_, err := svc.CreatePlatformDomain(context.Background(), "retired.site", pluginDb.DomainNamespaceICANN, 7, true)
			require.Error(tb, err)
			assert.Contains(tb, err.Error(), "deleted zone")
		}, TestOptions)
	})
}

func TestDeletePlatformDomain_SoftDelete(t *testing.T) {
	// C5: deleting a platform root is a soft delete — it disappears from
	// lookups but can be re-registered (the unique key includes deleted_at).
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		pd := createPlatformRoot(tb, ctx, "pinner.site", pluginDb.DomainNamespaceICANN, 7, true)

		// Still resolvable before deletion.
		got, err := svc.GetEnabledPlatformDomainByDomain(context.Background(), "pinner.site")
		require.NoError(tb, err)
		require.NotNil(tb, got)

		require.NoError(tb, svc.DeletePlatformDomain(context.Background(), pd.ID))

		// Soft-deleted root is filtered from lookups.
		got, err = svc.GetEnabledPlatformDomainByDomain(context.Background(), "pinner.site")
		require.NoError(tb, err)
		assert.Nil(tb, got)

		// Re-registration of the same domain works (unique key includes deleted_at).
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "pinner.site").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "pinner.site"}, nil).Once()
		recreated, err := svc.CreatePlatformDomain(context.Background(), "pinner.site", pluginDb.DomainNamespaceICANN, 7, true)
		require.NoError(tb, err)
		assert.Equal(tb, "pinner.site", recreated.Domain)
		_ = db
	}, TestOptions)
}

func TestGetEnabledPlatformDomain_NamespaceAmbiguity(t *testing.T) {
	// C4: when the same root is registered under multiple namespaces, the
	// resolver must not silently pick one; it either disambiguates by namespace
	// or returns an ambiguity error.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		createPlatformRoot(tb, ctx, "pinner", pluginDb.DomainNamespaceHNS, 1, true)
		createPlatformRoot(tb, ctx, "pinner", pluginDb.DomainNamespaceICANN, 2, true)

		// Ambiguous without a namespace.
		_, err := svc.GetEnabledPlatformDomain(context.Background(), "pinner", "")
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "multiple namespaces")

		// Disambiguating by ICANN picks the ICANN root.
		pd, err := svc.GetEnabledPlatformDomain(context.Background(), "pinner", pluginDb.DomainNamespaceICANN)
		require.NoError(tb, err)
		require.NotNil(tb, pd)
		assert.Equal(tb, pluginDb.DomainNamespaceICANN, pd.Namespace)

		// Disambiguating by HNS picks the HNS root.
		pd, err = svc.GetEnabledPlatformDomain(context.Background(), "pinner", pluginDb.DomainNamespaceHNS)
		require.NoError(tb, err)
		require.NotNil(tb, pd)
		assert.Equal(tb, pluginDb.DomainNamespaceHNS, pd.Namespace)
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

func TestPlatformRootForDomain(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		createPlatformRoot(tb, ctx, "pinner.site", pluginDb.DomainNamespaceICANN, 1, true)
		// A nested, longer root that would win longest-match.
		createPlatformRoot(tb, ctx, "api.pinner.site", pluginDb.DomainNamespaceICANN, 2, true)
		createPlatformRoot(tb, ctx, "disabled.pinner.site", pluginDb.DomainNamespaceICANN, 3, false)
		// A single-label HNS root (alt-root namespace).
		createPlatformRoot(tb, ctx, "pinner", pluginDb.DomainNamespaceHNS, 4, true)

		t.Run("multi_label_match", func(t *testing.T) {
			pd := svc.platformRootForDomain(context.Background(), "myblog.pinner.site")
			require.NotNil(t, pd)
			assert.Equal(t, "pinner.site", pd.Domain)
		})
		t.Run("longest_nested_match_wins", func(t *testing.T) {
			pd := svc.platformRootForDomain(context.Background(), "foo.api.pinner.site")
			require.NotNil(t, pd)
			assert.Equal(t, "api.pinner.site", pd.Domain)
		})
		t.Run("single_label_altroot_match", func(t *testing.T) {
			pd := svc.platformRootForDomain(context.Background(), "mylabel.pinner")
			require.NotNil(t, pd)
			assert.Equal(t, "pinner", pd.Domain)
		})
		t.Run("apex_of_root_is_not_subdomain_of_itself", func(t *testing.T) {
			pd := svc.platformRootForDomain(context.Background(), "pinner.site")
			assert.Nil(t, pd)
		})
		t.Run("unrelated_domain_does_not_match", func(t *testing.T) {
			pd := svc.platformRootForDomain(context.Background(), "other.example.com")
			assert.Nil(t, pd)
		})
		t.Run("disabled_root_excluded", func(t *testing.T) {
			pd := svc.platformRootForDomain(context.Background(), "x.disabled.pinner.site")
			require.NotNil(t, pd)
			// Longest-match would prefer disabled.pinner.site, but it is disabled,
			// so it falls back to the enabled pinner.site winner.
			assert.Equal(t, "pinner.site", pd.Domain)
		})
	}, TestOptions)
}

func TestCheckAvailability(t *testing.T) {
	// Availability must be multi-root, exclude disabled roots, and never probe
	// user-managed zones — it only reads the platform label table.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		createPlatformRoot(tb, ctx, "pinner.site", pluginDb.DomainNamespaceICANN, 1, true)
		createPlatformRoot(tb, ctx, "pinner", pluginDb.DomainNamespaceHNS, 2, true)
		createPlatformRoot(tb, ctx, "off.site", pluginDb.DomainNamespaceICANN, 3, false)

		// Occupy one label under root "pinner" (HNS) to prove availability is
		// namespace-scoped: the same label under "pinner.site" stays available.
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "taken.pinner", Namespace: pluginDb.DomainNamespaceHNS,
		}).Error)

		results, err := svc.CheckAvailability(context.Background(), "taken")
		require.NoError(tb, err)
		require.Len(tb, results, 2) // disabled root excluded

		byDomain := map[string]PlatformDomainAvailability{}
		for _, r := range results {
			byDomain[r.PlatformDomain] = r
		}
		assert.True(tb, byDomain["pinner.site"].Available, "icann label should be free")
		assert.False(tb, byDomain["pinner"].Available, "hns label is taken")
	}, TestOptions)
}

func TestCreatePlatformSubdomain_ExplicitLabelAlreadyTaken(t *testing.T) {
	// C3-ish: claiming an explicit label already held by another binding must
	// surface a clean "already taken" error (unique-key collision on insert),
	// not a 500.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")
		pd := createPlatformRoot(tb, ctx, "pinner.site", pluginDb.DomainNamespaceICANN, 7, true)

		// A live binding already occupying "taken.pinner.site".
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: website.ID, UserID: 1, Domain: "taken.pinner.site", Namespace: pluginDb.DomainNamespaceICANN,
		}).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		_, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "taken", false)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "already taken")
	}, TestOptions)
}

func TestCreatePlatformSubdomain_Generate_HappyPath(t *testing.T) {
	// Claiming a generated label creates a binding under the operator's zone,
	// marks it PlatformDomainID, and sets it active (platform controls both ends
	// of the DNS check, so no user TXT verification is needed).
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")
		pd := createPlatformRoot(tb, ctx, "pinner.site", pluginDb.DomainNamespaceICANN, 7, true)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		// resolveManagedZone (platform-managed) resolves the operator zone.
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "pinner.site").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "pinner.site"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(7), mock.Anything, mock.Anything).Return(nil).Once()

		wd, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "", true)
		require.NoError(tb, err)
		require.NotNil(tb, wd)
		require.NotNil(tb, wd.PlatformDomainID)
		assert.Equal(tb, pd.ID, *wd.PlatformDomainID)
		assert.Equal(tb, pluginDb.DomainStatusActive, wd.Status)
		assert.True(tb, wd.DNSHostingEnabled)
		assert.Contains(tb, wd.Domain, ".pinner.site")
	}, TestOptions)
}

func TestCreatePlatformSubdomain_RequiresLabelWhenNotGenerate(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")
		pd := createPlatformRoot(tb, ctx, "pinner.site", pluginDb.DomainNamespaceICANN, 7, true)

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
		pd := createPlatformRoot(tb, ctx, "pinner.site", pluginDb.DomainNamespaceICANN, 7, false)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		_, err := svc.CreatePlatformSubdomain(context.Background(), website.ID, 1, pd.ID, "foo", false)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "disabled")
	}, TestOptions)
}

// TestCreateDomain_NonPlatformSubdomainOfRoot_Rejected proves the S1 security
// gate: only a genuine platform claim may relax into the operator's zone. A
// normal (non-platform) binding for "x.pinner.site" must go through the normal
// parent-zone path and be rejected because the operator's zone belongs to a
// different user — it must NOT silently reuse the platform zone.
func TestCreateDomain_NonPlatformSubdomainOfRoot_Rejected(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")

		// Operator-owned zone for the root, owned by user 10 (not the actor).
		require.NoError(tb, db.Create(&pluginDb.DNSZone{
			UserID: 10, Domain: "pinner.site", Status: string(pluginDb.DNSZoneStatusActive),
			PowerDNSZoneID: "pdns-1",
		}).Error)
		var opZone pluginDb.DNSZone
		require.NoError(tb, db.Where("domain = ?", "pinner.site").First(&opZone).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		// platform-managed=false: the subdomain must NOT resolve to the operator
		// zone; instead it hits the ordinary parent lookup and is rejected as
		// foreign-owned. The platform zone must never be consulted as a relax.
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "pinner.site").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: opZone.ID}, Domain: "pinner.site", UserID: 10}, nil).Once()
		mockDNS.AssertNotCalled(tb, "CreateZone", mock.Anything, mock.Anything, mock.Anything)

		_, err := svc.CreateDomain(context.Background(), "icann", "x.pinner.site", website.ID, 1, true, false, nil, false)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "owned by another user")
	}, TestOptions)
}

var _ = mock.Anything
