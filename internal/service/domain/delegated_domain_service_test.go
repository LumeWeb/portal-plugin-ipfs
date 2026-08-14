package domain

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/testopts"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

var TestOptions = coreTesting.CombineOptions(
	coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
	testopts.NewMockPluginBuilder().
		WithService(pluginCore.DELEGATED_DOMAIN_SERVICE, NewDelegatedDomainServiceFactory).
		WithServiceConfig(pluginCore.DELEGATED_DOMAIN_SERVICE, &pluginConfig.DelegatedDomainConfig{}).
		WithMigrations(map[core.DBType]fs.FS{
			core.DB_TYPE_SQLITE: migrations.GetSQLite(),
		}).BuilderOption(),
)

func createTestWebsite(tb testing.TB, db *gorm.DB, userID uint, domain string) *pluginDb.Website {
	website := &pluginDb.Website{
		UserID:          userID,
		TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
		TargetMultihash: []byte("test"),
		CIDVersion:      func() *uint8 { v := uint8(1); return &v }(),
		CIDType:         func() *uint8 { v := uint8(85); return &v }(),
		Status:          string(pluginDb.WebsiteStatusPendingValidation),
		ValidationToken: os.Getenv("TEST_VALIDATION_TOKEN"),
	}
	require.NoError(tb, db.Create(website).Error)
	return website
}

func TestDelegatedDomainService_CreateDomain(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()

		website := createTestWebsite(tb, db, 1, "example.com")

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, mockDNS)

		mockDNS.EXPECT().CreateZone(mock.Anything, "example.com", uint(1)).
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 1}, Domain: "example.com"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(1), mock.Anything, mock.Anything).Return(nil).Once()

		// Setting the website's primary domain fires the admin "created"
		// notification from the service layer.
		mockWebsite := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockWebsite.EXPECT().NotifyAdminWebsiteCreated(mock.Anything, website.ID).Return(nil).Once()

		wd, err := svc.CreateDomain(context.Background(), "icann", "example.com", website.ID, 1, true, true, nil)
		assert.NoError(tb, err)
		assert.NotNil(tb, wd)
		assert.Equal(tb, "example.com", wd.Domain)
		assert.Equal(tb, pluginDb.DomainNamespaceICANN, wd.Namespace)
		assert.Equal(tb, uint(1), wd.ZoneID)
		// Managed-DNS binding: the flag is persisted to match the created zone.
		assert.True(tb, wd.DNSHostingEnabled)
	}, TestOptions)
}

// TestDelegatedDomainService_CreateDomain_NotifyEvenWhenPrimarySet ensures the
// created notification is gated on the notifyCreated flag alone — not on the
// website lacking a primary domain. A managed-DNS create on a website that
// already has a primary still fires when notifyCreated is true.
func TestDelegatedDomainService_CreateDomain_NotifyEvenWhenPrimarySet(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()

		website := createTestWebsite(tb, db, 1, "example.com")
		existingPrimary := uint(999)
		require.NoError(tb, db.Model(website).Update("primary_domain_id", existingPrimary).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockDNS.EXPECT().CreateZone(mock.Anything, "alt-example.com", uint(1)).
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 2}, Domain: "alt-example.com"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(2), mock.Anything, mock.Anything).Return(nil).Once()

		// Primary already set, but notifyCreated=true must still emit.
		mockWebsite := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockWebsite.EXPECT().NotifyAdminWebsiteCreated(mock.Anything, website.ID).Return(nil).Once()

		wd, err := svc.CreateDomain(context.Background(), "icann", "alt-example.com", website.ID, 1, true, true, nil)
		assert.NoError(tb, err)
		assert.NotNil(tb, wd)
		assert.Equal(tb, "alt-example.com", wd.Domain)

		// The pre-existing primary is left untouched.
		var reloaded pluginDb.Website
		require.NoError(tb, db.First(&reloaded, website.ID).Error)
		require.NotNil(tb, reloaded.PrimaryDomainID)
		assert.Equal(tb, existingPrimary, *reloaded.PrimaryDomainID)
	}, TestOptions)
}

func TestDelegatedDomainService_CreateDomain_SelfHosted(t *testing.T) {
	// A self-hosted DNS binding (dnsHostingEnabled=false) must NOT create a
	// PowerDNS zone, DNSLink, apex, or delegation — the user runs the
	// authoritative server. It is marked self_hosted with no zone.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()

		website := createTestWebsite(tb, db, 1, "example.com")

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, mockDNS)

		// No zone, DNSLink, apex, or delegation calls for a self-hosted binding.
		mockDNS.AssertNotCalled(tb, "CreateZone", mock.Anything, mock.Anything, mock.Anything)

		// Self-hosted creation still fires the service-layer created
		// notification (the binding is the new website's primary).
		mockWebsite := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockWebsite.EXPECT().NotifyAdminWebsiteCreated(mock.Anything, website.ID).Return(nil).Once()

		wd, err := svc.CreateDomain(context.Background(), "icann", "example.com", website.ID, 1, false, true, nil)
		assert.NoError(tb, err)
		assert.NotNil(tb, wd)
		assert.Equal(tb, pluginDb.DomainStatusSelfHosted, wd.Status)
		assert.Equal(tb, uint(0), wd.ZoneID)
		assert.False(tb, wd.DNSHostingEnabled)
		assert.Nil(tb, wd.DelegationData)

		// The binding is recorded as the website's primary so the website
		// service resolves the apex domain via PrimaryDomainID (the
		// status=active fallback would miss a self-hosted, non-active binding).
		var reloaded pluginDb.Website
		require.NoError(tb, db.First(&reloaded, website.ID).Error)
		require.NotNil(tb, reloaded.PrimaryDomainID)
		assert.Equal(tb, wd.ID, *reloaded.PrimaryDomainID)
	}, TestOptions)
}

func TestDelegatedDomainService_CreateDomain_SelfHostedDANEFailurePurgesBinding(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example")

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		_, err := svc.CreateDomain(context.Background(), "hns", "example", website.ID, 1, false, false, nil)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "failed to bootstrap DANE identity")

		var count int64
		require.NoError(tb, db.Unscoped().Model(&pluginDb.WebsiteDomain{}).
			Where("domain = ? AND namespace = ?", "example", pluginDb.DomainNamespaceHNS).
			Count(&count).Error)
		assert.Zero(tb, count, "failed self-hosted DANE bootstrap must purge the binding")
	}, TestOptions)
}

func TestDelegatedDomainService_CreateDomain_SubdomainReusesParentZone(t *testing.T) {
	// A managed subdomain lives inside its parent's zone; it must NOT create a
	// new PowerDNS zone. resolveManagedZone should return the parent zone.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()

		website := createTestWebsite(tb, db, 1, "example.com")

		// A parent zone owned by the same user must be reused.
		require.NoError(tb, db.Create(&pluginDb.DNSZone{
			UserID: 1, Domain: "example.com", Status: string(pluginDb.DNSZoneStatusActive),
			PowerDNSZoneID: "parent-pdns-id",
		}).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, mockDNS)

		// GetZoneByDomain resolves the parent; CreateZone must NOT be called for
		// the subdomain (a new zone would be an apex owned separately).
		var parent pluginDb.DNSZone
		require.NoError(tb, ctx.DB().WithContext(context.Background()).Where("domain = ?", "example.com").
			First(&parent).Error)

		// resolveManagedZone reuses the parent zone: GetZoneByDomain returns the
		// parent, and CreateZone must NOT be called for the subdomain.
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "example.com").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: parent.ID}, Domain: "example.com", UserID: 1}, nil).Once()
		// The subdomain's records must be named after the subdomain (not the
		// parent apex) so they don't collide with the parent's own records. The
		// apex record only fires when a gateway host is configured (it's absent
		// in this harness), so assert its domain-name strictly if it runs.
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(parent.ID), "docs.example.com", mock.Anything).Return(nil).Once()
		mockDNS.EXPECT().CreateApexRecord(mock.Anything, uint(parent.ID), "docs.example.com", mock.Anything, mock.Anything).Return(nil).Maybe()
		mockDNS.AssertNotCalled(tb, "CreateZone", mock.Anything, "docs.example.com", mock.Anything)

		// The subdomain becomes the website's primary binding, firing the
		// admin "created" notification from the service layer.
		mockWebsite := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockWebsite.EXPECT().NotifyAdminWebsiteCreated(mock.Anything, website.ID).Return(nil).Once()

		wd, err := svc.CreateDomain(context.Background(), "icann", "docs.example.com", website.ID, 1, true, true, nil)
		assert.NoError(tb, err)
		assert.NotNil(tb, wd)
		// DNSLink/apex/delegation are written into the reused parent zone; the
		// binding references it via ZoneName but owns no separate zone ID.
		assert.Equal(tb, pluginDb.DomainStatusRecordsGenerated, wd.Status)
		assert.True(tb, wd.DNSHostingEnabled)
		assert.Equal(tb, uint(parent.ID), wd.ZoneID)
	}, TestOptions)
}

func TestDelegatedDomainService_CreateDomain_SubdomainForeignParentRejected(t *testing.T) {
	// One-zone invariant: a subdomain must NOT create a competing authoritative
	// zone when a parent zone exists but belongs to another user (subdomain-zone
	// squatting). It must error instead.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()

		website := createTestWebsite(tb, db, 1, "example.com")

		// Parent zone owned by a DIFFERENT user (2).
		require.NoError(tb, db.Create(&pluginDb.DNSZone{
			UserID: 2, Domain: "example.com", Status: string(pluginDb.DNSZoneStatusActive),
			PowerDNSZoneID: "foreign-pdns-id",
		}).Error)

		var parent pluginDb.DNSZone
		require.NoError(tb, ctx.DB().WithContext(context.Background()).Where("domain = ?", "example.com").
			First(&parent).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, mockDNS)

		// Parent lookup returns the foreign-owned zone; CreateZone for the
		// subdomain must NEVER be called.
		mockDNS.EXPECT().GetZoneByDomain(mock.Anything, "example.com").
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: parent.ID}, Domain: "example.com", UserID: 2}, nil).Once()
		mockDNS.AssertNotCalled(tb, "CreateZone", mock.Anything, "docs.example.com", mock.Anything)

		_, err := svc.CreateDomain(context.Background(), "icann", "docs.example.com", website.ID, 1, true, false, nil)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "owned by another user")
	}, TestOptions)
}

func TestDelegatedDomainService_CreateDomain_UnsupportedNamespace(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		_, err := svc.CreateDomain(context.Background(), "ens", "example.eth", 1, 1, true, false, nil)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "unsupported namespace")
	}, TestOptions)
}

func TestDelegatedDomainService_GetWebsiteDomainByName(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()

		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "example", Namespace: pluginDb.DomainNamespaceHNS,
		}).Error)
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: 2, UserID: 2, Domain: "example.com", Namespace: pluginDb.DomainNamespaceICANN,
		}).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		t.Run("find_hns_domain", func(t *testing.T) {
			wd, err := svc.GetWebsiteDomainByName(t.Context(), "example")
			require.NoError(t, err)
			assert.Equal(t, "example", wd.Domain)
			assert.Equal(t, pluginDb.DomainNamespaceHNS, wd.Namespace)
		})

		t.Run("find_icann_domain", func(t *testing.T) {
			wd, err := svc.GetWebsiteDomainByName(t.Context(), "example.com")
			require.NoError(t, err)
			assert.Equal(t, "example.com", wd.Domain)
			assert.Equal(t, pluginDb.DomainNamespaceICANN, wd.Namespace)
		})

		t.Run("not_found", func(t *testing.T) {
			_, err := svc.GetWebsiteDomainByName(t.Context(), "nonexistent")
			assert.Error(t, err)
			assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
		})
	}, TestOptions)
}

func TestDelegatedDomainService_GetWebsiteDomainByDomainAndNamespace(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()

		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "example", Namespace: pluginDb.DomainNamespaceHNS,
		}).Error)
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: 2, UserID: 2, Domain: "example", Namespace: pluginDb.DomainNamespaceICANN,
		}).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		t.Run("find_by_namespace", func(t *testing.T) {
			wd, err := svc.GetWebsiteDomainByDomainAndNamespace(t.Context(), "example", pluginDb.DomainNamespaceHNS)
			require.NoError(t, err)
			assert.Equal(t, pluginDb.DomainNamespaceHNS, wd.Namespace)
		})

		t.Run("find_other_namespace", func(t *testing.T) {
			wd, err := svc.GetWebsiteDomainByDomainAndNamespace(t.Context(), "example", pluginDb.DomainNamespaceICANN)
			require.NoError(t, err)
			assert.Equal(t, pluginDb.DomainNamespaceICANN, wd.Namespace)
		})

		t.Run("not_found", func(t *testing.T) {
			_, err := svc.GetWebsiteDomainByDomainAndNamespace(t.Context(), "nonexistent", pluginDb.DomainNamespaceHNS)
			assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
		})
	}, TestOptions)
}

func TestDelegatedDomainService_VerifyDomain_SelfHealsDNSSEC(t *testing.T) {
	// Regression: a managed (HNS) zone reporting "no active signing key"
	// (GetActiveDNSSECDS -> ("", nil)) must self-heal by running the
	// idempotent EnableDNSSEC, then re-reading the live DS — instead of
	// silently verifying NS-only and leaving the zone without a DS. This
	// recovers zones bound before the cryptokey-id fix whose key was never
	// readable, without requiring the user to re-bind.
	t.Run("hns_no_key_triggers_enable", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			db := ctx.DB()
			require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "example", Namespace: pluginDb.DomainNamespaceHNS,
				ZoneID: 42, Status: pluginDb.DomainStatusWaitingDelegation,
			}).Error)

			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, mockDNS)

			// First read: no active key. Self-heal calls EnableDNSSEC, then
			// re-reads and finds a live DS (the minted key).
			mockDNS.EXPECT().GetActiveDNSSECDS(mock.Anything, uint(42)).
				Return("", nil).Once()
			mockDNS.EXPECT().EnableDNSSEC(mock.Anything, uint(42)).
				Return("257 3 13 dGVzdA==", nil).Once()
			mockDNS.EXPECT().GetActiveDNSSECDS(mock.Anything, uint(42)).
				Return("60776 13 2 abc", nil).Once()
			// SOA MNAME self-heal runs for the managed zone (best-effort).
			mockDNS.EXPECT().EnsureSOAMNAME(mock.Anything, uint(42), "example", mock.Anything).
				Return(nil).Once()

			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "example", Namespace: pluginDb.DomainNamespaceHNS, ZoneID: 42,
			}

			// VerifyDomain proceeds to HNSProvider.VerifyDelegation, which
			// returns the "resolver not configured" error in a unit harness
			// (no real HNS resolver). The self-heal assertions below are what
			// this test guards — EnableDNSSEC MUST have been invoked, the DS
			// re-read, and the SOA MNAME self-heal fired, before delegation
			// verification.
			verified, err := svc.VerifyDomain(context.Background(), wd)
			_ = verified
			if err != nil {
				// Allowed: resolver-not-configured after the self-heal mocks
				// fired. The point of this test is the self-heal sequence.
				tb.Logf("expected post-self-heal delegation error: %v", err)
			}

			mockDNS.AssertCalled(tb, "EnableDNSSEC", mock.Anything, uint(42))
			mockDNS.AssertNumberOfCalls(tb, "GetActiveDNSSECDS", 2)
			mockDNS.AssertCalled(tb, "EnsureSOAMNAME", mock.Anything, uint(42), "example", mock.Anything)
			mockDNS.AssertExpectations(tb)
		}, TestOptions)
	})

	// The SOA MNAME self-heal applies to any portal-managed PowerDNS zone, so
	// an ICANN-hosted binding gets its SOA re-ensured on verify too.
	t.Run("icann_soa_healed_dnssec_skipped", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			db := ctx.DB()
			require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "example.com", Namespace: pluginDb.DomainNamespaceICANN,
				ZoneID: 42, Status: pluginDb.DomainStatusWaitingDelegation,
			}).Error)

			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, mockDNS)

			// ICANN has no portal DNSSEC: GetActiveDNSSECDS returns ("", nil)
			// by default and EnableDNSSEC must NOT be touched. Only the SOA
			// MNAME self-heal fires for the portal-managed zone.
			mockDNS.EXPECT().GetActiveDNSSECDS(mock.Anything, uint(42)).
				Return("", nil).Once()
			mockDNS.EXPECT().EnsureSOAMNAME(mock.Anything, uint(42), "example.com", mock.Anything).
				Return(nil).Once()

			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "example.com", Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 42,
			}

			verified, err := svc.VerifyDomain(context.Background(), wd)
			_ = verified
			if err != nil {
				// Allowed: post-heal delegation error (system resolver).
				tb.Logf("expected post-heal delegation error: %v", err)
			}

			mockDNS.AssertCalled(tb, "EnsureSOAMNAME", mock.Anything, uint(42), "example.com", mock.Anything)
			mockDNS.AssertNotCalled(tb, "EnableDNSSEC")
			mockDNS.AssertExpectations(tb)
		}, TestOptions)
	})

	// Regression: an ICANN root must not fail delegation verification when the
	// live DS cannot be resolved (e.g. transient PowerDNS slowness/timeout). DS
	// is only meaningful for managed-DNSSEC namespaces (HNS); ICANN verifies on
	// NS visibility and ignores DS. A GetActiveDNSSECDS error on an ICANN zone
	// used to abort VerifyDomain with "resolve live DS for zone N" and pin the
	// domain at "Domain delegation not yet published" forever.
	t.Run("icann_ds_error_does_not_fail_verification", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			db := ctx.DB()
			require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "lumeweb.com", Namespace: pluginDb.DomainNamespaceICANN,
				ZoneID: 8, Status: pluginDb.DomainStatusWaitingDelegation,
			}).Error)

			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, mockDNS)

			// DS resolution fails (the production symptom: context canceled /
			// PowerDNS timeout). This must NOT abort verification for an ICANN
			// root. SOA MNAME self-heal still fires for the portal-managed zone.
			mockDNS.EXPECT().GetActiveDNSSECDS(mock.Anything, uint(8)).
				Return("", errors.New("context canceled")).Once()
			mockDNS.EXPECT().EnsureSOAMNAME(mock.Anything, uint(8), "lumeweb.com", mock.Anything).
				Return(nil).Once()

			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "lumeweb.com", Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 8,
			}

			verified, err := svc.VerifyDomain(context.Background(), wd)
			_ = verified
			if err != nil {
				// Allowed: post-heal ICANN delegation error (system resolver).
				// The key assertion is that the DS error itself was swallowed —
				// err must NOT wrap "resolve live DS for zone 8".
				tb.Logf("expected post-heal delegation error: %v", err)
				require.NotContains(tb, err.Error(), "resolve live DS")
			}

			mockDNS.AssertCalled(tb, "EnsureSOAMNAME", mock.Anything, uint(8), "lumeweb.com", mock.Anything)
			mockDNS.AssertExpectations(tb)
		}, TestOptions)
	})
}

func TestDelegatedDomainService_GetPendingWebsiteDomainsPaginated(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()

		for i := 0; i < 3; i++ {
			require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
				WebsiteID: uint(i + 1), UserID: 1, Domain: fmt.Sprintf("d%d.com", i),
				Namespace: pluginDb.DomainNamespaceICANN, Status: pluginDb.DomainStatusWaitingDelegation,
			}).Error)
		}

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		wds, err := svc.GetPendingWebsiteDomainsPaginated(t.Context(), pluginDb.DomainStatusWaitingDelegation, 2, 0)
		require.NoError(t, err)
		assert.Len(t, wds, 2)

		wds2, err := svc.GetPendingWebsiteDomainsPaginated(t.Context(), pluginDb.DomainStatusWaitingDelegation, 2, 2)
		require.NoError(t, err)
		assert.Len(t, wds2, 1)
	}, TestOptions)
}

// TestDelegatedDomainService_UpdateTLSA_PublishesToManagedZone is the
// regression test for the "TLSA record is never served" bug. When a cert is
// pushed via UpdateTLSAFromCert for a domain whose WebsiteDomain carries a
// ZoneID (i.e. a portal-managed, DNSSEC-signed authoritative zone), the TLSA
// record MUST be published to PowerDNS via SetTLSARecord — otherwise DANE
// validators get NXDOMAIN for `_443._tcp.<domain>` because the TLSA was only
// stored in DB DelegationData and never served.
func TestDelegatedDomainService_UpdateTLSA_PublishesToManagedZone(t *testing.T) {
	t.Run("hns_publishes_tlsa", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			db := ctx.DB()
			require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "example", Namespace: pluginDb.DomainNamespaceHNS,
				ZoneID: 42,
			}).Error)

			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, mockDNS)

			// The TLSA must be pushed to the managed zone (zone 42) as
			// "usage selector matching hash" rdata.
			mockDNS.EXPECT().
				SetTLSARecord(mock.Anything, uint(42), mock.Anything, mock.Anything).
				Run(func(_ context.Context, zoneID uint, domain string, content string) {
					assert.Regexp(tb, `^3 1 1 [0-9a-fA-F]+$`, content,
						"TLSA rdata must be '<usage> <selector> <matching> <digest>'")
				}).
				Return(nil).
				Once()

			certPEM, _ := issueCertFromKey(t, mustGenerateKey(t), "example")
			_, _, err := svc.UpdateTLSAFromCert(ctx, "hns", "example", certPEM, "")
			require.NoError(tb, err)

			mockDNS.AssertExpectations(tb)
		}, keyTestOptions)
	})

	// Regression: ICANN domains get a portal-managed ZoneID too, but they do
	// not use DANE, so a cert push must NOT publish a spurious TLSA record.
	t.Run("icann_does_not_publish_tlsa", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			db := ctx.DB()
			require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "example.com", Namespace: pluginDb.DomainNamespaceICANN,
				ZoneID: 42,
			}).Error)

			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, mockDNS)
			mockDNS.AssertNotCalled(tb, "SetTLSARecord")

			certPEM, _ := issueCertFromKey(t, mustGenerateKey(t), "example.com")
			_, _, err := svc.UpdateTLSAFromCert(ctx, "icann", "example.com", certPEM, "")
			require.NoError(tb, err)

			mockDNS.AssertNotCalled(tb, "SetTLSARecord")
		}, keyTestOptions)
	})
}
