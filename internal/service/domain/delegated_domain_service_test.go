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
	"gorm.io/datatypes"
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

		wd, err := svc.CreateDomain(context.Background(), "icann", "example.com", website.ID, 1, true, true, nil, nil)
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

		wd, err := svc.CreateDomain(context.Background(), "icann", "alt-example.com", website.ID, 1, true, true, nil, nil)
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

		wd, err := svc.CreateDomain(context.Background(), "icann", "example.com", website.ID, 1, false, true, nil, nil)
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

func TestDelegatedDomainService_CreateDomain_DuplicateKey(t *testing.T) {
	// Re-binding a domain that is already live-bound (the create-time race) must
	// fail on the (domain, namespace) unique key. On MySQL this surfaces as
	// gorm.ErrDuplicatedKey (translated from 1062) so the API rolls back the
	// just-created website and returns 409 instead of leaking a dangling row and
	// raw 500; the SQLite test driver reports it as a generic UNIQUE constraint
	// error, so we assert only that the duplicate bind is rejected.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example.com")

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		// First bind succeeds (self-hosted to avoid DNS provisioning) and fires
		// the created notification.
		mockWebsite := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockWebsite.EXPECT().NotifyAdminWebsiteCreated(mock.Anything, website.ID).Return(nil).Once()

		_, err := svc.CreateDomain(context.Background(), "icann", "example.com", website.ID, 1, false, true, nil, nil)
		require.NoError(tb, err)

		// A second bind of the same (domain, namespace) hits the unique key.
		_, err = svc.CreateDomain(context.Background(), "icann", "example.com", website.ID, 1, false, true, nil, nil)
		require.Error(tb, err)
	}, TestOptions)
}

func TestDelegatedDomainService_CreateDomain_SelfHostedDANEFailurePurgesBinding(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "example")

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		_, err := svc.CreateDomain(context.Background(), "hns", "example", website.ID, 1, false, false, nil, nil)
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

		wd, err := svc.CreateDomain(context.Background(), "icann", "docs.example.com", website.ID, 1, true, true, nil, nil)
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

		_, err := svc.CreateDomain(context.Background(), "icann", "docs.example.com", website.ID, 1, true, false, nil, nil)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "owned by another user")
	}, TestOptions)
}

// TestDelegatedDomainService_CreateDomain_PlatformRootClaimRequired guards the
// platform-root bypass: a subdomain nested under an operator-owned, registered
// platform root must NOT be minted via the normal bind path (which skips label
// validation, availability, and PlatformDomainID) even when the requesting user
// owns the parent zone. It must be directed to the platform claim flow.
func TestDelegatedDomainService_CreateDomain_PlatformRootClaimRequired(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()

		website := createTestWebsite(tb, db, 1, "pinned.site")

		// Register an enabled ICANN platform root (pinned.site ends in the
		// ICANN TLD "site") whose zone is owned by the SAME requesting user
		// (the operator-admin case from the bug report).
		pd := createPlatformRoot(tb, ctx, "pinned.site", pluginDb.DomainNamespaceICANN, 7, true)
		require.NoError(tb, db.Create(&pluginDb.DNSZone{
			UserID: 1, Domain: pd.Domain, Status: string(pluginDb.DNSZoneStatusActive),
			PowerDNSZoneID: "pdns-pinned",
		}).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, mockDNS)

		// The parent zone exists and is owned by the user — but the parent is a
		// registered platform root, so CreateZone and zone reuse must both never
		// happen; the claim must be rejected before any DNS work.
		mockDNS.AssertNotCalled(tb, "CreateZone", mock.Anything, "starter.pinned.site", mock.Anything)
		mockDNS.AssertNotCalled(tb, "GetZoneByDomain", mock.Anything, "pinned.site")

		_, err := svc.CreateDomain(context.Background(), "icann", "starter.pinned.site", website.ID, 1, true, false, nil, nil)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "platform subdomain flow")
	}, TestOptions)
}

func TestDelegatedDomainService_CreateDomain_UnsupportedNamespace(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		_, err := svc.CreateDomain(context.Background(), "ens", "example.eth", 1, 1, true, false, nil, nil)
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
			_, err := svc.VerifyDomain(context.Background(), wd)
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

			_, err := svc.VerifyDomain(context.Background(), wd)
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

			_, err := svc.VerifyDomain(context.Background(), wd)
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

// TestDelegatedDomainService_VerifyDomain_PlatformNamespaceMismatch_Rejects
// guards the platform auto-activation path: a binding carrying a
// PlatformDomainID may only be marked Active when its namespace matches the
// platform root's namespace (the operator zone the records were written into).
// A mismatch means the pointer is corrupt/stale and must NOT auto-activate.
func TestDelegatedDomainService_VerifyDomain_PlatformNamespaceMismatch_Rejects(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()

		// Registered HNS platform root...
		pd := createPlatformRoot(tb, ctx, "altroot", pluginDb.DomainNamespaceHNS, 7, true)

		// ...but the binding was (incorrectly) recorded as ICANN.
		wd := &pluginDb.WebsiteDomain{
			WebsiteID:        1,
			UserID:           1,
			Domain:           "starter.altroot",
			Namespace:        pluginDb.DomainNamespaceICANN,
			ZoneID:           7,
			Status:           pluginDb.DomainStatusRecordsGenerated,
			PlatformDomainID: &pd.ID,
		}
		require.NoError(tb, db.Create(wd).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		verified, err := svc.VerifyDomain(context.Background(), wd)
		require.Error(tb, err, "namespace mismatch must not auto-activate")
		assert.Contains(tb, err.Error(), "namespace mismatch")
		assert.NotEqual(tb, DelegationVerified, verified.State,
			"a failed platform trust check must never report delegation verified")

		// Must not have been promoted to Active.
		var reloaded pluginDb.WebsiteDomain
		require.NoError(tb, db.First(&reloaded, wd.ID).Error)
		assert.NotEqual(tb, pluginDb.DomainStatusActive, reloaded.Status)
	}, TestOptions)
}

// TestDelegatedDomainService_VerifyDomain_PlatformMatching_Activates guards the
// happy path: a platform binding with a matching namespace still auto-activates
// without touching DNS (operator controls both sides of the check).
func TestDelegatedDomainService_VerifyDomain_PlatformMatching_Activates(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()

		pd := createPlatformRoot(tb, ctx, "altroot", pluginDb.DomainNamespaceHNS, 7, true)
		wd := &pluginDb.WebsiteDomain{
			WebsiteID:        1,
			UserID:           1,
			Domain:           "starter.altroot",
			Namespace:        pluginDb.DomainNamespaceHNS,
			ZoneID:           7,
			Status:           pluginDb.DomainStatusRecordsGenerated,
			PlatformDomainID: &pd.ID,
		}
		require.NoError(tb, db.Create(wd).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		verified, err := svc.VerifyDomain(context.Background(), wd)
		require.NoError(tb, err)
		assert.Equal(tb, DelegationVerified, verified.State)

		var reloaded pluginDb.WebsiteDomain
		require.NoError(tb, db.First(&reloaded, wd.ID).Error)
		assert.Equal(tb, pluginDb.DomainStatusActive, reloaded.Status)
	}, TestOptions)
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

// testOptionsWithHNSResolver is TestOptions with the DNS service config's
// HNSResolver pointed at the given address, so the factory's HNSProvider
// performs HIP-5 inspection against a live test server. It intentionally does
// NOT combine with the package-level TestOptions: building a second mock
// plugin would re-register the ipfs plugin and panic.
func testOptionsWithHNSResolver(addr string) coreTesting.TestContextBuilderOption {
	// Combine with the package-level TestOptions (which registers the ipfs
	// plugin once) and push the resolver into the config manager via the
	// core-testing service-config option — GetServiceConfig reads the config
	// manager, not the mock service's GetConfig().
	return coreTesting.CombineOptions(
		TestOptions,
		coreTesting.WithServiceConfig(internal.ProtocolName, pluginCore.DNS_SERVICE, &pluginConfig.DnsConfig{HNSResolver: addr}),
	)
}

func TestDelegatedDomainService_CreateDomain_Hip5OnchainManaged(t *testing.T) {
	// A Handshake name whose NS record is a HIP-5 TX record must bind as on-chain
	// managed: no zone, no DANE, TXT-verification status. A managed-DNS request
	// (dnsHostingEnabled=true, the UX default) is coerced to onchain_managed with
	// dns_hosting_enabled=false — portal hosting is impossible for a
	// contract-served name — rather than failing the bind.
	const domain = "myname"

	addr, _ := startCustomPortDNSServer(t, domain+".",
		[]string{"0x36fc69f0983e536d1787cc83f481581f22cca2a1._eth."})

	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		// The test DNS server answers NS queries with the HIP-5 record for any
		// name, so each subtest uses a distinct bound domain to avoid colliding
		// on the (domain, namespace) unique key.
		domains := []string{"myname", "myname2"}

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		// Point the registered HNS provider at the live test resolver. The
		// factory only reads DnsConfig.HNSResolver at startup; assigning the
		// provider's resolver here keeps the test deterministic.
		hnsProv := svc.registry.Get("hns").(*HNSProvider)
		require.NotNil(tb, hnsProv)
		hnsProv.resolverAddr = addr

		mockWebsite := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

		for i, hostingRequest := range []bool{true, false} {
			name := "hosting request coerced"
			if !hostingRequest {
				name = "explicit self-hosted request"
			}
			t.Run(name, func(t *testing.T) {
				d := domains[i]
				website := createTestWebsite(tb, db, 1, d)

				mockWebsite.EXPECT().NotifyAdminWebsiteCreated(mock.Anything, website.ID).Return(nil).Once()

				wd, err := svc.CreateDomain(context.Background(), "hns", d, website.ID, 1, hostingRequest, true, nil, nil)
				require.NoError(tb, err)
				assert.Equal(tb, pluginDb.DomainStatusOnchainManaged, wd.Status)
				assert.Equal(tb, uint(0), wd.ZoneID)
				// Portal DNS hosting is coerced off for on-chain managed names,
				// even when the caller requested managed DNS.
				assert.False(tb, wd.DNSHostingEnabled)
				assert.Nil(tb, wd.DelegationData)

				// The binding is recorded as the website's primary so the website
				// service resolves the apex domain via PrimaryDomainID.
				var reloaded pluginDb.Website
				require.NoError(tb, db.First(&reloaded, website.ID).Error)
				require.NotNil(tb, reloaded.PrimaryDomainID)
				assert.Equal(tb, wd.ID, *reloaded.PrimaryDomainID)

				// The persisted flag agrees with the absence of a zone.
				var persisted pluginDb.WebsiteDomain
				require.NoError(tb, db.First(&persisted, wd.ID).Error)
				assert.False(tb, persisted.DNSHostingEnabled)
			})
		}
	}, testOptionsWithHNSResolver(addr))
}

func TestDelegatedDomainService_ConvertToOnChain_HappyPath(t *testing.T) {
	// A native HNS binding (portal-hosted zone + DNSSEC/delegation) whose name
	// has since been pointed at an external contract converts to on-chain
	// managed: the managed zone is deleted, delegation/DNSSEC state is dropped,
	// but DANE/SSL state (ProtocolData) is retained and the website re-arms
	// validation.
	const domain = "convertme"
	const zoneID = uint(77)

	hip5Addr, _ := startCustomPortDNSServer(t, domain+".",
		[]string{"0x36fc69f0983e536d1787cc83f481581f22cca2a1._eth."})

	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, domain)
		// Simulate a website already serving the site so the conversion
		// re-arms validation back to pending.
		require.NoError(tb, db.Model(&website).Update("status", pluginDb.WebsiteStatusActive).Error)

		wd := &pluginDb.WebsiteDomain{
			WebsiteID:         website.ID,
			UserID:            1,
			Domain:            domain,
			Namespace:         pluginDb.DomainNamespaceHNS,
			ZoneName:          domain + ".",
			GatewayHost:       "1.2.3.4",
			ZoneID:            zoneID,
			Status:            pluginDb.DomainStatusActive,
			DNSHostingEnabled: true,
			DelegationData: datatypes.JSONMap{
				"mode": "delegated",
				"authoritative_records": []map[string]any{
					{"type": "NS", "value": "ns1.lumeweb\nns2.lumeweb"},
				},
			},
			// Simulated DANE state that must survive the conversion (DANE/SSL
			// still applies on-chain; only PowerDNS-served records go away).
			ProtocolData: datatypes.JSONMap{
				"dane_cert_pem":    "CERT",
				"tlsa":             "3 1 1 abc",
				"owner_name":       "_443._tcp." + domain + ".",
				"dane_private_key": "encrypted-key",
			},
		}
		require.NoError(tb, db.Create(wd).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)
		hnsProv := svc.registry.Get("hns").(*HNSProvider)
		hnsProv.resolverAddr = hip5Addr

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockDNS.EXPECT().DeleteZone(mock.Anything, zoneID).
			// The conversion persists the on-chain state BEFORE deleting the
			// zone: at delete time the DB row must already be onchain with the
			// zone cleared, so a delete failure can never strand a binding
			// pointing at a destroyed zone.
			Run(func(_ context.Context, z uint) {
				var now pluginDb.WebsiteDomain
				require.NoError(tb, ctx.DB().First(&now, wd.ID).Error)
				assert.Equal(tb, pluginDb.DomainStatusOnchainManaged, now.Status)
				assert.Equal(tb, uint(0), now.ZoneID)
			}).
			Return(nil).Once()

		converted, err := svc.ConvertToOnChain(context.Background(), website.ID, 1, wd.ID)
		require.NoError(tb, err)
		assert.Equal(tb, pluginDb.DomainStatusOnchainManaged, converted.Status)
		assert.Equal(tb, uint(0), converted.ZoneID)
		assert.False(tb, converted.DNSHostingEnabled)
		assert.Nil(tb, converted.DelegationData)
		// DANE/SSL ProtocolData is retained.
		require.NotNil(tb, converted.ProtocolData)
		assert.Equal(tb, "CERT", converted.ProtocolData["dane_cert_pem"])
		assert.Equal(tb, "3 1 1 abc", converted.ProtocolData["tlsa"])

		// Persisted state mirrors the in-memory binding.
		var persisted pluginDb.WebsiteDomain
		require.NoError(tb, db.First(&persisted, wd.ID).Error)
		assert.Equal(tb, pluginDb.DomainStatusOnchainManaged, persisted.Status)
		assert.Equal(tb, uint(0), persisted.ZoneID)
		assert.Nil(tb, persisted.DelegationData)
		assert.False(tb, persisted.DNSHostingEnabled)
		assert.Equal(tb, "CERT", persisted.ProtocolData["dane_cert_pem"])

		// The website re-armed validation (active -> pending_validation).
		var reloaded pluginDb.Website
		require.NoError(tb, db.First(&reloaded, website.ID).Error)
		assert.Equal(tb, string(pluginDb.WebsiteStatusPendingValidation), reloaded.Status)
	}, TestOptions)
}

func TestDelegatedDomainService_VerifyDomain_ReclassifiesExistingHIP5(t *testing.T) {
	const domain = "verify-convertme"
	const zoneID = uint(88)
	hip5Addr, _ := startCustomPortDNSServer(t, domain+".", []string{"ignored.target."})

	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, domain)
		require.NoError(tb, db.Model(&website).Update("status", pluginDb.WebsiteStatusActive).Error)
		wd := &pluginDb.WebsiteDomain{
			WebsiteID: website.ID, UserID: 1, Domain: domain,
			Namespace: pluginDb.DomainNamespaceHNS, ZoneID: zoneID,
			Status: pluginDb.DomainStatusWaitingDelegation, DNSHostingEnabled: true,
		}
		require.NoError(tb, db.Create(wd).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		hnsProv := svc.registry.Get("hns").(*HNSProvider)
		hnsProv.resolverAddr = hip5Addr
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockDNS.EXPECT().DeleteZone(mock.Anything, zoneID).Return(nil).Once()

		result, err := svc.VerifyDomain(context.Background(), wd)
		require.NoError(tb, err)
		assert.Equal(tb, DelegationNotApplicable, result.State)
		assert.Equal(tb, pluginDb.DomainStatusOnchainManaged, wd.Status)
		assert.Zero(tb, wd.ZoneID)
		assert.False(tb, wd.DNSHostingEnabled)
	}, TestOptions)
}

func TestDelegatedDomainService_ConvertToOnChain_NotHIP5Refused(t *testing.T) {
	// Conversion is refused until the name genuinely serves a HIP-5 record; it
	// never tears down DNS on the caller's word alone.
	const domain = "staysnative"
	nativeAddr, _ := startCustomPortDNSServerWithAuthority(t, domain+".", []string{"ns1.lumeweb."}, false)

	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, domain)
		wd := &pluginDb.WebsiteDomain{
			WebsiteID: website.ID, UserID: 1, Domain: domain,
			Namespace: pluginDb.DomainNamespaceHNS, ZoneID: 9,
			Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
			GatewayHost:    "1.2.3.4",
			DelegationData: datatypes.JSONMap{"mode": "delegated"},
		}
		require.NoError(tb, db.Create(wd).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		hnsProv := svc.registry.Get("hns").(*HNSProvider)
		hnsProv.resolverAddr = nativeAddr

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		_, err := svc.ConvertToOnChain(context.Background(), website.ID, 1, wd.ID)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "not yet on-chain managed")
		mockDNS.AssertNotCalled(tb, "DeleteZone", mock.Anything, mock.Anything)
	}, TestOptions)
}

func TestDelegatedDomainService_ConvertToOnChain_AlreadyOnchainRefused(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, "onchain")
		wd := &pluginDb.WebsiteDomain{
			WebsiteID: website.ID, UserID: 1, Domain: "onchain",
			Namespace: pluginDb.DomainNamespaceHNS,
			Status:    pluginDb.DomainStatusOnchainManaged,
		}
		require.NoError(tb, db.Create(wd).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		_, err := svc.ConvertToOnChain(context.Background(), website.ID, 1, wd.ID)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "already on-chain managed")
	}, TestOptions)
}

func TestDelegatedDomainService_ConvertToOnChain_SharedZoneRefused(t *testing.T) {
	// A zone shared with other live bindings (their parent/apex zone) must not
	// be deleted; conversion is refused so the owner detaches them first.
	const domain = "sharedzone"
	hip5Addr, _ := startCustomPortDNSServer(t, domain+".",
		[]string{"0xdeadbeef._eth."})

	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		website := createTestWebsite(tb, db, 1, domain)

		apex := &pluginDb.WebsiteDomain{
			WebsiteID: website.ID, UserID: 1, Domain: domain,
			Namespace: pluginDb.DomainNamespaceHNS, ZoneID: 55,
			Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
			GatewayHost:    "1.2.3.4",
			DelegationData: datatypes.JSONMap{"mode": "delegated"},
		}
		require.NoError(tb, db.Create(apex).Error)
		// A second binding under the same apex zone.
		sub := &pluginDb.WebsiteDomain{
			WebsiteID: website.ID, UserID: 1, Domain: "sub." + domain,
			Namespace: pluginDb.DomainNamespaceHNS, ZoneID: 55,
			Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
			GatewayHost:    "1.2.3.4",
			DelegationData: datatypes.JSONMap{"mode": "delegated"},
		}
		require.NoError(tb, db.Create(sub).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		hnsProv := svc.registry.Get("hns").(*HNSProvider)
		hnsProv.resolverAddr = hip5Addr

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		_, err := svc.ConvertToOnChain(context.Background(), website.ID, 1, apex.ID)
		require.Error(tb, err)
		assert.ErrorIs(tb, err, ErrDomainZoneShared)
		assert.Contains(tb, err.Error(), "shared by other bindings")
		mockDNS.AssertNotCalled(tb, "DeleteZone", mock.Anything, mock.Anything)
	}, TestOptions)
}

func TestDelegatedDomainService_ConvertToOnChain_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		_, err := svc.ConvertToOnChain(context.Background(), 1, 1, 999999)
		require.Error(tb, err)
		assert.True(tb, errors.Is(err, gorm.ErrRecordNotFound))
	}, TestOptions)
}
