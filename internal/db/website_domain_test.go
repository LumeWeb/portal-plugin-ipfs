package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

var dbTestOptions = coreTesting.WithSQLitePluginMigrations(
	internal.ProtocolName,
	migrations.GetSQLite(),
)

func TestWebsiteDomain_TableName(t *testing.T) {
	wd := WebsiteDomain{}
	assert.Equal(t, "website_domains", wd.TableName())
}

func TestWebsiteDomain_UniqueConstraint(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()

		// Insert first
		assert.NoError(tb, gormDB.Create(&WebsiteDomain{
			WebsiteID: 1,
			UserID:    1,
			Domain:    "example",
			Namespace: DomainNamespaceHNS,
		}).Error)

		// Insert duplicate domain+namespace should fail
		assert.Error(tb, gormDB.Create(&WebsiteDomain{
			WebsiteID: 2,
			UserID:    2,
			Domain:    "example",
			Namespace: DomainNamespaceHNS,
		}).Error)

		// Same domain, different namespace should succeed
		assert.NoError(tb, gormDB.Create(&WebsiteDomain{
			WebsiteID: 3,
			UserID:    3,
			Domain:    "example",
			Namespace: DomainNamespaceICANN,
		}).Error)
	}, dbTestOptions)
}

func TestWebsiteDomain_Class(t *testing.T) {
	tests := []struct {
		name string
		wd   WebsiteDomain
		want DomainClass
	}{
		{
			name: "onchain managed with zero zone is on-chain managed",
			wd: WebsiteDomain{
				Status: DomainStatusOnchainManaged,
			},
			want: ClassOnChainManaged,
		},
		{
			name: "onchain managed takes precedence over a stray zone id",
			wd: WebsiteDomain{
				Status: DomainStatusOnchainManaged,
				ZoneID: 7,
			},
			want: ClassOnChainManaged,
		},
		{
			name: "onchain managed with hosting enabled is still on-chain managed",
			wd: WebsiteDomain{
				Status:            DomainStatusOnchainManaged,
				DNSHostingEnabled: true,
			},
			want: ClassOnChainManaged,
		},
		{
			name: "zone is authoritative for portal-managed regardless of status",
			wd: WebsiteDomain{
				Status: DomainStatusError,
				ZoneID: 42,
			},
			want: ClassPortalManaged,
		},
		{
			name: "records-generated with a zone is portal-managed",
			wd: WebsiteDomain{
				Status: DomainStatusRecordsGenerated,
				ZoneID: 42,
			},
			want: ClassPortalManaged,
		},
		{
			name: "waiting-delegation with a zone is portal-managed",
			wd: WebsiteDomain{
				Status: DomainStatusWaitingDelegation,
				ZoneID: 42,
			},
			want: ClassPortalManaged,
		},
		{
			name: "active with a zone is portal-managed",
			wd: WebsiteDomain{
				Status: DomainStatusActive,
				ZoneID: 42,
			},
			want: ClassPortalManaged,
		},
		{
			name: "self-hosted status with a stray zone is portal-managed",
			wd: WebsiteDomain{
				Status: DomainStatusSelfHosted,
				ZoneID: 42,
			},
			want: ClassPortalManaged,
		},
		{
			name: "delegation-owned HNS with hosting disabled and retained zone is portal-managed",
			wd: WebsiteDomain{
				Namespace:         DomainNamespaceHNS,
				Status:            DomainStatusWaitingDelegation,
				ZoneID:            42,
				DNSHostingEnabled: false,
			},
			want: ClassPortalManaged,
		},
		{
			name: "self-hosted status with no zone is self-hosted",
			wd: WebsiteDomain{
				Status: DomainStatusSelfHosted,
			},
			want: ClassSelfHosted,
		},
		{
			name: "draft with no zone is unresolved, not self-hosted",
			wd: WebsiteDomain{
				Status: DomainStatusDraft,
			},
			want: ClassUnresolved,
		},
		{
			name: "empty status with no zone is unresolved",
			wd: WebsiteDomain{
				Status: "",
			},
			want: ClassUnresolved,
		},
		{
			name: "error status with no zone is unresolved",
			wd: WebsiteDomain{
				Status: DomainStatusError,
			},
			want: ClassUnresolved,
		},
		{
			name: "unknown status with no zone is unresolved",
			wd: WebsiteDomain{
				Status: DomainStatus("unexpected"),
			},
			want: ClassUnresolved,
		},
		{
			name: "dns_hosting_enabled is deliberately not an input",
			wd: WebsiteDomain{
				Status:            DomainStatusDraft,
				DNSHostingEnabled: true,
			},
			want: ClassUnresolved,
		},
		{
			name: "dns_hosting_enabled false is not an input for a zone-less draft",
			wd: WebsiteDomain{
				Status:            DomainStatusDraft,
				DNSHostingEnabled: false,
			},
			want: ClassUnresolved,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.wd.Class())
		})
	}
}

func TestWebsiteDomain_ClassSemanticHelpers(t *testing.T) {
	// The semantic helpers must express the same business decision as Class()
	// and never diverge from it, so routing code can rely on them without
	// re-deriving hosting rules from raw fields.
	portal := WebsiteDomain{Status: DomainStatusActive, ZoneID: 42}
	onchainStray := WebsiteDomain{Status: DomainStatusOnchainManaged, ZoneID: 42}
	selfHosted := WebsiteDomain{Status: DomainStatusSelfHosted}
	unresolved := WebsiteDomain{Status: DomainStatusDraft}
	onchain := WebsiteDomain{Status: DomainStatusOnchainManaged}

	t.Run("portal managed grants authority", func(t *testing.T) {
		assert.True(t, portal.HasPortalAuthority())
		assert.True(t, portal.NeedsDelegationVerification())
		assert.True(t, portal.CanPublishManagedZoneRecords())
	})

	t.Run("on-chain never grants portal authority even with a stray zone", func(t *testing.T) {
		assert.False(t, onchain.HasPortalAuthority())
		assert.False(t, onchainStray.HasPortalAuthority())
		assert.False(t, onchainStray.NeedsDelegationVerification())
		assert.False(t, onchainStray.CanPublishManagedZoneRecords())
		assert.False(t, onchain.CanPublishManagedZoneRecords())
	})

	t.Run("self-hosted never grants portal authority", func(t *testing.T) {
		assert.False(t, selfHosted.HasPortalAuthority())
		assert.False(t, selfHosted.NeedsDelegationVerification())
		assert.False(t, selfHosted.CanPublishManagedZoneRecords())
	})

	t.Run("unresolved never grants portal authority", func(t *testing.T) {
		assert.False(t, unresolved.HasPortalAuthority())
		assert.False(t, unresolved.NeedsDelegationVerification())
		assert.False(t, unresolved.CanPublishManagedZoneRecords())
	})
}

func TestWebsiteDomain_DelegationRecordsOwned(t *testing.T) {
	tests := []struct {
		name string
		wd   WebsiteDomain
		want bool
	}{
		{
			name: "native HNS delegation owns records",
			wd: WebsiteDomain{
				Namespace:      DomainNamespaceHNS,
				Status:         DomainStatusWaitingDelegation,
				DelegationData: map[string]any{"mode": "delegated"},
			},
			want: true,
		},
		{
			name: "onchain managed HNS does not own records",
			wd: WebsiteDomain{
				Namespace: DomainNamespaceHNS,
				Status:    DomainStatusOnchainManaged,
			},
			want: false,
		},
		{
			name: "onchain managed with stray delegation data still does not own records",
			wd: WebsiteDomain{
				Namespace:      DomainNamespaceHNS,
				Status:         DomainStatusOnchainManaged,
				DelegationData: map[string]any{"mode": "hip5"},
			},
			want: false,
		},
		{
			name: "ICANN with delegation data owns records",
			wd: WebsiteDomain{
				Namespace:      DomainNamespaceICANN,
				Status:         DomainStatusRecordsGenerated,
				DelegationData: map[string]any{"nameservers": []string{"ns1.com."}},
			},
			want: true,
		},
		{
			name: "ICANN without delegation data does not own records",
			wd: WebsiteDomain{
				Namespace: DomainNamespaceICANN,
				Status:    DomainStatusActive,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.wd.DelegationRecordsOwned())
		})
	}
}
