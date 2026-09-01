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
