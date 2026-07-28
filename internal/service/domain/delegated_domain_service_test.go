package domain

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
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
		Domain:          domain,
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
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(1), mock.Anything).Return(nil).Once()

		wd, err := svc.CreateDomain(context.Background(), "icann", "example.com", website.ID, 1, nil)
		assert.NoError(tb, err)
		assert.NotNil(tb, wd)
		assert.Equal(tb, "example.com", wd.Domain)
		assert.Equal(tb, pluginDb.DomainNamespaceICANN, wd.Namespace)
		assert.Equal(tb, uint(1), wd.ZoneID)
	}, TestOptions)
}

func TestDelegatedDomainService_CreateDomain_UnsupportedNamespace(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		_, err := svc.CreateDomain(context.Background(), "ens", "example.eth", 1, 1, nil)
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
