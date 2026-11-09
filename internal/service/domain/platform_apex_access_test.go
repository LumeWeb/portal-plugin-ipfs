package domain

import (
	"context"
	"errors"
	"io/fs"
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

func TestBindPlatformRootApex_RequiresDNSService(t *testing.T) {
	options := coreTesting.CombineOptions(
		coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
		testopts.NewBaseMockPluginBuilder().
			WithService(pluginCore.DELEGATED_DOMAIN_SERVICE, func() (core.Service, []core.ContextBuilderOption, error) {
				registry := NewRegistry()
				registry.Register(NewICANNProvider(nil))
				return NewDelegatedDomainService(registry, nil), nil, nil
			}).
			WithServiceConfig(pluginCore.DELEGATED_DOMAIN_SERVICE, &pluginConfig.DelegatedDomainConfig{}).
			WithMigrations(map[core.DBType]fs.FS{core.DB_TYPE_SQLITE: migrations.GetSQLite()}).
			BuilderOption(),
	)
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		website := createTestWebsite(tb, ctx.DB(), 1, "pinned.site")
		root := createPlatformRoot(tb, ctx, "pinned.site", pluginDb.DomainNamespaceICANN, 7, true)
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

		var binding *pluginDb.WebsiteDomain
		var err error
		require.NotPanics(tb, func() {
			binding, err = svc.BindPlatformRootApex(context.Background(), website.ID, 1, root.ID)
		})
		require.EqualError(tb, err, "DNS service not configured")
		require.Nil(tb, binding)

		var count int64
		require.NoError(tb, ctx.DB().Model(&pluginDb.WebsiteDomain{}).Where("website_id = ?", website.ID).Count(&count).Error)
		assert.Zero(tb, count)
	}, options)
}

func TestBindPlatformRootApex_RequiresZoneOwner(t *testing.T) {
	for _, tc := range []struct {
		name string
		zone *pluginDb.DNSZone
		err  error
	}{
		{"different_owner", &pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "pinned.site", UserID: 2}, nil},
		{"missing_zone", nil, nil},
		{"zone_lookup_error", nil, errors.New("zone unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				website := createTestWebsite(tb, ctx.DB(), 1, "pinned.site")
				pd := createPlatformRoot(tb, ctx, "pinned.site", pluginDb.DomainNamespaceICANN, 7, true)
				dns := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
				dns.EXPECT().GetZone(mock.Anything, uint(7)).Return(tc.zone, tc.err).Once()

				svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
				binding, err := svc.BindPlatformRootApex(context.Background(), website.ID, 1, pd.ID)
				require.Error(tb, err)
				require.Nil(tb, binding)

				var count int64
				require.NoError(tb, ctx.DB().Model(&pluginDb.WebsiteDomain{}).Where("website_id = ?", website.ID).Count(&count).Error)
				assert.Zero(tb, count)
				dns.AssertNotCalled(tb, "CreateDNSLinkRecord", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
				dns.AssertNotCalled(tb, "CreateApexRecord", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			}, platformApexTestOptions)
		})
	}
}
