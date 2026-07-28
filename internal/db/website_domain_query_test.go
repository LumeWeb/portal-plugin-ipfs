package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

func TestWebsiteDomain_Queries(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()

		// Insert test records
		require.NoError(tb, gormDB.Create(&WebsiteDomain{
			WebsiteID: 1,
			UserID:    1,
			Domain:    "example",
			Namespace: DomainNamespaceHNS,
		}).Error)

		require.NoError(tb, gormDB.Create(&WebsiteDomain{
			WebsiteID: 2,
			UserID:    2,
			Domain:    "example.com",
			Namespace: DomainNamespaceICANN,
		}).Error)

		t.Run("find_hns_domain", func(t *testing.T) {
			var wd WebsiteDomain
			err := gormDB.WithContext(t.Context()).Where("domain = ?", "example").First(&wd).Error
			require.NoError(t, err)
			assert.Equal(t, "example", wd.Domain)
			assert.Equal(t, DomainNamespaceHNS, wd.Namespace)
		})

		t.Run("find_icann_domain", func(t *testing.T) {
			var wd WebsiteDomain
			err := gormDB.WithContext(t.Context()).Where("domain = ?", "example.com").First(&wd).Error
			require.NoError(t, err)
			assert.Equal(t, "example.com", wd.Domain)
			assert.Equal(t, DomainNamespaceICANN, wd.Namespace)
		})

		t.Run("not_found", func(t *testing.T) {
			var wd WebsiteDomain
			err := gormDB.WithContext(t.Context()).Where("domain = ?", "nonexistent").First(&wd).Error
			assert.Error(t, err)
			assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
		})
	}, dbTestOptions)
}
