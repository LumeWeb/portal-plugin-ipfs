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
