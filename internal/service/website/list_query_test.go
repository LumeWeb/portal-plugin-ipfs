package website

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/queryutil"
)

func TestWebsiteListPreservesNestedDomainScopeAndInputs(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		var websites []*pluginDb.Website
		for i, domain := range []string{"one.site", "two.site", "other.site"} {
			userID := uint(1)
			if i == 2 {
				userID = 2
			}
			website := createTestIPFSWebsite(userID, domain, util.GenerateTestCID(t, domain).String())
			website.Status = string(pluginDb.WebsiteStatusActive)
			require.NoError(tb, ctx.DB().Create(website).Error)
			require.NoError(tb, ctx.DB().Create(&pluginDb.WebsiteDomain{WebsiteID: website.ID, UserID: userID, Domain: domain, Namespace: pluginDb.DomainNamespaceICANN, Status: pluginDb.DomainStatusActive}).Error)
			websites = append(websites, website)
		}
		original := []queryutil.CrudFilter{queryutil.Or(queryutil.Equal("domain", "one.site"), queryutil.Equal("domain", "other.site"))}
		for attempt := 0; attempt < 2; attempt++ {
			rows, total, err := svc.ListWebsites(context.Background(), 1, original, nil, queryutil.Pagination{})
			require.NoError(tb, err)
			require.EqualValues(tb, 1, total)
			require.Len(tb, rows, 1)
			require.Equal(tb, websites[0].ID, rows[0].ID)
		}
		require.Equal(tb, "domain", original[0].GetFilters()[0].GetField())
		require.Equal(tb, "one.site", original[0].GetFilters()[0].GetValue())
		not := []queryutil.CrudFilter{queryutil.Not(queryutil.Equal("domain", "one.site"))}
		rows, total, err := svc.ListWebsites(context.Background(), 1, not, nil, queryutil.Pagination{})
		require.NoError(tb, err)
		require.EqualValues(tb, 1, total)
		require.Len(tb, rows, 1)
		require.Equal(tb, websites[1].ID, rows[0].ID)
		rows, total, err = svc.ListWebsites(context.Background(), 1, []queryutil.CrudFilter{queryutil.Equal("domain", "one.site' OR 1=1 --")}, nil, queryutil.Pagination{})
		require.NoError(tb, err)
		require.Empty(tb, rows)
		require.Zero(tb, total)
	}, TestOptions)
}
