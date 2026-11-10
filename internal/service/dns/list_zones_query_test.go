package dns

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

func TestListZonesForUserKeepsScopeAndUnpaginatedTotal(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		zones := []pluginDb.DNSZone{{UserID: 1, Domain: "one.site", Status: "active"}, {UserID: 1, Domain: "two.site", Status: "active"}, {UserID: 2, Domain: "other.site", Status: "active"}}
		require.NoError(tb, ctx.DB().Create(&zones).Error)
		for _, fs := range [][]queryutil.CrudFilter{
			{queryutil.Equal("id) OR 1=1 --", 1)},
			{queryutil.Or(queryutil.Equal("domain", "one.site"), queryutil.Equal("user_id", 2))},
		} {
			rows, total, err := svc.ListZonesForUser(context.Background(), 1, fs, nil, queryutil.Pagination{})
			require.ErrorIs(tb, err, filter.ErrInvalidQuery)
			require.Empty(tb, rows)
			require.Zero(tb, total)
		}
		fs := []queryutil.CrudFilter{queryutil.Or(queryutil.Equal("domain", "one.site"), queryutil.Equal("domain", "other.site"))}
		rows, total, err := svc.ListZonesForUser(context.Background(), 1, fs, nil, queryutil.Pagination{})
		require.NoError(tb, err)
		require.EqualValues(tb, 1, total)
		require.Len(tb, rows, 1)
		require.Equal(tb, uint(1), rows[0].UserID)
		page, err := queryutil.NewPagination(1, 1)
		require.NoError(tb, err)
		rows, total, err = svc.ListZonesForUser(context.Background(), 1, nil, []queryutil.Sort{{Field: "id", Order: filter.OrderAsc}}, page)
		require.NoError(tb, err)
		require.EqualValues(tb, 2, total)
		require.Len(tb, rows, 1)
		require.Equal(tb, "two.site", rows[0].Domain)
	}, getTestOptions())
}
