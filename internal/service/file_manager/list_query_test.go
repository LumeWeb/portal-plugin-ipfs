package filemanager

import (
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

func TestListFilesMatchesStoredCIDVersions(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		v0, err := (cid.Prefix{Version: 0, Codec: cid.DagProtobuf, MhType: mh.SHA2_256, MhLength: -1}).Sum([]byte("file CID representation"))
		require.NoError(tb, err)
		versions := []cid.Cid{v0, cid.NewCidV1(cid.DagProtobuf, v0.Hash())}
		for _, c := range versions {
			name := fmt.Sprintf("v%d", c.Version())
			createTestFilePath(t, ctx, 1, c, "/"+name, name, false)
			createTestFilePath(t, ctx, 2, c, "/other/"+name, name, false)
		}
		for _, c := range versions {
			for _, value := range []any{c.String(), c.Bytes()} {
				for _, op := range []filter.Operator{filter.OpEq, filter.OpNe, filter.OpIn, filter.OpNin} {
					t.Run(fmt.Sprintf("v%d/%T/%s", c.Version(), value, op), func(t *testing.T) {
						operand := value
						if op.RequiresArray() {
							operand = []any{value}
						}
						fs := []queryutil.CrudFilter{queryutil.NewLogicalFilter("cid", op, operand)}
						rows, total, err := svc.ListFiles(context.Background(), 1, fs, nil, queryutil.Pagination{})
						require.NoError(t, err)
						require.EqualValues(t, 1, total)
						require.Len(t, rows, 1)
						require.Equal(t, uint(1), rows[0].UserID)
						if op == filter.OpEq || op == filter.OpIn {
							require.Equal(t, c.Bytes(), rows[0].CID)
						} else {
							require.NotEqual(t, c.Bytes(), rows[0].CID)
						}
					})
				}
			}
		}
	}, TestOptions)
}

func TestListFilesRejectsUncheckedQueriesAndKeepsOwnerScope(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		c := util.GenerateTestCID(t, "list query")
		createTestFilePath(t, ctx, 1, c, "/one", "one", false)
		createTestFilePath(t, ctx, 1, c, "/two", "two", false)
		createTestFilePath(t, ctx, 2, c, "/other", "other", false)
		for _, fs := range [][]queryutil.CrudFilter{
			{queryutil.Equal("id) OR 1=1 --", 1)},
			{queryutil.Or(queryutil.Equal("name", "one"), queryutil.Equal("user_id", 2))},
			{nil},
		} {
			rows, total, err := svc.ListFiles(context.Background(), 1, fs, nil, queryutil.Pagination{})
			require.ErrorIs(tb, err, filter.ErrInvalidQuery)
			require.Empty(tb, rows)
			require.Zero(tb, total)
		}
		fs := []queryutil.CrudFilter{queryutil.Or(queryutil.Equal("name", "one"), queryutil.Equal("name", "other"))}
		rows, total, err := svc.ListFiles(context.Background(), 1, fs, nil, queryutil.Pagination{})
		require.NoError(tb, err)
		require.EqualValues(tb, 1, total)
		require.Len(tb, rows, 1)
		require.Equal(tb, "one", rows[0].Name)
	}, TestOptions)
}

func TestListFilesCountsBeforePaginationAndKeepsNestedHierarchy(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
		c := util.GenerateTestCID(t, "hierarchy query")
		createTestFilePath(t, ctx, 1, c, "/docs/a", "a", false)
		createTestFilePath(t, ctx, 1, c, "/docs/sub/b", "b", false)
		createTestFilePath(t, ctx, 1, c, "/docs-other/c", "c", false)
		fs := []queryutil.CrudFilter{queryutil.And(queryutil.Equal("parent_path", "/docs"), queryutil.NotEqual("name", "absent"))}
		page, err := queryutil.NewPagination(1, 1)
		require.NoError(tb, err)
		rows, total, err := svc.ListFiles(context.Background(), 1, fs, []queryutil.Sort{{Field: "name", Order: filter.OrderAsc}}, page)
		require.NoError(tb, err)
		require.EqualValues(tb, 2, total)
		require.Len(tb, rows, 1)
		require.Equal(tb, "b", rows[0].Name)
		require.Equal(tb, "/docs", fs[0].GetFilters()[0].GetValue())
	}, TestOptions)
}
