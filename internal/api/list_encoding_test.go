package api

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/queryutil"
)

func TestFileListRejectsMalformedQueries(t *testing.T) {
	for _, query := range []string{
		"filters[name][eq]=ok&filters[unknown][eq]=%ZZ",
		"filters[size][gt]=not-a-number",
		"filters[created_at][gt]=not-a-date",
	} {
		t.Run(query, func(t *testing.T) {
			coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				helper := newMockHelper(t, ctx)
				token, _ := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "list encoding"))
				svc := core.GetService[*mocks.MockFileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
				svc.EXPECT().ListFiles(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*pluginDb.FilePath{}, int64(0), nil).Maybe()
				response := helper.makeAuthenticatedRequest(http.MethodGet, "/api/files?"+query, token, nil)
				require.Equal(tb, http.StatusBadRequest, response.Code, response.Body.String())
			}, TestOptions)
		})
	}
}

func TestFileListPreservesTextThroughPolicyAndParser(t *testing.T) {
	for _, name := range []string{"000123", "a%2Bb"} {
		t.Run(name, func(t *testing.T) {
			coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				helper := newMockHelper(t, ctx)
				token, userID := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "list text"))
				svc := core.GetService[*mocks.MockFileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)
				svc.EXPECT().ListFiles(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).
					Run(func(_ context.Context, _ uint, filters []queryutil.CrudFilter, _ []queryutil.Sort, _ queryutil.Pagination) {
						require.Len(tb, filters, 1)
						require.Equal(tb, name, filters[0].GetValue())
					}).Return([]*pluginDb.FilePath{}, int64(0), nil).Once()
				q := url.Values{"filters[name][eq]": {name}}
				response := helper.makeAuthenticatedRequest(http.MethodGet, "/api/files?"+q.Encode(), token, nil)
				require.Equal(tb, http.StatusOK, response.Code, response.Body.String())
			}, TestOptions)
		})
	}
}
