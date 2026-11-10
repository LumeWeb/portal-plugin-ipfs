package api

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestAPIRejectsUnsupportedListQueries(t *testing.T) {
	for _, path := range []string{"/api/ipns/keys", "/api/files", "/api/files/directory", "/api/files/breadcrumbs", "/api/websites", "/api/websites/1/domains", "/api/workspaces", "/api/dns/zones", "/api/websites/platform-domains"} {
		t.Run(path, func(t *testing.T) {
			coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				helper := newMockHelper(t, ctx)
				token, _ := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "list query route"))
				query := url.Values{"filters[id) OR 1=1 --][eq]": {"1"}}
				response := helper.makeAuthenticatedRequest(http.MethodGet, path+"?"+query.Encode(), token, nil)
				require.Equal(tb, http.StatusBadRequest, response.Code, response.Body.String())
			}, TestOptions)
		})
	}
}

func TestFileNavigationRejectsMissingOrGroupedPaths(t *testing.T) {
	for _, path := range []string{"/api/files/directory", "/api/files/breadcrumbs"} {
		for _, query := range []string{"", "?filters[or][0][parent_path][eq]=/a", "?filters[or][0][path][eq]=/a"} {
			t.Run(path+query, func(t *testing.T) {
				coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
					helper := newMockHelper(t, ctx)
					token, _ := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "navigation query"))
					response := helper.makeAuthenticatedRequest(http.MethodGet, path+query, token, nil)
					require.Equal(tb, http.StatusBadRequest, response.Code, response.Body.String())
				}, TestOptions)
			})
		}
	}
}
