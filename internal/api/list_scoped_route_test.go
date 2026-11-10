package api

import (
	"net/http"
	"net/http/httptest"
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
)

func TestDNSRecordRouteRejectsUnsupportedQueryAfterOwnershipCheck(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, userID := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "record query"))
		svc := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		svc.EXPECT().GetZone(mock.Anything, uint(1)).Return(&pluginDb.DNSZone{UserID: userID}, nil).Once()
		query := url.Values{"filters[zone_id][eq]": {"2"}}
		response := helper.makeAuthenticatedRequest(http.MethodGet, "/api/dns/zones/1/records?"+query.Encode(), token, nil)
		require.Equal(tb, http.StatusBadRequest, response.Code, response.Body.String())
	}, TestOptions)
}

func TestAdminPlatformDomainRouteRejectsUnsupportedQuery(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		_, _, err := coreTesting.GetMockUserService(ctx).UserExists(1)
		require.NoError(tb, err)
		access := core.GetService[core.AccessService](ctx, core.ACCESS_SERVICE)
		require.NoError(tb, access.AssignRoleToUser(ctx.GetContext(), 1, core.ACCESS_ADMIN_ROLE))
		token, err := coreTesting.NewJWTHelper(ctx).CreateLoginToken(1)
		require.NoError(tb, err)
		query := url.Values{"_sort": {"(SELECT 1)"}}
		req := ctx.NewAPIRequest(http.MethodGet, "/api/ipfs/platform-domains?"+query.Encode(), nil)
		setAuthHeader(req, token)
		response := httptest.NewRecorder()
		ctx.Router().ServeHTTP(response, req)
		require.Equal(tb, http.StatusBadRequest, response.Code, response.Body.String())
	}, adminRouteAccessOptions)
}
