package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-middleware/auth/jwt"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-router"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/service"
)

var adminAccessRoutes = []struct {
	method string
	path   string
}{
	{http.MethodPost, "/api/ipfs/websites/:id/block"},
	{http.MethodPost, "/api/ipfs/websites/:id/unblock"},
	{http.MethodPost, "/api/ipfs/ipns/republish"},
	{http.MethodPost, "/api/ipfs/platform-domains"},
	{http.MethodPost, "/api/ipfs/platform-domains/:id/bind"},
	{http.MethodGet, "/api/ipfs/platform-domains"},
	{http.MethodPatch, "/api/ipfs/platform-domains/:id"},
	{http.MethodDelete, "/api/ipfs/platform-domains/:id"},
}

var adminRouteAccessOptions = coreTesting.CombineOptions(
	AdminTestOptions,
	coreTesting.WithService(core.ACCESS_SERVICE, service.NewAccessService),
	func(ctx coreTesting.TestContext) (coreTesting.TestContext, error) {
		return coreTesting.WithAPI("admin", func() (core.API, []core.ContextBuilderOption, error) {
			api := coreTesting.NewMockAPI(ctx.T(), "admin").
				WithSubdomain("admin").
				WithConfigure(func(router.Router, core.AccessService) error { return nil })
			return api, nil, nil
		})(ctx)
	},
)

func newAdminAPIRequest(ctx coreTesting.TestContext, method, path string, body []byte) *http.Request {
	token, _ := createTestUser(ctx)
	req := ctx.NewAPIRequest(method, path, body)
	setAuthHeader(req, token)
	return req
}

func TestAdminExtension_RoutePermissions(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		access := core.GetService[core.AccessService](ctx, core.ACCESS_SERVICE)
		require.NoError(tb, access.AssignRoleToUser(ctx.GetContext(), 1, core.ACCESS_USER_ROLE))
		require.NoError(tb, access.AssignRoleToUser(ctx.GetContext(), 2, core.ACCESS_ADMIN_ROLE))

		for _, route := range adminAccessRoutes {
			t.Run(route.method+" "+route.path, func(t *testing.T) {
				path := strings.ReplaceAll(route.path, ":id", "123")
				for _, tc := range []struct {
					userID    uint
					subdomain string
					allowed   bool
				}{
					{1, "admin", false},
					{2, "admin", true},
					{1, "ipfs", false},
					{2, "ipfs", false},
				} {
					allowed, err := access.CheckAccess(ctx.GetContext(), tc.userID, tc.subdomain+"."+ctx.Config().Config().Core.Domain, path, route.method)
					require.NoError(t, err)
					assert.Equal(t, tc.allowed, allowed, "user %d on %s", tc.userID, tc.subdomain)
				}
			})
		}
	}, adminRouteAccessOptions)
}

func TestAdminExtension_RequiresRouteAccess(t *testing.T) {
	for _, identity := range []string{"anonymous", "invalid", "expired", "otp", "user_login", "user_api", "missing_account"} {
		t.Run(identity, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				helper := coreTesting.NewJWTHelper(ctx)
				access := core.GetService[core.AccessService](ctx, core.ACCESS_SERVICE)
				users := coreTesting.GetMockUserService(ctx)
				if identity == "missing_account" {
					_, _, err := users.UserDoesNotExist(1)
					require.NoError(tb, err)
				} else {
					_, _, err := users.UserExists(1)
					require.NoError(tb, err)
				}
				role := core.ACCESS_ADMIN_ROLE
				if identity == "user_login" || identity == "user_api" {
					role = core.ACCESS_USER_ROLE
				}
				require.NoError(tb, access.AssignRoleToUser(ctx.GetContext(), 1, role))

				var token string
				var err error
				switch identity {
				case "invalid":
					token = "invalid"
				case "expired":
					token, err = helper.CreateExpiredToken(1, jwt.PurposeLogin)
				case "otp":
					token, err = helper.Create2FAToken(1)
				case "user_login", "user_api":
					if identity == "user_login" {
						token, err = helper.CreateLoginToken(1)
					} else {
						token, err = helper.CreateAPIKeyToken(1)
					}
				case "missing_account":
					token, err = helper.CreateLoginToken(1)
				}
				require.NoError(tb, err)

				for _, route := range adminAccessRoutes {
					t.Run(route.method+" "+route.path, func(t *testing.T) {
						path := strings.ReplaceAll(route.path, ":id", "123")
						req := ctx.NewAPIRequest(route.method, path, nil)
						if token != "" {
							setAuthHeader(req, token)
						}
						rec := httptest.NewRecorder()
						ctx.Router().ServeHTTP(rec, req)
						require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
					})
				}

				website := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
				website.AssertNotCalled(tb, "BlockWebsite", mock.Anything, mock.Anything)
				website.AssertNotCalled(tb, "UnblockWebsite", mock.Anything, mock.Anything)
				keys := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
				keys.AssertNotCalled(tb, "ListPublished", mock.Anything)

				if identity != "missing_account" {
					require.NoError(tb, access.AssignRoleToUser(ctx.GetContext(), 1, core.ACCESS_ADMIN_ROLE))
					website.EXPECT().BlockWebsite(mock.Anything, uint(123)).Return(nil).Once()
					adminToken, err := helper.CreateLoginToken(1)
					require.NoError(tb, err)
					req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/123/block", nil)
					setAuthHeader(req, adminToken)
					rec := httptest.NewRecorder()
					ctx.Router().ServeHTTP(rec, req)
					require.Equal(tb, http.StatusOK, rec.Code, rec.Body.String())
				}
			}, adminRouteAccessOptions)
		})
	}
}

func TestAdminExtension_AllowsAdminTokens(t *testing.T) {
	for _, purpose := range []jwt.Purpose{jwt.PurposeLogin, jwt.PurposeAPI} {
		t.Run(string(purpose), func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				_, _, err := coreTesting.GetMockUserService(ctx).UserExists(1)
				require.NoError(tb, err)
				access := core.GetService[core.AccessService](ctx, core.ACCESS_SERVICE)
				require.NoError(tb, access.AssignRoleToUser(ctx.GetContext(), 1, core.ACCESS_ADMIN_ROLE))
				website := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
				website.EXPECT().BlockWebsite(mock.Anything, uint(123)).Return(nil).Once()

				token, err := coreTesting.NewJWTHelper(ctx).CreateToken(1, purpose)
				require.NoError(tb, err)
				req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/123/block", nil)
				setAuthHeader(req, token)
				rec := httptest.NewRecorder()
				ctx.Router().ServeHTTP(rec, req)
				require.Equal(tb, http.StatusOK, rec.Code, rec.Body.String())
			}, adminRouteAccessOptions)
		})
	}
}

func TestAPI_RequiresUserRouteAccess(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		_, _, err := coreTesting.GetMockUserService(ctx).UserExists(1)
		require.NoError(tb, err)
		token, err := coreTesting.NewJWTHelper(ctx).CreateLoginToken(1)
		require.NoError(tb, err)

		for _, path := range []string{"/pins", "/api/files", "/api/ipns/keys", "/api/websites", "/api/workspaces", "/api/dns/zones"} {
			t.Run(path, func(t *testing.T) {
				req := ctx.NewAPIRequest(http.MethodGet, path, nil)
				setAuthHeader(req, token)
				rec := httptest.NewRecorder()
				ctx.Router().ServeHTTP(rec, req)
				require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
			})
		}
		access := core.GetService[core.AccessService](ctx, core.ACCESS_SERVICE)
		require.NoError(tb, access.AssignRoleToUser(ctx.GetContext(), 1, core.ACCESS_USER_ROLE))
		pins := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		pins.EXPECT().ListPinsForUser(mock.Anything, uint(1), mock.Anything, mock.Anything, mock.Anything).
			Return(nil, 0, nil).Once()
		req := ctx.NewAPIRequest(http.MethodGet, "/pins", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)
		require.Equal(tb, http.StatusOK, rec.Code, rec.Body.String())
	}, TestOptions, coreTesting.WithService(core.ACCESS_SERVICE, service.NewAccessService))
}
