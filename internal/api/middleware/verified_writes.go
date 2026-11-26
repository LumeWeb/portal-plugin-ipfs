package middleware

import (
	"net/http"
	"strings"

	echo "github.com/labstack/echo/v4"
	portalMw "go.lumeweb.com/portal-middleware/middleware"
	"go.lumeweb.com/portal/core"
)

// writeLikeQueries lists query parameters that turn a GET into a mutating
// request. GET /workspaces/:id/access?rotate=true rotates credentials, so it
// is treated as a write despite the safe method.
var writeLikeQueries = []string{"rotate"}

// VerifiedWrites returns middleware that requires the authenticated account to
// have a verified email for state-changing requests (any method except
// GET/HEAD/OPTIONS). Read-only requests pass through unchanged, as do paths
// matching any of the exemptSuffixes (POST-based reads).
//
// It must be registered after the auth middleware so the user ID is available
// in the request context — e.g. as part of the common middleware chain:
// auth, access, verified-writes.
//
// Unauthenticated requests are rejected with 401, unverified accounts with
// 403, and user-lookup failures with 500 (delegated to the portal
// AccountVerified middleware).
func VerifiedWrites(ctx core.Context, exemptSuffixes ...string) echo.MiddlewareFunc {
	verified := portalMw.AccountVerifiedMiddleware(ctx)

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			switch c.Request().Method {
			case http.MethodHead, http.MethodOptions:
				return next(c)
			case http.MethodGet:
				for _, param := range writeLikeQueries {
					if c.QueryParam(param) != "" {
						return verified(next)(c)
					}
				}
				return next(c)
			}

			path := c.Request().URL.Path
			for _, suffix := range exemptSuffixes {
				if strings.HasSuffix(path, suffix) {
					return next(c)
				}
			}

			return verified(next)(c)
		}
	}
}
