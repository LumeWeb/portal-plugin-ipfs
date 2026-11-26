package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.lumeweb.com/portal-middleware/context"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func executeVerifiedRequest(t *testing.T, ctx coreTesting.TestContext, method, path string, withUser bool) (*httptest.ResponseRecorder, bool) {
	return executeVerifiedRequestQuery(t, ctx, method, path, "", withUser)
}

func executeVerifiedRequestQuery(t *testing.T, ctx coreTesting.TestContext, method, path, query string, withUser bool) (*httptest.ResponseRecorder, bool) {
	t.Helper()

	mw := VerifiedWrites(ctx, "/block/meta/batch")
	called := false
	handler := mw(func(c echo.Context) error {
		called = true
		return c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(method, path+"?"+query, nil)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	if withUser {
		c.Set(string(mcontext.UserIDKey), uint(1))
	}

	err := handler(c)
	// Simulate echo's router behavior: an HTTPError returned from middleware
	// is written to the response by the router's error handler.
	if err != nil {
		if he, ok := err.(*echo.HTTPError); ok {
			rec.Code = he.Code
		}
	}
	return rec, called
}

func TestVerifiedWrites_UnverifiedWriteForbidden(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		coreTesting.GetMockUserService(ctx).EXPECT().IsAccountVerified(mock.Anything, uint(1)).Return(false, nil).Maybe()

		rec, called := executeVerifiedRequest(t, ctx, http.MethodPost, "/pins", true)
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.False(t, called, "handler must not run for unverified accounts")
		_ = rec
	})
}

func TestVerifiedWrites_VerifiedWriteAllowed(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		coreTesting.GetMockUserService(ctx).EXPECT().IsAccountVerified(mock.Anything, uint(1)).Return(true, nil).Maybe()

		rec, called := executeVerifiedRequest(t, ctx, http.MethodPost, "/pins", true)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.True(t, called)
	})
}

func TestVerifiedWrites_ReadsPassThroughUnverified(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// No IsAccountVerified expectation: a checker call would fail the test.

		rec, called := executeVerifiedRequest(t, ctx, http.MethodGet, "/pins", false)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.True(t, called)
	})
}

func TestVerifiedWrites_UnauthenticatedWriteUnauthorized(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// No user in context: rejected before the checker runs.

		rec, called := executeVerifiedRequest(t, ctx, http.MethodDelete, "/pins/123", false)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.False(t, called, "handler must not run without a user")
		_ = rec
	})
}

func TestVerifiedWrites_ExemptPathPassesUnverified(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// POST /block/meta/batch is a read; no IsAccountVerified expectation.

		rec, called := executeVerifiedRequest(t, ctx, http.MethodPost, "/api/ipfs/block/meta/batch", false)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.True(t, called)
	})
}

func TestVerifiedWrites_WriteLikeGetQueryRequiresVerification(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// GET /workspaces/:id/access?rotate=true rotates credentials, so it is
		// treated as a write despite the safe method.
		coreTesting.GetMockUserService(ctx).EXPECT().IsAccountVerified(mock.Anything, uint(1)).Return(false, nil).Maybe()

		rec, called := executeVerifiedRequestQuery(t, ctx, http.MethodGet, "/workspaces/1/access", "rotate=true", true)
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.False(t, called, "rotating credentials must not run for unverified accounts")
	})
}

func TestVerifiedWrites_PlainGetWithQueryPassesUnverified(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Unrelated query parameters do not turn a GET into a write; no
		// IsAccountVerified expectation: a checker call would fail the test.

		rec, called := executeVerifiedRequestQuery(t, ctx, http.MethodGet, "/pins", "limit=10", false)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.True(t, called)
	})
}
