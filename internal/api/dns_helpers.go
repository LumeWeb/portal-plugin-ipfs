package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/samber/lo"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"gorm.io/gorm"
)

// handleIPFSError is a DRY helper for handling IPFSError with proper context
func (a *API) handleIPFSError(err error, c echo.Context) (error, bool) {
	if err == nil {
		return nil, false
	}
	if apiErr, ok := errors.AsType[*IPFSError](err); ok {
		ctx := httputil.Context(c)
		return ctx.Error(apiErr, apiErr.HttpStatus()), true
	}
	return err, false
}

// verifyZoneOwnership checks if a zone exists and belongs to the current user
// Returns the zone if valid, otherwise returns an appropriate error response
func (a *API) verifyZoneOwnership(c echo.Context, zoneID uint) (*db.DNSZone, error) {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	zone, err := a.dnsService.GetZone(reqCtx, zoneID)
	if err != nil {
		apiErr := NewError(ErrKeyZoneNotFound, lo.Ternary(err == gorm.ErrRecordNotFound, nil, err))
		return nil, ctx.Error(apiErr, apiErr.HttpStatus())
	}

	user, err := mcontext.GetUserID(c)
	if err != nil {
		apiErr := NewError(ErrKeyUnauthorized, err)
		return nil, ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if zone.UserID != user {
		apiErr := NewError(ErrKeyPermissionDenied, nil)
		return nil, ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return zone, nil
}

// getAuthenticatedUser retrieves the authenticated user ID from the context
// Returns an error response if authentication fails
func (a *API) getAuthenticatedUser(c echo.Context) (uint, error) {
	ctx := httputil.Context(c)

	user, err := mcontext.GetUserID(c)
	if err != nil {
		apiErr := NewError(ErrKeyUnauthorized, err)
		return 0, ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return user, nil
}

// parseZoneIDParam extracts and validates the zone ID from URL parameters
func parseZoneIDParam(c echo.Context) (uint, error) {
	zoneIDRaw, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		return 0, err
	}
	return uint(zoneIDRaw), nil
}

// parseZoneIDParamWithResponse extracts and validates the zone ID from URL parameters
// Returns an error if parsing fails, which the caller should handle
func parseZoneIDParamWithResponse(c echo.Context) (uint, error) {
	zoneIDRaw, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidIdentifier, nil)
		return 0, apiErr
	}
	return uint(zoneIDRaw), nil
}

// handleNoContent returns a 204 No Content response
func handleNoContent(c echo.Context) error {
	return c.NoContent(http.StatusNoContent)
}
