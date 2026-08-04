package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/samber/lo"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"gorm.io/gorm"
)

// handleIPFSError handles core.Error with proper context
func (a *API) handleIPFSError(err error, c echo.Context) (error, bool) {
	if err == nil {
		return nil, false
	}
	if apiErr, ok := errors.AsType[*core.Error](err); ok {
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
		apiErr := NewError(ErrKeyInvalidPathID, nil)
		return 0, apiErr
	}
	return uint(zoneIDRaw), nil
}

// handleNoContent returns a 204 No Content response
func handleNoContent(c echo.Context) error {
	return c.NoContent(http.StatusNoContent)
}

// mapDNSErrorToAPIError converts DNS service errors to appropriate API error types
// Returns the correct error key to ensure proper HTTP status codes:
// - ErrKeyZoneNotFound (404): when zone is not found
// - ErrKeyRecordNotFound (404): when DNS record is not found
// - ErrKeyDuplicateRecord (409): when PowerDNS returns a 409 Conflict
// - ErrKeyValidationFailed (422): when PowerDNS returns a 422 Unprocessable Entity
// - ErrKeyUpdateFailed (500): for all other internal errors
//
// The resourceType parameter distinguishes between zone and record not found errors:
// - "zone": returns ErrKeyZoneNotFound for gorm.ErrRecordNotFound
// - "record": returns ErrKeyRecordNotFound for gorm.ErrRecordNotFound
func mapDNSErrorToAPIError(err error, resourceType string) core.ErrorType {
	if err == nil {
		return ErrKeyUpdateFailed
	}

	// Check for gorm.ErrRecordNotFound - distinguish based on resource type
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if resourceType == "record" {
			return ErrKeyRecordNotFound
		}
		return ErrKeyZoneNotFound
	}

	// Check for PowerDNS API errors by examining the error message
	// The handleResponse function in powerdns_client.go includes the HTTP status code
	errMsg := err.Error()

	// Check for HTTP 409 Conflict (duplicate record)
	if containsStatusCode(errMsg, 409) {
		return ErrKeyDuplicateRecord
	}

	// Check for HTTP 404 Not Found from PowerDNS
	if containsStatusCode(errMsg, 404) {
		return ErrKeyZoneNotFound
	}

	// Check for HTTP 422 Unprocessable Entity (validation error)
	if containsStatusCode(errMsg, 422) {
		return ErrKeyValidationFailed
	}

	// For all other errors, return 500 Internal Server Error
	return ErrKeyUpdateFailed
}

// containsStatusCode checks if an error message contains a specific HTTP status code
func containsStatusCode(errMsg string, statusCode int) bool {
	// Look for specific status code patterns with word boundaries to avoid false positives
	patterns := []string{
		fmt.Sprintf("status %d", statusCode),
		fmt.Sprintf("returned %d", statusCode),
		fmt.Sprintf("HTTP %d", statusCode),
		fmt.Sprintf("code %d", statusCode),
	}

	for _, pattern := range patterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}
	return false
}
