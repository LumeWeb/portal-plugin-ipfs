package api

import (
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/samber/lo"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	queryutilHttp "go.lumeweb.com/queryutil/http"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	dnsservice "go.lumeweb.com/portal-plugin-ipfs/internal/service/dns"
	"go.uber.org/zap"
)

// createZone creates a new DNS zone
func (a *API) createZone(c echo.Context) error {
	user, err := a.getAuthenticatedUser(c)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	var req dto.ZoneRequest
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	zone, err := a.dnsService.CreateZone(reqCtx, req.Domain, user)
	if err != nil {
		a.Logger().Error("Failed to create DNS zone", zap.Error(err), zap.Uint("user_id", user), zap.String("domain", req.Domain))
		apiErr := NewError(ErrKeyInvalidDomainFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var resp dto.ZoneResponse
	ctx.Response().Before(func() {
		ctx.Response().Status = http.StatusCreated
	})
	return httputil.EncodeResponse(ctx, zone, &resp)
}

// listZones lists DNS zones for the authenticated user
func (a *API) listZones(c echo.Context) error {
	user, err := a.getAuthenticatedUser(c)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	// Create a service function that includes user filtering
	listFunc := func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*db.DNSZone, int64, error) {
		// Add user_id filter to ensure user only sees their own zones
		userFilter := filter.NewLogicalFilter("user_id", filter.OpEq, user)
		// Prepend user filter to a new slice to avoid mutating the original filters slice
		allFilters := append([]queryutil.CrudFilter{userFilter}, filters...)
		return a.dnsService.ListZones(reqCtx, allFilters, sorts, pagination)
	}

	return queryutilHttp.ProcessListRequest[*db.DNSZone, dto.ZoneListResponse](
		c.Response(),
		c.Request(),
		"dns-zones",
		listFunc,
		func(zone *db.DNSZone) dto.ZoneListResponse {
			var resp dto.ZoneListResponse
			_ = resp.FromModel(zone)
			return resp
		},
	)
}

// getZone gets a specific DNS zone by ID
func (a *API) getZone(c echo.Context) error {
	zoneID, err := parseZoneIDParamWithResponse(c)
	if err != nil {
		return err
	}

	zone, err := a.verifyZoneOwnership(c, zoneID)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	var resp dto.ZoneResponse
	return httputil.EncodeResponse(ctx, zone, &resp)
}

// updateZone updates a DNS zone
func (a *API) updateZone(c echo.Context) error {
	zoneID, err := parseZoneIDParamWithResponse(c)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	var req dto.ZoneRequest
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	zone, err := a.verifyZoneOwnership(c, zoneID)
	if err != nil {
		return err
	}

	// Note: Domain cannot be changed after creation
	// This endpoint currently only supports status updates
	// Nameservers are managed separately via DNS hosting provider

	// Update zone status to active if nameservers are provided
	if len(req.Nameservers) > 0 {
		reqCtx := ctx.Context.Request().Context()
		err = a.dnsService.UpdateZone(reqCtx, zoneID, db.DNSZoneStatusActive)
		if err != nil {
			a.Logger().Error("Failed to update DNS zone", zap.Error(err), zap.Uint("zone_id", zoneID))
			apiErr := NewError(ErrKeyUpdateFailed, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}

		zone, err = a.dnsService.GetZone(ctx.Context.Request().Context(), zoneID)
		if err != nil {
			a.Logger().Error("Failed to retrieve updated DNS zone", zap.Error(err), zap.Uint("zone_id", zoneID))
			apiErr := NewError(ErrKeyZoneNotFound, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
	}
	if err != nil {
		a.Logger().Error("Failed to retrieve updated DNS zone", zap.Error(err), zap.Uint("zone_id", zoneID))
		apiErr := NewError(ErrKeyZoneNotFound, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var resp dto.ZoneResponse
	return httputil.EncodeResponse(ctx, zone, &resp)
}

// deleteZone deletes a DNS zone
func (a *API) deleteZone(c echo.Context) error {
	zoneID, err := parseZoneIDParamWithResponse(c)
	if err != nil {
		return err
	}

	_, err = a.verifyZoneOwnership(c, zoneID)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	err = a.dnsService.DeleteZone(reqCtx, zoneID)
	if err != nil {
		a.Logger().Error("Failed to delete DNS zone", zap.Error(err), zap.Uint("zone_id", zoneID))
		apiErr := NewError(ErrKeyDeleteFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return handleNoContent(c)
}

// validateZone validates nameservers for a zone
func (a *API) validateZone(c echo.Context) error {
	zoneID, err := parseZoneIDParamWithResponse(c)
	if err != nil {
		return err
	}

	_, err = a.verifyZoneOwnership(c, zoneID)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	valid, err := a.dnsService.ValidateNameservers(reqCtx, zoneID)
	if err != nil {
		a.Logger().Error("Failed to validate DNS zone", zap.Error(err), zap.Uint("zone_id", zoneID))
		apiErr := NewError(ErrKeyValidationFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	message := "Validation failed"
	if valid {
		message = "Validation successful"
	}

	resp := dto.ValidationResponse{
		Valid:     valid,
		Message:   message,
		CheckedAt: time.Now(),
	}

	return c.JSON(http.StatusOK, resp)
}

// getZoneStatus gets the status of a DNS zone
func (a *API) getZoneStatus(c echo.Context) error {
	zoneID, err := parseZoneIDParamWithResponse(c)
	if err != nil {
		return err
	}

	zone, err := a.verifyZoneOwnership(c, zoneID)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	var resp dto.ZoneResponse
	return httputil.EncodeResponse(ctx, zone, &resp)
}

// listRecords lists DNS records for a zone
func (a *API) listRecords(c echo.Context) error {
	zoneID, err := parseZoneIDParamWithResponse(c)
	if err != nil {
		return err
	}

	_, err = a.verifyZoneOwnership(c, zoneID)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	// Create a service function that includes zone_id filtering
	listFunc := func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*dto.DNSRecord, int64, error) {
		// Add zone_id filter to ensure only records for this zone are returned
		zoneFilter := filter.NewLogicalFilter("zone_id", filter.OpEq, zoneID)
		// Prepend zone filter to a new slice to avoid mutating the original filters slice
		allFilters := append([]queryutil.CrudFilter{zoneFilter}, filters...)
		return a.dnsService.GetZoneRecords(reqCtx, zoneID, allFilters, sorts, pagination)
	}

	return queryutilHttp.ProcessListRequest[*dto.DNSRecord, dto.RecordResponse](
		c.Response(),
		c.Request(),
		"dns-records",
		listFunc,
		func(record *dto.DNSRecord) dto.RecordResponse {
			var resp dto.RecordResponse
			_ = resp.FromModel(record)
			return resp
		},
	)
}

// getRecord gets a specific DNS record by name and type
func (a *API) getRecord(c echo.Context) error {
	zoneID, err := parseZoneIDParam(c)
	if err != nil {
		return err
	}

	_, err = a.verifyZoneOwnership(c, zoneID)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	name := c.Param("name")
	recordType := c.Param("type")

	records, err := a.dnsService.GetRRSet(reqCtx, zoneID, name, recordType)
	if err != nil {
		a.Logger().Error("Failed to get DNS record", zap.Error(err), zap.String("name", name), zap.String("type", recordType))
		apiErr := NewError(ErrKeyRecordNotFound, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if len(records) == 0 {
		apiErr := NewError(ErrKeyRecordNotFound, nil)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var resp dto.RecordResponse
	return httputil.EncodeResponse(ctx, records[0], &resp)
}

// createRecord creates a new DNS record
func (a *API) createRecord(c echo.Context) error {
	zoneID, err := parseZoneIDParam(c)
	if err != nil {
		return err
	}

	_, err = a.verifyZoneOwnership(c, zoneID)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	var req dto.RecordRequest
	_, ok := httputil.DecodeAndValidateRequest(ctx, &req)
	if !ok {
		return nil
	}

	ttl := uint(dnsservice.DefaultTTL) // Using constant from service layer

	record, err := a.dnsService.CreateRecord(reqCtx, zoneID, req.Name, req.Type, req.Content, ttl)
	if err != nil {
		a.Logger().Error("Failed to create DNS record", zap.Error(err), zap.Uint("zone_id", zoneID))
		apiErr := NewError(ErrKeyDuplicateRecord, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var resp dto.RecordResponse
	return httputil.EncodeResponse(ctx, record, &resp)
}

// updateRecord updates a DNS record
func (a *API) updateRecord(c echo.Context) error {
	zoneID, err := parseZoneIDParam(c)
	if err != nil {
		return err
	}

	_, err = a.verifyZoneOwnership(c, zoneID)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	name := c.Param("name")
	recordType := c.Param("type")

	var req dto.RecordRequest
	_, ok := httputil.DecodeAndValidateRequest(ctx, &req)
	if !ok {
		return nil
	}

	ttl := dnsservice.DefaultTTL // Using constant from service layer

	records, err := a.dnsService.UpdateRecord(reqCtx, zoneID, name, recordType, []string{req.Content}, ttl)
	if err != nil {
		a.Logger().Error("Failed to update DNS record", zap.Error(err), zap.String("name", name), zap.String("type", recordType))
		apiErr := NewError(ErrKeyRecordNotFound, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if len(records) == 0 {
		apiErr := NewError(ErrKeyRecordNotFound, nil)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var resp dto.RecordResponse
	return httputil.EncodeResponse(ctx, records[0], &resp)
}

// deleteRecord deletes a DNS record
func (a *API) deleteRecord(c echo.Context) error {
	zoneID, err := parseZoneIDParam(c)
	if err != nil {
		return err
	}

	_, err = a.verifyZoneOwnership(c, zoneID)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	name := c.Param("name")
	recordType := c.Param("type")

	err = a.dnsService.DeleteRecord(reqCtx, zoneID, name, recordType)
	if err != nil {
		a.Logger().Error("Failed to delete DNS record", zap.Error(err), zap.String("name", name), zap.String("type", recordType))
		apiErr := NewError(ErrKeyRecordNotFound, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return handleNoContent(c)
}

// bulkRecords handles bulk operations on DNS records
func (a *API) bulkRecords(c echo.Context) error {
	zoneID, err := parseZoneIDParam(c)
	if err != nil {
		return err
	}

	_, err = a.verifyZoneOwnership(c, zoneID)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	var req dto.BulkRecordRequest
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	// For bulk operations, we'll process each record individually
	// PowerDNS doesn't have a bulk API, so we'll make multiple calls
	recordResponses := lo.Map(req.Records, func(r dto.RecordRequest, _ int) dto.RecordResponse {
		ttl := dnsservice.GetDefaultTTL(r.TTL)
		record, err := a.dnsService.CreateRecord(reqCtx, zoneID, r.Name, r.Type, r.Content, ttl)
		if err != nil {
			// Error will be handled by the caller - return empty response on error
			a.Logger().Error("Failed to create DNS record", zap.Error(err), zap.Uint("zone_id", zoneID))
			return dto.RecordResponse{}
		}
		var resp dto.RecordResponse
		_ = resp.FromModel(record)
		return resp
	})

	resp := dto.BulkRecordRecords{
		Records: recordResponses,
	}

	return c.JSON(http.StatusOK, resp)
}

// bulkDeleteRecords handles bulk delete operations on DNS records
func (a *API) bulkDeleteRecords(c echo.Context) error {
	zoneID, err := parseZoneIDParam(c)
	if err != nil {
		return err
	}

	zone, err := a.verifyZoneOwnership(c, zoneID)
	if err != nil {
		return err
	}

	userID := zone.UserID
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	var req dto.BulkDeleteRequest
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	if len(req.Records) == 0 {
		apiErr := NewError(ErrKeyInvalidRequest, nil)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	response, err := a.dnsService.BulkDeleteRecords(reqCtx, zoneID, userID, req.Records, req.DryRun)
	if err != nil {
		a.Logger().Error("Failed to bulk delete DNS records", zap.Error(err), zap.Uint("zone_id", zoneID), zap.Int("record_count", len(req.Records)))
		apiErr := NewError(ErrKeyInvalidRequest, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return c.JSON(http.StatusOK, response)
}
