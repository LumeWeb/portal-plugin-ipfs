package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginEvents "go.lumeweb.com/portal-plugin-ipfs/internal/errors"
	pluginservice "go.lumeweb.com/portal-plugin-ipfs/internal/service/website"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	"go.uber.org/zap"
)

// ipnsKeyCIDResolver adapts IPNSKeyService to satisfy dto.IPNSKeyCIDResolver
type ipnsKeyCIDResolver struct {
	svc pluginCore.IPNSKeyService
	ctx context.Context
}

func (r ipnsKeyCIDResolver) GetKeyLastPublishedCID(userID uint, keyID uint) string {
	key, err := r.svc.GetKeyByID(r.ctx, userID, keyID)
	if err != nil || key == nil {
		return ""
	}
	return key.LastPublishedCID
}

// Website Handlers

const DefaultWebsiteEnabled = true

func (a *API) gatewayDomain() string {
	if a.dnsConfig != nil {
		return a.dnsConfig.GatewayDomain
	}
	return ""
}

func (a *API) verificationTokenKey() string {
	if a.dnsConfig != nil && a.dnsConfig.VerificationTokenKey != "" {
		return a.dnsConfig.VerificationTokenKey
	}
	return "lumeweb-verify"
}

func (a *API) zoneDomain(ctx context.Context, dnsZoneID *uint) string {
	if dnsZoneID == nil || a.dnsService == nil {
		return ""
	}
	zone, err := a.dnsService.GetZone(ctx, *dnsZoneID)
	if err != nil || zone == nil {
		return ""
	}
	return zone.Domain
}

func (a *API) getWebsiteConfig(c echo.Context) error {
	ctx := httputil.Context(c)
	cfg := &dto.WebsiteConfig{
		GatewayDomain: a.gatewayDomain(),
	}
	if a.dnsConfig != nil && a.dnsConfig.Enabled && len(a.dnsConfig.Nameservers) > 0 {
		cfg.Nameservers = a.dnsConfig.Nameservers
	}
	return httputil.EncodeResponse(ctx, cfg, &dto.WebsiteConfigResponse{})
}

// handleWebsiteValidationError is a DRY helper for handling website validation errors with proper context
func (a *API) handleWebsiteValidationError(err error, c echo.Context) (error, bool) {
	if err == nil {
		return nil, false
	}
	
	ctx := httputil.Context(c)
	
	// Check for specific validation errors using errors.Is
	if errors.Is(err, pluginservice.ErrInvalidCID) {
		apiErr := NewError(ErrKeyInvalidCID, err)
		return ctx.Error(apiErr, apiErr.HttpStatus()), true
	}
	
	if errors.Is(err, pluginservice.ErrInvalidIPNS) {
		apiErr := NewError(ErrKeyInvalidTarget, err)
		return ctx.Error(apiErr, apiErr.HttpStatus()), true
	}
	
	if errors.Is(err, pluginservice.ErrInvalidTarget) {
		apiErr := NewError(ErrKeyInvalidTarget, err)
		return ctx.Error(apiErr, apiErr.HttpStatus()), true
	}
	
	if errors.Is(err, pluginservice.ErrInvalidDomain) {
		apiErr := NewError(ErrKeyInvalidDomainFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus()), true
	}
	
	return err, false
}

func (a *API) createWebsite(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	var req dto.WebsiteRequest
	model, ok := httputil.DecodeAndValidateRequest(ctx, &req)
	if !ok {
		return nil
	}

	// Set user ID
	model.UserID = user

	website, err := a.websiteService.CreateWebsite(reqCtx, model)
	if err != nil {
		a.Logger().Error("Failed to create website", zap.Error(err), zap.Uint("user_id", user), zap.String("domain", req.Domain))
		if handledErr, wasHandled := a.handleWebsiteValidationError(err, c); wasHandled {
			return handledErr
		}
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Check if website is broken and return 410 Gone
	if website.Status == string(pluginDb.WebsiteStatusBroken) {
		return ctx.Error(fmt.Errorf("website target is broken"), http.StatusGone)
	}

	var resp dto.WebsiteResponse
	if err := resp.FromModel(website); err != nil {
		a.Logger().Error("Failed to convert website to response", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	resp.GatewayDomain = a.gatewayDomain()
	resp.SetSubdomainInfo(a.zoneDomain(reqCtx, website.DNSZoneID))
	resp.SetValidationRecordInfo(a.verificationTokenKey())
	resp.EnrichActiveCID(ipnsKeyCIDResolver{svc: a.ipnsKeyService, ctx: reqCtx}, user, website)

	ctx.Response().Before(func() {
		ctx.Response().Status = http.StatusCreated
	})

	return httputil.EncodeResponse(ctx, website, &resp)
}

func (a *API) listWebsites(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	websiteFilter := dto.WebsiteFilter{}
	if _, ok := httputil.DecodeAndValidateQueryRequest(ctx, &websiteFilter); !ok {
		return nil
	}

	// Build filters from request
	var filters []queryutil.CrudFilter
	if websiteFilter.Domain != nil {
		filters = append(filters, queryutil.NewLogicalFilter("domain", filter.OpEq, *websiteFilter.Domain))
	}
	if websiteFilter.TargetType != nil {
		filters = append(filters, queryutil.NewLogicalFilter("target_type", filter.OpEq, *websiteFilter.TargetType))
	}
	if websiteFilter.Status != nil {
		filters = append(filters, queryutil.NewLogicalFilter("status", filter.OpEq, *websiteFilter.Status))
	}

	// Add user filter
	filters = append(filters, queryutil.NewLogicalFilter("user_id", filter.OpEq, user))

	// Default pagination
	pagination := filter.DefaultPagination

	websites, total, err := a.websiteService.ListWebsites(reqCtx, user, filters, []queryutil.Sort{}, pagination)
	if err != nil {
		a.Logger().Error("Failed to list websites", zap.Error(err), zap.Uint("user_id", user))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	responses := make([]dto.WebsiteResponse, len(websites))
	for i, website := range websites {
		if err := responses[i].FromModel(website); err != nil {
			a.Logger().Error("Failed to convert website to response", zap.Error(err))
			apiErr := NewError(ErrKeyFileProcessingFailed, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		responses[i].GatewayDomain = a.gatewayDomain()
		responses[i].SetSubdomainInfo(a.zoneDomain(reqCtx, website.DNSZoneID))
		responses[i].SetValidationRecordInfo(a.verificationTokenKey())
		responses[i].EnrichActiveCID(ipnsKeyCIDResolver{svc: a.ipnsKeyService, ctx: reqCtx}, user, website)
	}

	// Convert to WebsiteItem for list response
	items := make([]dto.WebsiteItem, len(responses))
	for i := range responses {
		items[i] = dto.WebsiteItem(responses[i])
	}

	result := queryutil.BuildResponse(items, total)

	return ctx.JSON(http.StatusOK, result)
}

func (a *API) getWebsite(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidRequest, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	website, err := a.websiteService.GetWebsite(reqCtx, user, uint(websiteID))
	if err != nil {
		a.Logger().Error("Failed to get website", zap.Error(err), zap.Uint("website_id", uint(websiteID)), zap.Uint("user_id", user))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Check if website is broken or deleted and return 410 Gone
	isBroken := website.Status == string(pluginDb.WebsiteStatusBroken)
	if isBroken || website.DeletedAt.Valid {
		var resp dto.WebsiteResponse
		if err := resp.FromModel(website); err == nil {
			resp.GatewayDomain = a.gatewayDomain()
			resp.SetSubdomainInfo(a.zoneDomain(reqCtx, website.DNSZoneID))
			resp.SetValidationRecordInfo(a.verificationTokenKey())
			resp.EnrichActiveCID(ipnsKeyCIDResolver{svc: a.ipnsKeyService, ctx: reqCtx}, user, website)
			ctx.Response().Before(func() {
				ctx.Response().Status = http.StatusGone
			})
			return httputil.EncodeResponse(ctx, website, &resp)
		}
	}

	resp := &dto.WebsiteResponse{}
	resp.GatewayDomain = a.gatewayDomain()
	resp.SetSubdomainInfo(a.zoneDomain(reqCtx, website.DNSZoneID))
	resp.SetValidationRecordInfo(a.verificationTokenKey())
	resp.EnrichActiveCID(ipnsKeyCIDResolver{svc: a.ipnsKeyService, ctx: reqCtx}, user, website)
	return httputil.EncodeResponse(ctx, website, resp)
}

func (a *API) updateWebsite(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidRequest, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var req dto.WebsiteUpdateRequest
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	if !req.HasUpdates() {
		return ctx.Error(NewError(ErrKeyInvalidRequest, fmt.Errorf("at least one field must be provided")), http.StatusUnprocessableEntity)
	}

	// Build updates map from non-nil fields
	updates := map[string]interface{}{}

	if req.Domain != nil {
		updates["domain"] = *req.Domain
	}
	if req.TargetType != nil || req.TargetHash != nil {
		if req.TargetType != nil && req.TargetHash == nil {
			updates["target_type"] = string(*req.TargetType)
		} else if req.TargetHash != nil && req.TargetType == nil {
			return ctx.Error(NewError(ErrKeyInvalidRequest, fmt.Errorf("target_type is required when target_hash is provided")), http.StatusUnprocessableEntity)
		} else {
			updates["target_type"] = string(*req.TargetType)
			updates["target_hash"] = *req.TargetHash
		}
	}
	if req.DNSEnabled != nil {
		updates["dns_enabled"] = *req.DNSEnabled
	}

	website, err := a.websiteService.UpdateWebsite(reqCtx, user, uint(websiteID), updates)
	if err != nil {
		a.Logger().Error("Failed to update website", zap.Error(err), zap.Uint("website_id", uint(websiteID)), zap.Uint("user_id", user))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var resp dto.WebsiteResponse
	if err := resp.FromModel(website); err != nil {
		a.Logger().Error("Failed to convert website to response", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	resp.GatewayDomain = a.gatewayDomain()
	resp.SetSubdomainInfo(a.zoneDomain(reqCtx, website.DNSZoneID))
	resp.SetValidationRecordInfo(a.verificationTokenKey())
	resp.EnrichActiveCID(ipnsKeyCIDResolver{svc: a.ipnsKeyService, ctx: reqCtx}, user, website)

	return httputil.EncodeResponse(ctx, website, &resp)
}

func (a *API) deleteWebsite(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidRequest, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if err := a.websiteService.DeleteWebsite(reqCtx, user, uint(websiteID)); err != nil {
		a.Logger().Error("Failed to delete website", zap.Error(err), zap.Uint("website_id", uint(websiteID)), zap.Uint("user_id", user))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return ctx.NoContent(http.StatusNoContent)
}

func (a *API) validateWebsiteDNS(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidRequest, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	result, err := a.websiteService.ValidateDNS(reqCtx, user, uint(websiteID))
	if err != nil {
		a.Logger().Error("Failed to validate website DNS", zap.Error(err), zap.Uint("website_id", uint(websiteID)), zap.Uint("user_id", user))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	website, err := a.websiteService.GetWebsite(reqCtx, user, uint(websiteID))
	if err != nil {
		a.Logger().Error("Failed to get website after validation", zap.Error(err))
		apiErr := NewError(ErrKeyPinFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	resp := dto.WebsiteValidateResponse{
		ID:      website.ID,
		Domain:  website.Domain,
		Valid:   result.Valid,
		Message: result.Message,
		Reason:  string(result.Reason),
	}

	return ctx.JSON(http.StatusOK, resp)
}

func (a *API) getSSLStatus(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	domain := c.Param("domain")
	if domain == "" {
		apiErr := NewError(pluginEvents.ErrInvalidDomain, fmt.Errorf("domain is required"))
		return ctx.Error(apiErr, http.StatusBadRequest)
	}

	website, _, err := a.websiteService.GetWebsiteByDomain(reqCtx, domain)
	if err != nil {
		if strings.Contains(err.Error(), "website not found") {
			apiErr := NewError(pluginEvents.ErrWebsiteNotFound, err)
			return ctx.Error(apiErr, http.StatusNotFound)
		}
		a.Logger().Error("Failed to get website SSL status", zap.Error(err), zap.String("domain", domain))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if website == nil {
		apiErr := NewError(pluginEvents.ErrWebsiteNotFound, fmt.Errorf("website not found: %s", domain))
		return ctx.Error(apiErr, http.StatusNotFound)
	}

	resp := &dto.WebsiteResponse{}
	resp.GatewayDomain = a.gatewayDomain()
	resp.SetSubdomainInfo(a.zoneDomain(reqCtx, website.DNSZoneID))
	resp.SetValidationRecordInfo(a.verificationTokenKey())
	resp.EnrichActiveCID(ipnsKeyCIDResolver{svc: a.ipnsKeyService, ctx: reqCtx}, website.UserID, website)
	return httputil.EncodeResponse(ctx, website, resp)
}

func (a *API) updateSSLStatus(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	domain := c.Param("domain")
	if domain == "" {
		apiErr := NewError(pluginEvents.ErrInvalidDomain, fmt.Errorf("domain is required"))
		return ctx.Error(apiErr, http.StatusBadRequest)
	}

	var req dto.SSLStatusUpdateRequest
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	var timestamp *time.Time
	if req.Timestamp != "" {
		parsed, err := time.Parse(time.RFC3339, req.Timestamp)
		if err != nil {
			apiErr := NewError(pluginEvents.ErrInvalidTimestamp, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		timestamp = &parsed
	}

	website, err := a.websiteService.UpdateSSLStatus(reqCtx, domain, req.Status, req.Error, timestamp)
	if err != nil {
		if strings.Contains(err.Error(), "website not found") {
			apiErr := NewError(pluginEvents.ErrWebsiteNotFound, err)
			return ctx.Error(apiErr, http.StatusNotFound)
		}
		a.Logger().Error("Failed to update SSL status", zap.Error(err), zap.String("domain", domain))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	resp := &dto.WebsiteResponse{}
	resp.GatewayDomain = a.gatewayDomain()
	resp.SetSubdomainInfo(a.zoneDomain(reqCtx, website.DNSZoneID))
	resp.SetValidationRecordInfo(a.verificationTokenKey())
	resp.EnrichActiveCID(ipnsKeyCIDResolver{svc: a.ipnsKeyService, ctx: reqCtx}, website.UserID, website)
	return httputil.EncodeResponse(ctx, website, resp)
}
