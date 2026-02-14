package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	"go.uber.org/zap"
)

// Website Handlers

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
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &websiteFilter); !ok {
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
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
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
			ctx.Response().Before(func() {
				ctx.Response().Status = http.StatusGone
			})
			return httputil.EncodeResponse(ctx, website, &resp)
		}
	}

	var resp dto.WebsiteResponse
	if err := resp.FromModel(website); err != nil {
		a.Logger().Error("Failed to convert website to response", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return httputil.EncodeResponse(ctx, website, &resp)
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
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var req dto.WebsiteRequest
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	// Build updates map
	updates := map[string]interface{}{
		"domain":      req.Domain,
		"target_type": req.TargetType,
		"target_hash": req.TargetHash,
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
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
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
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	valid, err := a.websiteService.ValidateDNS(reqCtx, user, uint(websiteID))
	if err != nil {
		a.Logger().Error("Failed to validate website DNS", zap.Error(err), zap.Uint("website_id", uint(websiteID)), zap.Uint("user_id", user))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Get website for response
	website, err := a.websiteService.GetWebsite(reqCtx, user, uint(websiteID))
	if err != nil {
		a.Logger().Error("Failed to get website after validation", zap.Error(err))
		apiErr := NewError(ErrKeyPinFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	message := "DNS validation failed"
	if valid {
		message = "DNS validation successful"
	}

	resp := dto.WebsiteValidateResponse{
		ID:      website.ID,
		Domain:  website.Domain,
		Valid:   valid,
		Message: message,
	}

	return ctx.JSON(http.StatusOK, resp)
}
