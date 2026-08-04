package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/queryutil"
	queryutilHttp "go.lumeweb.com/queryutil/http"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Domain Handlers

func (a *API) createDomain(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	userID, err := mcontext.GetUserID(c)
	if err != nil {
		return ctx.Error(err, http.StatusUnauthorized)
	}

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidRequest, errors.New("invalid website ID"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	req := dto.DomainRequest{}
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	if a.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var configRaw json.RawMessage
	if req.Config != nil {
		configRaw, _ = json.Marshal(req.Config)
	}

	wd, err := a.delegatedDomainSvc.CreateDomain(reqCtx, req.Namespace, req.Domain, uint(websiteID), userID, configRaw)
	if err != nil {
		a.Logger().Error("Failed to create domain", zap.Error(err))
		apiErr := NewError(ErrKeyValidationFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	resp := dto.DomainResponse{}
	if err := resp.FromModel(wd); err != nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	ctx.Response().Before(func() {
		ctx.Response().Status = http.StatusCreated
	})
	return httputil.EncodeResponse(ctx, wd, &resp)
}

func (a *API) listDomains(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidRequest, errors.New("invalid website ID"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	return queryutilHttp.ProcessListRequest[*pluginDb.WebsiteDomain, dto.DomainResponse](
		c.Response(),
		c.Request(),
		"domains",
		func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.WebsiteDomain, int64, error) {
			query := a.DB().WithContext(reqCtx).Model(&pluginDb.WebsiteDomain{}).Where("website_id = ? AND user_id = ?", websiteID, user)

			// Apply filters and sorts from query params
			query = queryutil.ApplyFilters(query, filters, nil)
			query = queryutil.ApplySort(query, sorts)

			// Count total before pagination
			var total int64
			if err := query.Count(&total).Error; err != nil {
				return nil, 0, err
			}

			// Apply pagination
			query = queryutil.ApplyPagination(query, pagination)

			var domains []pluginDb.WebsiteDomain
			if err := query.Find(&domains).Error; err != nil {
				return nil, 0, err
			}

			ptrs := make([]*pluginDb.WebsiteDomain, len(domains))
			for i := range domains {
				ptrs[i] = &domains[i]
			}
			return ptrs, total, nil
		},
		func(d *pluginDb.WebsiteDomain) dto.DomainResponse {
			var resp dto.DomainResponse
			_ = resp.FromModel(d)
			return resp
		},
	)
}

func (a *API) deleteDomain(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	userID, err := mcontext.GetUserID(c)
	if err != nil {
		return ctx.Error(err, http.StatusUnauthorized)
	}

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidRequest, errors.New("invalid website ID"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	domainID, err := strconv.ParseUint(c.Param("domain_id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidRequest, errors.New("invalid domain ID"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	res := a.DB().WithContext(reqCtx).
		Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
		Unscoped().Delete(&pluginDb.WebsiteDomain{})
	if res.Error != nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, res.Error)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	if res.RowsAffected == 0 {
		apiErr := NewError(ErrKeyDomainNotFound, errors.New("domain not found"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return ctx.NoContent(http.StatusNoContent)
}

func (a *API) verifyDomain(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	userID, err := mcontext.GetUserID(c)
	if err != nil {
		return ctx.Error(err, http.StatusUnauthorized)
	}

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidRequest, errors.New("invalid website ID"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	domainID, err := strconv.ParseUint(c.Param("domain_id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidRequest, errors.New("invalid domain ID"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var wd pluginDb.WebsiteDomain
	if err := a.DB().WithContext(reqCtx).
		Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
		First(&wd).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			apiErr := NewError(ErrKeyDomainNotFound, errors.New("domain not found"))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if a.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if _, err := a.delegatedDomainSvc.VerifyDomain(reqCtx, &wd); err != nil {
		a.Logger().Error("Failed to verify domain", zap.Error(err))
		apiErr := NewError(ErrKeyValidationFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	resp := dto.DomainResponse{}
	if err := resp.FromModel(&wd); err != nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return httputil.EncodeResponse(ctx, &wd, &resp)
}
