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
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/queryutil"
	queryutilHttp "go.lumeweb.com/queryutil/http"
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
		return ctx.Error(errors.New("invalid website ID"), http.StatusBadRequest)
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
		return ctx.Error(err, http.StatusInternalServerError)
	}

	resp := dto.DomainResponse{}
	if err := resp.FromModel(wd); err != nil {
		return ctx.Error(err, http.StatusInternalServerError)
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
		return ctx.Error(errors.New("invalid website ID"), http.StatusBadRequest)
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
		return ctx.Error(errors.New("invalid website ID"), http.StatusBadRequest)
	}

	domainID, err := strconv.ParseUint(c.Param("domain_id"), 10, 64)
	if err != nil {
		return ctx.Error(errors.New("invalid domain ID"), http.StatusBadRequest)
	}

	res := a.DB().WithContext(reqCtx).
		Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
		Unscoped().Delete(&pluginDb.WebsiteDomain{})
	if res.Error != nil {
		return ctx.Error(res.Error, http.StatusInternalServerError)
	}
	if res.RowsAffected == 0 {
		return ctx.Error(errors.New("domain not found"), http.StatusNotFound)
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
		return ctx.Error(errors.New("invalid website ID"), http.StatusBadRequest)
	}

	domainID, err := strconv.ParseUint(c.Param("domain_id"), 10, 64)
	if err != nil {
		return ctx.Error(errors.New("invalid domain ID"), http.StatusBadRequest)
	}

	var wd pluginDb.WebsiteDomain
	if err := a.DB().WithContext(reqCtx).
		Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
		First(&wd).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ctx.Error(errors.New("domain not found"), http.StatusNotFound)
		}
		return ctx.Error(err, http.StatusInternalServerError)
	}

	if a.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if _, err := a.delegatedDomainSvc.VerifyDomain(reqCtx, &wd); err != nil {
		return ctx.Error(err, http.StatusInternalServerError)
	}

	resp := dto.DomainResponse{}
	if err := resp.FromModel(&wd); err != nil {
		return ctx.Error(err, http.StatusInternalServerError)
	}
	return httputil.EncodeResponse(ctx, &wd, &resp)
}
