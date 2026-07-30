package api

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.uber.org/zap"
)

// getGatewayWebsite retrieves website configuration for the gateway
func (a *API) getGatewayWebsite(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	domain := c.Param("domain")
	if domain == "" {
		return ctx.Error(errors.New("domain parameter is required"), http.StatusBadRequest)
	}

	// Get website by domain
	website, ns, err := a.websiteService.GetWebsiteByDomain(reqCtx, domain)
	if err != nil {
		a.Logger().Error("Failed to get website by domain", zap.String("domain", domain), zap.Error(err))
		return ctx.Error(err, http.StatusInternalServerError)
	}

	if website == nil {
		return ctx.Error(errors.New("website not found"), http.StatusNotFound)
	}

	// Check if website is broken or deleted
	isBroken := website.Status == string(pluginDb.WebsiteStatusBroken)
	if isBroken || website.DeletedAt.Valid {
		ctx.Response().Before(func() {
			ctx.Response().Status = http.StatusGone
		})
	}

	resp := &dto.GatewayWebsiteResponse{Namespace: ns}
	return httputil.EncodeResponse(ctx, website, resp)
}

// getGatewayWebsiteStatus retrieves website status for the gateway
func (a *API) getGatewayWebsiteStatus(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	domain := c.Param("domain")
	if domain == "" {
		return ctx.Error(errors.New("domain parameter is required"), http.StatusBadRequest)
	}

	// Get website by domain
	website, _, err := a.websiteService.GetWebsiteByDomain(reqCtx, domain)
	if err != nil {
		a.Logger().Error("Failed to get website by domain", zap.String("domain", domain), zap.Error(err))
		return ctx.Error(err, http.StatusInternalServerError)
	}

	if website == nil {
		return ctx.Error(errors.New("website not found"), http.StatusNotFound)
	}

	// Determine if website is broken
	isBroken := website.Status == string(pluginDb.WebsiteStatusBroken)

	// Return 410 Gone for broken or deleted websites
	if isBroken || website.DeletedAt.Valid {
		ctx.Response().Before(func() {
			ctx.Response().Status = http.StatusGone
		})
	}

	return httputil.EncodeResponse(ctx, website, &dto.GatewayWebsiteStatusResponse{})
}

func (a *API) handlePing(c echo.Context) error {
	ctx := httputil.Context(c)
	return httputil.EncodeResponse(ctx, &dto.PingModel{Status: "ok"}, &dto.PingResponse{})
}
