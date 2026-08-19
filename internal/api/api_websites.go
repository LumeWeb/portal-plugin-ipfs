package api

import (
	"context"
	"encoding/json"
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
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pluginEvents "go.lumeweb.com/portal-plugin-ipfs/internal/errors"
	pluginservice "go.lumeweb.com/portal-plugin-ipfs/internal/service/website"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	"go.uber.org/zap"
	"gorm.io/gorm"
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

func (a *API) zoneDomain(ctx context.Context, zoneID uint) string {
	if zoneID == 0 || a.dnsService == nil {
		return ""
	}
	zone, err := a.dnsService.GetZone(ctx, zoneID)
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

	// Transparently create the primary domain binding. Creating a website
	// creates its primary WebsiteDomain (which now owns DNS hosting state);
	// the domain is name-spaced (icann default, or hns as specified).
	dnsEnabled := dto.DefaultWebsiteEnabled
	if req.DNSEnabled != nil {
		dnsEnabled = *req.DNSEnabled
	}
	if a.delegatedDomainSvc != nil {
		namespace := string(pluginDb.DomainNamespaceICANN)
		if req.Namespace != nil {
			namespace = string(*req.Namespace)
		}
		var cfgRaw json.RawMessage
		if _, err := a.delegatedDomainSvc.CreateDomain(reqCtx, namespace, req.Domain, website.ID, user, dnsEnabled, true, cfgRaw); err != nil {
			a.Logger().Error("Failed to create primary domain for website",
				zap.Uint("website_id", website.ID), zap.String("domain", req.Domain), zap.Error(err))
			apiErr := NewError(ErrKeyFileProcessingFailed, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		// Enable per-domain DNS hosting (default true) so the binding is set up
		// for DNS as the legacy website-level dns_hosting_enabled did.
		if a.websiteService != nil {
			// The primary binding just created is the website's primary domain.
			primary, perr := a.websiteService.GetApexDomainBinding(reqCtx, website.ID)
			if perr == nil && primary != nil {
				if _, derr := a.websiteService.SetDomainDNSEnabled(reqCtx, user, website.ID, primary.ID, dnsEnabled); derr != nil {
					a.Logger().Error("failed to set DNS hosting on primary domain",
						zap.Uint("domain_id", primary.ID), zap.Error(derr))
					apiErr := NewError(ErrKeyFileProcessingFailed, derr)
					return ctx.Error(apiErr, apiErr.HttpStatus())
				}
			}
		}
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
	// Resolve the primary domain for the response's domain/DNS fields.
	primary, perr := a.websiteService.GetApexDomainBinding(reqCtx, website.ID)
	if perr != nil {
		a.Logger().Warn("website has no primary domain binding", zap.Uint("website_id", website.ID), zap.Error(perr))
	}
	resp.SetPrimaryDomain(primary)
	resp.GatewayDomain = a.gatewayDomain()
	if primary != nil {
		resp.SetSubdomainInfo(a.zoneDomain(reqCtx, primary.ZoneID))
	} else {
		resp.SetSubdomainInfo("")
	}
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
		primary, perr := a.websiteService.GetApexDomainBinding(reqCtx, website.ID)
		if perr != nil {
			a.Logger().Warn("website has no primary domain binding", zap.Uint("website_id", website.ID), zap.Error(perr))
		}
		responses[i].SetPrimaryDomain(primary)
		responses[i].GatewayDomain = a.gatewayDomain()
		if primary != nil {
			responses[i].SetSubdomainInfo(a.zoneDomain(reqCtx, primary.ZoneID))
		} else {
			responses[i].SetSubdomainInfo("")
		}
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
		apiErr := NewError(ErrKeyInvalidPathID, err)
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
			primary, _ := a.websiteService.GetApexDomainBinding(reqCtx, website.ID)
			resp.SetPrimaryDomain(primary)
			resp.GatewayDomain = a.gatewayDomain()
			if primary != nil {
				resp.SetSubdomainInfo(a.zoneDomain(reqCtx, primary.ZoneID))
			} else {
				resp.SetSubdomainInfo("")
			}
			resp.SetValidationRecordInfo(a.verificationTokenKey())
			resp.EnrichActiveCID(ipnsKeyCIDResolver{svc: a.ipnsKeyService, ctx: reqCtx}, user, website)
			ctx.Response().Before(func() {
				ctx.Response().Status = http.StatusGone
			})
			return httputil.EncodeResponse(ctx, website, &resp)
		}
	}

	primary, perr := a.websiteService.GetApexDomainBinding(reqCtx, website.ID)
	if perr != nil {
		a.Logger().Warn("website has no primary domain binding", zap.Uint("website_id", website.ID), zap.Error(perr))
	}
	resp := &dto.WebsiteResponse{}
	if err := resp.FromModel(website); err != nil {
		a.Logger().Error("Failed to convert website to response", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	resp.SetPrimaryDomain(primary)
	resp.GatewayDomain = a.gatewayDomain()
	if primary != nil {
		resp.SetSubdomainInfo(a.zoneDomain(reqCtx, primary.ZoneID))
	} else {
		resp.SetSubdomainInfo("")
	}
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
		apiErr := NewError(ErrKeyInvalidPathID, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var req dto.WebsiteUpdateRequest
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	if !req.HasUpdates() {
		return ctx.Error(NewError(ErrKeyInvalidRequest, fmt.Errorf("at least one field must be provided")), http.StatusUnprocessableEntity)
	}

	// Build updates map from non-nil fields. Target type and hash update the
	// Website record directly. Domain and DNS hosting are per-domain state that
	// lives on the primary WebsiteDomain; they are applied after the update
	// (domain change repoints the primary binding, DNS toggle updates the
	// primary binding's dns_hosting_enabled).
	updates := map[string]interface{}{}

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

	website, err := a.websiteService.UpdateWebsite(reqCtx, user, uint(websiteID), updates)
	if err != nil {
		a.Logger().Error("Failed to update website", zap.Error(err), zap.Uint("website_id", uint(websiteID)), zap.Uint("user_id", user))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Apply per-domain changes sourced from the website update request.
	var primary *pluginDb.WebsiteDomain

	// 1. Change the primary domain: create the requested domain binding and
	// make it the website's new primary (apex) domain. Setting a domain is
	// idempotent for a binding that already belongs to this website: the
	// deployed domain is re-used and repointed as primary rather than
	// re-created (CreateDomain is create-only and would otherwise trip the
	// (domain, namespace) unique key with a 500 on every re-deploy). A domain
	// live-bound to a different website is an ownership conflict (409), not a
	// 500.
	if req.Domain != nil {
		if a.delegatedDomainSvc == nil {
			return ctx.Error(NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service unavailable")), http.StatusInternalServerError)
		}
		namespace := string(pluginDb.DomainNamespaceICANN)
		if req.Namespace != nil {
			namespace = string(*req.Namespace)
		}
		var cfgRaw json.RawMessage
		// Managed-DNS by default; explicit DNSEnabled override flows through.
		newDomainDNS := true
		if req.DNSEnabled != nil {
			newDomainDNS = *req.DNSEnabled
		}

		// Reuse an existing live binding for this (domain, namespace) when it
		// already belongs to the website; otherwise fall through to a genuine
		// create (change-primary). A binding owned by a different website is
		// surfaced as an explicit ownership conflict.
		existing, eerr := a.delegatedDomainSvc.GetWebsiteDomainByDomainAndNamespace(reqCtx, *req.Domain, pluginDb.DomainNamespace(namespace))
		switch {
		case eerr == nil && !existing.DeletedAt.Valid && existing.WebsiteID == website.ID:
			// Re-deploy to an already-bound primary: reuse, don't re-create.
			// The binding's DNS hosting state is preserved as-is; only an
			// explicit dns_hosting_enabled override (step 2 below) changes it.
			// Silently defaulting DNS hosting on here would re-provision a
			// PowerDNS zone on a binding the user deliberately self-hosted,
			// defeating the idempotent re-deploy guarantee.
			wd := existing
			rerp, derr := a.websiteService.SetPrimaryDomain(reqCtx, user, website.ID, wd.ID)
			if derr != nil {
				a.Logger().Error("Failed to set primary domain", zap.Uint("domain_id", wd.ID), zap.Error(derr))
				apiErr := NewError(ErrKeyFileProcessingFailed, derr)
				return ctx.Error(apiErr, apiErr.HttpStatus())
			}
			primary = rerp
		case eerr == nil:
			// Domain is live-bound to a different website — refuse to repoint.
			a.Logger().Warn("Refusing to bind domain owned by another website",
				zap.Uint("website_id", website.ID), zap.String("domain", *req.Domain), zap.Uint("domain_owner_website_id", existing.WebsiteID))
			apiErr := NewError(ErrKeyDomainInUse, fmt.Errorf("domain %q is already in use by another website", *req.Domain))
			return ctx.Error(apiErr, http.StatusConflict)
		default:
			if !errors.Is(eerr, gorm.ErrRecordNotFound) {
				a.Logger().Error("Failed to look up existing domain binding", zap.String("domain", *req.Domain), zap.Error(eerr))
				apiErr := NewError(ErrKeyFileProcessingFailed, eerr)
				return ctx.Error(apiErr, apiErr.HttpStatus())
			}
			wd, derr := a.delegatedDomainSvc.CreateDomain(reqCtx, namespace, *req.Domain, website.ID, user, newDomainDNS, false, cfgRaw)
			if derr != nil {
				a.Logger().Error("Failed to create primary domain for website",
					zap.Uint("website_id", website.ID), zap.String("domain", *req.Domain), zap.Error(derr))
				apiErr := NewError(ErrKeyFileProcessingFailed, derr)
				return ctx.Error(apiErr, apiErr.HttpStatus())
			}
			primary, derr = a.websiteService.SetPrimaryDomain(reqCtx, user, website.ID, wd.ID)
			if derr != nil {
				a.Logger().Error("Failed to set primary domain", zap.Uint("domain_id", wd.ID), zap.Error(derr))
				apiErr := NewError(ErrKeyFileProcessingFailed, derr)
				return ctx.Error(apiErr, apiErr.HttpStatus())
			}
		}
	}

	// 2. Toggle DNS hosting on the primary domain binding. Resolved after any
	// domain change so the toggle applies to the current primary.
	if req.DNSEnabled != nil {
		if primary == nil {
			p, perr := a.websiteService.GetApexDomainBinding(reqCtx, website.ID)
			if perr != nil {
				a.Logger().Warn("cannot toggle DNS hosting: no primary domain binding", zap.Uint("website_id", website.ID), zap.Error(perr))
			} else {
				primary = p
			}
		}
		if primary != nil {
			updated, derr := a.websiteService.SetDomainDNSEnabled(reqCtx, user, website.ID, primary.ID, *req.DNSEnabled)
			if derr != nil {
				a.Logger().Error("failed to set DNS hosting on primary domain", zap.Uint("domain_id", primary.ID), zap.Error(derr))
				apiErr := NewError(ErrKeyFileProcessingFailed, derr)
				return ctx.Error(apiErr, apiErr.HttpStatus())
			}
			// Use the toggled binding for the response so Enabled/ZoneID
			// reflect the actual DNS state rather than the pre-toggle apex.
			primary = updated
		}
	}

	var resp dto.WebsiteResponse
	if err := resp.FromModel(website); err != nil {
		a.Logger().Error("Failed to convert website to response", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	if primary == nil {
		var perr error
		primary, perr = a.websiteService.GetApexDomainBinding(reqCtx, website.ID)
		if perr != nil {
			a.Logger().Warn("website has no primary domain binding", zap.Uint("website_id", website.ID), zap.Error(perr))
		}
	}
	resp.SetPrimaryDomain(primary)
	resp.GatewayDomain = a.gatewayDomain()
	if primary != nil {
		resp.SetSubdomainInfo(a.zoneDomain(reqCtx, primary.ZoneID))
	} else {
		resp.SetSubdomainInfo("")
	}
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
		apiErr := NewError(ErrKeyInvalidPathID, err)
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
		apiErr := NewError(ErrKeyInvalidPathID, err)
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

	primary, perr := a.websiteService.GetApexDomainBinding(reqCtx, website.ID)
	if perr != nil {
		a.Logger().Warn("website has no primary domain binding", zap.Uint("website_id", website.ID), zap.Error(perr))
	}
	resp := dto.WebsiteValidateResponse{
		ID:      website.ID,
		Valid:   result.Valid,
		Message: result.Message,
		Reason:  string(result.Reason),
	}
	if primary != nil {
		resp.Domain = primary.Domain
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
	primary, perr := a.websiteService.GetApexDomainBinding(reqCtx, website.ID)
	if perr == nil && primary != nil {
		resp.SetPrimaryDomain(primary)
		resp.SetSubdomainInfo(a.zoneDomain(reqCtx, primary.ZoneID))
	} else {
		// Fall back to the requested domain when the primary binding can't be
		// resolved; the lookup was itself by domain string.
		resp.Domain = domain
		resp.SetSubdomainInfo("")
	}
	resp.SetValidationRecordInfo(a.verificationTokenKey())
	resp.EnrichActiveCID(ipnsKeyCIDResolver{svc: a.ipnsKeyService, ctx: reqCtx}, website.UserID, website)
	a.applyApexSSLStatus(reqCtx, resp, website)
	return httputil.EncodeResponse(ctx, website, resp)
}

// applyApexSSLStatus synthesizes the website-level SSL status from the apex
// (primary) domain binding, preserving backward-compatible site-level SSL
// presentation now that the source of truth lives per-domain on WebsiteDomain.
func (a *API) applyApexSSLStatus(ctx context.Context, resp *dto.WebsiteResponse, website *pluginDb.Website) {
	if website == nil {
		return
	}
	apex, err := a.websiteService.GetApexDomainBinding(ctx, website.ID)
	if err != nil || apex == nil || apex.SSLStatus == "" {
		return
	}
	resp.SSL = sslStatusInfoFromDomain(apex)
}

// sslStatusInfoFromDomain converts a domain binding's SSL state to the DTO.
func sslStatusInfoFromDomain(wd *pluginDb.WebsiteDomain) *dto.SSLStatusInfo {
	info := &dto.SSLStatusInfo{
		Status: wd.SSLStatus,
		Error:  wd.SSLError,
	}
	if wd.SSLIssuedAt != nil {
		v := *wd.SSLIssuedAt
		info.IssuedAt = &v
	}
	if wd.SSLLastUpdatedAt != nil {
		v := *wd.SSLLastUpdatedAt
		info.LastUpdatedAt = &v
	}
	return info
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

	wd, err := a.websiteService.UpdateSSLStatus(reqCtx, domain, req.Status, req.Error, timestamp)
	if err != nil {
		if strings.Contains(err.Error(), "website not found") {
			apiErr := NewError(pluginEvents.ErrWebsiteNotFound, err)
			return ctx.Error(apiErr, http.StatusNotFound)
		}
		a.Logger().Error("Failed to update SSL status", zap.Error(err), zap.String("domain", domain))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	website, err := a.websiteService.GetWebsite(reqCtx, wd.UserID, wd.WebsiteID)
	if err != nil {
		a.Logger().Error("Failed to load website for SSL response", zap.Error(err), zap.String("domain", domain))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	if website == nil {
		apiErr := NewError(pluginEvents.ErrWebsiteNotFound, fmt.Errorf("website not found: %s", domain))
		return ctx.Error(apiErr, http.StatusNotFound)
	}

	resp := &dto.WebsiteResponse{}
	resp.GatewayDomain = a.gatewayDomain()
	primary, perr := a.websiteService.GetApexDomainBinding(reqCtx, website.ID)
	if perr == nil && primary != nil {
		resp.SetPrimaryDomain(primary)
		resp.SetSubdomainInfo(a.zoneDomain(reqCtx, primary.ZoneID))
	} else {
		// Fall back to the requested domain when the primary binding can't be
		// resolved; the lookup was itself by domain string.
		resp.Domain = domain
		resp.SetSubdomainInfo("")
	}
	resp.SetValidationRecordInfo(a.verificationTokenKey())
	resp.EnrichActiveCID(ipnsKeyCIDResolver{svc: a.ipnsKeyService, ctx: reqCtx}, website.UserID, website)
	a.applyApexSSLStatus(reqCtx, resp, website)
	return httputil.EncodeResponse(ctx, website, resp)
}
