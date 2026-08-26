package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pluginEvents "go.lumeweb.com/portal-plugin-ipfs/internal/errors"
	pluginservice "go.lumeweb.com/portal-plugin-ipfs/internal/service/website"
	core "go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
	queryUtilHttp "go.lumeweb.com/queryutil/http"
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

	if errors.Is(err, pluginservice.ErrCIDNotPinned) {
		apiErr := NewError(ErrKeyCIDNotPinned, err)
		return ctx.Error(apiErr, apiErr.HttpStatus()), true
	}

	if errors.Is(err, pluginservice.ErrIPNSKeyNotFound) {
		apiErr := NewError(ErrKeyIPNSKeyNotFound, err)
		return ctx.Error(apiErr, apiErr.HttpStatus()), true
	}

	return err, false
}

// rejectPlatformRootDomain returns an API error (for ctx.Error) when domain is
// the apex of an enabled platform root that an end user must not claim directly,
// or nil if it may be bound. It returns nil when the delegated domain service is
// unavailable so callers keep their own nil-service handling. Shared by the
// website-create, website-update, and domain-create paths.
func (a *API) rejectPlatformRootDomain(ctx context.Context, domain string) *core.Error {
	if a.delegatedDomainSvc == nil {
		return nil
	}
	isRoot, rerr := a.delegatedDomainSvc.IsPlatformRootDomain(ctx, domain)
	if rerr != nil {
		return NewError(ErrKeyInvalidRequest, rerr)
	}
	if isRoot {
		return NewError(ErrKeyInvalidRequest,
			fmt.Errorf("domain %q is a platform root and cannot be claimed directly; request a subdomain under it instead", domain))
	}
	return nil
}

// rejectDNSDisableForPlatformSubdomain returns an API error (for ctx.Error) when
// an attempt is made to turn DNS hosting OFF on a platform subdomain, whose DNS
// is forced on because its records live in the operator's shared zone. It
// returns nil when the binding is not a platform subdomain or DNS is being
// enabled or left unchanged. Shared by the website-update and domain-update
// paths.
func (a *API) rejectDNSDisableForPlatformSubdomain(binding *pluginDb.WebsiteDomain, dnsEnabled bool) *core.Error {
	if binding.PlatformDomainID != nil && !dnsEnabled {
		return NewError(ErrKeyDNSHostingReadOnly, fmt.Errorf("DNS hosting is read-only for platform subdomains"))
	}
	return nil
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

	// Domain ownership guard: a given (domain, namespace) can be live-bound to
	// only one website. CreateDomain is create-only and would otherwise surface
	// a raw MySQL 1062 duplicate-key as a 500 (and, because the website is
	// persisted below before the binding, leave a dangling website row behind).
	// Mirror the update path: refuse up front with a 409 ownership conflict
	// instead. Soft-deleted tombstones still occupy the unique key but are
	// purged by CreateDomain, so they fall through to a fresh binding.
	namespace := string(pluginDb.DomainNamespaceICANN)
	if req.Namespace != nil {
		namespace = string(*req.Namespace)
	}

	// A user-owned domain and a platform-subdomain claim are mutually exclusive
	// destinations. Supplying both is ambiguous, so reject it rather than
	// silently ignoring one and persisting a website bound to the wrong target.
	if req.Domain != "" && req.IsPlatformClaim() {
		apiErr := NewError(ErrKeyInvalidRequest, fmt.Errorf("supply a custom domain OR claim a platform subdomain, not both"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	// A website needs a destination: either a user-owned domain (non-empty
	// Domain) or an explicit platform-subdomain claim. An empty domain with no
	// platform claim would otherwise persist an orphan website with no binding,
	// so reject it before CreateWebsite.
	if !req.IsPlatformClaim() && req.Domain == "" {
		apiErr := NewError(ErrKeyInvalidRequest, fmt.Errorf("a domain is required unless claiming a platform subdomain"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	// A domain or platform-subdomain claim can only be bound when the delegated
	// domain service is available; otherwise the website row would persist with
	// no primary domain (an orphan). Reject up front, matching the update and
	// domain-create flows. Every check below relies on the delegated domain
	// service, so it is guaranteed non-nil from here on.
	if a.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service unavailable"))
		return ctx.Error(apiErr, http.StatusInternalServerError)
	}

	// Platform root apex guard: the apex of a platform root (e.g. "pinned.site")
	// is operator-owned and must never be claimed by an end user as a custom
	// domain. A request that names a platform root as its primary domain must go
	// through the platform-subdomain claim flow (or omit the domain so a
	// subdomain is minted); otherwise the site would silently sit on the
	// operator's apex.
	if apiErr := a.rejectPlatformRootDomain(reqCtx, req.Domain); apiErr != nil {
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	// Normalize before the lookup: CreateDomain persists the canonical apex
	// form, so a www.-prefixed or mixed-case request for an already
	// live-bound domain must still hit the ownership guard (otherwise the
	// raw 1062 duplicate-key 500 this guard replaces would surface).
	domain := pluginDb.NormalizeDomain(req.Domain)
	existing, eerr := a.delegatedDomainSvc.GetWebsiteDomainByDomainAndNamespace(reqCtx, domain, pluginDb.DomainNamespace(namespace))
	switch {
	case eerr == nil && !existing.DeletedAt.Valid:
		// Domain is live-bound to another website — refuse to rebind.
		a.Logger().Warn("Refusing to bind domain owned by another website",
			zap.String("domain", req.Domain), zap.Uint("domain_owner_website_id", existing.WebsiteID))
		apiErr := NewError(ErrKeyDomainInUse, fmt.Errorf("domain %q is already in use by another website", req.Domain))
		return ctx.Error(apiErr, http.StatusConflict)
	case eerr == nil:
		// Soft-deleted tombstone: fall through so CreateDomain purges it.
	default:
		if !errors.Is(eerr, gorm.ErrRecordNotFound) {
			a.Logger().Error("Failed to look up existing domain binding", zap.String("domain", req.Domain), zap.Error(eerr))
			apiErr := NewError(ErrKeyFileProcessingFailed, eerr)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
	}

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
	if err := a.bindPrimaryDomain(ctx, reqCtx, user, website.ID, req, namespace, dnsEnabled); err != nil {
		return err
	}

	// Check if website is broken and return 410 Gone
	if website.Status == string(pluginDb.WebsiteStatusBroken) {
		return ctx.Error(fmt.Errorf("website target is broken"), http.StatusGone)
	}

	resp, err := a.websiteResponse(reqCtx, website, user, nil)
	if err != nil {
		a.Logger().Error("Failed to convert website to response", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	ctx.Response().Before(func() {
		ctx.Response().Status = http.StatusCreated
	})

	return httputil.EncodeResponse(ctx, website, &resp)
}

// bindPrimaryDomain attaches the website's primary domain binding after the row
// is persisted: either a free platform-subdomain claim or a user-owned custom
// domain. On any failure it rolls back the just-created website (which has no
// domain binding yet), leaving no orphan row behind.
func (a *API) bindPrimaryDomain(ctx httputil.RequestContext, reqCtx context.Context, user, websiteID uint, req dto.WebsiteRequest, namespace string, dnsEnabled bool) error {
	if req.IsPlatformClaim() {
		return a.claimPlatformSubdomain(ctx, reqCtx, user, websiteID, req, dnsEnabled)
	}
	return a.bindCustomDomain(ctx, reqCtx, user, websiteID, req, namespace, dnsEnabled)
}

// claimPlatformSubdomain claims a free subdomain under an operator-owned
// platform root (mirrors the domain-bind flow). Platform subdomains are created
// active and DNS-hosted, so an explicit dns_hosting_enabled=false is
// contradictory and rejected here. Failures are user-correctable (label taken,
// reserved, disabled) and rolled back before surfacing.
func (a *API) claimPlatformSubdomain(ctx httputil.RequestContext, reqCtx context.Context, user, websiteID uint, req dto.WebsiteRequest, dnsEnabled bool) error {
	if !dnsEnabled {
		return a.rollbackAndFail(ctx, reqCtx, user, websiteID, ErrKeyInvalidRequest, fmt.Errorf("DNS hosting cannot be disabled for a platform subdomain"))
	}
	var platformNS pluginDb.DomainNamespace
	if req.PlatformNamespace != "" {
		platformNS = pluginDb.DomainNamespace(req.PlatformNamespace)
	}
	pd, perr := a.resolvePlatformDomainForClaim(reqCtx, req, platformNS)
	if perr != nil {
		a.Logger().Error("Failed to resolve platform domain", zap.String("platform_domain", req.PlatformDomain), zap.Error(perr))
		return a.rollbackAndFail(ctx, reqCtx, user, websiteID, ErrKeyInvalidRequest, perr)
	}
	if pd == nil {
		return a.rollbackAndFail(ctx, reqCtx, user, websiteID, ErrKeyInvalidRequest, fmt.Errorf("no enabled platform domain configured; specify platform_domain or label"))
	}
	if _, cerr := a.delegatedDomainSvc.CreatePlatformSubdomain(reqCtx, websiteID, user, pd.ID, req.Label, req.Generate); cerr != nil {
		if isDuplicateKeyError(cerr) || strings.Contains(cerr.Error(), "already taken") {
			a.Logger().Warn("Platform subdomain already claimed",
				zap.String("platform_domain", req.PlatformDomain), zap.String("label", req.Label), zap.Error(cerr))
			return a.rollbackAndFail(ctx, reqCtx, user, websiteID, ErrKeyDomainInUse, cerr)
		}
		a.Logger().Error("Failed to create platform subdomain for website",
			zap.Uint("website_id", websiteID), zap.String("platform_domain", req.PlatformDomain), zap.Error(cerr))
		return a.rollbackAndFail(ctx, reqCtx, user, websiteID, ErrKeyPlatformSubdomainRequired, cerr)
	}
	return nil
}

// resolvePlatformDomainForClaim returns the platform root under which a
// subdomain is claimed. An explicit PlatformDomain is honored via the standard
// lookup; when omitted (the auto-mint path — generate/label with no root), it
// falls back to the single enabled platform root matching the namespace so a
// bare `generate: true` request can mint a free subdomain. It returns
// (nil, nil) when no enabled root applies, so callers surface a clear message.
func (a *API) resolvePlatformDomainForClaim(ctx context.Context, req dto.WebsiteRequest, platformNS pluginDb.DomainNamespace) (*pluginDb.PlatformDomain, error) {
	if req.PlatformDomain != "" {
		return a.delegatedDomainSvc.GetEnabledPlatformDomain(ctx, req.PlatformDomain, platformNS)
	}
	domains, _, err := a.delegatedDomainSvc.ListEnabledPlatformDomains(ctx, queryutil.LargePagination)
	if err != nil {
		return nil, err
	}
	var candidates []*pluginDb.PlatformDomain
	for _, pd := range domains {
		if platformNS != "" && pd.Namespace != platformNS {
			continue
		}
		candidates = append(candidates, pd)
	}
	switch {
	case len(candidates) == 0:
		return nil, nil
	case len(candidates) > 1:
		return nil, fmt.Errorf("multiple enabled platform domains; specify platform_domain")
	default:
		return candidates[0], nil
	}
}

// bindCustomDomain attaches a user-owned custom domain as the primary binding
// and enables per-domain DNS hosting (default true) as the legacy website-level
// dns_hosting_enabled did.
func (a *API) bindCustomDomain(ctx httputil.RequestContext, reqCtx context.Context, user, websiteID uint, req dto.WebsiteRequest, namespace string, dnsEnabled bool) error {
	var cfgRaw json.RawMessage
	if _, err := a.delegatedDomainSvc.CreateDomain(reqCtx, namespace, req.Domain, websiteID, user, dnsEnabled, true, cfgRaw, nil); err != nil {
		// A plain domain that actually sits under an operator-owned platform
		// root must be claimed via the explicit platform shape. Surface a
		// precise 422 instead of the misleading 500 file-processing fallback.
		if strings.Contains(err.Error(), "must be claimed via the platform subdomain flow") {
			a.Logger().Warn("Requested domain is a platform subdomain but not claimed via the platform shape",
				zap.String("domain", req.Domain), zap.Uint("website_id", websiteID))
			return a.rollbackAndFail(ctx, reqCtx, user, websiteID, ErrKeyPlatformSubdomainRequired,
				fmt.Errorf("domain %q must be claimed via the platform subdomain shape (platform_domain + label/generate)", req.Domain))
		}
		// A concurrent create may have won the (domain, namespace) unique key
		// race after this request's pre-check passed. The guard is not atomic,
		// so on a duplicate-key violation roll back the just-persisted website
		// so a clean 409 is surfaced with no dangling row left behind.
		if isDuplicateKeyError(err) {
			a.Logger().Warn("Refusing to bind domain raced/owned by another website",
				zap.String("domain", req.Domain), zap.Uint("website_id", websiteID))
			return a.rollbackAndFail(ctx, reqCtx, user, websiteID, ErrKeyDomainInUse,
				fmt.Errorf("domain %q is already in use by another website", req.Domain))
		}
		a.Logger().Error("Failed to create primary domain for website",
			zap.Uint("website_id", websiteID), zap.String("domain", req.Domain), zap.Error(err))
		return a.rollbackAndFail(ctx, reqCtx, user, websiteID, ErrKeyFileProcessingFailed, err)
	}
	// Enable per-domain DNS hosting (default true) so the binding is set up for
	// DNS as the legacy website-level dns_hosting_enabled did. The website now
	// has a binding, so a failure here returns the error without rolling back.
	if a.websiteService != nil {
		primary, perr := a.websiteService.GetApexDomainBinding(reqCtx, websiteID)
		if perr == nil && primary != nil {
			if _, derr := a.websiteService.SetDomainDNSEnabled(reqCtx, user, websiteID, primary.ID, dnsEnabled); derr != nil {
				a.Logger().Error("failed to set DNS hosting on primary domain",
					zap.Uint("domain_id", primary.ID), zap.Error(derr))
				apiErr := NewError(ErrKeyFileProcessingFailed, derr)
				return ctx.Error(apiErr, apiErr.HttpStatus())
			}
		}
	}
	return nil
}

// rollbackAndFail rolls back a just-created website after its primary domain
// bind failed (it has no domain binding, so its row is removed best-effort) and
// writes the translated API error. The error key is passed explicitly so each
// call site keeps its intent.
func (a *API) rollbackAndFail(ctx httputil.RequestContext, reqCtx context.Context, user, websiteID uint, key core.ErrorType, err error) error {
	a.rollbackWebsite(reqCtx, user, websiteID)
	apiErr := NewError(key, err)
	return ctx.Error(apiErr, apiErr.HttpStatus())
}

// rollbackWebsite removes a just-created website row after its primary domain
// bind failed. The website has no primary domain binding and would otherwise
// linger as an orphan, so it is rolled back best-effort (any error is logged,
// not returned). Delegates to the website service's delete so DB access stays
// in the service layer.
func (a *API) rollbackWebsite(ctx context.Context, userID uint, websiteID uint) {
	if a.websiteService == nil {
		return
	}
	if derr := a.websiteService.DeleteWebsite(ctx, userID, websiteID); derr != nil {
		a.Logger().Error("Failed to roll back website after domain bind failure",
			zap.Uint("website_id", websiteID), zap.Error(derr))
	}
}

// isDuplicateKeyError reports whether err is a database unique-key (duplicate)
// violation. GORM only returns gorm.ErrDuplicatedKey when gorm.Config{
// TranslateError:true} is set — this deployment does not enable it — so on
// MySQL a duplicate surfaces as a raw *mysql.MySQLError with code 1062 that
// errors.Is cannot match. Mirror the portal core detection (see
// go.lumeweb.com/portal/service/user.go isDuplicateKeyError): check the GORM
// sentinel, the structured MySQL error, and fall back to driver-agnostic
// string matching.
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr != nil && mysqlErr.Number == 1062 {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "Duplicate entry") ||
		strings.Contains(msg, "duplicate key value")
}

// websiteResponse builds a WebsiteResponse for the given website, resolving its
// primary (apex) domain binding (or accepting a pre-resolved one) and enriching
// it with the gateway, subdomain, validation-record, and active-CID context used
// across the create/update/get/list handlers.
func (a *API) websiteResponse(ctx context.Context, website *pluginDb.Website, user uint, primary *pluginDb.WebsiteDomain) (dto.WebsiteResponse, error) {
	var resp dto.WebsiteResponse
	if err := resp.FromModel(website); err != nil {
		return resp, err
	}
	if primary == nil {
		p, perr := a.websiteService.GetApexDomainBinding(ctx, website.ID)
		if perr != nil {
			a.Logger().Warn("website has no primary domain binding", zap.Uint("website_id", website.ID), zap.Error(perr))
		}
		primary = p
	}
	resp.SetPrimaryDomain(primary)
	resp.GatewayDomain = a.gatewayDomain()
	if primary != nil {
		resp.SetSubdomainInfo(a.zoneDomain(ctx, primary.ZoneID))
	} else {
		resp.SetSubdomainInfo("")
	}
	resp.SetValidationRecordInfo(a.verificationTokenKey())
	resp.EnrichActiveCID(ipnsKeyCIDResolver{svc: a.ipnsKeyService, ctx: ctx}, user, website)
	return resp, nil
}

// sslWebsiteResponse builds a WebsiteResponse for an SSL-status handler, where
// the website is looked up by domain string. When the primary binding cannot be
// resolved the response falls back to showing the requested domain. Shared by
// the SSL-status getter and the cert webhook.
func (a *API) sslWebsiteResponse(ctx context.Context, website *pluginDb.Website, requestedDomain string) *dto.WebsiteResponse {
	resp := &dto.WebsiteResponse{}
	resp.GatewayDomain = a.gatewayDomain()
	primary, perr := a.websiteService.GetApexDomainBinding(ctx, website.ID)
	if perr == nil && primary != nil {
		resp.SetPrimaryDomain(primary)
		resp.SetSubdomainInfo(a.zoneDomain(ctx, primary.ZoneID))
	} else {
		resp.Domain = requestedDomain
		resp.SetSubdomainInfo("")
	}
	resp.SetValidationRecordInfo(a.verificationTokenKey())
	resp.EnrichActiveCID(ipnsKeyCIDResolver{svc: a.ipnsKeyService, ctx: ctx}, website.UserID, website)
	return resp
}

func (a *API) listWebsites(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	return queryUtilHttp.ProcessListRequest[*dto.WebsiteItem, dto.WebsiteItem](
		c.Response(),
		c.Request(),
		"websites",
		func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*dto.WebsiteItem, int64, error) {
			// The website service always scopes queries to the authenticated
			// user, so drop any client-supplied user_id filter to prevent
			// conflicting access conditions.
			scopedFilters := make([]queryutil.CrudFilter, 0, len(filters))
			for _, f := range filters {
				if f.GetField() != "user_id" {
					scopedFilters = append(scopedFilters, f)
				}
			}

			websites, total, err := a.websiteService.ListWebsites(reqCtx, user, scopedFilters, sorts, pagination)
			if err != nil {
				return nil, 0, err
			}

			items := make([]*dto.WebsiteItem, len(websites))
			for i, website := range websites {
				resp, err := a.websiteResponse(reqCtx, website, user, nil)
				if err != nil {
					return nil, 0, err
				}
				item := dto.WebsiteItem(resp)
				items[i] = &item
			}

			return items, total, nil
		},
		func(item *dto.WebsiteItem) dto.WebsiteItem {
			return *item
		},
	)
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
		// A broken or soft-deleted website is returned with 410 Gone so the
		// client can distinguish it from a 404.
		resp, err := a.websiteResponse(reqCtx, website, user, nil)
		if err == nil {
			ctx.Response().Before(func() {
				ctx.Response().Status = http.StatusGone
			})
			return httputil.EncodeResponse(ctx, website, &resp)
		}
	}

	resp, err := a.websiteResponse(reqCtx, website, user, nil)
	if err != nil {
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

	// Platform root apex guard: an end user must never bind a platform root
	// apex (e.g. "pinned.site") as a website's primary domain via update, just
	// as they cannot create one. Only reject genuine NEW claims (no live binding
	// already owned by this website) before mutating, so a refused domain change
	// cannot leave a partial update behind; an apex the website already owns
	// (e.g. an operator apex-bound site) still reaches the reuse branch below.
	if req.Domain != nil && a.delegatedDomainSvc != nil {
		namespace := string(pluginDb.DomainNamespaceICANN)
		if req.Namespace != nil {
			namespace = string(*req.Namespace)
		}
		existing, eerr := a.delegatedDomainSvc.GetWebsiteDomainByDomainAndNamespace(reqCtx, pluginDb.NormalizeDomain(*req.Domain), pluginDb.DomainNamespace(namespace))
		ownedReuse := eerr == nil && existing != nil && !existing.DeletedAt.Valid && existing.WebsiteID == uint(websiteID)
		if !ownedReuse {
			if apiErr := a.rejectPlatformRootDomain(reqCtx, *req.Domain); apiErr != nil {
				return ctx.Error(apiErr, apiErr.HttpStatus())
			}
		}
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
		// Surface user-correctable validation errors with specific messages
		// (e.g. CID is not pinned, invalid CID/IPNS target) instead of a
		// generic 500, mirroring the create-website path.
		if handledErr, wasHandled := a.handleWebsiteValidationError(err, c); wasHandled {
			return handledErr
		}
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Apply per-domain changes sourced from the website update request.
	var primary *pluginDb.WebsiteDomain

	// 1. Change the primary domain: create the requested domain binding and
	// make it the website's new primary (apex) domain. Idempotent — see
	// applyDomainChange for the reuse/conflict/create semantics.
	if req.Domain != nil {
		if a.delegatedDomainSvc == nil {
			return ctx.Error(NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service unavailable")), http.StatusInternalServerError)
		}
		namespace := string(pluginDb.DomainNamespaceICANN)
		if req.Namespace != nil {
			namespace = string(*req.Namespace)
		}
		var err error
		primary, err = a.applyDomainChange(ctx, reqCtx, user, website, req, namespace)
		if err != nil {
			return err
		}
	}

	// 2. Toggle DNS hosting on the primary domain binding. Resolved after any
	// domain change so the toggle applies to the current primary.
	if req.DNSEnabled != nil {
		var err error
		primary, err = a.toggleDomainDNS(ctx, reqCtx, user, website, req, primary)
		if err != nil {
			return err
		}
	}

	resp, err := a.websiteResponse(reqCtx, website, user, primary)
	if err != nil {
		a.Logger().Error("Failed to convert website to response", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return httputil.EncodeResponse(ctx, website, &resp)
}

// applyDomainChange repoints the website's primary (apex) domain binding to the
// requested domain. It is idempotent: an existing live binding already owned by
// this website is reused and re-promoted as primary (rather than re-created, to
// avoid tripping the (domain, namespace) unique key), a domain owned by another
// website is an ownership conflict (409), and anything else is a fresh binding
// created and promoted. Returns the new primary binding on success.
func (a *API) applyDomainChange(ctx httputil.RequestContext, reqCtx context.Context, user uint, website *pluginDb.Website, req dto.WebsiteUpdateRequest, namespace string) (*pluginDb.WebsiteDomain, error) {
	var cfgRaw json.RawMessage
	// Managed-DNS by default; explicit DNSEnabled override flows through.
	newDomainDNS := true
	if req.DNSEnabled != nil {
		newDomainDNS = *req.DNSEnabled
	}
	// The lookup compares the canonical apex form (lowercased, www.-stripped)
	// that CreateDomain stores, so a www.-prefixed or mixed-case request
	// resolves correctly.
	domain := pluginDb.NormalizeDomain(*req.Domain)
	existing, eerr := a.delegatedDomainSvc.GetWebsiteDomainByDomainAndNamespace(reqCtx, domain, pluginDb.DomainNamespace(namespace))
	switch {
	case eerr == nil && !existing.DeletedAt.Valid && existing.WebsiteID == website.ID:
		// Re-deploy to an already-bound primary: reuse, don't re-create. The
		// binding's DNS hosting state is preserved as-is; only an explicit
		// dns_hosting_enabled override (the toggle step) changes it. Silently
		// defaulting DNS hosting on here would re-provision a PowerDNS zone on
		// a binding the user deliberately self-hosted, defeating the idempotent
		// re-deploy guarantee.
		wd := existing
		rerp, derr := a.websiteService.SetPrimaryDomain(reqCtx, user, website.ID, wd.ID)
		if derr != nil {
			a.Logger().Error("Failed to set primary domain", zap.Uint("domain_id", wd.ID), zap.Error(derr))
			apiErr := NewError(ErrKeyFileProcessingFailed, derr)
			return nil, ctx.Error(apiErr, apiErr.HttpStatus())
		}
		return rerp, nil
	case eerr == nil:
		// Domain is live-bound to a different website — refuse to repoint.
		a.Logger().Warn("Refusing to bind domain owned by another website",
			zap.Uint("website_id", website.ID), zap.String("domain", *req.Domain), zap.Uint("domain_owner_website_id", existing.WebsiteID))
		apiErr := NewError(ErrKeyDomainInUse, fmt.Errorf("domain %q is already in use by another website", *req.Domain))
		return nil, ctx.Error(apiErr, http.StatusConflict)
	default:
		if !errors.Is(eerr, gorm.ErrRecordNotFound) {
			a.Logger().Error("Failed to look up existing domain binding", zap.String("domain", *req.Domain), zap.Error(eerr))
			apiErr := NewError(ErrKeyFileProcessingFailed, eerr)
			return nil, ctx.Error(apiErr, apiErr.HttpStatus())
		}
		wd, derr := a.delegatedDomainSvc.CreateDomain(reqCtx, namespace, *req.Domain, website.ID, user, newDomainDNS, false, cfgRaw, nil)
		if derr != nil {
			a.Logger().Error("Failed to create primary domain for website",
				zap.Uint("website_id", website.ID), zap.String("domain", *req.Domain), zap.Error(derr))
			apiErr := NewError(ErrKeyFileProcessingFailed, derr)
			return nil, ctx.Error(apiErr, apiErr.HttpStatus())
		}
		primary, derr := a.websiteService.SetPrimaryDomain(reqCtx, user, website.ID, wd.ID)
		if derr != nil {
			a.Logger().Error("Failed to set primary domain", zap.Uint("domain_id", wd.ID), zap.Error(derr))
			apiErr := NewError(ErrKeyFileProcessingFailed, derr)
			return nil, ctx.Error(apiErr, apiErr.HttpStatus())
		}
		return primary, nil
	}
}

// toggleDomainDNS applies an explicit dns_hosting_enabled override to the
// website's primary domain binding, resolving the primary first when no domain
// change happened this request. Platform subdomains have DNS hosting forced on,
// so attempts to disable it are rejected (records in the operator's shared zone
// must not be torn out). Returns the binding carrying the effective DNS state.
func (a *API) toggleDomainDNS(ctx httputil.RequestContext, reqCtx context.Context, user uint, website *pluginDb.Website, req dto.WebsiteUpdateRequest, primary *pluginDb.WebsiteDomain) (*pluginDb.WebsiteDomain, error) {
	if primary == nil {
		p, perr := a.websiteService.GetApexDomainBinding(reqCtx, website.ID)
		if perr != nil {
			a.Logger().Warn("cannot toggle DNS hosting: no primary domain binding", zap.Uint("website_id", website.ID), zap.Error(perr))
		} else {
			primary = p
		}
	}
	if primary == nil {
		return nil, nil
	}
	if apiErr := a.rejectDNSDisableForPlatformSubdomain(primary, *req.DNSEnabled); apiErr != nil {
		return nil, ctx.Error(apiErr, apiErr.HttpStatus())
	}
	updated, derr := a.websiteService.SetDomainDNSEnabled(reqCtx, user, website.ID, primary.ID, *req.DNSEnabled)
	if derr != nil {
		a.Logger().Error("failed to set DNS hosting on primary domain", zap.Uint("domain_id", primary.ID), zap.Error(derr))
		apiErr := NewError(ErrKeyFileProcessingFailed, derr)
		return nil, ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return updated, nil
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

// isDNSResolutionError reports whether err is (or wraps) a genuine
// domain-not-found resolution failure — *net.DNSError with IsNotFound set,
// e.g. net.LookupTXT returning NXDOMAIN because a verification record is
// missing or the domain does not resolve. Transient resolver faults (timeouts,
// SERVFAIL, connection failures) are platform-side and deliberately excluded
// so they stay on the generic 500 path.
func isDNSResolutionError(err error) bool {
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr) && dnsErr.IsNotFound
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
		// A DNS resolution failure (e.g. the verification TXT record not yet
		// published, or the domain not resolving) is user-correctable, so
		// surface it with a specific message instead of a generic 500.
		if isDNSResolutionError(err) {
			apiErr := NewError(ErrKeyDNSValidationFailed, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
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

	resp := a.sslWebsiteResponse(reqCtx, website, domain)
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

	resp := a.sslWebsiteResponse(reqCtx, website, domain)
	a.applyApexSSLStatus(reqCtx, resp, website)
	return httputil.EncodeResponse(ctx, website, resp)
}
