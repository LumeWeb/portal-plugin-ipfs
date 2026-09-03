package api

import (
	"context"
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
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
	domsvc "go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
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
		apiErr := NewError(ErrKeyInvalidPathID, errors.New("invalid website ID"))
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

	// Managed-DNS by default: bindings are created DNS-hosted unless the
	// request explicitly opts out. Pass the resolved flag so CreateDomain
	// provisions (or skips) the authoritative zone accordingly.
	dnsHostingEnabled := true
	if req.DNSHostingEnabled != nil {
		dnsHostingEnabled = *req.DNSHostingEnabled
	}

	var wd *pluginDb.WebsiteDomain

	if req.IsPlatformClaim() {
		// Free subdomain under an operator-owned platform root. The platform
		// root supplies the namespace and zone; DNS hosting is forced on (a
		// user cannot self-host DNS for a subdomain of someone else's zone).
		// The optional namespace disambiguates the root when it is registered
		// under more than one alt-root namespace.
		var platformNS pluginDb.DomainNamespace
		if req.PlatformNamespace != "" {
			platformNS = pluginDb.DomainNamespace(req.PlatformNamespace)
		}
		pd, perr := a.delegatedDomainSvc.GetEnabledPlatformDomain(reqCtx, req.PlatformDomain, platformNS)
		if perr != nil {
			a.Logger().Error("Failed to lookup platform domain", zap.Error(perr))
			apiErr := NewError(ErrKeyValidationFailed, perr)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		if pd == nil {
			apiErr := NewError(ErrKeyValidationFailed, fmt.Errorf("platform domain %q not found or disabled", req.PlatformDomain))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		wd, err = a.delegatedDomainSvc.CreatePlatformSubdomain(reqCtx, uint(websiteID), userID, pd.ID, req.Label, req.Generate, false)
	} else {
		// User-owned domain binding.
		if req.Domain == "" || req.Namespace == "" {
			apiErr := NewError(ErrKeyValidationFailed, fmt.Errorf("either domain+namespace or platform_domain must be provided"))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		// Platform root apex guard: mirror the website-create check. An end user
		// must never bind a platform root apex as a custom domain; that apex is
		// operator-owned and reachable only via the admin apex-binding flow.
		if apiErr := a.rejectPlatformRootDomain(reqCtx, req.Domain); apiErr != nil {
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		wd, err = a.delegatedDomainSvc.CreateDomain(reqCtx, req.Namespace, req.Domain, uint(websiteID), userID, dnsHostingEnabled, false, configRaw, nil)
	}

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

// updateDomain manages a bound domain's per-domain DNS control: toggling portal
// DNS hosting (dns_hosting_enabled) for this specific binding and/or making it
// the website's primary (apex) binding. This is the per-domain equivalent of
// what the website-level update endpoint used to do, so a site with several
// bound domains can manage each one's DNS state independently.
func (a *API) updateDomain(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	userID, err := mcontext.GetUserID(c)
	if err != nil {
		return ctx.Error(err, http.StatusUnauthorized)
	}

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidPathID, errors.New("invalid website ID"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	domainID, err := strconv.ParseUint(c.Param("domain_id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidPathID, errors.New("invalid domain ID"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	req := dto.DomainUpdateRequest{}
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	if !req.HasUpdates() {
		apiErr := NewError(ErrKeyInvalidRequest, errors.New("at least one of dns_hosting_enabled or primary must be provided"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Verify the binding exists and belongs to the user before applying
	// per-domain changes, so a bogus domain_id surfaces as 404 not 500.
	var existing pluginDb.WebsiteDomain
	if err := a.DB().WithContext(reqCtx).
		Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
		First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			apiErr := NewError(ErrKeyDomainNotFound, errors.New("domain not found"))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var primary *pluginDb.WebsiteDomain

	// Order matters when both fields are set. SetDomainDNSEnabled runs the
	// side-effect-heavy DNS transition (zone/IPNS/records) and only persists
	// dns_hosting_enabled after that transition fully succeeds, so a transition
	// failure returns an error with DNS state unchanged. SetPrimaryDomain is a
	// lightweight metadata write. Run the DNS transition first so a successful
	// primary promotion is never coupled to a transition that could have failed.
	// A failure of SetPrimaryDomain (a pure DB error) leaves DNS already enabled
	// but is idempotent and recoverable — the less severe reverse failure mode.
	if req.DNSHostingEnabled != nil {
		// Platform subdomains have DNS hosting forced on; reject attempts to
		// disable it so records in the operator's shared zone are not torn out.
		if apiErr := a.rejectDNSDisableForPlatformSubdomain(&existing, *req.DNSHostingEnabled); apiErr != nil {
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		d, derr := a.websiteService.SetDomainDNSEnabled(reqCtx, userID, uint(websiteID), uint(domainID), *req.DNSHostingEnabled)
		if derr != nil {
			a.Logger().Error("Failed to set domain DNS hosting", zap.Uint("domain_id", uint(domainID)), zap.Bool("enabled", *req.DNSHostingEnabled), zap.Error(derr))
			apiErr := NewError(ErrKeyFileProcessingFailed, derr)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		primary = d
	}

	// Promote to primary after DNS side effects have been confirmed applied.
	if req.Primary != nil {
		p, perr := a.websiteService.SetPrimaryDomain(reqCtx, userID, uint(websiteID), uint(domainID))
		if perr != nil {
			a.Logger().Error("Failed to set primary domain", zap.Uint("domain_id", uint(domainID)), zap.Error(perr))
			apiErr := NewError(ErrKeyFileProcessingFailed, perr)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		primary = p
	}

	// Reload the binding so the response reflects every applied change. Prefer
	// the binding returned by the service calls (which carry the post-transition
	// state); fall back to a fresh read if neither service path produced one.
	var result *pluginDb.WebsiteDomain
	if primary != nil {
		result = primary
	} else {
		var reloaded pluginDb.WebsiteDomain
		if err := a.DB().WithContext(reqCtx).
			Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
			First(&reloaded).Error; err != nil {
			apiErr := NewError(ErrKeyFileProcessingFailed, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		result = &reloaded
	}

	resp := dto.DomainResponse{}
	if err := resp.FromModel(result); err != nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return httputil.EncodeResponse(ctx, result, &resp)
}

func (a *API) listDomains(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidPathID, errors.New("invalid website ID"))
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
		apiErr := NewError(ErrKeyInvalidPathID, errors.New("invalid website ID"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	domainID, err := strconv.ParseUint(c.Param("domain_id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidPathID, errors.New("invalid domain ID"))
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
		apiErr := NewError(ErrKeyInvalidPathID, errors.New("invalid website ID"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	domainID, err := strconv.ParseUint(c.Param("domain_id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidPathID, errors.New("invalid domain ID"))
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

	res, err := a.delegatedDomainSvc.VerifyDomain(reqCtx, &wd)
	if err != nil {
		a.Logger().Error("Failed to verify domain", zap.Error(err))
		apiErr := NewError(ErrKeyValidationFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if res.State != domsvc.DelegationVerified {
		a.Logger().Info("delegation not verified",
			zap.String("domain", wd.Domain),
			zap.String("namespace", string(wd.Namespace)),
			zap.Uint("id", wd.ID),
			zap.Uint("zone_id", wd.ZoneID),
			zap.Uint8("state", uint8(res.State)),
			zap.Strings("approved_ns", res.ApprovedNS),
			zap.Strings("live_ns", res.LiveNS))
	}

	resp := dto.DomainResponse{}
	if err := resp.FromModel(&wd); err != nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return httputil.EncodeResponse(ctx, &wd, &resp)
}

// domainPathParams carries the request context and the authenticated user +
// website/domain path IDs shared by every /websites/:id/domains/:domain_id
// handler. When err is non-nil the handler must return it immediately: the
// 401/400 response has already been written.
type domainPathParams struct {
	ctx       httputil.RequestContext
	reqCtx    context.Context
	userID    uint
	websiteID uint
	domainID  uint
	err       error
}

// parseDomainPath resolves the authenticated user and the :id/:domain_id path
// parameters common to all per-domain handlers, writing the 401/400 response
// on failure (returned as params.err for the handler to return).
func (a *API) parseDomainPath(c echo.Context) domainPathParams {
	p := domainPathParams{ctx: httputil.Context(c)}
	p.reqCtx = p.ctx.Context.Request().Context()

	p.userID, p.err = mcontext.GetUserID(c)
	if p.err != nil {
		p.err = p.ctx.Error(p.err, http.StatusUnauthorized)
		return p
	}

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidPathID, errors.New("invalid website ID"))
		p.err = p.ctx.Error(apiErr, apiErr.HttpStatus())
		return p
	}
	domainID, err := strconv.ParseUint(c.Param("domain_id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidPathID, errors.New("invalid domain ID"))
		p.err = p.ctx.Error(apiErr, apiErr.HttpStatus())
		return p
	}
	p.websiteID = uint(websiteID)
	p.domainID = uint(domainID)
	return p
}

// loadDomainBinding loads the WebsiteDomain row for the authenticated user at
// the parsed path, writing the 404/500 response on failure (returned as a
// non-nil error the handler must return).
func (a *API) loadDomainBinding(p *domainPathParams) (pluginDb.WebsiteDomain, error) {
	var wd pluginDb.WebsiteDomain
	err := a.DB().WithContext(p.reqCtx).
		Where("id = ? AND website_id = ? AND user_id = ?", p.domainID, p.websiteID, p.userID).
		First(&wd).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			apiErr := NewError(ErrKeyDomainNotFound, errors.New("domain not found"))
			return wd, p.ctx.Error(apiErr, apiErr.HttpStatus())
		}
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return wd, p.ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return wd, nil
}

// domainDNSRequirements returns the DNS delegation requirements (DS/NS/GLUE/TLSA
// parent + authoritative records) for a bound domain so a client can render
// DNS/DNSSEC setup guidance after binding. It reuses the same typed
// DomainResponse as create/verify so clients share one renderer.
func (a *API) domainDNSRequirements(c echo.Context) error {
	p := a.parseDomainPath(c)
	if p.err != nil {
		return p.err
	}
	wd, err := a.loadDomainBinding(&p)
	if err != nil {
		return err
	}
	ctx, reqCtx := p.ctx, p.reqCtx

	resp := dto.DomainResponse{}
	if err := resp.FromModel(&wd); err != nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// The DS to publish is computed live from PowerDNS's current active signing
	// key rather than read from stored delegation data (which would go stale on
	// key rotation). Only portal-managed, DNSSEC-required namespaces (e.g. HNS)
	// yield a DS and report DNSSEC state; ICANN domains have no parent DS and
	// no portal DNSSEC, so this is a no-op that omits the DNSSEC fields
	// entirely (per the DTO contract) and only defends against a stale DS.
	// Gating on the DNSSEC policy (not the TLSA/DANE capability) ensures a
	// provider that requires DNSSEC but does not publish DANE still receives
	// the live DS in the response.
	// A self-hosted binding owns no delegation bundle, so resp.Delegation is
	// nil and this block is naturally skipped.
	managed := a.delegatedDomainSvc != nil && a.delegatedDomainSvc.NamespaceRequiresDNSSEC(string(wd.Namespace))
	if resp.Delegation != nil && a.dnsService != nil {
		if managed {
			ds, dsErr := a.dnsService.GetActiveDNSSECDS(reqCtx, wd.ZoneID)
			switch {
			case dsErr != nil:
				// PowerDNS unavailable or a key rollover is in progress: the
				// live DS cannot be resolved. Surface the error and drop any
				// stored DS so a stale value is never presented as current.
				a.Logger().Warn("could not resolve live DS for dns-requirements",
					zap.Uint("zone_id", wd.ZoneID), zap.Error(dsErr))
				resp.Delegation.DNSSEC = "error"
				resp.Delegation.DNSSECError = dsErr.Error()
				resp.Delegation.ParentRecords = removeDSRecord(resp.Delegation.ParentRecords)
			case ds != "":
				// Active signing key present: DNSSEC is enabled and the live
				// DS must be injected so CLI renderers show the current value.
				resp.Delegation.DNSSEC = "enabled"
				resp.Delegation.DNSSECError = ""
				resp.Delegation.ParentRecords = upsertDSRecord(resp.Delegation.ParentRecords, ds)
			default:
				// No active signing key (never enabled, or key rotated away).
				// Surface "disabled" so the user knows DNSSEC isn't set up
				// until a verify self-heal mints the key, not a bare missing DS.
				resp.Delegation.DNSSEC = "disabled"
				resp.Delegation.DNSSECError = "no active signing key - DNSSEC not enabled"
				resp.Delegation.ParentRecords = removeDSRecord(resp.Delegation.ParentRecords)
			}
		} else {
			// Not a portal-DNSSEC namespace (e.g. ICANN): omit the DNSSEC
			// fields and ensure no stale DS leaks into parent_records.
			resp.Delegation.DNSSEC = ""
			resp.Delegation.DNSSECError = ""
			resp.Delegation.ParentRecords = removeDSRecord(resp.Delegation.ParentRecords)
		}
	}

	// Encode the DTO directly rather than via EncodeResponse(model, dto):
	// EncodeResponse re-derives the DTO from the model (FromModel -> mapDNSDelegation
	// -> removeStoredDS), which would discard the live DS injected into
	// parent_records above. resp already carries the complete, final delegation.
	return ctx.JSON(http.StatusOK, &resp)
}

// upsertDSRecord returns parent records with the DS record set to `ds`,
// replacing any existing DS entry or appending a new one. A nil/empty slice is
// preserved as nil so ICANN-shaped delegation (no parent records) stays bare.
func upsertDSRecord(records []dto.DNSDelegationRecord, ds string) []dto.DNSDelegationRecord {
	if len(records) == 0 {
		return records
	}
	out := records
	replaced := false
	for i := range out {
		if out[i].Type == "DS" {
			out[i].Value = ds
			replaced = true
			break
		}
	}
	if !replaced {
		out = append(out, dto.DNSDelegationRecord{Type: "DS", Value: ds})
	}
	return out
}

// removeDSRecord returns parent records with any DS entry stripped out. Used
// when the live DS cannot be resolved (PowerDNS down, key rollover) so a
// stale stored DS is never presented as current. A nil/empty slice is
// preserved as nil.
func removeDSRecord(records []dto.DNSDelegationRecord) []dto.DNSDelegationRecord {
	if len(records) == 0 {
		return records
	}
	out := records[:0]
	for _, r := range records {
		if r.Type != "DS" {
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// republishDomainDANE forces re-publication of a bound domain's DANE records
// (the TLSA for a portal-managed, DNSSEC-signed zone) into the authoritative
// PowerDNS zone. It re-pushes the stored certificate/key through
// UpdateTLSAFromCert, which idempotently rewrites the _443._tcp.<domain> TLSA
// RRset (PowerDNS REPLACE). This is the operator's escape hatch for a TLSA
// that was deleted or went missing and won't be re-triggered by cert renewal.
func (a *API) republishDomainDANE(c echo.Context) error {
	p := a.parseDomainPath(c)
	if p.err != nil {
		return p.err
	}
	wd, err := a.loadDomainBinding(&p)
	if err != nil {
		return err
	}
	ctx, reqCtx := p.ctx, p.reqCtx

	if a.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	ns := string(wd.Namespace)
	// Only DANE-capable namespaces whose provider publishes a managed-zone TLSA
	// can be force-republished. Non-DANE namespaces (e.g. ICANN) have no TLSA.
	if !a.delegatedDomainSvc.NamespaceUsesManagedZoneTLSA(ns) {
		apiErr := NewError(ErrKeyNoStoredCertificate, fmt.Errorf("namespace %q does not use managed-zone DANE", ns))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// The republish writes the TLSA into the portal-managed authoritative zone
	// resolved by the domain's ZoneID. If no zone is assigned there is nothing
	// to publish into -- short-circuit rather than return a false success.
	if wd.ZoneID == 0 {
		apiErr := NewError(ErrKeyNoStoredCertificate, fmt.Errorf("domain %q has no assigned managed zone; cannot republish", wd.Domain))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Only portal-managed bindings may be force-republished: on-chain,
	// self-hosted, and unresolved bindings never authorize managed-zone TLSA
	// writes, and an inconsistent on-chain binding with a stray zone must not
	// be routed through the DANE republish path at all.
	if !wd.CanPublishManagedZoneRecords() {
		apiErr := NewError(ErrKeyNoStoredCertificate, fmt.Errorf("domain %q is not portal-managed; cannot republish DANE", wd.Domain))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	cert, err := a.delegatedDomainSvc.GetCertificateKey(reqCtx, ns, wd.Domain)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			apiErr := NewError(ErrKeyNoStoredCertificate, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		a.Logger().Error("Failed to load stored certificate for DANE republish", zap.Error(err), zap.String("domain", wd.Domain))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	tlsa, ownerName, err := a.delegatedDomainSvc.UpdateTLSAFromCert(reqCtx, ns, wd.Domain, cert.CertPEM, cert.PrivateKeyPEM)
	if err != nil {
		a.Logger().Error("Failed to republish DANE TLSA", zap.Error(err), zap.String("domain", wd.Domain))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Reload wd after the publish: UpdateTLSAFromCert rewrites the row's
	// DelegationData authoritative TLSA records in the DB, so the response must
	// project the post-republish delegation state rather than the stale one we
	// loaded before the publish.
	if err := a.DB().WithContext(reqCtx).First(&wd, wd.ID).Error; err != nil {
		a.Logger().Error("Failed to reload domain after DANE republish", zap.Error(err), zap.Uint("domain_id", wd.ID))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	resp := dto.DomainDANERepublishResponse{}
	if err := resp.FromModel(&wd); err != nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	resp.TLSARData = tlsa
	resp.OwnerName = ownerName
	if ownerName != "" && tlsa != "" {
		resp.TLSARecord = fmt.Sprintf("%s. 3600 IN TLSA %s", ownerName, tlsa)
	}
	return httputil.EncodeResponse(ctx, &wd, &resp)
}

// convertDomainToOnChain converts a bound domain to on-chain managed (HIP-5),
// the explicit one-way transition for an HNS name whose NS now points at an
// external contract. It is destructive to the portal's PowerDNS state for that
// binding (the managed zone is deleted) but deliberately keeps the domain's
// DANE/SSL state (ProtocolData), which still applies on-chain.
func (a *API) convertDomainToOnChain(c echo.Context) error {
	p := a.parseDomainPath(c)
	if p.err != nil {
		return p.err
	}
	ctx, reqCtx, userID := p.ctx, p.reqCtx, p.userID

	if a.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	wd, err := a.delegatedDomainSvc.ConvertToOnChain(reqCtx, p.websiteID, userID, p.domainID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			apiErr := NewError(ErrKeyDomainNotFound, errors.New("domain not found"))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		// User-correctable state conflicts (already on-chain, not yet on-chain,
		// shared zone) are 422. Anything else is an infrastructure failure
		// (DB/DNS service) and must be a 500, not a validation error.
		switch {
		case errors.Is(err, domain.ErrDomainAlreadyOnChain),
			errors.Is(err, domain.ErrDomainNotOnChain),
			errors.Is(err, domain.ErrDomainZoneShared):
			apiErr := NewError(ErrKeyInvalidRequest, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		default:
			a.Logger().Error("Failed to convert domain on-chain", zap.Error(err))
			apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to convert domain on-chain"))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
	}

	resp := dto.DomainResponse{}
	if err := resp.FromModel(wd); err != nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return ctx.JSON(http.StatusOK, &resp)
}

// checkPlatformDomainAvailability returns, for the given label, whether it is
// claimable on each enabled platform root. Auth-only (per-user rate limiting is
// enforced by the auth middleware); it only ever probes platform roots, never
// user-managed zones, so it reveals nothing about other users' bindings.
func (a *API) checkPlatformDomainAvailability(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	req := dto.PlatformAvailabilityRequest{}
	if _, ok := httputil.DecodeAndValidateQueryRequest(ctx, &req); !ok {
		return nil
	}

	if a.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	results, err := a.delegatedDomainSvc.CheckAvailability(reqCtx, req.Label)
	if err != nil {
		a.Logger().Error("Failed to check platform domain availability", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	resp := dto.PlatformAvailabilityResponse{Label: req.Label, Results: make([]dto.PlatformAvailabilityResult, 0, len(results))}
	for _, r := range results {
		resp.Results = append(resp.Results, dto.PlatformAvailabilityResult{
			PlatformDomain: r.PlatformDomain,
			Namespace:      string(r.Namespace),
			Available:      r.Available,
		})
	}
	return httputil.EncodeResponse(ctx, req, &resp)
}

// listPlatformDomains lists the supported (enabled) platform roots available
// for free-subdomain claims. User-facing, so only enabled roots are returned;
// disabled roots are excluded.
func (a *API) listPlatformDomains(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	if a.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return queryutilHttp.ProcessListRequest[*pluginDb.PlatformDomain, dto.PlatformDomainResponse](
		c.Response(),
		c.Request(),
		"platform-domains",
		func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.PlatformDomain, int64, error) {
			return a.delegatedDomainSvc.ListEnabledPlatformDomains(reqCtx, pagination)
		},
		func(pd *pluginDb.PlatformDomain) dto.PlatformDomainResponse {
			var r dto.PlatformDomainResponse
			_ = r.FromModel(pd)
			return r
		},
	)
}
