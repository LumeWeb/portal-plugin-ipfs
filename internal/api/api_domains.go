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

// domainDNSRequirements returns the DNS delegation requirements (DS/NS/GLUE/TLSA
// parent + authoritative records) for a bound domain so a client can render
// DNS/DNSSEC setup guidance after binding. It reuses the same typed
// DomainResponse as create/verify so clients share one renderer.
func (a *API) domainDNSRequirements(c echo.Context) error {
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

	resp := dto.DomainResponse{}
	if err := resp.FromModel(&wd); err != nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// The DS to publish is computed live from PowerDNS's current active signing
	// key rather than read from stored delegation data (which would go stale on
	// key rotation). Only portal-managed, DNSSEC-signed namespaces (e.g. HNS)
	// yield a DS; ICANN domains have no parent DS and this is a no-op. The live
	// DS is injected into parent_records (which renderers draw from); there is
	// no separate DS field that could otherwise sit empty.
	if resp.Delegation != nil && a.dnsService != nil {
		if ds, dsErr := a.dnsService.GetActiveDNSSECDS(reqCtx, wd.ZoneID); dsErr == nil && ds != "" {
			// Ensure the rendered parent_records carries the live DS so CLI
			// renderers that draw from parent_records show the current value
			// (replace any stale stored DS entry, else append).
			resp.Delegation.ParentRecords = upsertDSRecord(resp.Delegation.ParentRecords, ds)
		}
	}

	return httputil.EncodeResponse(ctx, &wd, &resp)
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

// republishDomainDANE forces re-publication of a bound domain's DANE records
// (the TLSA for a portal-managed, DNSSEC-signed zone) into the authoritative
// PowerDNS zone. It re-pushes the stored certificate/key through
// UpdateTLSAFromCert, which idempotently rewrites the _443._tcp.<domain> TLSA
// RRset (PowerDNS REPLACE). This is the operator's escape hatch for a TLSA
// that was deleted or went missing and won't be re-triggered by cert renewal.
func (a *API) republishDomainDANE(c echo.Context) error {
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
