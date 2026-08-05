package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	dane "go.lumeweb.com/dane"
	danesvc "go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"gorm.io/gorm"
)


func (a *API) updateTLSA(c echo.Context) error {
	ctx := httputil.Context(c)

	req := dto.TLSAUpdateRequest{}
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	// The TLSA update endpoint carries no private key — key persistence is a
	// cert-push concern.
	return a.handleInternalTLSA(c, req.Namespace, req.Domain, req.CertPEM, "")
}

func (a *API) pushCert(c echo.Context) error {
	ctx := httputil.Context(c)

	req := dto.CertPushRequest{}
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	return a.handleInternalTLSA(c, req.Namespace, req.Domain, req.CertPEM, req.PrivateKeyPEM)
}

// getCert returns the stored DANE key material for a domain so Caddy can
// re-issue a certificate around the persisted key (stable SPKI). Returns 404
// when no identity is persisted yet (first bootstrap).
func (a *API) getCert(c echo.Context) error {
	ctx := httputil.Context(c)

	domain := c.Param("domain")
	if domain == "" {
		apiErr := NewError(ErrKeyInvalidPathID, fmt.Errorf("missing domain"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	namespace := c.QueryParam("namespace")
	if namespace == "" {
		namespace = string(pluginDb.DomainNamespaceHNS)
	}

	if a.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	stored, err := a.delegatedDomainSvc.GetCertificateKey(ctx.Request().Context(), namespace, domain)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		apiErr := NewError(ErrKeyDomainNotFound, nil)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	if err != nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	resp := dto.CertGetResponse{
		OK:            true,
		Domain:        domain,
		Namespace:     namespace,
		PrivateKeyPEM: stored.PrivateKeyPEM,
		CertPEM:       stored.CertPEM,
		TLSA:          stored.TLSA,
		OwnerName:     stored.OwnerName,
	}
	return httputil.EncodeResponse(ctx, resp, &resp)
}

// handleInternalTLSA handles the common logic for both /internal/dns/tlsa
// and cert push endpoints. It requires delegatedDomainSvc (returns error if
// not present — this is a DANE/alt-root feature endpoint).
func (a *API) handleInternalTLSA(c echo.Context, namespace, domain, certPEM, privateKeyPEM string) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	if a.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	tlsa, ownerName, err := a.delegatedDomainSvc.UpdateTLSAFromCert(reqCtx, namespace, domain, certPEM, privateKeyPEM)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Domain not bound yet: compute TLSA best-effort for Caddy.
			hash, cerr := dane.ComputeTLSAFromCert(certPEM)
			if cerr != nil {
				return ctx.Error(cerr, http.StatusBadRequest)
			}
			tlsa = danesvc.TLSAHashPrefix() + hash
			ownerName = dane.TLSAOwnerName(domain, danesvc.DaneTLSAPort, danesvc.DaneTLSATransport)
		} else if strings.Contains(err.Error(), "compute tlsa") {
			// Cert parsing failure — client error, not server error.
			return ctx.Error(err, http.StatusBadRequest)
		} else {
			return ctx.Error(err, http.StatusInternalServerError)
		}
	}

	resp := dto.CertPushResponse{
		OK:        true,
		TLSA:      tlsa,
		OwnerName: ownerName,
	}
	return httputil.EncodeResponse(ctx, resp, &resp)
}


