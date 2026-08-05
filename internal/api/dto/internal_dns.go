package dto

import (
	"github.com/Oudwins/zog"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

type TLSAUpdateRequest struct {
	Domain    string `json:"domain"`
	Namespace string `json:"namespace"`
	TLSA      string `json:"tlsa"`
	CertPEM   string `json:"cert_pem"`
}

func (r TLSAUpdateRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Domain":    zog.String().Required().Min(1).Max(255),
		"Namespace": zog.String().Required().OneOf([]string{string(db.DomainNamespaceICANN), string(db.DomainNamespaceHNS)}),
		"TLSA":      zog.String().Required().Min(1),
		"CertPEM":   zog.String().Required().Min(1),
	})
}

func (r *TLSAUpdateRequest) ToModel() (*TLSAUpdateRequest, error) {
	return r, nil
}

type CertPushRequest struct {
	Domain        string `json:"domain"`
	Namespace     string `json:"namespace"`
	CertPEM       string `json:"cert_pem"`
	PrivateKeyPEM string `json:"private_key_pem"`
}

func (r CertPushRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Domain":         zog.String().Required().Min(1).Max(255),
		"Namespace":      zog.String().Required().OneOf([]string{string(db.DomainNamespaceICANN), string(db.DomainNamespaceHNS)}),
		"CertPEM":        zog.String().Required().Min(1),
		"PrivateKeyPEM":  zog.String().Optional(),
	})
}

func (r *CertPushRequest) ToModel() (*CertPushRequest, error) {
	return r, nil
}

type CertPushResponse struct {
	OK        bool   `json:"ok"`
	TLSA      string `json:"tlsa"`
	OwnerName string `json:"owner_name"`
}

// CertGetResponse is the response for GET /internal/dns/cert/{domain}. It
// returns the stored DANE key material so a Caddy cert getter can re-issue a
// certificate around the persisted key (stable SPKI). The private key is
// returned only over the authenticated internal gateway channel.
type CertGetResponse struct {
	OK            bool   `json:"ok"`
	Domain        string `json:"domain"`
	Namespace     string `json:"namespace"`
	PrivateKeyPEM string `json:"private_key_pem"`
	CertPEM       string `json:"cert_pem"`
	TLSA          string `json:"tlsa"`
	OwnerName     string `json:"owner_name"`
}

var _ httputil.DTOResponse[CertGetResponse] = (*CertGetResponse)(nil)

func (r *CertGetResponse) FromModel(m CertGetResponse) error {
	*r = m
	return nil
}

var _ httputil.DTOValidator = (*TLSAUpdateRequest)(nil)
var _ httputil.DTORequest[*TLSAUpdateRequest] = (*TLSAUpdateRequest)(nil)
var _ httputil.DTOValidator = (*CertPushRequest)(nil)
var _ httputil.DTORequest[*CertPushRequest] = (*CertPushRequest)(nil)

func (r *CertPushResponse) FromModel(m CertPushResponse) error {
	*r = m
	return nil
}

var _ httputil.DTOResponse[CertPushResponse] = (*CertPushResponse)(nil)
