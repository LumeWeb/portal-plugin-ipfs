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
	Domain    string `json:"domain"`
	Namespace string `json:"namespace"`
	CertPEM   string `json:"cert_pem"`
}

func (r CertPushRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Domain":    zog.String().Required().Min(1).Max(255),
		"Namespace": zog.String().Required().OneOf([]string{string(db.DomainNamespaceICANN), string(db.DomainNamespaceHNS)}),
		"CertPEM":   zog.String().Required().Min(1),
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

var _ httputil.DTOValidator = (*TLSAUpdateRequest)(nil)
var _ httputil.DTORequest[*TLSAUpdateRequest] = (*TLSAUpdateRequest)(nil)
var _ httputil.DTOValidator = (*CertPushRequest)(nil)
var _ httputil.DTORequest[*CertPushRequest] = (*CertPushRequest)(nil)

func (r *CertPushResponse) FromModel(m CertPushResponse) error {
	*r = m
	return nil
}

var _ httputil.DTOResponse[CertPushResponse] = (*CertPushResponse)(nil)
