package dto

import (
	"github.com/Oudwins/zog"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// DomainRequest binds a domain to a website.
type DomainRequest struct {
	Domain    string         `json:"domain"`
	Namespace string         `json:"namespace"` // e.g. "icann", "hns"
	Config    map[string]any `json:"config,omitempty"`
}

func (r DomainRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Domain":    zog.String().Required().Min(1).Max(255),
		"Namespace": zog.String().Required().OneOf([]string{string(db.DomainNamespaceICANN), string(db.DomainNamespaceHNS)}),
		// Config is intentionally unvalidated here; the namespace provider validates its contents.
	})
}


func (r *DomainRequest) ToModel() (*DomainRequest, error) {
	return r, nil
}

// DomainResponse is a bound domain.
type DomainResponse struct {
	ID        uint   `json:"id"`
	Domain    string `json:"domain"`
	Namespace string `json:"namespace"`
	ZoneName  string `json:"zone_name,omitempty"`
}

func (r *DomainResponse) FromModel(m *db.WebsiteDomain) error {
	r.ID = m.ID
	r.Domain = m.Domain
	r.Namespace = string(m.Namespace)
	r.ZoneName = m.ZoneName
	return nil
}

var _ httputil.DTOValidator = (*DomainRequest)(nil)
var _ httputil.DTORequest[*DomainRequest] = (*DomainRequest)(nil)
var _ httputil.DTOResponse[*db.WebsiteDomain] = (*DomainResponse)(nil)
