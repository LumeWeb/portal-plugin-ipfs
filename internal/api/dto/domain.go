package dto

import (
	"encoding/json"

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

// DNSDelegationRecord is a single DNS record in a domain delegation. The fields
// mirror the provider Record type (hns_provider.go) so clients can render a
// generic table regardless of namespace.
type DNSDelegationRecord struct {
	Type    string `json:"type"` // NS|GLUE4|GLUE6|SYNTH4|SYNTH6|DS|TLSA
	Value   string `json:"value,omitempty"`
	NS      string `json:"ns,omitempty"`
	Address string `json:"address,omitempty"`
}

// DNSDelegation carries the namespace-specific records a user must publish to
// complete domain delegation.
//
// Parent records (NS + optional GLUE/SYNTH + DS) are published in the parent
// namespace — for HNS that is the HNS wallet/resource; for ICANN it is the
// registrar. Authoritative records (NS + TLSA) are configured on the
// authoritative DNS server. Nameservers is the ICANN shortcut. Clients render
// their own guidance from the namespace, mode, and record data that the server
// returns.
type DNSDelegation struct {
	Mode                 string                `json:"mode,omitempty"`
	Nameservers          []string              `json:"nameservers,omitempty"`
	DS                   string                `json:"ds,omitempty"`
	ParentRecords        []DNSDelegationRecord `json:"parent_records,omitempty"`
	AuthoritativeRecords []DNSDelegationRecord `json:"authoritative_records,omitempty"`
}

// DomainResponse is a bound domain.
type DomainResponse struct {
	ID          uint           `json:"id"`
	Domain      string         `json:"domain"`
	Namespace   string         `json:"namespace"`
	Status      string         `json:"status,omitempty"`
	ZoneName    string         `json:"zone_name,omitempty"`
	GatewayHost string         `json:"gateway_host,omitempty"`
	Delegation  *DNSDelegation `json:"delegation,omitempty"`
	// Per-domain SSL certificate state. SSL is a per-domain property, so it is
	// exposed on the domain record (not the website).
	SSL *SSLStatusInfo `json:"ssl,omitempty"`
}

func (r *DomainResponse) FromModel(m *db.WebsiteDomain) error {
	r.ID = m.ID
	r.Domain = m.Domain
	r.Namespace = string(m.Namespace)
	r.Status = string(m.Status)
	r.ZoneName = m.ZoneName
	r.GatewayHost = m.GatewayHost

	if m.SSLStatus != "" {
		r.SSL = &SSLStatusInfo{
			Status: m.SSLStatus,
			Error:  m.SSLError,
		}
		if m.SSLIssuedAt != nil {
			v := *m.SSLIssuedAt
			r.SSL.IssuedAt = &v
		}
		if m.SSLLastUpdatedAt != nil {
			v := *m.SSLLastUpdatedAt
			r.SSL.LastUpdatedAt = &v
		}
	}

	// Project the heterogeneous, provider-emitted DelegationData into the typed,
	// provider-agnostic shape. If it fails to map, leave Delegation nil rather
	// than erroring the whole domain response — the core fields are still valid.
	if len(m.DelegationData) > 0 {
		raw, _ := json.Marshal(m.DelegationData)
		if len(raw) > 0 {
			r.Delegation = mapDNSDelegation(raw)
		}
	}
	return nil
}

// DomainDANERepublishResponse is the result of forcing a DANE republish for a
// bound domain. It carries the (reloaded) domain state plus the TLSA record and
// owner name that were (re)published into the managed authoritative zone.
//
// NOTE: it deliberately duplicates the DomainResponse scalar fields rather than
// embedding DomainResponse, because the gswagger schema generator rejects Go
// embedded structs (potential infinite recursion).
type DomainDANERepublishResponse struct {
	ID          uint           `json:"id"`
	Domain      string         `json:"domain"`
	Namespace   string         `json:"namespace"`
	Status      string         `json:"status,omitempty"`
	ZoneName    string         `json:"zone_name,omitempty"`
	GatewayHost string         `json:"gateway_host,omitempty"`
	Delegation  *DNSDelegation `json:"delegation,omitempty"`
	SSL         *SSLStatusInfo `json:"ssl,omitempty"`
	TLSARecord  string         `json:"tlsa_record,omitempty"` // full "_443._tcp.<domain> ... TLSA ..." presentation
	OwnerName   string         `json:"owner_name,omitempty"`  // "_443._tcp.<domain>"
	TLSARData   string         `json:"tlsa_rdata,omitempty"`  // "3 1 1 <hex>"
}

// FromModel populates the DomainResponse-backed fields from a WebsiteDomain.
func (r *DomainDANERepublishResponse) FromModel(m *db.WebsiteDomain) error {
	return r.DomainResponseFromModel(m)
}

// DomainResponseFromModel copies the shared domain fields. It is defined so the
// scalar fields line up with DomainResponse.FromModel without embedding.
func (r *DomainDANERepublishResponse) DomainResponseFromModel(m *db.WebsiteDomain) error {
	r.ID = m.ID
	r.Domain = m.Domain
	r.Namespace = string(m.Namespace)
	r.Status = string(m.Status)
	r.ZoneName = m.ZoneName
	r.GatewayHost = m.GatewayHost
	if m.SSLStatus != "" {
		r.SSL = &SSLStatusInfo{
			Status: m.SSLStatus,
			Error:  m.SSLError,
		}
		if m.SSLIssuedAt != nil {
			v := *m.SSLIssuedAt
			r.SSL.IssuedAt = &v
		}
		if m.SSLLastUpdatedAt != nil {
			v := *m.SSLLastUpdatedAt
			r.SSL.LastUpdatedAt = &v
		}
	}
	if len(m.DelegationData) > 0 {
		raw, _ := json.Marshal(m.DelegationData)
		if len(raw) > 0 {
			r.Delegation = mapDNSDelegation(raw)
		}
	}
	return nil
}

// mapDNSDelegation converts the raw provider DelegationData (JSON) into the
// typed DNSDelegation. HNS emits a DelegationBundle with parent_records and
// authoritative_records; ICANN emits nameservers. It returns nil when the
// payload cannot be interpreted.
func mapDNSDelegation(raw []byte) *DNSDelegation {
	if len(raw) == 0 {
		return nil
	}

	// Try the HNS DelegationBundle shape first.
	var hns struct {
		Mode                 string                `json:"mode"`
		ParentRecords        []DNSDelegationRecord `json:"parent_records"`
		AuthoritativeRecords []DNSDelegationRecord `json:"authoritative_records"`
	}
	if err := json.Unmarshal(raw, &hns); err == nil &&
		(hns.Mode != "" || len(hns.ParentRecords) > 0 || len(hns.AuthoritativeRecords) > 0) {

		d := &DNSDelegation{
			Mode:                 hns.Mode,
			ParentRecords:        hns.ParentRecords,
			AuthoritativeRecords: hns.AuthoritativeRecords,
		}
		// The DS is NOT promoted from stored parent_records: the DS is a
		// derivative of the live PowerDNS signing key and is computed on the
		// fly (see API.domainDNSRequirements -> GetActiveDNSSECDS), never
		// persisted. A stored DS would go stale on key rotation.
		return d
	}

	// Fall back to the ICANN shape.
	var icann struct {
		Nameservers []string `json:"nameservers"`
	}
	if err := json.Unmarshal(raw, &icann); err == nil && len(icann.Nameservers) > 0 {
		return &DNSDelegation{
			Nameservers: icann.Nameservers,
		}
	}

	return nil
}

// DomainListResponse is a swagger-only DTO that represents the paginated response for domains.
// It merges the generic queryutil.Response[dto.DomainResponse] for OpenAPI documentation.
//
// This struct exists due to a TODO bug where queryutil.Response generics are not getting detected
// properly as an array type in the swagger documentation generation. By providing a concrete struct,
// we ensure the swagger docs correctly show the data field as an array of DomainResponse items.
//
// Note: This struct is only used for swagger documentation, not for actual encoding.
type DomainListResponse struct {
	Data  []DomainResponse `json:"data"`
	Total int64            `json:"total"`
}

var _ httputil.DTOValidator = (*DomainRequest)(nil)
var _ httputil.DTORequest[*DomainRequest] = (*DomainRequest)(nil)
var _ httputil.DTOResponse[*db.WebsiteDomain] = (*DomainResponse)(nil)
