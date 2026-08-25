package dto

import (
	"encoding/json"

	"github.com/Oudwins/zog"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// DomainRequest binds a domain to a website.
//
// Two mutually exclusive shapes are supported:
//   - A user-owned domain: set Domain + Namespace.
//   - A platform subdomain (free subdomain under an operator-owned root): set
//     PlatformDomain (the root, e.g. "pinned.site") plus exactly one of Label
//     (an explicit subdomain label) or Generate (true — let the platform choose
//     a computed label). The namespace and DNS hosting are derived from the
//     platform root.
type DomainRequest struct {
	Domain    string `json:"domain"`
	Namespace string `json:"namespace"` // e.g. "icann", "hns"

	// Platform subdomain claim (mutually exclusive with Domain/Namespace).
	PlatformDomain string `json:"platform_domain,omitempty"`
	// PlatformNamespace optionally disambiguates which alt-root namespace to
	// claim under when the same root is registered under multiple namespaces
	// (e.g. both ICANN and HNS). Empty means "the single registered/unique
	// namespace"; an error is raised if the root is ambiguous.
	PlatformNamespace string `json:"platform_namespace,omitempty"`
	Label             string `json:"label,omitempty"`
	Generate          bool   `json:"generate,omitempty"`

	// DNSHostingEnabled controls whether the portal manages DNS for this
	// binding. Managed-by-default: when omitted (nil), the binding is created
	// DNS-hosted, matching the creation of its authoritative zone.
	DNSHostingEnabled *bool          `json:"dns_hosting_enabled,omitempty"`
	Config            map[string]any `json:"config,omitempty"`
}

func (r DomainRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Domain":            zog.String().Min(1).Max(255),
		"Namespace":         zog.String().OneOf([]string{string(db.DomainNamespaceICANN), string(db.DomainNamespaceHNS)}),
		"PlatformDomain":    zog.String().Min(1).Max(255),
		"PlatformNamespace": zog.String().OneOf([]string{string(db.DomainNamespaceICANN), string(db.DomainNamespaceHNS)}),
		"Label":             zog.String().Min(1).Max(63),
		"Generate":          zog.Bool(),
		"DNSHostingEnabled": zog.Ptr(zog.Bool()),
		// Config is intentionally unvalidated here; the namespace provider validates its contents.
	})
}

// IsPlatformClaim reports whether this request claims a platform subdomain
// rather than a user-owned domain.
func (r DomainRequest) IsPlatformClaim() bool {
	return r.PlatformDomain != ""
}

func (r *DomainRequest) ToModel() (*DomainRequest, error) {
	return r, nil
}

// DomainUpdateRequest updates a bound domain's per-domain DNS control:
// whether the portal manages DNS hosting for this binding (dns_hosting_enabled)
// and whether it is the website's primary (apex) binding. Either field is
// optional; omitted fields are left unchanged.
type DomainUpdateRequest struct {
	DNSHostingEnabled *bool `json:"dns_hosting_enabled,omitempty"`
	Primary           *bool `json:"primary,omitempty"`
}

func (r DomainUpdateRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"DNSHostingEnabled": zog.Ptr(zog.Bool()),
		"Primary":           zog.Ptr(zog.Bool()),
	})
}

func (r *DomainUpdateRequest) ToModel() (*DomainUpdateRequest, error) {
	return r, nil
}

func (r DomainUpdateRequest) HasUpdates() bool {
	return r.DNSHostingEnabled != nil || r.Primary != nil
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

// removeStoredDS returns the records with any DS entry stripped out. A DS is
// a derivative of the live PowerDNS signing key, never something that should
// be served from persisted delegation data (stored DS goes stale on key
// rotation). It is stripped at read time so no endpoint ever surfaces a
// leftover/stale DS from the DB; dns-requirements injects the live one.
func removeStoredDS(records []DNSDelegationRecord) []DNSDelegationRecord {
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
	ParentRecords        []DNSDelegationRecord `json:"parent_records,omitempty"`
	AuthoritativeRecords []DNSDelegationRecord `json:"authoritative_records,omitempty"`
	// DNSSEC reports the portal-managed DNSSEC signing state for a
	// DNSSEC-signed namespace (e.g. HNS): "enabled" when the zone has an
	// active signing key (the DS injected into parent_records derives from
	// it), "disabled" when the zone is not DNSSEC-signed (ICANN or no key),
	// and "error" when enablement/readback failed (details in DNSSECError).
	// Populated by dns-requirements for portal-managed namespaces so an
	// absent DS is never silent. ICANN namespaces omit it (no portal DNSSEC).
	DNSSEC      string `json:"dnssec,omitempty"`
	DNSSECError string `json:"dnssec_error,omitempty"`
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
	// DNSHostingEnabled reports whether the portal manages DNS for this
	// specific domain binding (the per-domain DNS hosting flag).
	DNSHostingEnabled bool `json:"dns_hosting_enabled"`
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
	r.DNSHostingEnabled = m.DNSHostingEnabled

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
			ParentRecords:        removeStoredDS(hns.ParentRecords),
			AuthoritativeRecords: hns.AuthoritativeRecords,
		}
		// The DS is carried as a parent_records entry, not as a first-class
		// field: it is a derivative of the live PowerDNS signing key computed
		// on the fly (API.domainDNSRequirements -> GetActiveDNSSECDS). Any DS
		// persisted in the stored delegation data is a stale snapshot that went
		// out of sync on key rotation, so it is stripped here and never served
		// from storage — the live DS is injected fresh by dns-requirements.
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
