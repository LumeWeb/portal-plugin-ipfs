package db

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// DomainNamespace identifies the naming protocol.
type DomainNamespace string

const (
	DomainNamespaceICANN DomainNamespace = "icann"
	DomainNamespaceHNS   DomainNamespace = "hns"
)

// DomainStatus represents the lifecycle state of a delegated domain.
type DomainStatus string

const (
	DomainStatusDraft             DomainStatus = "draft"
	DomainStatusRecordsGenerated  DomainStatus = "records_generated"
	DomainStatusWaitingDelegation DomainStatus = "waiting_delegation"
	DomainStatusActive            DomainStatus = "active"
	DomainStatusSelfHosted        DomainStatus = "self_hosted"
	DomainStatusError             DomainStatus = "error"
	// DomainStatusOnchainManaged marks a name whose DNS is served on-chain
	// (e.g. a Handshake HIP-5 name whose NS record points at an external
	// contract). The portal provisions no zone, DNSSEC, or DANE for it; it owns
	// ownership verification only (TXT token resolved through the namespace
	// resolver). Distinct from self_hosted, which means the user runs their own
	// authoritative servers under their own delegation.
	DomainStatusOnchainManaged DomainStatus = "onchain_managed"
)

// DomainClass classifies a bound name by who serves its DNS. It is derived
// from persisted state (status + ZoneID), never stored. dns_hosting_enabled is
// excluded on purpose: when the flag disagrees with the zone reference it is a
// reconcile orphan that SetDomainDNSEnabled repairs, and classification must
// follow the same zone reference the rest of the code keys on.
//
// Lifecycle and hosting locus are separate concepts and must not be conflated.
// The lifecycle (DomainStatus) tracks draft/provisioning/waiting/active/error
// progress; the hosting locus (this class) answers who is authoritative for the
// name's DNS. A draft or error lifecycle status says nothing about hosting
// until provisioning either establishes a portal zone or records the user's
// explicit self-hosted decision, so a zone-less non-self-hosted binding is
// unresolved rather than self-hosted.
type DomainClass uint8

const (
	// ClassPortalManaged names served from a portal-created PowerDNS zone.
	// ZoneID != 0 is the authoritative marker: a non-on-chain binding with a
	// zone is portal-managed regardless of lifecycle status, including
	// delegation-owned HNS bindings whose DNSHostingEnabled flag is false.
	ClassPortalManaged DomainClass = iota
	// ClassSelfHosted names whose DNS the user runs themselves: status
	// self_hosted and no portal zone. The portal provisions no zone, so no
	// DS/TLSA/DNSSEC reconciliation applies. A stray non-zero ZoneID on a
	// self_hosted binding is classified portal-managed — the persisted zone
	// reference is authoritative and the status/flag disagreement is a
	// reconcile orphan, never a reason to treat the binding as unmanaged.
	ClassSelfHosted
	// ClassOnChainManaged marks a name whose DNS is served on-chain (e.g. a
	// Handshake HIP-5 name whose NS record points at an external contract).
	// The portal owns only the ownership check (TXT token through the
	// namespace resolver) and never provisions a zone, DNSSEC, or DANE.
	// This class takes precedence over a stray ZoneID: an on-chain binding
	// must never be treated as portal-managed no matter what zone reference
	// it carries.
	ClassOnChainManaged
	// ClassUnresolved names whose hosting locus is not yet determinable from
	// persisted state: draft, error, empty, or unknown lifecycle status with
	// no portal zone. This is deliberately NOT self-hosted — provisioning has
	// neither confirmed a portal zone nor recorded the user's explicit
	// self-hosted decision. No portal DNS side effects may be authorized for
	// an unresolved binding; it reconciles only through the explicit
	// enable/disable transition path.
	ClassUnresolved
)

// Class returns the binding's DNS-hosting locus. Precedence is fixed:
// on-chain managed status wins over any zone reference (a stray zone on an
// on-chain binding is data incoherence, never a portal authorization); a
// non-zero ZoneID marks a portal-managed binding; an explicit self_hosted
// status with no zone is self-hosted; everything else is unresolved.
// dns_hosting_enabled is deliberately not an input (see DomainClass).
func (wd *WebsiteDomain) Class() DomainClass {
	switch {
	case wd.Status == DomainStatusOnchainManaged:
		return ClassOnChainManaged
	case wd.ZoneID != 0:
		return ClassPortalManaged
	case wd.Status == DomainStatusSelfHosted:
		return ClassSelfHosted
	default:
		return ClassUnresolved
	}
}

// HasPortalAuthority reports whether the portal is authoritative for this
// binding's DNS: the binding references a managed PowerDNS zone (ZoneID != 0)
// and is not on-chain managed. All portal DNS reads/writes (managed records,
// delegation verification, DNSSEC, managed-zone TLSA) are gated on this.
func (wd *WebsiteDomain) HasPortalAuthority() bool {
	return wd.Class() == ClassPortalManaged
}

// NeedsDelegationVerification reports whether the portal should verify this
// binding's external delegation (NS/DS visibility) before treating it as
// active. Only portal-managed bindings have delegation for the portal to
// verify; self-hosted, on-chain, and unresolved bindings prove ownership by
// other means (hosting DNS / namespace TXT) or are not yet classifiable.
func (wd *WebsiteDomain) NeedsDelegationVerification() bool {
	return wd.Class() == ClassPortalManaged
}

// CanPublishManagedZoneRecords reports whether website/managed-DNS operations
// may create, update, or delete records in this binding's portal-managed zone.
// On-chain, self-hosted, and unresolved bindings can never authorize portal
// DNS writes — an inconsistent state (e.g. an on-chain binding carrying a
// stray zone ID) must not permit them merely because a zone reference exists.
func (wd *WebsiteDomain) CanPublishManagedZoneRecords() bool {
	return wd.Class() == ClassPortalManaged
}

// WebsiteDomain binds a domain (by namespace) to a website.
type WebsiteDomain struct {
	ID             uint `gorm:"primaryKey"`
	WebsiteID      uint
	UserID         uint
	Domain         string          `gorm:"index:idx_domain_namespace,unique"`
	Namespace      DomainNamespace `gorm:"index:idx_domain_namespace,unique"`
	ZoneName       string
	GatewayHost    string
	ZoneID         uint `gorm:"index"` // canonical reference to this binding's PowerDNS zone
	Status         DomainStatus
	DelegationData datatypes.JSONMap `gorm:"type:json"`
	ProtocolData   datatypes.JSONMap `gorm:"type:json"`

	// PlatformDomainID is set when this binding is a platform subdomain minted
	// under a registered PlatformDomain (see PlatformDomain). NULL for
	// user-owned apex/normal bindings. The binding's authoritative zone is the
	// PlatformDomain's zone (resolved via resolveManagedZone), owned by the
	// operator rather than the binding's UserID.
	PlatformDomainID *uint `gorm:"column:platform_domain_id;index:idx_website_domains_platform_domain_id"`

	// DNS hosting is a per-domain property, owning the PowerDNS hosting
	// lifecycle for this binding (having moved off the owning Website, which
	// now only references this domain via Website.PrimaryDomainID). The IPNS
	// key stays on the Website (it belongs to the site's target, not the
	// domain). ZoneID is the single canonical PowerDNS zone reference for the
	// binding — set by CreateDomain (via resolveManagedZone), or 0 when the
	// binding is self-hosted (user runs the authoritative server, no portal
	// zone).
	DNSHostingEnabled bool `gorm:"column:dns_hosting_enabled;default:false"`

	// SSL certificate state for this specific domain binding. SSL is a
	// per-hostname property (each bound domain may hold its own cert), so it
	// lives here rather than on the owning Website.
	SSLStatus        string     `gorm:"column:ssl_status;type:varchar(50);index:idx_website_domains_ssl_status;default:'pending'"`
	SSLError         string     `gorm:"column:ssl_error;type:text"`
	SSLIssuedAt      *time.Time `gorm:"column:ssl_issued_at;index:idx_website_domains_ssl_issued_at"`
	SSLLastUpdatedAt *time.Time `gorm:"column:ssl_last_updated_at;index:idx_website_domains_ssl_last_updated_at"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

func (WebsiteDomain) TableName() string {
	return "website_domains"
}

// DelegationRecordsOwned reports whether website lifecycle code must preserve
// shared DNSLink and apex records. HNS zones are DNSSEC-signed at the apex and
// their records are provisioned by the delegation path even if delegation_data
// or status is stale during a transition.
//
// On-chain managed (HIP-5) bindings are excluded: they own no portal-managed
// zone, so there is no shared record set to preserve even though the namespace
// is HNS.
func (wd *WebsiteDomain) DelegationRecordsOwned() bool {
	return wd.Class() != ClassOnChainManaged && (wd.Namespace == DomainNamespaceHNS || wd.DelegationOwned())
}

// DelegationOwned reports whether this binding's PowerDNS zone also hosts
// alt-root delegation (DNS/DS/DNSSEC for namespaces served from the managed
// zone). A binding that has progressed into the delegation lifecycle
// (delegation_data present, status past records_generated) owns its zone for
// delegation purposes, so website-DNS hosting teardown must NOT delete that
// zone or clear zone_id.
func (wd *WebsiteDomain) DelegationOwned() bool {
	if len(wd.DelegationData) == 0 {
		return false
	}
	switch wd.Status {
	case DomainStatusRecordsGenerated, DomainStatusWaitingDelegation, DomainStatusActive:
		return true
	default:
		return false
	}
}

// BeforeSave normalizes the bound domain to its canonical apex form on every
// write so a www.-prefixed hostname can never be persisted.
func (wd *WebsiteDomain) BeforeSave(_ *gorm.DB) error {
	wd.Domain = NormalizeDomain(wd.Domain)
	if wd.SSLStatus == "" {
		wd.SSLStatus = string(SSLStatusPending)
	}
	return nil
}
