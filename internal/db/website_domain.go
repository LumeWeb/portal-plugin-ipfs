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
)

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
func (wd *WebsiteDomain) DelegationRecordsOwned() bool {
	return wd.Namespace == DomainNamespaceHNS || wd.DelegationOwned()
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
