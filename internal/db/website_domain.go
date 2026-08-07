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
	ZoneID         uint `gorm:"index"` // FK to the delegation/zone record (alt-root/delegated delegation)
	Status         DomainStatus
	DelegationData datatypes.JSONMap `gorm:"type:json"`
	ProtocolData   datatypes.JSONMap `gorm:"type:json"`

	// DNS hosting is a per-domain property. DNSHostingEnabled, DNSZoneID, and
	// IPNSKeyID own the PowerDNS hosting lifecycle for this binding, having
	// moved off the owning Website (which now only references this domain via
	// Website.PrimaryDomainID). DNSZoneID is the PowerDNS hosting zone; keep it
	// distinct from ZoneID (the delegation/alt-root zone above).
	DNSHostingEnabled bool  `gorm:"column:dns_hosting_enabled;default:false"`
	DNSZoneID         *uint `gorm:"column:dns_zone_id;index:idx_website_domains_dns_zone_id"`
	IPNSKeyID         *uint `gorm:"column:ipns_key_id;index:idx_website_domains_ipns_key_id"`

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

// BeforeSave normalizes the bound domain to its canonical apex form on every
// write so a www.-prefixed hostname can never be persisted.
func (wd *WebsiteDomain) BeforeSave(_ *gorm.DB) error {
	wd.Domain = NormalizeDomain(wd.Domain)
	if wd.SSLStatus == "" {
		wd.SSLStatus = string(SSLStatusPending)
	}
	return nil
}
