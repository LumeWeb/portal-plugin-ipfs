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
	ID             uint              `gorm:"primaryKey"`
	WebsiteID      uint
	UserID         uint
	Domain         string `gorm:"index:idx_domain_namespace,unique"`
	Namespace      DomainNamespace `gorm:"index:idx_domain_namespace,unique"`
	ZoneName       string
	GatewayHost    string
	ZoneID         uint `gorm:"index"`
	Status         DomainStatus
	DelegationData datatypes.JSONMap `gorm:"type:json"`
	ProtocolData   datatypes.JSONMap `gorm:"type:json"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeletedAt      gorm.DeletedAt `gorm:"index"`
}

func (WebsiteDomain) TableName() string {
	return "website_domains"
}

// BeforeSave normalizes the bound domain to its canonical apex form on every
// write so a www.-prefixed hostname can never be persisted.
func (wd *WebsiteDomain) BeforeSave(_ *gorm.DB) error {
	wd.Domain = NormalizeDomain(wd.Domain)
	return nil
}
