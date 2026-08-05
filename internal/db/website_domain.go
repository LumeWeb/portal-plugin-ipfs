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

	// TLSPrivateKey holds the encrypted per-domain DANE private key. It is the
	// durable source of truth: DANE TLSA (selector 1) pins the SPKI derived from
	// this key, so keeping it stable across Caddy restarts/renewals means the
	// published TLSA record never needs to change. It is AES-256-GCM encrypted
	// at rest and must never appear in logs or API responses except via the
	// authenticated internal channel.
	TLSPrivateKey string `gorm:"type:text" json:"-"`
	// TLSCertPEM caches the last issued certificate for this key. It is NOT a
	// source of truth — a cert may be freely re-issued from the same private
	// key (identical SPKI) without touching DNS.
	TLSCertPEM string `gorm:"type:text" json:"-"`

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
	return nil
}
