package db

import (
	"time"

	"gorm.io/gorm"
)

// PlatformDomain is a platform-owned DNS root that users may claim subdomains
// on for their websites (e.g. "platform.test"). It exists purely as the trust
// anchor for the one-zone relaxation in
// DelegatedDomainService.resolveManagedZone: a subdomain whose parent matches a
// registered platform root is allowed to reuse the platform-owned DNSZone even
// though the binding's UserID differs from the zone owner.
//
// The registry is small by design: registration is a privileged, operator-only
// action (see the Admin API), so every platform subdomain provably descends
// from an operator-vouched root. Policy fields (label rules, reserved names,
// etc.) are deliberately absent — they live at the API layer until a real
// per-root variance requirement exists. Enabled gates whether new subdomains
// can be claimed on the root (a soft deprecation flag).
// DeletedAt is a soft-delete tombstone: toggling registration state (disable
// vs delete) is an operator action, and soft delete keeps existing
// website_domains.platform_domain_id references valid (no dangling hard-delete
// orphans). The unique key includes deleted_at so a soft-deleted root can later
// be re-registered without colliding with its own tombstone (MySQL/SQLite both
// permit multiple NULLs, so the live row is still unique per domain+namespace).
type PlatformDomain struct {
	ID        uint           `gorm:"primaryKey"`
	Domain    string         `gorm:"not null;uniqueIndex:idx_platform_domains_domain_namespace"`
	Namespace DomainNamespace `gorm:"not null;uniqueIndex:idx_platform_domains_domain_namespace"`
	ZoneID    uint           `gorm:"not null;index:idx_platform_domains_zone_id"` // platform-owned DNSZone, auto-created with the root
	// No `default:true` gorm tag: GORM applies a default tag to zero-value
	// fields on Create, which would silently persist an explicit Enabled=false
	// as true (disabling a root would never stick). The migration keeps a DB
	// DEFAULT 1 for raw inserts; the service always writes the explicit value.
	Enabled   bool
	DeletedAt gorm.DeletedAt `gorm:"uniqueIndex:idx_platform_domains_domain_namespace"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (PlatformDomain) TableName() string {
	return "platform_domains"
}
