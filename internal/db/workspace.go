package db

import (
	"time"

	"gorm.io/gorm"
)

// WorkspaceStatus represents the lifecycle state of a workspace.
//
// The runtime (e.g. WordPress) is deliberately not encoded here: workspace
// entities, statuses, services, and API names must stay runtime-agnostic so a
// later runtime does not require renaming them.
type WorkspaceStatus string

const (
	WorkspaceStatusProvisioning WorkspaceStatus = "provisioning"
	WorkspaceStatusReady        WorkspaceStatus = "ready"
	WorkspaceStatusSuspended    WorkspaceStatus = "suspended"
	WorkspaceStatusFailed       WorkspaceStatus = "failed"
	WorkspaceStatusDeleting     WorkspaceStatus = "deleting"
)

// Workspace is a persistent, resource-limited authoring environment,
// provisioned on a runtime provider (Coolify). Building in a Workspace is
// separate from publishing: a Workspace may exist without any Website record
// or domain, so WebsiteID is a nullable pointer.
//
// Live-uniqueness resolution (gorm.Model + soft delete): Workspace embeds
// gorm.Model, so rows are soft-deleted via deleted_at rather than physically
// removed. We keep the unique keys STRICT (they do not include deleted_at),
// matching the established convention documented in the platform_domains
// migration and the website/website_domain unique keys:
//
//   - website_id        -> UNIQUE nullable: one live workspace per ATTACHED
//     website, DB-enforced. NULL values are distinct inside a SQL UNIQUE
//     index (both MySQL and SQLite), so any number of unattached workspaces
//     (WebsiteID NULL) may coexist while an attached website may be claimed
//     by at most one live workspace.
//   - (platform_domain_id, label) -> UNIQUE: one hostname per platform root.
//   - application_resource_id -> UNIQUE nullable, so a provider application
//     UUID is never attached to two workspaces.
//
// There is deliberately NO per-workspace database resource ID. Every workspace
// shares ONE Coolify-managed MySQL/MariaDB resource (configured in
// WorkspaceConfig.Database.ResourceID). The portal provisions only a logical
// database + user for each workspace on that shared server; the shared
// resource's remote ID is never stored here and never passed to the runtime.
//
// Widening these keys with deleted_at is intentionally avoided. In both MySQL
// and SQLite NULL values are distinct inside a UNIQUE index, so a
// (website_id, deleted_at) key would let multiple live rows coexist for the
// same website and defeat the duplicate-key race detection the create path
// relies on. The cost is that a soft-deleted workspace still occupies its
// strict keys until the row is purged. The delete path therefore purges the
// tombstone (hard-delete with deleted_at IS NOT NULL via Unscoped) for the
// reclaimed website_id / label / provider-ID keys before any re-provision —
// the same tombstone-purge-before-insert contract CreatePlatformDomain uses.
//
// Ownership is stored on the Workspace itself (UserID) and is the
// authoritative owner for the workspace lifecycle. WebsiteID is only the
// optional publication link set by attach/publish; it is never required for
// ownership. The application resource ID and the API key ID are nullable
// because provisioning crosses system boundaries and each identifier is
// recorded as soon as the external system creates it.
type Workspace struct {
	gorm.Model

	// UserID is the authoritative owner of the workspace. It is required for
	// every workspace, attached or not, and drives ownership checks for
	// Get/List/Suspend/Resume/Delete/Access.
	UserID uint `gorm:"not null;index"`
	// WebsiteID is the optional Website the workspace is attached to (publish
	// link). NULL means the workspace is unattached and can exist without any
	// Website record or domain. Non-NULL must be unique across live
	// workspaces (one workspace per attached website).
	WebsiteID        *uint  `gorm:"uniqueIndex"`
	PlatformDomainID uint   `gorm:"not null;uniqueIndex:idx_workspaces_platform_domain_label"`
	Label            string `gorm:"type:varchar(63);not null;uniqueIndex:idx_workspaces_platform_domain_label"`

	// DatabaseName and DatabaseUser are the LOGICAL database and user
	// provisioned on the shared MySQL/MariaDB resource for this workspace.
	// They are stored to (a) generate the deterministic logical identifiers
	// across reconcile passes and (b) drop exactly this workspace's logical
	// database+user on deletion. The database PASSWORD is never stored: it is
	// deterministically derived with HKDF-SHA256 keyed by the portal identity
	// key (Core.Identity.PrivateKey) plus this workspace's random salt
	// (DatabasePasswordSalt), so no remote DB credential is persisted at rest
	// or injected from storage. The per-workspace salt allows an individual
	// workspace's derived password to be rotated independently (regenerate the
	// salt) without touching the identity key or other workspaces.
	DatabaseName *string `gorm:"type:varchar(64)"`
	DatabaseUser *string `gorm:"type:varchar(64)"`

	// DatabasePasswordSalt is the some random per-workspace salt (encoded
	// base64) prepended into the HKDF derivation for this workspace's logical
	// database password. It is generated and persisted lazily at first
	// provisioning, so it is never operator-configured. It is NOT a password
	// and carries no secrecy by itself; only the salt + identity key together
	// derive the (never-persisted) password.
	DatabasePasswordSalt *string `gorm:"type:varchar(255)"`

	ApplicationResourceID *string `gorm:"type:varchar(64);uniqueIndex"`
	APIKeyID              *uint   `gorm:"index"`

	// ProxyUsername and ProxyPassword are the workspace's reverse-proxy Basic
	// Auth credentials, generated at application creation and kept separate
	// from the portal API key. They authenticate the portal's own access to
	// the workspace (the /access endpoint) and must persist across a service
	// restart. They are never the database password, which is held in memory
	// only. Readiness is decided from the Coolify application status, not from
	// an HTTP probe of the workspace URL, so these credentials are not used
	// for any health/readiness check.
	ProxyUsername *string `gorm:"type:varchar(255)"`
	ProxyPassword *string `gorm:"type:varchar(255)"`

	Status    WorkspaceStatus `gorm:"type:varchar(32);not null;index"`
	LastError string          `gorm:"type:text"`

	// RetryCount is how many consecutive reconcile failures a workspace has
	// accumulated. It feeds bounded exponential backoff for transient failures
	// so a pathological workspace cannot hot-loop the reconciler.
	RetryCount int `gorm:"not null;default:0"`
	// NextRetryAt gates when a failed workspace becomes eligible for a retry.
	// It is set to a backoff time in the future on failure; the reconciler only
	// re-selects failed workspaces whose NextRetryAt has elapsed (or is nil).
	NextRetryAt *time.Time `gorm:"index"`
	// LastReconcileAt records when the reconciler last processed the workspace,
	// used to decide when a ready/suspended workspace needs a drift check.
	LastReconcileAt *time.Time `gorm:"index"`

	// PlatformDomain is the root domain the workspace's authoring hostname is
	// derived from; loaded eagerly only when needed for the hostname.
	PlatformDomain PlatformDomain `gorm:"foreignKey:PlatformDomainID"`
}

func (Workspace) TableName() string {
	return "workspaces"
}

// AccessCredentials is the owner-authorized view of a workspace's proxy Basic
// Auth credential. It deliberately never carries the portal API key or the
// database password: those are runtime-internal secrets and must not be
// exposed through the access route. It lives in the db package (rather than
// core or the api dto) so both the exported core interface and the api dto can
// reference it without an import cycle (core already imports this package).
type AccessCredentials struct {
	Username string
	Password string
}

// Hostname returns the workspace's authoring hostname formed from its label
// and the linked platform domain. The label must already be normalized to a
// valid DNS label by the caller before persisting.
func (w *Workspace) Hostname() string {
	return w.Label + "." + w.PlatformDomain.Domain
}
