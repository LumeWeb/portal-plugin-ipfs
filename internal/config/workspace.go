package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"go.lumeweb.com/portal/config"
)

// WorkspaceConfig is the service configuration for workspace provisioning.
//
// The Workspace domain model stays runtime-agnostic: the runtime (e.g.
// WordPress image, its port, or its database environment key names) is encoded
// here, not on the entity. This keeps the service generic so a later runtime
// does not require renaming entities, statuses, or API routes.
//
// Secrets (the Coolify API token) live in Provider.APIToken and must never be
// returned from an API or serialized in logs. Only the configured value is
// treated as authoritative; the struct itself has no String()/Marshal output
// and callers must not log it.
type WorkspaceConfig struct {
	// Enabled turns workspace provisioning on or off. When false, all other
	// fields are ignored and startup validation passes trivially.
	Enabled bool `config:"enabled"`

	// ReconcileInterval is how often the cron reconciler advances or checks
	// workspaces. The registered cron job runs on this cadence.
	ReconcileInterval time.Duration `config:"reconcile_interval"`
	// ReconcileBatchSize bounds how many workspaces a single reconciler pass
	// selects and processes, so a backlog does not starve a run.
	ReconcileBatchSize int `config:"reconcile_batch_size"`
	// RetryMaxAttempts is the number of reconcile attempts for a workspace
	// within one pass before a transient (timeout/429/5xx) failure is marked
	// for a later retry.
	RetryMaxAttempts int `config:"retry_max_attempts"`
	// RetryInitialDelay is the base exponential-backoff delay between reconcile
	// retry attempts.
	RetryInitialDelay time.Duration `config:"retry_initial_delay"`
	// RetryMaxDelay caps the exponential backoff delay for retries.
	RetryMaxDelay time.Duration `config:"retry_max_delay"`
	// RetryTotalLimit is the cross-pass retry budget: how many times a
	// workspace's transient reconcile failure may reschedule itself (via
	// next_retry_at) before the row stops being retried and strands in
	// `failed` until an operator resets it. Counts across passes via the
	// workspace's retry_count, which resets only on a successful reconcile.
	RetryTotalLimit int `config:"retry_total_limit"`
	// DriftCheckInterval is how often ready/suspended workspaces are verified
	// against the provider (their persisted resource IDs are re-checked; a
	// missing resource is drift and is never recreated automatically).
	DriftCheckInterval time.Duration `config:"drift_check_interval"`
	// RequestTimeout bounds an individual provider request.
	RequestTimeout time.Duration `config:"request_timeout"`
	// ProvisionTimeout is the overall budget for provisioning a workspace.
	ProvisionTimeout time.Duration `config:"provision_timeout"`
	// PollInterval is how long to wait between provider status polls.
	PollInterval time.Duration `config:"poll_interval"`

	// Provider holds Coolify placement/token configuration. The portal
	// API URL and platform domain are NOT configured here: the API URL is
	// derived at runtime from the portal core config (this plugin's API
	// subdomain on the core domain/secure/port), and the workspace hostname
	// root is selected DB-driven from the single enabled platform domain.
	Coolify WorkspaceCoolifyConfig `config:"coolify"`
	// Runtime holds the selected application image and its runtime settings.
	Runtime WorkspaceRuntimeConfig `config:"runtime"`
	// Database holds the dedicated database resource settings.
	Database WorkspaceDatabaseConfig `config:"database"`
}

// WorkspaceCoolifyConfig holds Coolify placement and authentication settings.
// It is named for the runtime it targets (Coolify) so a later provider does
// not inherit misleadingly generic naming.
type WorkspaceCoolifyConfig struct {
	APIURL          string `config:"api_url"`
	APIToken        string `config:"api_token"`
	ServerUUID      string `config:"server_uuid"`
	ProjectUUID     string `config:"project_uuid"`
	EnvironmentUUID string `config:"environment_uuid"`
	DestinationUUID string `config:"destination_uuid"`
	// InstallNamespace is an OPTIONAL short DNS-safe namespace prefix applied
	// to every provider resource name this installation creates, and the basis
	// of the installation-scoped Coolify tag ("<namespace>-workspaces") sent
	// with every application it provisions. It is used so two portal
	// installations that share the same Coolify placement
	// (server/project/environment/destination) never produce colliding
	// deterministic application/volume names - which is what recovery/adoption
	// keys on (Coolify v4.3.19 persists application tags and supports
	// GET /applications?tag=<tag>). Leave empty (the default) when this
	// installation owns its placement exclusively; recovery then falls back to
	// a deterministic-name listing. When set it must be a DNS label (RFC 1035)
	// and the combined resource name must stay bounded (<= 63 chars).
	InstallNamespace string `config:"install_namespace"`
}

// WorkspaceRuntimeConfig holds settings for the Docker-image application.
//
// Resource limits default to a single-user, low-traffic runtime tier
// (0.5 CPU / 512 MB limit, 256 MB reserved). Research consensus for a
// per-user WordPress container puts real usage at ~80-400 MB with CPU
// rarely as bottleneck; the shared database runs on its own resource, so
// only the application's footprint is budgeted here. Operators can still
// override each limit per installation.
type WorkspaceRuntimeConfig struct {
	Image             string `config:"image"`
	Tag               string `config:"tag"`
	Port              uint16 `config:"port"`
	MemoryLimit       string `config:"memory_limit"`
	MemoryReservation string `config:"memory_reservation"`
	CPULimit          string `config:"cpu_limit"`
	// HealthPath is the Docker/Coolify container health-check path (e.g.
	// /healthz). It is exercised by the container itself against localhost/its
	// own port — it is NOT a public Caddy route and Coolify Basic Auth is
	// deliberately not applied to it. If the runtime image guards the app's
	// own endpoints with an in-container auth, the image must exempt its own
	// localhost health check so the container healthcheck passes; the portal
	// never HTTP-probes this path.
	HealthPath  string                   `config:"health_path"`
	Storage     []WorkspaceStorageConfig `config:"storage"`
	DatabaseEnv DatabaseEnvironmentKeys  `config:"database_env"`
}

// WorkspaceDatabaseConfig holds settings for the SINGLE shared MySQL/MariaDB
// resource that every workspace uses. It is configured once by the operator and
// is shared by all workspaces: it is never created, stopped, or deleted by
// per-workspace reconciliation. The portal owns logical database/user
// provisioning on top of this shared server (see mysqlprovision).
//
// Only the Coolify resource ID is configured here. There is deliberately NO
// password secret: each workspace's logical database password is derived
// deterministically with HKDF-SHA256 keyed by the portal identity key (the
// portal's own ed25519 identity, already present in the portal core config) and
// a per-workspace random salt persisted on the workspace row. This keeps the
// portal's key as the single source of secrecy with no separate config secret,
// while the per-workspace salt allows an individual workspace's derived
// password to be independently rotated (regenerate the salt) without touching
// the identity key or other workspaces.
//
// The admin/root connection is NOT configured by the operator either: the
// portal fetches the shared resource through Coolify's existing
// GET /databases/{uuid} API and derives the admin host, port, user (root), and
// root password from its response (no Coolify modifications are required).
//
// Secret safety: the shared resource's root password is fetched transiently per
// operation and is never persisted here; derived password material is kept out
// of persistence, logs, and traces.
type WorkspaceDatabaseConfig struct {
	// ResourceID is the Coolify resource UUID of the shared MySQL/MariaDB
	// database resource. The portal resolves its current internal host/alias,
	// admin user, and root password by querying Coolify for this resource; it
	// is never passed into a workspace runtime and is never created/deleted by
	// reconciliation.
	ResourceID string `config:"resource_id"`
}

// Defaults returns the single-user runtime tier defaults for the application
// container's resource limits. Only the limits are defaulted; image, tag,
// port, storage, and database env key names stay required from configuration.
func (c WorkspaceRuntimeConfig) Defaults() map[string]any {
	return map[string]any{
		"MemoryLimit":       "512m",
		"MemoryReservation": "256m",
		"CPULimit":          "0.5",
	}
}

// WorkspaceStorageConfig describes one persistent volume mount for the
// workspace application.
type WorkspaceStorageConfig struct {
	// NameSuffix is appended to the resource name to form the volume name.
	NameSuffix string `config:"name_suffix"`
	// MountPath is the absolute container path the volume is mounted at.
	MountPath string `config:"mount_path"`
}

// DatabaseEnvironmentKeys maps the generated database values to the runtime's
// environment variable names. The initial runtime is WordPress, but the field
// and service remain generic.
type DatabaseEnvironmentKeys struct {
	Host     string `config:"host"`
	Port     string `config:"port"`
	Name     string `config:"name"`
	User     string `config:"user"`
	Password string `config:"password"`
}

// Compile-time assertions that the config structs satisfy the portal contracts
// they are registered as: config.ServiceConfig requires Defaults(); startup
// validation is driven through config.Validator. WorkspaceConfig owns only its
// own scalar-field validation; each nested struct validates itself
// independently (see each Validate method). WorkspaceConfig deliberately does
// NOT orchestrate child validation.
var _ config.ServiceConfig = (*WorkspaceConfig)(nil)
var _ config.Validator = (*WorkspaceConfig)(nil)
var _ config.Validator = (*WorkspaceCoolifyConfig)(nil)
var _ config.Validator = (*WorkspaceRuntimeConfig)(nil)
var _ config.Validator = (*WorkspaceStorageConfig)(nil)
var _ config.Validator = (*WorkspaceDatabaseConfig)(nil)
var _ config.Validator = (*DatabaseEnvironmentKeys)(nil)

// Defaults returns the conservative defaults for the workspace service. Only
// fields that are safe to default are set here; the Coolify API URL/token,
// placement IDs, and image/tag/port are all intentionally left empty and
// required by Validate() when Enabled is true. The portal API URL and platform
// domain are not configurable here at all (see WorkspaceConfig).
func (c WorkspaceConfig) Defaults() map[string]any {
	return map[string]any{
		"Enabled":            false,
		"ReconcileInterval":  time.Minute,
		"ReconcileBatchSize": 50,
		"RetryMaxAttempts":   3,
		"RetryInitialDelay":  30 * time.Second,
		"RetryMaxDelay":      5 * time.Minute,
		"RetryTotalLimit":    10,
		"DriftCheckInterval": 24 * time.Hour,
		"RequestTimeout":     30 * time.Second,
		"ProvisionTimeout":   15 * time.Minute,
		"PollInterval":       5 * time.Second,
	}
}

// memoryLimitRe matches a Docker/Coolify memory limit such as "512m", "1g",
// "1024", or "0.5g".
var memoryLimitRe = regexp.MustCompile(`^(?:\d+(?:\.\d+)?)(?:[bkmgBKMG])?$`)

// cpuLimitRe matches a Coolify CPU limit such as "1" or "0.5".
var cpuLimitRe = regexp.MustCompile(`^\d+(?:\.\d+)?$`)

// validDNSPrefixRe matches an RFC 1035 DNS label (1-63 chars) used for the
// optional install namespace prefix on deterministic resource names.
var validDNSPrefixRe = regexp.MustCompile(`^[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

// maxInstallNamespaceLen bounds the install namespace so the combined
// deterministic resource name stays within Coolify's 63-char resource-name
// ceiling.
const maxInstallNamespaceLen = 24

// Validate performs structural validation of the workspace service config. It
// validates ONLY the workspace service's own scalar fields; it deliberately does
// NOT orchestrate validation of its nested config structs (WorkspaceCoolifyConfig,
// WorkspaceRuntimeConfig, WorkspaceDatabaseConfig). Those own and run their own
// Validate methods, and the workspace service startup path reaches each of them
// independently when the service is enabled. Live checks (Coolify
// health/version, platform domain resolution, and the sensitive-database
// reachability check) are performed by the service startup/reconciliation path
// once the required dependencies exist.
func (c WorkspaceConfig) Validate() error {
	// When the service is disabled, the workspace config owns nothing to
	// validate and passes trivially. Remaining fields are ignored by the
	// service while disabled (see WorkspaceConfig).
	if !c.Enabled {
		return nil
	}

	// Reconcile/retry/drift settings must be valid when enabled.
	if c.ReconcileBatchSize <= 0 {
		return errors.New("workspace: reconcile_batch_size must be positive when enabled")
	}
	if c.RetryMaxAttempts < 0 {
		return errors.New("workspace: retry_max_attempts must not be negative when enabled")
	}
	if c.RetryInitialDelay < 0 {
		return errors.New("workspace: retry_initial_delay must not be negative when enabled")
	}
	if c.RetryMaxDelay < 0 {
		return errors.New("workspace: retry_max_delay must not be negative when enabled")
	}
	if c.RetryTotalLimit < 0 {
		return errors.New("workspace: retry_total_limit must not be negative when enabled")
	}
	if c.DriftCheckInterval < 0 {
		return errors.New("workspace: drift_check_interval must not be negative when enabled")
	}

	return nil
}

// Validate performs structural validation of the Coolify placement and
// authentication settings. It runs only when the workspace service is enabled
// (the service reaches it independently). The portal API URL and platform
// domain are intentionally NOT configured here (see WorkspaceConfig).
func (c WorkspaceCoolifyConfig) Validate() error {
	// 1. Coolify URL must be HTTPS unless it points at an explicitly allowed
	// loopback development host.
	if err := validateCoolifyURL(c.APIURL); err != nil {
		return err
	}

	// 2. API token must be present.
	if c.APIToken == "" {
		return errors.New("workspace: coolify.api_token is required when enabled")
	}

	// 3. Server, project, environment, and destination IDs must be present.
	if c.ServerUUID == "" {
		return errors.New("workspace: coolify.server_uuid is required when enabled")
	}
	if c.ProjectUUID == "" {
		return errors.New("workspace: coolify.project_uuid is required when enabled")
	}
	if c.EnvironmentUUID == "" {
		return errors.New("workspace: coolify.environment_uuid is required when enabled")
	}
	if c.DestinationUUID == "" {
		return errors.New("workspace: coolify.destination_uuid is required when enabled")
	}

	// 4. InstallNamespace is optional, but when set it must be a valid DNS
	// label (RFC 1035) so the deterministic application/volume resource names
	// it prefixes remain valid Coolify resource names (bounded, DNS-safe).
	if ns := c.InstallNamespace; ns != "" && !validDNSPrefixRe.MatchString(ns) {
		return fmt.Errorf("workspace: coolify.install_namespace %q is not a valid DNS label (RFC 1035)", ns)
	}
	if ns := c.InstallNamespace; len(ns) > maxInstallNamespaceLen {
		return fmt.Errorf("workspace: coolify.install_namespace %q exceeds %d chars", ns, maxInstallNamespaceLen)
	}

	return nil
}

// Validate performs structural validation of the runtime's own scalar fields
// (image, tag, port, and resource limits). It also reaches the runtime's
// structural leaves so their individual Validate methods run: each storage
// mount and the database environment key names. It does NOT read or validate
// anything owned by WorkspaceConfig or the Coolify/database sibling config.
func (c WorkspaceRuntimeConfig) Validate() error {
	// Image, tag, and port must be present.
	if c.Image == "" {
		return errors.New("workspace: runtime.image is required when enabled")
	}
	if c.Tag == "" {
		return errors.New("workspace: runtime.tag is required when enabled")
	}
	if c.Port == 0 {
		return errors.New("workspace: runtime.port is required when enabled")
	}

	// Resource limits must parse into accepted Coolify/Docker values. The
	// shared database resource's own limits live in Coolify, not this config;
	// only the application runtime resource limits are validated here.
	for _, v := range []struct {
		field string
		value string
	}{
		{"runtime.memory_limit", c.MemoryLimit},
		{"runtime.memory_reservation", c.MemoryReservation},
	} {
		if v.value != "" && !memoryLimitRe.MatchString(v.value) {
			return fmt.Errorf("workspace: %s %q is not a valid memory limit", v.field, v.value)
		}
	}
	if c.CPULimit != "" && !cpuLimitRe.MatchString(c.CPULimit) {
		return fmt.Errorf("workspace: runtime.cpu_limit %q is not a valid CPU limit", c.CPULimit)
	}

	// Storage mount paths must be absolute and unique across the runtime's
	// mounts.
	seenMounts := make(map[string]string, len(c.Storage))
	for i, m := range c.Storage {
		if err := m.Validate(); err != nil {
			return fmt.Errorf("workspace: runtime.storage[%d]: %w", i, err)
		}
		if prev, ok := seenMounts[m.MountPath]; ok {
			return fmt.Errorf("workspace: runtime.storage mount_path %q is duplicated (%s and %s)", m.MountPath, prev, m.NameSuffix)
		}
		seenMounts[m.MountPath] = m.NameSuffix
	}

	// Database environment key names must be non-empty and distinct.
	return c.DatabaseEnv.Validate()
}

// Validate performs structural validation of a single storage mount.
func (c WorkspaceStorageConfig) Validate() error {
	if strings.TrimSpace(c.NameSuffix) == "" {
		return errors.New("name_suffix is required")
	}
	if !filepath.IsAbs(c.MountPath) {
		return fmt.Errorf("mount_path %q must be absolute", c.MountPath)
	}
	return nil
}

// Validate performs structural validation of the shared MySQL/MariaDB resource
// config. Only the Coolify resource ID is configured here (see
// WorkspaceDatabaseConfig); it is required when the workspace service is
// enabled.
func (c WorkspaceDatabaseConfig) Validate() error {
	if c.ResourceID == "" {
		return errors.New("workspace: database.resource_id is required when enabled")
	}
	return nil
}

// Validate performs structural validation of the runtime's database environment
// variable names: each role must map to a non-empty, distinct key.
func (c DatabaseEnvironmentKeys) Validate() error {
	return validateDistinctEnvKeys(map[string]string{
		"host":     c.Host,
		"port":     c.Port,
		"name":     c.Name,
		"user":     c.User,
		"password": c.Password,
	})
}

func validateDistinctEnvKeys(keys map[string]string) error {
	seen := make(map[string]string, len(keys))
	for role, k := range keys {
		if strings.TrimSpace(k) == "" {
			return fmt.Errorf("workspace: runtime.database_env.%s is required when enabled", role)
		}
		if prev, ok := seen[k]; ok {
			return fmt.Errorf("workspace: runtime.database_env keys must be distinct: %q used for both %s and %s", k, prev, role)
		}
		seen[k] = role
	}
	return nil
}

// validateCoolifyURL requires an HTTPS scheme unless the target is an
// explicitly allowed loopback development host (http://localhost,
// http://127.0.0.1, http://[::1]).
func validateCoolifyURL(raw string) error {
	if raw == "" {
		return errors.New("workspace: coolify.api_url is required when enabled")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("workspace: coolify.api_url %q is not a valid URL: %w", raw, err)
	}
	if u.Scheme == "https" {
		return nil
	}
	if u.Scheme == "http" && isLoopbackHost(u.Hostname()) {
		return nil
	}
	return fmt.Errorf("workspace: coolify.api_url %q must use https (http is only allowed for loopback development hosts)", raw)
}

func isLoopbackHost(host string) bool {
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
