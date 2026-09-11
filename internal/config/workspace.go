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

	// PortalAPIURL is the public API base URL injected into the runtime as
	// PORTAL_API_URL.
	PortalAPIURL string `config:"portal_api_url"`

	// PlatformDomain is the root domain (e.g. build.example.com) from which the
	// configured enabled workspace hostname is derived.
	PlatformDomain string `config:"platform_domain"`
	// PlatformDomainNamespace is the DNS namespace (e.g. icann) the platform
	// domain is resolved under.
	PlatformDomainNamespace string `config:"platform_domain_namespace"`

	// Provider holds Coolify placement/token configuration.
	Provider WorkspaceProviderConfig `config:"provider"`
	// Runtime holds the selected application image and its runtime settings.
	Runtime WorkspaceRuntimeConfig `config:"runtime"`
	// Database holds the dedicated database resource settings.
	Database WorkspaceDatabaseConfig `config:"database"`
}

// WorkspaceProviderConfig holds Coolify placement and authentication settings.
type WorkspaceProviderConfig struct {
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

// Compile-time assertions that the config satisfies the portal contracts it is
// registered as: config.ServiceConfig requires Defaults(); startup validation
// is driven through config.Validator.
var _ config.ServiceConfig = (*WorkspaceConfig)(nil)
var _ config.Validator = (*WorkspaceConfig)(nil)

// Defaults returns the conservative defaults for the workspace service. Only
// fields that are safe to default are set here; the Coolify API URL/token,
// placement IDs, image/tag/port, portal API URL, and platform domain are all
// intentionally left empty and required by Validate() when Enabled is true.
func (c WorkspaceConfig) Defaults() map[string]any {
	return map[string]any{
		"Enabled":            false,
		"ReconcileInterval":  time.Minute,
		"ReconcileBatchSize": 50,
		"RetryMaxAttempts":   3,
		"RetryInitialDelay":  30 * time.Second,
		"RetryMaxDelay":      5 * time.Minute,
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

// Validate performs structural validation of the workspace config. When the
// service is disabled, validation passes trivially. Live checks (Coolify
// health/version, platform domain resolution, and the sensitive-database
// reachability check) are performed by the service startup/reconciliation path
// once the required dependencies exist.
func (c WorkspaceConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	// 1. Coolify URL must be HTTPS unless it points at an explicitly allowed
	// loopback development host.
	if err := validateCoolifyURL(c.Provider.APIURL); err != nil {
		return err
	}

	// 2. API token must be present.
	if c.Provider.APIToken == "" {
		return errors.New("workspace: provider.api_token is required when enabled")
	}

	// 3. Server, project, environment, and destination IDs must be present.
	if c.Provider.ServerUUID == "" {
		return errors.New("workspace: provider.server_uuid is required when enabled")
	}
	if c.Provider.ProjectUUID == "" {
		return errors.New("workspace: provider.project_uuid is required when enabled")
	}
	if c.Provider.EnvironmentUUID == "" {
		return errors.New("workspace: provider.environment_uuid is required when enabled")
	}
	if c.Provider.DestinationUUID == "" {
		return errors.New("workspace: provider.destination_uuid is required when enabled")
	}

	// 4. Image, tag, and port must be present.
	if c.Runtime.Image == "" {
		return errors.New("workspace: runtime.image is required when enabled")
	}
	if c.Runtime.Tag == "" {
		return errors.New("workspace: runtime.tag is required when enabled")
	}
	if c.Runtime.Port == 0 {
		return errors.New("workspace: runtime.port is required when enabled")
	}

	// Portal API URL and platform domain are required to build the runtime
	// environment and hostname.
	if c.PortalAPIURL == "" {
		return errors.New("workspace: portal_api_url is required when enabled")
	}
	if c.PlatformDomain == "" {
		return errors.New("workspace: platform_domain is required when enabled")
	}

	// 4a. The shared MySQL/MariaDB resource is a hard dependency of logical
	// database provisioning: its Coolify resource ID is required when enabled.
	// There is deliberately NO per-workspace database password secret here:
	// each workspace's logical database password is derived with HKDF-SHA256
	// keyed by the portal identity key (Core.Identity.PrivateKey) plus a
	// per-workspace random salt persisted on the workspace row, so no separate
	// config secret exists to leak or rotate globally. The admin/root
	// connection (host/port/user/password) is NOT configured here either — it
	// is derived from the shared resource via Coolify's existing
	// GET /databases/{uuid} API at provisioning time, so no Coolify
	// modifications are required. No password material is ever logged or
	// returned from an API here.
	if c.Database.ResourceID == "" {
		return errors.New("workspace: database.resource_id is required when enabled")
	}

	// 4a′. InstallNamespace is optional, but when set it must be a valid DNS
	// label (RFC 1035) so the deterministic application/volume resource names
	// it prefixes remain valid Coolify resource names (bounded, DNS-safe).
	if ns := c.Provider.InstallNamespace; ns != "" && !validDNSPrefixRe.MatchString(ns) {
		return fmt.Errorf("workspace: provider.install_namespace %q is not a valid DNS label (RFC 1035)", ns)
	}
	if ns := c.Provider.InstallNamespace; len(ns) > maxInstallNamespaceLen {
		return fmt.Errorf("workspace: provider.install_namespace %q exceeds %d chars", ns, maxInstallNamespaceLen)
	}

	// 4b. Reconcile settings must be valid when enabled.
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
	if c.DriftCheckInterval < 0 {
		return errors.New("workspace: drift_check_interval must not be negative when enabled")
	}

	// 5. Resource limits must parse into accepted Coolify/Docker values.
	// The shared database resource is configured once in Coolify by the
	// operator; its resource limits live in Coolify, not in this config. Only
	// the application runtime resource limits are validated here.
	for _, v := range []struct {
		field string
		value string
	}{
		{"runtime.memory_limit", c.Runtime.MemoryLimit},
		{"runtime.memory_reservation", c.Runtime.MemoryReservation},
	} {
		if v.value != "" && !memoryLimitRe.MatchString(v.value) {
			return fmt.Errorf("workspace: %s %q is not a valid memory limit", v.field, v.value)
		}
	}
	if c.Runtime.CPULimit != "" && !cpuLimitRe.MatchString(c.Runtime.CPULimit) {
		return fmt.Errorf("workspace: runtime.cpu_limit %q is not a valid CPU limit", c.Runtime.CPULimit)
	}

	// 6. Storage mount paths must be absolute and unique.
	seenMounts := make(map[string]string, len(c.Runtime.Storage))
	for i, m := range c.Runtime.Storage {
		if strings.TrimSpace(m.NameSuffix) == "" {
			return fmt.Errorf("workspace: runtime.storage[%d].name_suffix is required", i)
		}
		if !filepath.IsAbs(m.MountPath) {
			return fmt.Errorf("workspace: runtime.storage[%d].mount_path %q must be absolute", i, m.MountPath)
		}
		if prev, ok := seenMounts[m.MountPath]; ok {
			return fmt.Errorf("workspace: runtime.storage mount_path %q is duplicated (%s and %s)", m.MountPath, prev, m.NameSuffix)
		}
		seenMounts[m.MountPath] = m.NameSuffix
	}

	// 7. Database environment key names must be non-empty and distinct.
	if err := validateDistinctEnvKeys(map[string]string{
		"host":     c.Runtime.DatabaseEnv.Host,
		"port":     c.Runtime.DatabaseEnv.Port,
		"name":     c.Runtime.DatabaseEnv.Name,
		"user":     c.Runtime.DatabaseEnv.User,
		"password": c.Runtime.DatabaseEnv.Password,
	}); err != nil {
		return err
	}

	return nil
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
		return errors.New("workspace: provider.api_url is required when enabled")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("workspace: provider.api_url %q is not a valid URL: %w", raw, err)
	}
	if u.Scheme == "https" {
		return nil
	}
	if u.Scheme == "http" && isLoopbackHost(u.Hostname()) {
		return nil
	}
	return fmt.Errorf("workspace: provider.api_url %q must use https (http is only allowed for loopback development hosts)", raw)
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
