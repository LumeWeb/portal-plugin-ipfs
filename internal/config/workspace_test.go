package config

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// enabledConfig returns a fully valid workspace config for tests that exercise
// validation beyond the "disabled" fast path.
func enabledConfig() WorkspaceConfig {
	return WorkspaceConfig{
		Enabled:                 true,
		ReconcileInterval:       time.Minute,
		ReconcileBatchSize:      50,
		RetryMaxAttempts:        3,
		RetryInitialDelay:       30 * time.Second,
		RetryMaxDelay:           5 * time.Minute,
		DriftCheckInterval:      24 * time.Hour,
		RequestTimeout:          30 * time.Second,
		ProvisionTimeout:        15 * time.Minute,
		PollInterval:            5 * time.Second,
		PortalAPIURL:            "https://api.example.com",
		PlatformDomain:          "build.example.com",
		PlatformDomainNamespace: "icann",
		Provider: WorkspaceProviderConfig{
			APIURL:          "https://coolify.example.com",
			APIToken:        "secret-token",
			ServerUUID:      "server-1",
			ProjectUUID:     "project-1",
			EnvironmentUUID: "env-1",
			DestinationUUID: "dest-1",
		},
		Runtime: WorkspaceRuntimeConfig{
			Image:             "wordpress",
			Tag:               "6.4",
			Port:              80,
			MemoryLimit:       "512m",
			MemoryReservation: "256m",
			CPULimit:          "1",
			HealthPath:        "/wp-admin/install.php",
			Storage: []WorkspaceStorageConfig{
				{NameSuffix: "data", MountPath: "/var/www/html"},
			},
			DatabaseEnv: DatabaseEnvironmentKeys{
				Host:     "WORDPRESS_DB_HOST",
				Port:     "WORDPRESS_DB_PORT",
				Name:     "WORDPRESS_DB_NAME",
				User:     "WORDPRESS_DB_USER",
				Password: "WORDPRESS_DB_PASSWORD",
			},
		},
		Database: WorkspaceDatabaseConfig{
			ResourceID: "shared-db-resource",
		},
	}
}

func TestWorkspaceConfig_Defaults(t *testing.T) {
	d := (WorkspaceConfig{}).Defaults()

	assert.Equal(t, false, d["Enabled"])
	assert.Equal(t, time.Minute, d["ReconcileInterval"])
	assert.Equal(t, 30*time.Second, d["RequestTimeout"])
	assert.Equal(t, 15*time.Minute, d["ProvisionTimeout"])
	assert.Equal(t, 5*time.Second, d["PollInterval"])

	// Conservative by design: required runtime/provider values are not defaulted.
	assert.NotContains(t, d, "Provider")
	assert.NotContains(t, d, "PortalAPIURL")
	assert.NotContains(t, d, "PlatformDomain")
	assert.NotContains(t, d, "Runtime")
}

func TestWorkspaceConfig_ImplementsContracts(t *testing.T) {
	// WorkspaceConfig must be usable as config.ServiceConfig (Defaults) and as a
	// config.Validator; both are also compile-time asserted via the var _ lines.
	var _ interface{ Defaults() map[string]any } = WorkspaceConfig{}
	var _ interface{ Validate() error } = WorkspaceConfig{}
}

func TestWorkspaceConfig_Validate_Disabled(t *testing.T) {
	// A completely empty config validates when the service is disabled.
	require.NoError(t, (WorkspaceConfig{}).Validate())
}

func TestWorkspaceConfig_Validate_Valid(t *testing.T) {
	require.NoError(t, enabledConfig().Validate())
}

func TestWorkspaceConfig_Validate_MissingToken(t *testing.T) {
	c := enabledConfig()
	c.Provider.APIToken = ""
	require.ErrorContains(t, c.Validate(), "api_token")
}

func TestWorkspaceConfig_Validate_NonHTTPSURL(t *testing.T) {
	c := enabledConfig()
	c.Provider.APIURL = "http://coolify.example.com"
	require.ErrorContains(t, c.Validate(), "https")
}

func TestWorkspaceConfig_Validate_LoopbackURLAllowed(t *testing.T) {
	for _, u := range []string{"http://localhost:8000", "http://127.0.0.1:8000", "http://[::1]:8000"} {
		c := enabledConfig()
		c.Provider.APIURL = u
		require.NoError(t, c.Validate(), u)
	}
}

func TestWorkspaceConfig_Validate_MissingPlacement(t *testing.T) {
	c := enabledConfig()
	c.Provider.ServerUUID = ""
	require.ErrorContains(t, c.Validate(), "server_uuid")

	c = enabledConfig()
	c.Provider.ProjectUUID = ""
	require.ErrorContains(t, c.Validate(), "project_uuid")

	c = enabledConfig()
	c.Provider.EnvironmentUUID = ""
	require.ErrorContains(t, c.Validate(), "environment_uuid")

	c = enabledConfig()
	c.Provider.DestinationUUID = ""
	require.ErrorContains(t, c.Validate(), "destination_uuid")
}

func TestWorkspaceConfig_Validate_MissingRuntime(t *testing.T) {
	c := enabledConfig()
	c.Runtime.Image = ""
	require.ErrorContains(t, c.Validate(), "image")

	c = enabledConfig()
	c.Runtime.Tag = ""
	require.ErrorContains(t, c.Validate(), "tag")

	c = enabledConfig()
	c.Runtime.Port = 0
	require.ErrorContains(t, c.Validate(), "port")
}

func TestWorkspaceConfig_Validate_MissingPortalURLAndDomain(t *testing.T) {
	c := enabledConfig()
	c.PortalAPIURL = ""
	require.ErrorContains(t, c.Validate(), "portal_api_url")

	c = enabledConfig()
	c.PlatformDomain = ""
	require.ErrorContains(t, c.Validate(), "platform_domain")
}

func TestWorkspaceConfig_Validate_ResourceLimits(t *testing.T) {
	c := enabledConfig()
	c.Runtime.MemoryLimit = "not-a-limit"
	require.ErrorContains(t, c.Validate(), "memory_limit")

	c = enabledConfig()
	c.Runtime.CPULimit = "1.5.2"
	require.ErrorContains(t, c.Validate(), "cpu_limit")
}

func TestWorkspaceConfig_Validate_SharedDatabase(t *testing.T) {
	c := enabledConfig()
	c.Database.ResourceID = ""
	require.ErrorContains(t, c.Validate(), "database.resource_id")

	// There is deliberately NO password secret: each workspace's logical
	// database password is derived with HKDF-SHA256 keyed by the portal
	// identity key plus a per-workspace random salt persisted on the row. The
	// struct therefore exposes only the shared resource ID — no password
	// secret, and no admin host/port/user/password fields (the admin/root
	// connection is derived from the shared resource via Coolify's
	// GET /databases/{uuid} API at provisioning time).
	typ := reflect.TypeOf(WorkspaceDatabaseConfig{})
	fields := make(map[string]bool, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		fields[typ.Field(i).Name] = true
	}
	assert.Equal(t, map[string]bool{"ResourceID": true}, fields,
		"WorkspaceDatabaseConfig must expose only the shared resource ID; no password secret or admin credential fields")
	for _, bad := range []string{"PasswordSecret", "AdminHost", "AdminPort", "AdminUser", "AdminPassword"} {
		assert.False(t, fields[bad], "config field %s must not exist", bad)
	}
}

func TestWorkspaceConfig_Validate_Storage(t *testing.T) {
	c := enabledConfig()
	c.Runtime.Storage = []WorkspaceStorageConfig{{NameSuffix: "data", MountPath: "relative/path"}}
	require.ErrorContains(t, c.Validate(), "absolute")

	c = enabledConfig()
	c.Runtime.Storage = []WorkspaceStorageConfig{
		{NameSuffix: "data", MountPath: "/var/www/html"},
		{NameSuffix: "cache", MountPath: "/var/www/html"},
	}
	require.ErrorContains(t, c.Validate(), "duplicated")

	c = enabledConfig()
	c.Runtime.Storage = []WorkspaceStorageConfig{{NameSuffix: " ", MountPath: "/var/www/html"}}
	require.ErrorContains(t, c.Validate(), "name_suffix")
}

func TestWorkspaceConfig_Validate_DatabaseEnvKeys(t *testing.T) {
	c := enabledConfig()
	c.Runtime.DatabaseEnv.Host = ""
	require.ErrorContains(t, c.Validate(), "database_env.host")

	c = enabledConfig()
	c.Runtime.DatabaseEnv.Password = c.Runtime.DatabaseEnv.User
	require.ErrorContains(t, c.Validate(), "distinct")
}
