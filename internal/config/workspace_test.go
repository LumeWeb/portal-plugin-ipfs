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
		Enabled:            true,
		ReconcileInterval:  time.Minute,
		ReconcileBatchSize: 50,
		RetryMaxAttempts:   3,
		RetryInitialDelay:  30 * time.Second,
		RetryMaxDelay:      5 * time.Minute,
		DriftCheckInterval: 24 * time.Hour,
		RequestTimeout:     30 * time.Second,
		ProvisionTimeout:   15 * time.Minute,
		PollInterval:       5 * time.Second,
		Coolify: WorkspaceCoolifyConfig{
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
	assert.NotContains(t, d, "Coolify")
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

// TestWorkspaceConfig_Validate_DoesNotValidateChildren proves the parent
// WorkspaceConfig validator owns ONLY its own fields: an enabled config with
// invalid nested (Coolify/Runtime/Database) values still passes
// WorkspaceConfig.Validate() because the parent does not orchestrate child
// validation. Each child is reached by the service startup path independently
// (see the child-level tests below).
func TestWorkspaceConfig_Validate_DoesNotValidateChildren(t *testing.T) {
	c := enabledConfig()
	// All children are invalid, but the parent must not flag them.
	c.Coolify.APIToken = ""
	c.Coolify.ServerUUID = ""
	c.Runtime.Image = ""
	c.Runtime.Storage = []WorkspaceStorageConfig{{NameSuffix: " ", MountPath: "relative"}}
	c.Database.ResourceID = ""
	require.NoError(t, c.Validate(), "WorkspaceConfig must not validate its nested config structs")
}

func TestWorkspaceConfig_Validate_OwnReconcileFields(t *testing.T) {
	c := enabledConfig()
	c.ReconcileBatchSize = 0
	require.ErrorContains(t, c.Validate(), "reconcile_batch_size")

	c = enabledConfig()
	c.RetryMaxAttempts = -1
	require.ErrorContains(t, c.Validate(), "retry_max_attempts")

	c = enabledConfig()
	c.RetryTotalLimit = -1
	require.ErrorContains(t, c.Validate(), "retry_total_limit")

	c = enabledConfig()
	c.DriftCheckInterval = -time.Second
	require.ErrorContains(t, c.Validate(), "drift_check_interval")
}

func TestWorkspaceCoolifyConfig_Validate_MissingToken(t *testing.T) {
	c := enabledConfig().Coolify
	c.APIToken = ""
	require.ErrorContains(t, c.Validate(), "api_token")
}

func TestWorkspaceCoolifyConfig_Validate_NonHTTPSURL(t *testing.T) {
	c := enabledConfig().Coolify
	c.APIURL = "http://coolify.example.com"
	require.ErrorContains(t, c.Validate(), "https")
}

func TestWorkspaceCoolifyConfig_Validate_LoopbackURLAllowed(t *testing.T) {
	for _, u := range []string{"http://localhost:8000", "http://127.0.0.1:8000", "http://[::1]:8000"} {
		c := enabledConfig().Coolify
		c.APIURL = u
		require.NoError(t, c.Validate(), u)
	}
}

func TestWorkspaceCoolifyConfig_Validate_MissingPlacement(t *testing.T) {
	c := enabledConfig().Coolify
	c.ServerUUID = ""
	require.ErrorContains(t, c.Validate(), "server_uuid")

	c = enabledConfig().Coolify
	c.ProjectUUID = ""
	require.ErrorContains(t, c.Validate(), "project_uuid")

	c = enabledConfig().Coolify
	c.EnvironmentUUID = ""
	require.ErrorContains(t, c.Validate(), "environment_uuid")

	c = enabledConfig().Coolify
	c.DestinationUUID = ""
	require.ErrorContains(t, c.Validate(), "destination_uuid")
}

func TestWorkspaceCoolifyConfig_Validate_InstallNamespace(t *testing.T) {
	c := enabledConfig().Coolify
	c.InstallNamespace = "INVALID_Label"
	require.ErrorContains(t, c.Validate(), "install_namespace")
}

func TestWorkspaceRuntimeConfig_Validate_MissingRuntime(t *testing.T) {
	c := enabledConfig().Runtime
	c.Image = ""
	require.ErrorContains(t, c.Validate(), "image")

	c = enabledConfig().Runtime
	c.Tag = ""
	require.ErrorContains(t, c.Validate(), "tag")

	c = enabledConfig().Runtime
	c.Port = 0
	require.ErrorContains(t, c.Validate(), "port")
}

func TestWorkspaceRuntimeConfig_Validate_ResourceLimits(t *testing.T) {
	c := enabledConfig().Runtime
	c.MemoryLimit = "not-a-limit"
	require.ErrorContains(t, c.Validate(), "memory_limit")

	c = enabledConfig().Runtime
	c.CPULimit = "1.5.2"
	require.ErrorContains(t, c.Validate(), "cpu_limit")
}

func TestWorkspaceDatabaseConfig_Validate_SharedDatabase(t *testing.T) {
	c := enabledConfig().Database
	c.ResourceID = ""
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

func TestWorkspaceRuntimeConfig_Validate_Storage(t *testing.T) {
	// Given an invalid storage entry, the runtime reaches each mount's own
	// validator.
	c := enabledConfig().Runtime
	c.Storage = []WorkspaceStorageConfig{{NameSuffix: "data", MountPath: "relative/path"}}
	require.ErrorContains(t, c.Validate(), "absolute")

	c = enabledConfig().Runtime
	c.Storage = []WorkspaceStorageConfig{
		{NameSuffix: "data", MountPath: "/var/www/html"},
		{NameSuffix: "cache", MountPath: "/var/www/html"},
	}
	require.ErrorContains(t, c.Validate(), "duplicated")

	c = enabledConfig().Runtime
	c.Storage = []WorkspaceStorageConfig{{NameSuffix: " ", MountPath: "/var/www/html"}}
	require.ErrorContains(t, c.Validate(), "name_suffix")
}

// TestWorkspaceStorageConfig_Validate_Independently proves the leaf storage
// validator owns a single mount's checks and is reachable directly, independent
// of the runtime/WorkspaceConfig parents.
func TestWorkspaceStorageConfig_Validate_Independently(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  WorkspaceStorageConfig
		want string
	}{
		{"empty name suffix", WorkspaceStorageConfig{MountPath: "/var/www/html"}, "name_suffix"},
		{"relative mount path", WorkspaceStorageConfig{NameSuffix: "data", MountPath: "relative"}, "absolute"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, tc.cfg.Validate(), tc.want)
		})
	}
	require.NoError(t, (WorkspaceStorageConfig{NameSuffix: "data", MountPath: "/var/www/html"}).Validate())
}

func TestWorkspaceRuntimeConfig_Validate_DatabaseEnvKeys(t *testing.T) {
	c := enabledConfig().Runtime
	c.DatabaseEnv.Host = ""
	require.ErrorContains(t, c.Validate(), "database_env.host")

	c = enabledConfig().Runtime
	c.DatabaseEnv.Password = c.DatabaseEnv.User
	require.ErrorContains(t, c.Validate(), "distinct")
}

// TestDatabaseEnvironmentKeys_Validate_Independently proves the leaf env-key
// validator is reachable directly.
func TestDatabaseEnvironmentKeys_Validate_Independently(t *testing.T) {
	keys := DatabaseEnvironmentKeys{
		Host: "DB_HOST", Port: "DB_PORT", Name: "DB_NAME", User: "DB_USER", Password: "DB_PASSWORD",
	}
	require.NoError(t, keys.Validate())

	dup := keys
	dup.Password = keys.User
	require.ErrorContains(t, dup.Validate(), "distinct")

	empty := keys
	empty.Name = ""
	require.ErrorContains(t, empty.Validate(), "database_env.name")
}
