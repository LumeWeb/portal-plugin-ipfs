package workspace

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	dashboardCore "go.lumeweb.com/portal-plugin-dashboard/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/coolify"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/mysqlprovision"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

// fakeEngineer is a test fake for mysqlprovision.Engineer. It records the
// logical database/user provisioning and drop requests so tests can assert on
// what the workspace service provisioned against the SHARED MySQL resource.
type fakeEngineer struct {
	ensureCalls []mysqlprovision.EnsureRequest
	ensureErr   error
	dropCalls   []mysqlprovision.EnsureRequest
	dropErr     error
}

func (f *fakeEngineer) EnsureDatabase(_ context.Context, req mysqlprovision.EnsureRequest) error {
	f.ensureCalls = append(f.ensureCalls, req)
	return f.ensureErr
}

func (f *fakeEngineer) DropDatabase(_ context.Context, req mysqlprovision.EnsureRequest) error {
	f.dropCalls = append(f.dropCalls, req)
	return f.dropErr
}

func (f *fakeEngineer) Close() error { return nil }

// fakeWorkspaceProvider is a test fake for the provider-neutral
// coolify.WorkspaceProvider. It embeds the interface (nil) and overrides only
// the DB resource-resolution and application-read methods used by the shared
// database / drift paths under test. It deliberately has NO per-workspace
// database create/start/stop/delete methods, matching the corrected
// architecture.
type fakeWorkspaceProvider struct {
	coolify.WorkspaceProvider

	// shared database resolution (ResolveDatabaseResource)
	resolveCalls int
	resolveErr   error
	database     coolify.DatabaseResource

	// application read (GetApplication), used by the drift path
	appGetCalls int
	appGetErr   error
	appStatus   coolify.ResourceStatus

	findCalls int
	findErr   error
	resources []coolify.Resource

	// createCalls is never incremented in this fake; the drift tests assert it
	// stays zero ("drift must never create a resource").
	createCalls int
}

func (f *fakeWorkspaceProvider) ResolveDatabaseResource(_ context.Context, _ string) (coolify.DatabaseResource, error) {
	f.resolveCalls++
	if f.resolveErr != nil {
		return coolify.DatabaseResource{}, f.resolveErr
	}
	r := f.database
	if r.ID == "" {
		// Default to a running shared MariaDB resource (with root password) so
		// tests that do not pin a resource still resolve via the provider
		// (there is no AdminHost override anymore).
		r = runningDatabase()
	}
	if r.Status == "" {
		r.Status = coolify.ResourceStatusRunning
	}
	return r, nil
}

func (f *fakeWorkspaceProvider) GetApplication(_ context.Context, _ string) (coolify.ApplicationResource, error) {
	f.appGetCalls++
	if f.appGetErr != nil {
		return coolify.ApplicationResource{}, f.appGetErr
	}
	st := f.appStatus
	if st == "" {
		st = coolify.ResourceStatusRunning
	}
	return coolify.ApplicationResource{ID: "app-created", Status: st}, nil
}

func (f *fakeWorkspaceProvider) FindApplicationByName(_ context.Context, _ string) ([]coolify.Resource, error) {
	f.findCalls++
	if f.findErr != nil {
		return nil, f.findErr
	}
	return f.resources, nil
}

// runningDatabase returns a running shared MariaDB database resource whose
// details (internal URL, root password) Coolify reports via GET /databases/{uuid}.
func runningDatabase() coolify.DatabaseResource {
	return coolify.DatabaseResource{
		ID:           "shared-db",
		Status:       coolify.ResourceStatusRunning,
		Type:         "mariadb",
		InternalURL:  "mysql://root:secret@db.internal:3306/",
		Host:         "db.internal",
		Port:         3306,
		Database:     "",
		Username:     "root",
		Password:     "super-secret-password",
		RootPassword: "admin-secret",
	}
}

// sharedDBConfig returns a WorkspaceDatabaseConfig describing the SINGLE shared
// MySQL/MariaDB resource. Only the resource ID is configured; there is no
// password secret (each workspace's logical database password is derived with
// HKDF-SHA256 from the portal identity key plus a per-workspace salt), and the
// admin host/port/user/root password are fetched from Coolify per operation.
func sharedDBConfig() pluginConfig.WorkspaceDatabaseConfig {
	return pluginConfig.WorkspaceDatabaseConfig{
		ResourceID: "shared-db",
	}
}

// testIdentityPrivateKey returns a fixed ed25519 private key used as the
// stand-in for the portal identity key in tests. Never a real secret; it only
// makes the derived-password assertions deterministic.
func testIdentityPrivateKey() ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))
}

// newProvisionService builds a workspace service wired to a DB, a fake
// provider, a dashboard API-key service, and an injected fake Engineer, with a
// fast poll interval.
func newProvisionService(tb coreTesting.TB, db *gorm.DB, provider *fakeWorkspaceProvider, apiKey dashboardCore.APIKeyService, eng mysqlprovision.Engineer) *WorkspaceService {
	tb.Helper()
	bc := &core.BaseComponent{}
	bc.SetDB(db)
	return &WorkspaceService{
		BaseComponent: bc,
		config: &pluginConfig.WorkspaceConfig{
			Enabled:          true,
			ProvisionTimeout: time.Minute,
			PollInterval:     time.Millisecond,
			Provider: pluginConfig.WorkspaceProviderConfig{
				ServerUUID:      "srv-1",
				ProjectUUID:     "proj-1",
				EnvironmentUUID: "env-1",
				DestinationUUID: "dest-1",
			},
			Database: sharedDBConfig(),
		},
		provider:    provider,
		apiKeySvc:   apiKey,
		mysqlProv:   eng,
		identityKey: testIdentityPrivateKey(),
	}
}

// insertWorkspace inserts a bare workspace row bound to a website.
func insertWorkspace(tb coreTesting.TB, db *gorm.DB, id, websiteID, platformDomainID uint, status pluginDb.WorkspaceStatus) *pluginDb.Workspace {
	tb.Helper()
	ws := &pluginDb.Workspace{
		UserID:           1,
		WebsiteID:        new(uint(websiteID)),
		PlatformDomainID: platformDomainID,
		Label:            "ws-test",
		Status:           status,
	}
	require.NoError(tb, db.Create(ws).Error)
	return ws
}

func TestReconcileDatabase_ProvisionsLogicalDBOnSharedResource(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		fake := &fakeWorkspaceProvider{}
		eng := &fakeEngineer{}
		svc := newProvisionService(tb, db, fake, nil, eng)

		creds, err := svc.ReconcileDatabase(context.Background(), ws)
		require.NoError(tb, err)

		// The shared resource is NEVER created per workspace: no provider
		// create/start/stop/delete is possible (the fake has none). The only
		// provider interaction is the read-only shared-resource lookup to fetch
		// its admin host/port/root password.
		assert.Equal(tb, 1, fake.resolveCalls, "one shared-resource lookup (never create/start/stop/delete)")
		assert.Len(tb, eng.ensureCalls, 1, "exactly one logical provisioning ensure")

		// Deterministic logical identifiers derived from the workspace ID.
		req := eng.ensureCalls[0]
		assert.Equal(tb, "workspace_"+itoa(ws.ID), req.Database)
		assert.Equal(tb, "workspace_"+itoa(ws.ID), req.User)

		// The password is derived (strong, deterministic from the identity key
		// + per-workspace salt) and NOT persisted. Only the non-secret salt is
		// stored on the row; re-deriving from it reproduces the same password.
		require.NotNil(tb, ws.DatabasePasswordSalt)
		salt, err := base64.RawURLEncoding.DecodeString(*ws.DatabasePasswordSalt)
		require.NoError(tb, err)
		wantPass, err := deriveDatabasePassword(testIdentityPrivateKey(), ws.ID, salt)
		require.NoError(tb, err)
		assert.Equal(tb, wantPass, req.Password)
		assert.GreaterOrEqual(tb, len(req.Password), 32)

		// Credentials carry the shared resource's host/port plus the logical
		// database/user. NO remote database resource ID is present.
		require.NotNil(tb, creds)
		assert.Equal(tb, "db.internal", creds.Host)
		assert.Equal(tb, uint16(3306), creds.Port)
		assert.Equal(tb, "workspace_"+itoa(ws.ID), creds.Database)
		assert.Equal(tb, "workspace_"+itoa(ws.ID), creds.Username)
		assert.Equal(tb, wantPass, creds.Password)

		// Logical name/user persisted on the row; password is not.
		require.NotNil(tb, ws.DatabaseName)
		require.NotNil(tb, ws.DatabaseUser)
		assert.Equal(tb, "workspace_"+itoa(ws.ID), *ws.DatabaseName)
		assert.Equal(tb, "workspace_"+itoa(ws.ID), *ws.DatabaseUser)
		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		require.NotNil(tb, persisted.DatabaseName)
		assert.Equal(tb, "workspace_"+itoa(ws.ID), *persisted.DatabaseName)
		assert.Empty(tb, persisted.LastError)
		assert.Nil(tb, persisted.ApplicationResourceID)
	}, workspaceTestOptions)
}

func TestReconcileDatabase_IsIdempotentOnRetry(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		fake := &fakeWorkspaceProvider{}
		eng := &fakeEngineer{}
		svc := newProvisionService(tb, db, fake, nil, eng)

		_, err := svc.ReconcileDatabase(context.Background(), ws)
		require.NoError(tb, err)
		_, err = svc.ReconcileDatabase(context.Background(), ws)
		require.NoError(tb, err)

		// Two repeats produce identical deterministic requests (the underlying
		// MySQL engineer is idempotent: CREATE/GRANT ... IF EXISTS + ALTER
		// USER converges the password). A retry looks up the shared resource
		// each time but never creates anything new.
		require.Len(tb, eng.ensureCalls, 2)
		assert.Equal(tb, eng.ensureCalls[0], eng.ensureCalls[1])
		assert.Equal(tb, 2, fake.resolveCalls)
	}, workspaceTestOptions)
}

func TestReconcileDatabase_ResolvesSharedResourceViaProvider(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		// Resolution always goes through Coolify by the configured shared
		// ResourceID, parsing the internal DB URL and reading the root password
		// (there is no configured admin host/port anymore).
		cfg := sharedDBConfig()
		bc := &core.BaseComponent{}
		bc.SetDB(db)
		spec := pluginConfig.WorkspaceConfig{
			Enabled:  true,
			Database: cfg,
		}
		svc := &WorkspaceService{
			BaseComponent: bc,
			config:        &spec,
			provider:      &fakeWorkspaceProvider{database: runningDatabase()},
			mysqlProv:     &fakeEngineer{},
			identityKey:   testIdentityPrivateKey(),
		}

		creds, err := svc.ReconcileDatabase(context.Background(), ws)
		require.NoError(tb, err)
		require.NotNil(tb, creds)
		assert.Equal(tb, "db.internal", creds.Host)
		assert.Equal(tb, uint16(3306), creds.Port)
	}, workspaceTestOptions)
}

func TestReconcileDatabase_MissingRootPasswordFailsClosed(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		// Coolify reports running with an internal URL but omits the root
		// password (token without read:sensitive). Must fail closed before any
		// logical provisioning.
		missing := runningDatabase()
		missing.RootPassword = ""
		cfg := sharedDBConfig()
		bc := &core.BaseComponent{}
		bc.SetDB(db)
		svc := &WorkspaceService{
			BaseComponent: bc,
			config:        &pluginConfig.WorkspaceConfig{Enabled: true, Database: cfg},
			provider:      &fakeWorkspaceProvider{database: missing},
			mysqlProv:     &fakeEngineer{},
			identityKey:   testIdentityPrivateKey(),
		}

		_, err := svc.ReconcileDatabase(context.Background(), ws)
		require.ErrorIs(tb, err, ErrDatabaseCredentialFieldMissing)
		assert.Nil(tb, svc.mysqlProv.(*fakeEngineer).ensureCalls)
	}, workspaceTestOptions)
}

func TestReconcileDatabase_UnknownResourceTypeFailsClosed(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		// A resource whose type is neither mysql nor mariadb yields no root
		// password, so provisioning fails closed (no admin credentials leak).
		unknown := runningDatabase()
		unknown.Type = "postgresql"
		unknown.RootPassword = ""
		cfg := sharedDBConfig()
		bc := &core.BaseComponent{}
		bc.SetDB(db)
		svc := &WorkspaceService{
			BaseComponent: bc,
			config:        &pluginConfig.WorkspaceConfig{Enabled: true, Database: cfg},
			provider:      &fakeWorkspaceProvider{database: unknown},
			mysqlProv:     &fakeEngineer{},
			identityKey:   testIdentityPrivateKey(),
		}

		_, err := svc.ReconcileDatabase(context.Background(), ws)
		require.ErrorIs(tb, err, ErrDatabaseCredentialFieldMissing)
		assert.Nil(tb, svc.mysqlProv.(*fakeEngineer).ensureCalls)
	}, workspaceTestOptions)
}

func TestReconcileDatabase_MissingSensitiveFields(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		// Coolify reports running but omits the internal DB URL (token without
		// read:sensitive). This is a config/permission error, resolved before
		// any logical provisioning.
		missing := runningDatabase()
		missing.InternalURL = ""
		cfg := sharedDBConfig()
		bc := &core.BaseComponent{}
		bc.SetDB(db)
		svc := &WorkspaceService{
			BaseComponent: bc,
			config:        &pluginConfig.WorkspaceConfig{Enabled: true, Database: cfg},
			provider:      &fakeWorkspaceProvider{database: missing},
			mysqlProv:     &fakeEngineer{},
			identityKey:   testIdentityPrivateKey(),
		}

		_, err := svc.ReconcileDatabase(context.Background(), ws)
		require.ErrorIs(tb, err, ErrDatabaseCredentialFieldMissing)
		// No logical provisioning happened.
		assert.Nil(tb, svc.mysqlProv.(*fakeEngineer).ensureCalls)
	}, workspaceTestOptions)
}

func TestReconcileDatabase_MalformedInternalURL(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		bad := runningDatabase()
		bad.InternalURL = "not a url"
		cfg := sharedDBConfig()
		bc := &core.BaseComponent{}
		bc.SetDB(db)
		svc := &WorkspaceService{
			BaseComponent: bc,
			config:        &pluginConfig.WorkspaceConfig{Enabled: true, Database: cfg},
			provider:      &fakeWorkspaceProvider{database: bad},
			mysqlProv:     &fakeEngineer{},
			identityKey:   testIdentityPrivateKey(),
		}

		_, err := svc.ReconcileDatabase(context.Background(), ws)
		require.ErrorIs(tb, err, ErrDatabaseURLMalformed)
	}, workspaceTestOptions)
}

func TestReconcileDatabase_TerminalServerFailureRecordsLastError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		cfg := sharedDBConfig()
		bc := &core.BaseComponent{}
		bc.SetDB(db)
		svc := &WorkspaceService{
			BaseComponent: bc,
			config:        &pluginConfig.WorkspaceConfig{Enabled: true, Database: cfg},
			provider:      &fakeWorkspaceProvider{database: coolify.DatabaseResource{ID: "shared-db", Status: coolify.ResourceStatusFailed}},
			mysqlProv:     &fakeEngineer{},
			identityKey:   testIdentityPrivateKey(),
		}

		_, err := svc.ReconcileDatabase(context.Background(), ws)
		require.ErrorIs(tb, err, ErrDatabaseServerFailed)
		// no logical provisioning against a dead shared server
		assert.Nil(tb, svc.mysqlProv.(*fakeEngineer).ensureCalls)
	}, workspaceTestOptions)
}

func TestReconcileDatabase_NotReadyIsRetryable(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		cfg := sharedDBConfig()
		bc := &core.BaseComponent{}
		bc.SetDB(db)
		svc := &WorkspaceService{
			BaseComponent: bc,
			config:        &pluginConfig.WorkspaceConfig{Enabled: true, Database: cfg},
			provider:      &fakeWorkspaceProvider{database: coolify.DatabaseResource{ID: "shared-db", Status: coolify.ResourceStatusStarting}},
			mysqlProv:     &fakeEngineer{},
			identityKey:   testIdentityPrivateKey(),
		}

		_, err := svc.ReconcileDatabase(context.Background(), ws)
		require.ErrorIs(tb, err, ErrSharedDatabaseNotReady)
		assert.Equal(tb, catRetryable, classifyError(err).category)
	}, workspaceTestOptions)
}

func TestReconcileDatabase_EnsureFailureRecordsLastError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		fake := &fakeWorkspaceProvider{}
		eng := &fakeEngineer{ensureErr: errors.New("ERROR 1045: access denied")}
		svc := newProvisionService(tb, db, fake, nil, eng)

		_, err := svc.ReconcileDatabase(context.Background(), ws)
		require.ErrorIs(tb, err, ErrDatabaseProvisionFailed)

		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		assert.NotEmpty(tb, persisted.LastError)
		// The failure message and its wrapped error never leak the admin
		// credentials or the generated password.
		assert.NotContains(tb, persisted.LastError, "admin-secret")
		assert.NotContains(tb, persisted.LastError, "test-password-secret")
	}, workspaceTestOptions)
}

func TestDropLogicalDatabase_DropsOnlyLogicalDBUser(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusReady)
		name, user := "workspace_5", "workspace_5"
		ws.DatabaseName, ws.DatabaseUser = &name, &user
		require.NoError(tb, db.Model(ws).Updates(map[string]any{"database_name": name, "database_user": user}).Error)

		fake := &fakeWorkspaceProvider{}
		eng := &fakeEngineer{}
		svc := newProvisionService(tb, db, fake, nil, eng)

		err := svc.DropLogicalDatabase(context.Background(), ws)
		require.NoError(tb, err)

		require.Len(tb, eng.dropCalls, 1)
		assert.Equal(tb, name, eng.dropCalls[0].Database)
		assert.Equal(tb, user, eng.dropCalls[0].User)
		// Only the logical database/user is dropped: the shared resource is
		// looked up (one resolve call) but never stopped or deleted at the
		// provider (the fake has no stop/delete database methods at all).
		assert.Equal(tb, 1, fake.resolveCalls)
	}, workspaceTestOptions)
}

func TestDropLogicalDatabase_NoopWhenNothingProvisioned(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		eng := &fakeEngineer{}
		svc := newProvisionService(tb, db, &fakeWorkspaceProvider{}, nil, eng)

		err := svc.DropLogicalDatabase(context.Background(), ws)
		require.NoError(tb, err)
		assert.Len(tb, eng.dropCalls, 0, "deleting an un-provisioned workspace must not call the engineer")
	}, workspaceTestOptions)
}

func TestReconcileAPIKey_IssueOnFresh(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1) // website 1 owned by user 1
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		mockKey := dashboardCore.NewMockAPIKeyService(tb)
		mockKey.EXPECT().IssueAPIKey(mock.Anything, uint(1), "workspace-"+itoa(ws.ID), apiKeyTTL).
			Return(&dashboardCore.IssuedAPIKey{ID: 42, Token: "jwt-abc", Name: "workspace-" + itoa(ws.ID)}, nil)

		fake := &fakeWorkspaceProvider{}
		svc := newProvisionService(tb, db, fake, mockKey, &fakeEngineer{})

		issued, err := svc.ReconcileAPIKey(context.Background(), ws)
		require.NoError(tb, err)
		require.NotNil(tb, issued)
		assert.Equal(tb, "jwt-abc", issued.Token) // one-time token, in memory only

		require.NotNil(tb, ws.APIKeyID)
		assert.Equal(tb, uint(42), *ws.APIKeyID)
		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		require.NotNil(tb, persisted.APIKeyID)
		assert.Equal(tb, uint(42), *persisted.APIKeyID)
	}, workspaceTestOptions)
}

func TestReconcileAPIKey_ReissueWhenRowExists(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)
		keyID := uint(7)
		require.NoError(tb, db.Model(ws).Update("api_key_id", keyID).Error)
		require.NoError(tb, db.First(ws, ws.ID).Error)

		mockKey := dashboardCore.NewMockAPIKeyService(tb)
		// Reissue, never a second Issue (no duplicate key rows). An unexpected
		// IssueAPIKey call would fail the mock.
		mockKey.EXPECT().ReissueAPIKey(mock.Anything, uint(1), keyID, apiKeyTTL).
			Return(&dashboardCore.IssuedAPIKey{ID: keyID, Token: "jwt-refreshed", Name: "workspace-" + itoa(ws.ID)}, nil)

		fake := &fakeWorkspaceProvider{}
		svc := newProvisionService(tb, db, fake, mockKey, &fakeEngineer{})

		issued, err := svc.ReconcileAPIKey(context.Background(), ws)
		require.NoError(tb, err)
		require.NotNil(tb, issued)
		assert.Equal(tb, "jwt-refreshed", issued.Token)
		assert.Equal(tb, uint(7), *ws.APIKeyID)
	}, workspaceTestOptions)
}

// itoa is a tiny non-allocating uint formatter for the helpers above (the
// tests keep names aligned with the production deterministic naming).
func itoa(v uint) string {
	if v == 0 {
		return "0"
	}
	buf := [20]byte{}
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
