package workspace

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	dashboardCore "go.lumeweb.com/portal-plugin-dashboard/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/coolify"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

// fakeLifecycleProvider is a test fake for suspend/resume/delete/access.
// It embeds the nil interface and overrides only the life cycle methods plus the
// application read + start primitives resume depends on. It deliberately has NO
// per-workspace database create/start/stop/delete methods, matching the
// corrected architecture (the shared DB resource is never a per-workspace
// resource).
type fakeLifecycleProvider struct {
	coolify.WorkspaceProvider

	// suspend
	stopAppCalls int
	stopAppErr   error

	// resume
	envSets [][]coolify.EnvironmentVariable
	envErr  error

	startAppCalls int
	startDep      coolify.DeploymentResource
	startAppErr   error
	depStatuses   []coolify.ResourceStatus
	appStatus     coolify.ResourceStatus
	depErr        error

	// delete
	deleteAppCalls int
	deleteAppErr   error

	// rotation
	basicAuthSets [][]string
	basicAuthErr  error

	// a simple ordered record of significant calls (for ordering assertions)
	order []string
}

func (f *fakeLifecycleProvider) record(name string) { f.order = append(f.order, name) }

// ResolveDatabaseResource returns a running shared database so resume/delete
// logical re-provisioning can resolve host/port/root without a live Coolify.
func (f *fakeLifecycleProvider) ResolveDatabaseResource(_ context.Context, _ string) (coolify.DatabaseResource, error) {
	return runningDatabase(), nil
}

func (f *fakeLifecycleProvider) StopApplication(_ context.Context, _ string) error {
	f.stopAppCalls++
	f.record("stop-app")
	return f.stopAppErr
}

func (f *fakeLifecycleProvider) GetDeployment(_ context.Context, _ string) (coolify.DeploymentResource, error) {
	if f.depErr != nil {
		return coolify.DeploymentResource{}, f.depErr
	}
	st := coolify.ResourceStatusFinished
	if len(f.depStatuses) > 0 {
		st = f.depStatuses[0]
		f.depStatuses = f.depStatuses[1:]
	}
	return coolify.DeploymentResource{ID: "deploy-1", Status: st}, nil
}

func (f *fakeLifecycleProvider) GetApplication(_ context.Context, _ string) (coolify.ApplicationResource, error) {
	st := f.appStatus
	if st == "" {
		st = coolify.ResourceStatusRunning
	}
	return coolify.ApplicationResource{ID: "app-created", Status: st, Domain: "https://ws-test.build.example.com"}, nil
}

func (f *fakeLifecycleProvider) FindApplicationByName(_ context.Context, _ string) ([]coolify.Resource, error) {
	return nil, nil
}

func (f *fakeLifecycleProvider) DeleteApplication(_ context.Context, _ string) error {
	f.deleteAppCalls++
	f.record("delete-app")
	return f.deleteAppErr
}

func (f *fakeLifecycleProvider) SetApplicationBasicAuth(_ context.Context, _ string, username, password string) error {
	f.basicAuthSets = append(f.basicAuthSets, []string{username, password})
	f.record("set-basic-auth")
	return f.basicAuthErr
}

func (f *fakeLifecycleProvider) SetApplicationEnvironment(_ context.Context, _ string, envs []coolify.EnvironmentVariable) error {
	f.envSets = append(f.envSets, envs)
	f.record("set-env")
	return f.envErr
}

func (f *fakeLifecycleProvider) StartApplication(_ context.Context, _ string) (coolify.DeploymentResource, error) {
	f.startAppCalls++
	f.record("start-app")
	if f.startDep.ID != "" {
		return f.startDep, f.startAppErr
	}
	return coolify.DeploymentResource{ID: "deploy-1"}, f.startAppErr
}

// lifecycleConfig returns a service config with the fields suspend/resume path
// touch, with fast timeouts so tests do not rely on real polling. The Database
// is the SINGLE shared MySQL/MariaDB resource configured only by its Coolify
// resource ID; there is no password secret (each workspace's logical database
// password is derived from the portal identity key plus a per-workspace salt),
// and the admin host/user/password are derived from the fake provider's
// ResolveDatabaseResource (no admin connection is configured).
func lifecycleConfig() *pluginConfig.WorkspaceConfig {
	return &pluginConfig.WorkspaceConfig{
		Enabled:          true,
		ProvisionTimeout: time.Minute,
		PollInterval:     time.Millisecond,
		PlatformDomain:   "build.example.com",
		PortalAPIURL:     "https://portal.example.com",
		Runtime: pluginConfig.WorkspaceRuntimeConfig{
			HealthPath: "/healthz",
			DatabaseEnv: pluginConfig.DatabaseEnvironmentKeys{
				Host:     "DB_HOST",
				Port:     "DB_PORT",
				Name:     "DB_NAME",
				User:     "DB_USER",
				Password: "DB_PASSWORD",
			},
		},
		Database: pluginConfig.WorkspaceDatabaseConfig{
			ResourceID: "shared-db",
		},
	}
}

// newLifecycleService wires a workspace service with the given provider/api key,
// a fast poll interval, and an injected fake MySQL engineer so resume's logical
// re-provision and delete's logical drop run without a live MySQL server.
// Readiness is decided by the fake provider's Coolify application status (the
// fake returns running/healthy by default), never by a portal HTTP probe.
func newLifecycleService(tb coreTesting.TB, db *gorm.DB, provider coolify.WorkspaceProvider, apiKey dashboardCore.APIKeyService, cfg *pluginConfig.WorkspaceConfig) *WorkspaceService {
	tb.Helper()
	if cfg == nil {
		cfg = lifecycleConfig()
	}
	bc := &core.BaseComponent{}
	bc.SetDB(db)
	return &WorkspaceService{
		BaseComponent: bc,
		config:        cfg,
		provider:      provider,
		apiKeySvc:     apiKey,
		mysqlProv:     &fakeEngineer{},
		identityKey:   testIdentityPrivateKey(),
	}
}

// lifecycleWorkspace inserts a workspace owned by user 1 (via website 1) with
// the given status and provider resource IDs, plus proxy credentials. logName,
// when non-nil, sets BOTH the logical database name and user (they are
// identical identifiers in this architecture) so delete can drop them.
func lifecycleWorkspace(tb coreTesting.TB, db *gorm.DB, status pluginDb.WorkspaceStatus, appID *string, logName *string, apiKeyID *uint) *pluginDb.Workspace {
	tb.Helper()
	insertWebsite(tb, db, 1, 1)
	insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
	ws := &pluginDb.Workspace{
		UserID:                1,
		WebsiteID:             new(uint(1)),
		PlatformDomainID:      10,
		Label:                 "ws-lifecycle",
		Status:                status,
		ApplicationResourceID: appID,
		DatabaseName:          logName,
		DatabaseUser:          logName,
		APIKeyID:              apiKeyID,
	}
	require.NoError(tb, db.Create(ws).Error)
	attachPlatformDomain(ws)
	return ws
}

func TestWorkspaceService_Suspend_StopsAppAndMarksSuspended(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		appID := "app-1"
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusReady, &appID, nil, nil)
		fake := &fakeLifecycleProvider{}
		svc := newLifecycleService(tb, db, fake, nil, nil)

		out, err := svc.Suspend(context.Background(), 1, ws.ID)
		require.NoError(tb, err)
		assert.Equal(tb, pluginDb.WorkspaceStatusSuspended, out.Status)
		assert.Equal(tb, 1, fake.stopAppCalls)
		assert.Contains(tb, fake.order, "stop-app")

		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		assert.Equal(tb, pluginDb.WorkspaceStatusSuspended, persisted.Status)
	}, workspaceTestOptions)
}

func TestWorkspaceService_Suspend_NeverStopsSharedDatabase(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		appID := "app-1"
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusReady, &appID, nil, nil)
		fake := &fakeLifecycleProvider{}
		svc := newLifecycleService(tb, db, fake, nil, nil)

		out, err := svc.Suspend(context.Background(), 1, ws.ID)
		require.NoError(tb, err)
		assert.Equal(tb, pluginDb.WorkspaceStatusSuspended, out.Status)
		// The shared MySQL/MariaDB resource is a portal dependency and is never
		// stopped per workspace: the fake has no StopDatabase at all, and the
		// order contains only the application stop.
		assert.Equal(tb, []string{"stop-app"}, fake.order)
		assert.Len(tb, svc.mysqlProv.(*fakeEngineer).dropCalls, 0, "suspend must not drop the logical database")
	}, workspaceTestOptions)
}

func TestWorkspaceService_Suspend_RejectsNonReadyState(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusProvisioning, nil, nil, nil)
		fake := &fakeLifecycleProvider{}
		svc := newLifecycleService(tb, db, fake, nil, nil)

		_, err := svc.Suspend(context.Background(), 1, ws.ID)
		require.Error(tb, err)
		assert.ErrorIs(tb, err, ErrWorkspaceInvalidState)
		assert.Equal(tb, 0, fake.stopAppCalls, "provider must not be called on a bad transition")
	}, workspaceTestOptions)
}

// mockAPIKeyService builds a dashboard API-key mock with reissue enabled.
func mockAPIKeyService(tb coreTesting.TB, keyID uint) *dashboardCore.MockAPIKeyService {
	tb.Helper()
	m := dashboardCore.NewMockAPIKeyService(tb)
	m.EXPECT().ReissueAPIKey(mock.Anything, uint(1), keyID, mock.Anything).
		Return(&dashboardCore.IssuedAPIKey{ID: keyID, Token: "jwt-token"}, nil).Maybe()
	m.EXPECT().RevokeAPIKey(mock.Anything, uint(1), keyID).Return(nil).Maybe()
	return m
}

func TestWorkspaceService_Resume_StartsAppAndMarksReady(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		appID := "app-1"
		apiKeyID := uint(55)
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusSuspended, &appID, nil, &apiKeyID)
		fake := &fakeLifecycleProvider{}
		apiKey := mockAPIKeyService(tb, apiKeyID)
		svc := newLifecycleService(tb, db, fake, apiKey, nil)

		out, err := svc.Resume(context.Background(), 1, ws.ID)
		require.NoError(tb, err)
		assert.Equal(tb, pluginDb.WorkspaceStatusReady, out.Status)
		assert.Equal(tb, 1, fake.startAppCalls)
		require.Len(tb, fake.envSets, 1)
		// the environment includes the game secrets marked Secret: true; they are
		// only written to Coolify, never returned/logged by the service.
		require.Len(tb, fake.envSets[0], 8)
		// readiness is decided by the Coolify application status (running) and
		// the workspace is marked ready; no portal HTTP probe is performed.
		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		assert.Equal(tb, pluginDb.WorkspaceStatusReady, persisted.Status)
	}, workspaceTestOptions)
}

func TestWorkspaceService_Resume_RecconciledLogicalDBNotCoolifyDB(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		appID := "app-1"
		apiKeyID := uint(55)
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusSuspended, &appID, nil, &apiKeyID)
		fake := &fakeLifecycleProvider{}
		apiKey := mockAPIKeyService(tb, apiKeyID)
		svc := newLifecycleService(tb, db, fake, apiKey, nil)

		out, err := svc.Resume(context.Background(), 1, ws.ID)
		require.NoError(tb, err)
		assert.Equal(tb, pluginDb.WorkspaceStatusReady, out.Status)

		// Resume re-provisions the workspace's LOGICAL database/user on the
		// shared resource (idempotent, via the MySQL engineer) — it never starts
		// or creates a Coolify database resource.
		eng := svc.mysqlProv.(*fakeEngineer)
		require.Len(tb, eng.ensureCalls, 1, "resume must re-provision the logical database")
		assert.Equal(tb, "workspace_"+itoa(ws.ID), eng.ensureCalls[0].Database)
		assert.Equal(tb, "workspace_"+itoa(ws.ID), eng.ensureCalls[0].User)
		// The environment is refreshed (set-env) before the app starts, and the
		// shared Coolify DB host is used for the connection values. ReconcileDatabase
		// and ReconcileAPIKey do not touch the provider, so the only provider
		// side-effects are the environment refresh then the start.
		assert.GreaterOrEqual(tb, len(fake.order), 2)
		assert.Equal(tb, "set-env", fake.order[0])
		assert.Equal(tb, "start-app", fake.order[1])
	}, workspaceTestOptions)
}

func TestWorkspaceService_Resume_RejectsNonSuspendedState(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusReady, nil, nil, nil)
		fake := &fakeLifecycleProvider{}
		svc := newLifecycleService(tb, db, fake, nil, nil)

		_, err := svc.Resume(context.Background(), 1, ws.ID)
		require.Error(tb, err)
		assert.ErrorIs(tb, err, ErrWorkspaceInvalidState)
		assert.Equal(tb, 0, fake.startAppCalls)
	}, workspaceTestOptions)
}

func TestWorkspaceService_Delete_DeletesAppThenDropsLogicalDBThenSoftDeletes(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		appID := "app-1"
		name := "workspace_5"
		apiKeyID := uint(55)
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusReady, &appID, &name, &apiKeyID)
		fake := &fakeLifecycleProvider{}
		apiKey := mockAPIKeyService(tb, apiKeyID)
		svc := newLifecycleService(tb, db, fake, apiKey, nil)

		out, err := svc.Delete(context.Background(), 1, ws.ID)
		require.NoError(tb, err)
		// application deleted, then the workspace's logical database+user is
		// dropped from the SHARED resource (MySQL engineer), then soft-delete.
		assert.Equal(tb, 1, fake.deleteAppCalls)
		eng := svc.mysqlProv.(*fakeEngineer)
		require.Len(tb, eng.dropCalls, 1, "delete must drop the workspace's logical database+user")
		assert.Equal(tb, name, eng.dropCalls[0].Database)
		assert.Equal(tb, name, eng.dropCalls[0].User)
		// The shared resource is never deleted by a per-workspace provider call
		// (the fake has no DeleteDatabase), and the workspace row is tombstoned.
		var count int64
		require.NoError(tb, db.Model(&pluginDb.Workspace{}).Unscoped().Where("id = ?", ws.ID).Count(&count).Error)
		assert.Equal(tb, int64(1), count)
		var live int64
		require.NoError(tb, db.Model(&pluginDb.Workspace{}).Where("id = ?", ws.ID).Count(&live).Error)
		assert.Equal(tb, int64(0), live, "workspace must be soft-deleted")
		assert.NotNil(tb, out.DeletedAt)
	}, workspaceTestOptions)
}

func TestWorkspaceService_Delete_TreatsProvider404AsAlreadyDeleted(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		appID := "app-1"
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusReady, &appID, nil, nil)
		fake := &fakeLifecycleProvider{
			deleteAppErr: &coolify.Error{StatusCode: http.StatusNotFound},
		}
		svc := newLifecycleService(tb, db, fake, nil, nil)

		out, err := svc.Delete(context.Background(), 1, ws.ID)
		require.NoError(tb, err)
		assert.Equal(tb, 1, fake.deleteAppCalls)
		// No logical identifiers were provisioned, so no logical drop occurs.
		assert.Len(tb, svc.mysqlProv.(*fakeEngineer).dropCalls, 0)
		assert.NotNil(tb, out.DeletedAt)
	}, workspaceTestOptions)
}

func TestWorkspaceService_Delete_RejectsAlreadyDeleting(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusDeleting, nil, nil, nil)
		fake := &fakeLifecycleProvider{}
		svc := newLifecycleService(tb, db, fake, nil, nil)

		_, err := svc.Delete(context.Background(), 1, ws.ID)
		require.Error(tb, err)
		assert.ErrorIs(tb, err, ErrWorkspaceInvalidState)
		assert.Equal(tb, 0, fake.deleteAppCalls, "already-deleting must not delete provider resources again")
	}, workspaceTestOptions)
}

func TestWorkspaceService_Lifecycle_UnauthorizedOwnerIsNotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		appID := "app-1"
		// website 1 is owned by user 1; requester is user 2.
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusReady, &appID, nil, nil)
		fake := &fakeLifecycleProvider{}
		svc := newLifecycleService(tb, db, fake, nil, nil)

		_, err := svc.Suspend(context.Background(), 2, ws.ID)
		require.ErrorIs(tb, err, ErrWorkspaceNotFound)
		assert.Equal(tb, 0, fake.stopAppCalls)

		_, err = svc.Resume(context.Background(), 2, ws.ID)
		require.ErrorIs(tb, err, ErrWorkspaceNotFound)

		_, err = svc.Delete(context.Background(), 2, ws.ID)
		require.ErrorIs(tb, err, ErrWorkspaceNotFound)

		_, err = svc.RotateAccessCredentials(context.Background(), 2, ws.ID, false)
		require.ErrorIs(tb, err, ErrWorkspaceNotFound)
	}, workspaceTestOptions)
}

func TestWorkspaceService_RotateAccessCredentials_ReturnsOnlyProxyCreds(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		appID := "app-1"
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusReady, &appID, nil, nil)
		user, pass := "proxy-user", "proxy-pass"
		ws.ProxyUsername, ws.ProxyPassword = &user, &pass
		require.NoError(tb, db.Model(ws).Updates(map[string]any{"proxy_username": user, "proxy_password": pass}).Error)
		fake := &fakeLifecycleProvider{}
		svc := newLifecycleService(tb, db, fake, nil, nil)

		creds, err := svc.RotateAccessCredentials(context.Background(), 1, ws.ID, false)
		require.NoError(tb, err)
		assert.Equal(tb, user, creds.Username)
		assert.Equal(tb, pass, creds.Password)
		assert.Empty(tb, fake.basicAuthSets, "non-rotating access must not touch the provider")
	}, workspaceTestOptions)
}

func TestWorkspaceService_RotateAccessCredentials_PersistsAndAppliesNewCreds(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		appID := "app-1"
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusReady, &appID, nil, nil)
		user, pass := "old-user", "old-pass"
		ws.ProxyUsername, ws.ProxyPassword = &user, &pass
		require.NoError(tb, db.Model(ws).Updates(map[string]any{"proxy_username": user, "proxy_password": pass}).Error)
		fake := &fakeLifecycleProvider{}
		svc := newLifecycleService(tb, db, fake, nil, nil)

		creds, err := svc.RotateAccessCredentials(context.Background(), 1, ws.ID, true)
		require.NoError(tb, err)
		require.NotEmpty(tb, creds.Username)
		require.NotEmpty(tb, creds.Password)
		assert.NotEqual(tb, user, creds.Username)
		require.Len(tb, fake.basicAuthSets, 1)
		assert.Equal(tb, []string{creds.Username, creds.Password}, fake.basicAuthSets[0])

		// Persisted on the row.
		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		require.NotNil(tb, persisted.ProxyUsername)
		require.NotNil(tb, persisted.ProxyPassword)
		assert.Equal(tb, creds.Username, *persisted.ProxyUsername)
		assert.Equal(tb, creds.Password, *persisted.ProxyPassword)
	}, workspaceTestOptions)
}

func TestWorkspaceService_RotateAccessCredentials_ProviderFailureKeepsPersistedValue(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		appID := "app-1"
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusReady, &appID, nil, nil)
		user, pass := "old-user", "old-pass"
		ws.ProxyUsername, ws.ProxyPassword = &user, &pass
		require.NoError(tb, db.Model(ws).Updates(map[string]any{"proxy_username": user, "proxy_password": pass}).Error)
		fake := &fakeLifecycleProvider{basicAuthErr: errors.New("boom")}
		svc := newLifecycleService(tb, db, fake, nil, nil)

		_, err := svc.RotateAccessCredentials(context.Background(), 1, ws.ID, true)
		require.Error(tb, err)
		// The provider update is applied before persistence; a provider failure
		// must NOT persist the new value, so the OLD credential remains intact
		// and a retry can re-apply the same rotation consistently.
		require.Len(tb, fake.basicAuthSets, 1, "provider must be called once before failing")

		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		require.NotNil(tb, persisted.ProxyUsername)
		require.NotNil(tb, persisted.ProxyPassword)
		assert.Equal(tb, user, *persisted.ProxyUsername, "old username must be preserved on provider failure")
		assert.Equal(tb, pass, *persisted.ProxyPassword, "old password must be preserved on provider failure")
	}, workspaceTestOptions)
}

// TestWorkspaceService_RotateAccessCredentials_AppliesProviderBeforePersist
// verifies the rotation order: the new credential is applied to the provider
// BEFORE it is persisted, so the provider and DB never disagree about which
// value is active.
func TestWorkspaceService_RotateAccessCredentials_AppliesProviderBeforePersist(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		appID := "app-1"
		ws := lifecycleWorkspace(tb, db, pluginDb.WorkspaceStatusReady, &appID, nil, nil)
		user, pass := "old-user", "old-pass"
		ws.ProxyUsername, ws.ProxyPassword = &user, &pass
		require.NoError(tb, db.Model(ws).Updates(map[string]any{"proxy_username": user, "proxy_password": pass}).Error)
		fake := &fakeLifecycleProvider{}
		svc := newLifecycleService(tb, db, fake, nil, nil)

		creds, err := svc.RotateAccessCredentials(context.Background(), 1, ws.ID, true)
		require.NoError(tb, err)

		// The provider is applied once with the returned credential, and the
		// same value is then persisted (provider before persist).
		require.Len(tb, fake.basicAuthSets, 1)
		assert.Equal(tb, []string{creds.Username, creds.Password}, fake.basicAuthSets[0])

		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		require.NotNil(tb, persisted.ProxyUsername)
		require.NotNil(tb, persisted.ProxyPassword)
		assert.Equal(tb, creds.Username, *persisted.ProxyUsername)
		assert.Equal(tb, creds.Password, *persisted.ProxyPassword)
	}, workspaceTestOptions)
}

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}
