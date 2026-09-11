package workspace

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dashboardCore "go.lumeweb.com/portal-plugin-dashboard/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/coolify"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

// attachPlatformDomain sets the workspace's PlatformDomain on the in-memory
// object so the service's ensurePlatformDomain short-circuits instead of
// reloading the row from the DB (which would wipe in-memory-only fields such as
// injected proxy credentials). The DB row keeps platform_domain_id=10.
func attachPlatformDomain(ws *pluginDb.Workspace) *pluginDb.Workspace {
	ws.PlatformDomain = pluginDb.PlatformDomain{ID: ws.PlatformDomainID, Domain: "build.example.com"}
	return ws
}

// fakeAppProvider is a test fake for the provider-neutral coolify.WorkspaceProvider
// focused on the application phase. It embeds the interface (nil) and overrides
// only the application-phase methods under test.
type fakeAppProvider struct {
	coolify.WorkspaceProvider

	createCalls int
	lastCreate  coolify.CreateApplicationRequest
	createFn    func(coolify.CreateApplicationRequest) (coolify.CreatedResource, error)

	findCalls int
	// resources is returned by FindApplicationByName (the name fallback when no
	// installation tag is configured).
	resources []coolify.Resource
	// tagResources is returned by FindApplicationsByTag (the tag-scoped
	// recovery path). Simulates Coolify v4.3.19 returning only the applications
	// carrying the installation tag.
	tagResources []coolify.Resource
	lastTag      string
	findErr      error

	getCalls  int
	getErr    error
	appStatus coolify.ResourceStatus
	// appStatuses, when set, is consumed as a queue by GetApplication (so a
	// wait loop can observe starting -> running transitions).
	appStatuses []coolify.ResourceStatus
	app         coolify.ApplicationResource

	envSets [][]coolify.EnvironmentVariable
	envErr  error

	storageCalls int
	storageMount []coolify.StorageMount
	storageErr   error

	startCalls int
	startDep   coolify.DeploymentResource
	startErr   error

	depStatuses []coolify.ResourceStatus
	depErr      error
}

func (f *fakeAppProvider) CreateApplication(_ context.Context, req coolify.CreateApplicationRequest) (coolify.CreatedResource, error) {
	f.createCalls++
	f.lastCreate = req
	if f.createFn != nil {
		return f.createFn(req)
	}
	return coolify.CreatedResource{UUID: "app-created"}, nil
}

func (f *fakeAppProvider) FindApplicationByName(_ context.Context, _ string) ([]coolify.Resource, error) {
	f.findCalls++
	if f.findErr != nil {
		return nil, f.findErr
	}
	return f.resources, nil
}

func (f *fakeAppProvider) FindApplicationsByTag(_ context.Context, tag string) ([]coolify.Resource, error) {
	f.findCalls++
	f.lastTag = tag
	if f.findErr != nil {
		return nil, f.findErr
	}
	return f.tagResources, nil
}

func (f *fakeAppProvider) GetApplication(_ context.Context, _ string) (coolify.ApplicationResource, error) {
	f.getCalls++
	if f.getErr != nil {
		return coolify.ApplicationResource{}, f.getErr
	}
	a := f.app
	if a.ID == "" {
		a.ID = "app-created"
	}
	if len(f.appStatuses) > 0 {
		a.Status = f.appStatuses[0]
		f.appStatuses = f.appStatuses[1:]
	} else if a.Status == "" {
		a.Status = f.appStatus
	}
	if a.Status == "" {
		a.Status = coolify.ResourceStatusRunning
	}
	a.Domain = "https://ws-test.build.example.com"
	return a, nil
}

func (f *fakeAppProvider) SetApplicationEnvironment(_ context.Context, _ string, envs []coolify.EnvironmentVariable) error {
	f.envSets = append(f.envSets, envs)
	return f.envErr
}

func (f *fakeAppProvider) EnsureApplicationStorage(_ context.Context, _ string, mounts []coolify.StorageMount) error {
	f.storageCalls++
	f.storageMount = mounts
	return f.storageErr
}

func (f *fakeAppProvider) StartApplication(_ context.Context, _ string) (coolify.DeploymentResource, error) {
	f.startCalls++
	return f.startDep, f.startErr
}

func (f *fakeAppProvider) GetDeployment(_ context.Context, _ string) (coolify.DeploymentResource, error) {
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

// appRuntimeConfig returns a runtime config wired for the application tests.
func appRuntimeConfig() pluginConfig.WorkspaceRuntimeConfig {
	return pluginConfig.WorkspaceRuntimeConfig{
		Image:       "wordpress",
		Tag:         "6.7",
		Port:        80,
		HealthPath:  "/wp-admin/install.php",
		MemoryLimit: "512m",
		Storage: []pluginConfig.WorkspaceStorageConfig{
			{NameSuffix: "data", MountPath: "/var/www/html"},
		},
		DatabaseEnv: pluginConfig.DatabaseEnvironmentKeys{
			Host:     "WORDPRESS_DB_HOST",
			Port:     "WORDPRESS_DB_PORT",
			Name:     "WORDPRESS_DB_NAME",
			User:     "WORDPRESS_DB_USER",
			Password: "WORDPRESS_DB_PASSWORD",
		},
	}
}

// newAppService builds a workspace service wired to a DB, an application-phase
// fake provider, and a fast poll interval + runtime config.
func newAppService(tb coreTesting.TB, db *gorm.DB, provider *fakeAppProvider, rt pluginConfig.WorkspaceRuntimeConfig) *WorkspaceService {
	tb.Helper()
	bc := &core.BaseComponent{}
	bc.SetDB(db)
	return &WorkspaceService{
		BaseComponent: bc,
		config: &pluginConfig.WorkspaceConfig{
			Enabled:          true,
			ProvisionTimeout: time.Minute,
			PollInterval:     time.Millisecond,
			PortalAPIURL:     "https://api.example.com",
			Provider: pluginConfig.WorkspaceProviderConfig{
				ServerUUID:      "srv-1",
				ProjectUUID:     "proj-1",
				EnvironmentUUID: "env-1",
				DestinationUUID: "dest-1",
			},
			Runtime:  rt,
			Database: pluginConfig.WorkspaceDatabaseConfig{ResourceID: "shared-db-resource"},
		},
		provider: provider,
	}
}

func TestReconcileApplication_CreatePersistsIDAndProxyCreds(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))

		fake := &fakeAppProvider{appStatus: coolify.ResourceStatusRunning}
		svc := newAppService(tb, db, fake, appRuntimeConfig())
		// This installation shares placement with other installations, so it
		// sets a namespace: the deterministic name is prefixed and every
		// application carries the installation-scoped tag.
		svc.config.Provider.InstallNamespace = "portalx"

		res, err := svc.ReconcileApplication(context.Background(), ws)
		require.NoError(tb, err)
		require.NotNil(tb, res)

		// Exactly one create, deterministic name, correct placement and runtime
		// settings from config.
		require.Equal(tb, 1, fake.createCalls)
		req := fake.lastCreate
		// Namespace-prefixed deterministic name.
		assert.Equal(tb, "portalx-workspace-"+itoa(ws.ID)+"-app", req.Name)
		// The installation-scoped tag is sent (Coolify v4.3.19 persists
		// application tags), enabling tag-scoped recovery.
		assert.Equal(tb, []string{"portalx-workspaces"}, req.Tags)
		assert.Equal(tb, "srv-1", req.ServerUUID)
		assert.Equal(tb, "proj-1", req.ProjectUUID)
		assert.Equal(tb, "env-1", req.EnvironmentUUID)
		assert.Equal(tb, "dest-1", req.DestinationUUID)
		assert.Equal(tb, "wordpress", req.Image)
		assert.Equal(tb, "6.7", req.Tag)
		assert.Equal(tb, "80", req.Port)
		// Hostname from the workspace label + platform domain, HTTPS with
		// noindex for that hostname.
		assert.Equal(tb, "https://ws-test.build.example.com", req.Domain)
		assert.Equal(tb, []string{"ws-test.build.example.com"}, req.NoindexDomains)
		// Proxy Basic Auth enabled with generated credentials (a strong random
		// password, separate from the portal API key).
		require.NotEmpty(tb, req.BasicAuthUsername)
		require.NotEmpty(tb, req.BasicAuthPassword)
		assert.GreaterOrEqual(tb, len(req.BasicAuthPassword), 32)

		// Resource ID persisted immediately; proxy credentials persisted.
		require.NotNil(tb, ws.ApplicationResourceID)
		assert.Equal(tb, "app-created", *ws.ApplicationResourceID)
		require.NotNil(tb, ws.ProxyUsername)
		require.NotNil(tb, ws.ProxyPassword)
		assert.Equal(tb, req.BasicAuthUsername, *ws.ProxyUsername)
		assert.Equal(tb, req.BasicAuthPassword, *ws.ProxyPassword)

		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		require.NotNil(tb, persisted.ApplicationResourceID)
		assert.Equal(tb, "app-created", *persisted.ApplicationResourceID)
		require.NotNil(tb, persisted.ProxyUsername)
	}, workspaceTestOptions)
}

func TestReconcileApplication_ProxyCredsReusedOnResume(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))

		fake := &fakeAppProvider{appStatus: coolify.ResourceStatusRunning}
		svc := newAppService(tb, db, fake, appRuntimeConfig())

		_, err := svc.ReconcileApplication(context.Background(), ws)
		require.NoError(tb, err)
		firstUser, firstPass := *ws.ProxyUsername, *ws.ProxyPassword

		// A second pass (e.g. re-reconcile) reuses the same credentials and
		// does not create a second application.
		var reloaded pluginDb.Workspace
		require.NoError(tb, db.Preload("PlatformDomain").First(&reloaded, ws.ID).Error)
		_, err = svc.ReconcileApplication(context.Background(), &reloaded)
		require.NoError(tb, err)

		assert.Equal(tb, 1, fake.createCalls)
		require.NotNil(tb, reloaded.ProxyUsername)
		require.NotNil(tb, reloaded.ProxyPassword)
		assert.Equal(tb, firstUser, *reloaded.ProxyUsername)
		assert.Equal(tb, firstPass, *reloaded.ProxyPassword)
	}, workspaceTestOptions)
}

func TestReconcileApplication_PartialFailureResumeDoesNotDuplicate(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))

		// First pass: create persists the ID, but a transient GetApplication
		// failure surfaces after provisioning. The ID is already persisted, so
		// a second pass must never create again.
		fake := &fakeAppProvider{appStatus: coolify.ResourceStatusRunning, getErr: errors.New("transient: read timeout")}
		svc := newAppService(tb, db, fake, appRuntimeConfig())

		_, err := svc.ReconcileApplication(context.Background(), ws)
		require.Error(tb, err)

		// Reload fresh (as a reconciler restart would) and retry.
		var resumed pluginDb.Workspace
		require.NoError(tb, db.Preload("PlatformDomain").First(&resumed, ws.ID).Error)
		require.NotNil(tb, resumed.ApplicationResourceID)
		fake.getErr = nil

		_, err = svc.ReconcileApplication(context.Background(), &resumed)
		require.NoError(tb, err)

		// Exactly one create across both passes.
		assert.Equal(tb, 1, fake.createCalls)
		assert.GreaterOrEqual(tb, fake.getCalls, 2)
	}, workspaceTestOptions)
}

func TestReconcileApplication_AdoptExistingAfterAmbiguousCreate(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))

		// No installation namespace is configured, so recovery falls back to a
		// deterministic-name listing (FindApplicationByName).
		name := "workspace-" + itoa(ws.ID) + "-app"
		fake := &fakeAppProvider{
			resources: []coolify.Resource{
				{ID: "app-existing", Name: name, Type: "application", Status: coolify.ResourceStatusRunning},
			},
			appStatus: coolify.ResourceStatusRunning,
		}
		svc := newAppService(tb, db, fake, appRuntimeConfig())

		_, err := svc.ReconcileApplication(context.Background(), ws)
		require.NoError(tb, err)

		// Adopted via the deterministic-name fallback, not created.
		assert.Equal(tb, 0, fake.createCalls)
		require.NotNil(tb, ws.ApplicationResourceID)
		assert.Equal(tb, "app-existing", *ws.ApplicationResourceID)
	}, workspaceTestOptions)
}

func TestReconcileApplication_TagScopedAdoptionExactName(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))

		name := "portalx-workspace-" + itoa(ws.ID) + "-app"
		fake := &fakeAppProvider{
			// Simulates GET /applications?tag=portalx-workspaces returning this
			// installation's apps: the same-named otherx app is absent because
			// it does not carry this installation's tag.
			tagResources: []coolify.Resource{
				{ID: "app-tagged", Name: name, Type: "application", Status: coolify.ResourceStatusRunning},
			},
			appStatus: coolify.ResourceStatusRunning,
		}
		svc := newAppService(tb, db, fake, appRuntimeConfig())
		svc.config.Provider.InstallNamespace = "portalx"

		_, err := svc.ReconcileApplication(context.Background(), ws)
		require.NoError(tb, err)

		// Tag-scoped adoption: queried by the installation tag and matched the
		// exact deterministic name; adopted, not created.
		assert.Equal(tb, "portalx-workspaces", fake.lastTag)
		assert.Equal(tb, 0, fake.createCalls)
		require.NotNil(tb, ws.ApplicationResourceID)
		assert.Equal(tb, "app-tagged", *ws.ApplicationResourceID)
	}, workspaceTestOptions)
}

func TestReconcileApplication_SameNameWithoutTagNotAdopted(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))

		name := "portalx-workspace-" + itoa(ws.ID) + "-app"
		fake := &fakeAppProvider{
			// A same-named resource EXISTS but carries a different/other tag, so
			// the tag-scoped query returns nothing for this installation.
			resources: []coolify.Resource{
				{ID: "app-other-tag", Name: name, Type: "application", Status: coolify.ResourceStatusRunning},
			},
			tagResources: nil, // no application with the installation tag
			appStatus:    coolify.ResourceStatusRunning,
		}
		svc := newAppService(tb, db, fake, appRuntimeConfig())
		svc.config.Provider.InstallNamespace = "portalx"

		_, err := svc.ReconcileApplication(context.Background(), ws)
		require.NoError(tb, err)

		// Because recovery is tag-scoped, the same-named resource without the
		// installation tag is NOT adopted; a new application is created.
		assert.Equal(tb, "portalx-workspaces", fake.lastTag)
		assert.Equal(tb, 1, fake.createCalls)
		require.NotNil(tb, ws.ApplicationResourceID)
		assert.Equal(tb, "app-created", *ws.ApplicationResourceID)
	}, workspaceTestOptions)
}

func TestReconcileApplication_TagScopedAmbiguityFailsClosed(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))

		name := "portalx-workspace-" + itoa(ws.ID) + "-app"
		fake := &fakeAppProvider{
			// Two applications carry the installation tag and share the exact
			// deterministic name: ambiguous, must fail closed.
			tagResources: []coolify.Resource{
				{ID: "app-1", Name: name, Type: "application", Status: coolify.ResourceStatusRunning},
				{ID: "app-2", Name: name, Type: "application", Status: coolify.ResourceStatusRunning},
			},
		}
		svc := newAppService(tb, db, fake, appRuntimeConfig())
		svc.config.Provider.InstallNamespace = "portalx"

		_, err := svc.ReconcileApplication(context.Background(), ws)
		require.ErrorIs(tb, err, ErrApplicationAdoptionAmbiguous)
		// Nothing adopted, nothing created.
		assert.Equal(tb, 0, fake.createCalls)
		require.Nil(tb, ws.ApplicationResourceID)
	}, workspaceTestOptions)
}

func TestReconcileApplication_DomainConflictIsPermanent(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))

		fake := &fakeAppProvider{
			createFn: func(coolify.CreateApplicationRequest) (coolify.CreatedResource, error) {
				return coolify.CreatedResource{}, &coolify.Error{StatusCode: 409, Message: "domain in use"}
			},
		}
		svc := newAppService(tb, db, fake, appRuntimeConfig())

		_, err := svc.ReconcileApplication(context.Background(), ws)
		require.ErrorIs(tb, err, ErrApplicationDomainConflict)

		// The conflict is recorded and no application resource id is set.
		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		assert.NotEmpty(tb, persisted.LastError)
		assert.Nil(tb, persisted.ApplicationResourceID)
	}, workspaceTestOptions)
}

func TestSetApplicationEnvironment_SecretsAndKeys(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))

		fake := &fakeAppProvider{}
		svc := newAppService(tb, db, fake, appRuntimeConfig())

		dbCreds := &DatabaseCredentials{
			Host: "db.internal", Port: 3306, Database: "wsdb",
			Username: "ws", Password: "db-super-secret",
		}
		apiKey := &dashboardCore.IssuedAPIKey{ID: 42, Token: "portal-jwt-secret", Name: "workspace-" + itoa(ws.ID)}

		err := svc.SetApplicationEnvironment(context.Background(), ws, "app-created", dbCreds, apiKey)
		require.NoError(tb, err)
		require.Len(tb, fake.envSets, 1)
		envs := fake.envSets[0]

		byKey := make(map[string]coolify.EnvironmentVariable, len(envs))
		for _, e := range envs {
			byKey[e.Key] = e
		}

		// Portal wiring from config + the issued key.
		assert.Equal(tb, "https://api.example.com", byKey["PORTAL_API_URL"].Value)
		assert.Equal(tb, "portal-jwt-secret", byKey["PORTAL_API_KEY"].Value)
		assert.True(tb, byKey["PORTAL_API_KEY"].Secret, "PORTAL_API_KEY must be marked a secret")
		assert.Equal(tb, "https://ws-test.build.example.com", byKey["PORTAL_WORKSPACE_URL"].Value)

		// Regression: the runtime receives NO workspace or website numeric ID.
		// Runtime identity comes from Coolify's built-in COOLIFY_RESOURCE_UUID
		// plus the workspace PORTAL_API_KEY; the portal never injects a
		// workspace/website ID (or a duplicate Coolify application ID) env var.
		assert.NotContains(tb, byKey, "PORTAL_WORKSPACE_ID")
		assert.NotContains(tb, byKey, "PORTAL_WEBSITE_ID")
		assert.NotContains(tb, byKey, "PORTAL_COOLIFY_APPLICATION_ID")

		// Database values injected under the CONFIGURED key names.
		assert.Equal(tb, "db.internal", byKey["WORDPRESS_DB_HOST"].Value)
		assert.Equal(tb, "3306", byKey["WORDPRESS_DB_PORT"].Value)
		assert.Equal(tb, "wsdb", byKey["WORDPRESS_DB_NAME"].Value)
		assert.Equal(tb, "ws", byKey["WORDPRESS_DB_USER"].Value)
		assert.Equal(tb, "db-super-secret", byKey["WORDPRESS_DB_PASSWORD"].Value)
		assert.True(tb, byKey["WORDPRESS_DB_PASSWORD"].Secret, "database password must be marked a secret")

		// No plaintext values are leaked anywhere: assert the non-secret keys
		// are not marked as secrets.
		assert.False(tb, byKey["PORTAL_API_URL"].Secret)
		assert.False(tb, byKey["WORDPRESS_DB_HOST"].Secret)

		// Regression: the environment carries ONLY the generic portal contract
		// plus the configured logical database values. There is deliberately NO
		// workspace/website numeric ID, no Coolify/resource-DB ID, and no
		// per-workspace database resource UUID in the runtime environment
		// (COOLIFY_RESOURCE_UUID is injected by Coolify itself, not by the
		// portal). Fixing the count catches any accidental
		// remote-ID/extra key added to buildEnvironment.
		assert.Len(tb, envs, 8, "runtime environment must contain exactly the portal contract + logical DB keys, with no workspace/website/resource ID")
		for _, e := range envs {
			assert.NotContains(tb, strings.ToLower(e.Key), "resource")
			assert.NotContains(tb, strings.ToLower(e.Key), "coolify")
		}
	}, workspaceTestOptions)
}

func TestReconcileApplicationStorage_Idempotent(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))

		rt := appRuntimeConfig()
		rt.Storage = []pluginConfig.WorkspaceStorageConfig{
			{NameSuffix: "data", MountPath: "/var/www/html"},
			{NameSuffix: "uploads", MountPath: "/var/www/html/wp-content/uploads"},
		}
		fake := &fakeAppProvider{}
		svc := newAppService(tb, db, fake, rt)

		// Repeated calls produce identical, deterministic mount requests. The
		// provider-side EnsureApplicationStorage is idempotent (it skips mounts
		// already attached at the target path), so repeated service calls never
		// duplicate.
		require.NoError(tb, svc.ReconcileApplicationStorage(context.Background(), ws, "app-created"))
		require.NoError(tb, svc.ReconcileApplicationStorage(context.Background(), ws, "app-created"))

		assert.Equal(tb, 2, fake.storageCalls)
		want := []coolify.StorageMount{
			{Name: "workspace-" + itoa(ws.ID) + "-data", MountPath: "/var/www/html"},
			{Name: "workspace-" + itoa(ws.ID) + "-uploads", MountPath: "/var/www/html/wp-content/uploads"},
		}
		assert.Equal(tb, want, fake.storageMount)
	}, workspaceTestOptions)
}

func TestStartAndObserveApplication_Success(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))
		user := "wsuser"
		pass := "wspass"
		ws.ProxyUsername = &user
		ws.ProxyPassword = &pass

		fake := &fakeAppProvider{
			startDep:    coolify.DeploymentResource{ID: "deploy-1", Status: coolify.ResourceStatusQueued},
			depStatuses: []coolify.ResourceStatus{coolify.ResourceStatusQueued, coolify.ResourceStatusFinished},
			appStatuses: []coolify.ResourceStatus{coolify.ResourceStatusStarting, coolify.ResourceStatusRunning},
		}
		svc := newAppService(tb, db, fake, appRuntimeConfig())

		err := svc.StartAndObserveApplication(context.Background(), ws, "app-created")
		require.NoError(tb, err)

		// Readiness is decided by the Coolify deployment/application status
		// reaching running — the portal performs no HTTP probe of the public
		// workspace URL, so no readiness HTTP request is ever made. The fake
		// provider has no HTTP client, and this test does not inject one.
		// Workspace marked ready once the application reports running.
		assert.Equal(tb, pluginDb.WorkspaceStatusReady, ws.Status)
		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		assert.Equal(tb, pluginDb.WorkspaceStatusReady, persisted.Status)
		assert.Empty(tb, persisted.LastError)
	}, workspaceTestOptions)
}

func TestStartAndObserveApplication_DeploymentFailure(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))

		fake := &fakeAppProvider{
			startDep:    coolify.DeploymentResource{ID: "deploy-1"},
			depStatuses: []coolify.ResourceStatus{coolify.ResourceStatusFailed},
		}
		svc := newAppService(tb, db, fake, appRuntimeConfig())

		err := svc.StartAndObserveApplication(context.Background(), ws, "app-created")
		require.ErrorIs(tb, err, ErrDeploymentFailed)

		// Never ready; a bounded last error is recorded.
		assert.NotEqual(tb, pluginDb.WorkspaceStatusReady, ws.Status)
		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		assert.NotEqual(tb, pluginDb.WorkspaceStatusReady, persisted.Status)
		assert.NotEmpty(tb, persisted.LastError)
	}, workspaceTestOptions)
}

func TestStartAndObserveApplication_UnhealthyApplication(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := attachPlatformDomain(insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning))

		// The deployment finishes, but the application reports the Docker
		// health-check state "unhealthy" via the Coolify API. Readiness is
		// decided solely from that status — the portal performs no HTTP probe.
		fake := &fakeAppProvider{
			startDep:  coolify.DeploymentResource{ID: "deploy-1"},
			appStatus: coolify.ResourceStatusUnhealthy,
		}
		svc := newAppService(tb, db, fake, appRuntimeConfig())

		err := svc.StartAndObserveApplication(context.Background(), ws, "app-created")
		require.ErrorIs(tb, err, ErrApplicationNotHealthy)
		require.ErrorContains(tb, err, "unhealthy")

		// Never ready; a bounded last error is recorded.
		assert.NotEqual(tb, pluginDb.WorkspaceStatusReady, ws.Status)
		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		assert.NotEqual(tb, pluginDb.WorkspaceStatusReady, persisted.Status)
		assert.NotEmpty(tb, persisted.LastError)
	}, workspaceTestOptions)
}

func TestWorkspaceTagDerivation(t *testing.T) {
	// With an install namespace, the tag is the installation-scoped
	// "<ns>-workspaces" token; the name is namespace-prefixed.
	assert.Equal(t, "portalx-workspaces", deterministicWorkspaceTag("portalx"))
	assert.Equal(t, "portalx-workspace-7-app", deterministicApplicationName("portalx", 7))
	// Without a namespace, no tag is emitted and the name is unprefixed.
	assert.Equal(t, "", deterministicWorkspaceTag(""))
	assert.Equal(t, "workspace-7-app", deterministicApplicationName("", 7))

	// applicationTags wraps applicationTag: empty when no namespace.
	svc := &WorkspaceService{config: &pluginConfig.WorkspaceConfig{
		Provider: pluginConfig.WorkspaceProviderConfig{InstallNamespace: "portalx"},
	}}
	assert.Equal(t, []string{"portalx-workspaces"}, svc.applicationTags())
	assert.Equal(t, "portalx-workspaces", svc.applicationTag())

	svcNoNS := &WorkspaceService{config: &pluginConfig.WorkspaceConfig{}}
	assert.Nil(t, svcNoNS.applicationTags())
	assert.Equal(t, "", svcNoNS.applicationTag())
}
