package workspace

import (
	"context"
	"fmt"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/testopts"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/queryutil"
	"gorm.io/gorm"
)

var workspaceTestOptions = coreTesting.CombineOptions(
	coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
	testopts.NewBaseMockPluginBuilder().
		WithServiceConfig(pluginCore.WORKSPACE_SERVICE, &pluginConfig.WorkspaceConfig{}).
		WithMigrations(map[core.DBType]fs.FS{
			core.DB_TYPE_SQLITE: migrations.GetSQLite(),
		}).BuilderOption(),
)

// strptr returns a pointer to v for optional string fields.
func strptr(v string) *string { return &v }

// fakePlatformResolver is a fake platformDomainResolver for tests that returns
// a single configured enabled PlatformDomain matching the requested domain.
type fakePlatformResolver struct {
	id        uint
	domain    string
	namespace pluginDb.DomainNamespace
	enabled   bool
}

func (f *fakePlatformResolver) GetEnabledPlatformDomain(_ context.Context, domain string, namespace pluginDb.DomainNamespace) (*pluginDb.PlatformDomain, error) {
	if !f.enabled || f.domain == "" || domain != f.domain {
		return nil, nil
	}
	if namespace != "" && f.namespace != "" && namespace != f.namespace {
		return nil, nil
	}
	return &pluginDb.PlatformDomain{ID: f.id, Domain: f.domain, Namespace: f.namespace, Enabled: f.enabled}, nil
}

func enabledFakeResolver(id uint, domain, namespace string) *fakePlatformResolver {
	return &fakePlatformResolver{id: id, domain: domain, namespace: pluginDb.DomainNamespace(namespace), enabled: true}
}

// newTestService constructs a workspace service wired to the given DB, mocks,
// and a fixed label generator. It is constructed directly (not through the
// factory) so tests can set Enabled=true and inject a fake platform resolver,
// and it bypasses the Coolify health check in startupValidate.
func newTestService(tb coreTesting.TB, db *gorm.DB, websiteSvc pluginCore.WebsiteService, platformSvc platformDomainResolver) *WorkspaceService {
	tb.Helper()
	bc := &core.BaseComponent{}
	bc.SetDB(db)
	svc := &WorkspaceService{
		BaseComponent: bc,
		config: &pluginConfig.WorkspaceConfig{
			Enabled:                 true,
			PlatformDomain:          "build.example.com",
			PlatformDomainNamespace: "icann",
		},
		websiteSvc:  websiteSvc,
		platformSvc: platformSvc,
		slugGen: func() (string, error) {
			return "ws-test123", nil
		},
	}
	return svc
}

// insertWebsite inserts a bare website row directly (bypassing model hooks) so
// the ownership JOIN in Get/List has a row to match. Only columns that exist in
// the migration are written.
func insertWebsite(tb coreTesting.TB, db *gorm.DB, id, userID uint) {
	tb.Helper()
	require.NoError(tb, db.Exec(
		"INSERT INTO ipfs_websites (id, user_id, target_type, target_multihash, status, validation_token, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
		id, userID, string(pluginDb.WebsiteTargetTypeIPFS), []byte("sometarget"), "active", "tok",
	).Error)
}

func insertPlatformDomain(tb coreTesting.TB, db *gorm.DB, id uint, domain, namespace string, enabled bool) {
	tb.Helper()
	require.NoError(tb, db.Exec(
		"INSERT INTO platform_domains (id, domain, namespace, zone_id, enabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
		id, domain, namespace, 0, enabled,
	).Error)
}

func TestWorkspaceService_Create_InsertsProvisioningRow(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)

		mockWS := mocks.NewMockWebsiteService(tb)
		mockWS.EXPECT().GetWebsite(mock.Anything, uint(1), uint(1)).
			Return(&pluginDb.Website{ID: 1, UserID: 1}, nil)

		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "build.example.com", "icann"))

		ws, err := svc.Create(context.Background(), 1, new(uint(1)))
		require.NoError(tb, err)
		assert.Equal(tb, pluginDb.WorkspaceStatusProvisioning, ws.Status)
		assert.NotZero(tb, ws.ID)
		assert.Equal(tb, new(uint(1)), ws.WebsiteID)
		assert.Equal(tb, uint(10), ws.PlatformDomainID)
		assert.Equal(tb, "ws-test123", ws.Label)

		var persisted pluginDb.Workspace
		require.NoError(tb, db.First(&persisted, ws.ID).Error)
		assert.Equal(tb, pluginDb.WorkspaceStatusProvisioning, persisted.Status)
		assert.Equal(tb, ws.Label, persisted.Label)
	}, workspaceTestOptions)
}

func TestWorkspaceService_Create_LabelCollisionRetries(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)

		// A workspace already claims label "dup" on this platform domain.
		require.NoError(tb, db.Create(&pluginDb.Workspace{
			UserID:           1,
			WebsiteID:        new(uint(99)),
			PlatformDomainID: 10,
			Label:            "dup",
			Status:           pluginDb.WorkspaceStatusReady,
		}).Error)

		mockWS := mocks.NewMockWebsiteService(tb)
		mockWS.EXPECT().GetWebsite(mock.Anything, uint(1), uint(1)).
			Return(&pluginDb.Website{ID: 1, UserID: 1}, nil)

		calls := 0
		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "build.example.com", "icann"))
		svc.slugGen = func() (string, error) {
			calls++
			if calls == 1 {
				return "dup", nil
			}
			return "unique", nil
		}

		ws, err := svc.Create(context.Background(), 1, new(uint(1)))
		require.NoError(tb, err)
		assert.Equal(tb, "unique", ws.Label, "create must retry with a fresh label on collision")
		assert.GreaterOrEqual(tb, calls, 2)
	}, workspaceTestOptions)
}

func TestWorkspaceService_Create_NotEnabled(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		mockWS := mocks.NewMockWebsiteService(tb)
		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "build.example.com", "icann"))
		svc.config.Enabled = false

		_, err := svc.Create(context.Background(), 1, new(uint(1)))
		assert.ErrorIs(tb, err, ErrWorkspaceNotEnabled)
	}, workspaceTestOptions)
}

func TestWorkspaceService_Create_WebsiteNotOwnedOrMissing(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		mockWS := mocks.NewMockWebsiteService(tb)
		// GetWebsite enforces ownership and returns nil when the website is
		// missing or not owned.
		mockWS.EXPECT().GetWebsite(mock.Anything, uint(2), uint(1)).Return(nil, nil)
		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "build.example.com", "icann"))

		_, err := svc.Create(context.Background(), 2, new(uint(1)))
		assert.ErrorIs(tb, err, ErrWorkspaceNotFound)
	}, workspaceTestOptions)
}

func TestWorkspaceService_Create_PlatformDomainUnavailable(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		// Platform-domain resolution happens before any website lookup, so an
		// unavailable platform domain fails fast without contacting the website
		// service.
		mockWS := mocks.NewMockWebsiteService(tb)
		_ = db

		// No enabled platform domain matches the config.
		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "other.example.com", "icann"))

		_, err := svc.Create(context.Background(), 1, new(uint(1)))
		assert.ErrorIs(tb, err, ErrWorkspacePlatformDomainUnavailable)
	}, workspaceTestOptions)
}

func TestWorkspaceService_Create_OneWorkspacePerWebsite(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		// Website 1 already has a live workspace.
		require.NoError(tb, db.Create(&pluginDb.Workspace{
			UserID:           1,
			WebsiteID:        new(uint(1)),
			PlatformDomainID: 10,
			Label:            "existing",
			Status:           pluginDb.WorkspaceStatusReady,
		}).Error)

		mockWS := mocks.NewMockWebsiteService(tb)
		mockWS.EXPECT().GetWebsite(mock.Anything, uint(1), uint(1)).
			Return(&pluginDb.Website{ID: 1, UserID: 1}, nil)

		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "build.example.com", "icann"))

		_, err := svc.Create(context.Background(), 1, new(uint(1)))
		assert.ErrorIs(tb, err, ErrWorkspaceAlreadyExists)
	}, workspaceTestOptions)
}

func TestWorkspaceService_Get_OwnershipEnforced(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertWebsite(tb, db, 2, 2)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		// Website 1 (user 1) has workspace id=1; website 2 (user 2) has workspace id=2.
		require.NoError(tb, db.Create(&pluginDb.Workspace{
			Model: gorm.Model{ID: 1}, UserID: 1, WebsiteID: new(uint(1)), PlatformDomainID: 10, Label: "a1",
			Status: pluginDb.WorkspaceStatusProvisioning,
		}).Error)
		require.NoError(tb, db.Create(&pluginDb.Workspace{
			Model: gorm.Model{ID: 2}, UserID: 2, WebsiteID: new(uint(2)), PlatformDomainID: 10, Label: "a2",
			Status: pluginDb.WorkspaceStatusReady,
		}).Error)

		svc := newTestService(tb, db, nil, enabledFakeResolver(10, "build.example.com", "icann"))

		// Owner can read their own workspace.
		ws, err := svc.Get(context.Background(), 1, 1)
		require.NoError(tb, err)
		require.NotNil(tb, ws)
		assert.Equal(tb, uint(1), ws.ID)

		// A non-owner cannot read it (nil, nil — no existence leak).
		ws, err = svc.Get(context.Background(), 2, 1)
		require.NoError(tb, err)
		assert.Nil(tb, ws)

		// Missing id also returns (nil, nil).
		ws, err = svc.Get(context.Background(), 1, 999)
		require.NoError(tb, err)
		assert.Nil(tb, ws)
	}, workspaceTestOptions)
}

func TestWorkspaceService_List_OwnershipEnforcedAndPaginated(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertWebsite(tb, db, 2, 1) // user 1 owns two websites
		insertWebsite(tb, db, 3, 2) // user 2 owns one website
		insertWebsite(tb, db, 4, 3) // user 3 owns one website, no workspace
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)

		// One workspace per website: websites 1,2 (user 1) and 3 (user 2).
		require.NoError(tb, db.Create(&pluginDb.Workspace{
			UserID: 1, WebsiteID: new(uint(1)), PlatformDomainID: 10, Label: "a", Status: pluginDb.WorkspaceStatusProvisioning,
		}).Error)
		require.NoError(tb, db.Create(&pluginDb.Workspace{
			UserID: 1, WebsiteID: new(uint(2)), PlatformDomainID: 10, Label: "b", Status: pluginDb.WorkspaceStatusReady,
		}).Error)
		require.NoError(tb, db.Create(&pluginDb.Workspace{
			UserID: 2, WebsiteID: new(uint(3)), PlatformDomainID: 10, Label: "c", Status: pluginDb.WorkspaceStatusProvisioning,
		}).Error)

		svc := newTestService(tb, db, nil, enabledFakeResolver(10, "build.example.com", "icann"))

		all, total, err := svc.List(context.Background(), 1, nil, nil, queryutil.Pagination{})
		require.NoError(tb, err)
		assert.Equal(tb, int64(2), total)
		assert.Len(tb, all, 2)

		// Pagination (page size 1) returns one row and the correct total.
		one, total2, err := svc.List(context.Background(), 1, nil, nil, queryutil.Pagination{PageSize: 1})
		require.NoError(tb, err)
		assert.Equal(tb, int64(2), total2)
		assert.Len(tb, one, 1)

		// A user with no workspaces gets an empty result.
		empty, total3, err := svc.List(context.Background(), 3, nil, nil, queryutil.Pagination{})
		require.NoError(tb, err)
		assert.Equal(tb, int64(0), total3)
		assert.Len(tb, empty, 0)
	}, workspaceTestOptions)
}

func TestWorkspaceService_GenerateOpaqueLabel_DNSFormat(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		label, err := generateOpaqueLabel()
		require.NoError(tb, err)
		require.Len(tb, label, len("ws-")+labelRandomChars)
		assert.Equal(tb, "ws-", label[:3])
		// DNS label chars only: lower-case alphanumeric plus hyphens, no leading
		// or trailing hyphen.
		for i := 0; i < len(label); i++ {
			c := label[i]
			assert.True(tb, (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (i > 0 && i < len(label)-1 && c == '-'))
		}
	}, workspaceTestOptions)
}

// TestWorkspaceService_Create_NoWebsite_UnattachedWorkspaces verifies that a
// workspace may be created without any Website record or domain, and that any
// number of unattached workspaces (WebsiteID NULL) can coexist for one owner.
func TestWorkspaceService_Create_NoWebsite_UnattachedWorkspaces(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		// No insertWebsite and no GetWebsite mock: the create must not touch the
		// website service at all.
		mockWS := mocks.NewMockWebsiteService(tb)
		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "build.example.com", "icann"))
		// Produce a distinct label per create so (platform_domain_id, label)
		// collisions do not interfere with the unattached-workspace assertions.
		n := 0
		svc.slugGen = func() (string, error) {
			n++
			return fmt.Sprintf("ws-u%d", n), nil
		}

		// Several unattached workspaces for the same owner all succeed.
		var ids []uint
		for i := 0; i < 3; i++ {
			ws, err := svc.Create(context.Background(), 1, nil)
			require.NoError(tb, err)
			require.NotNil(tb, ws)
			assert.Nil(tb, ws.WebsiteID, "unattached workspace must have nil WebsiteID")
			assert.Equal(tb, uint(1), ws.UserID)
			ids = append(ids, ws.ID)
		}
		require.Len(tb, ids, 3)
		assert.NotEqual(tb, ids[0], ids[1])

		// Attached or not, the workspaces remain visible to their owner via Get.
		ws, err := svc.Get(context.Background(), 1, ids[0])
		require.NoError(tb, err)
		require.NotNil(tb, ws)
		assert.Nil(tb, ws.WebsiteID)
	}, workspaceTestOptions)
}

// TestWorkspaceService_Attach_LinksUnattachedWorkspace verifies the attach
// operation links an existing unattached workspace (owned by the user) to a
// website the user owns, and only records the publish link (never touching the
// authoring hostname).
func TestWorkspaceService_Attach_LinksUnattachedWorkspace(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)

		// Create an unattached workspace for user 1.
		mockWS := mocks.NewMockWebsiteService(tb)
		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "build.example.com", "icann"))
		n := 0
		svc.slugGen = func() (string, error) {
			n++
			return fmt.Sprintf("ws-a%d", n), nil
		}
		base, err := svc.Create(context.Background(), 1, nil)
		require.NoError(tb, err)
		assert.Nil(tb, base.WebsiteID)

		mockWS.EXPECT().GetWebsite(mock.Anything, uint(1), uint(1)).
			Return(&pluginDb.Website{ID: 1, UserID: 1}, nil)

		attached, err := svc.Attach(context.Background(), 1, base.ID, 1)
		require.NoError(tb, err)
		require.NotNil(tb, attached.WebsiteID)
		assert.Equal(tb, uint(1), *attached.WebsiteID)
		// The authoring hostname/platform domain is unchanged by attach.
		assert.Equal(tb, base.PlatformDomainID, attached.PlatformDomainID)

		// A second attach of the same workspace must fail (already attached).
		_, err = svc.Attach(context.Background(), 1, base.ID, 1)
		assert.ErrorIs(tb, err, ErrWorkspaceAlreadyAttached)

		// The same website cannot be attached to another workspace.
		other, err := svc.Create(context.Background(), 1, nil)
		require.NoError(tb, err)
		mockWS.EXPECT().GetWebsite(mock.Anything, uint(1), uint(1)).
			Return(&pluginDb.Website{ID: 1, UserID: 1}, nil)
		_, err = svc.Attach(context.Background(), 1, other.ID, 1)
		assert.ErrorIs(tb, err, ErrWorkspaceAlreadyExists)
	}, workspaceTestOptions)
}

// TestWorkspaceService_Attach_OwnershipEnforced verifies attach rejects a
// website the user does not own and a workspace the user does not own.
func TestWorkspaceService_Attach_OwnershipEnforced(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1) // website 1 owned by user 1
		insertWebsite(tb, db, 2, 2) // website 2 owned by user 2
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)

		mockWS := mocks.NewMockWebsiteService(tb)
		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "build.example.com", "icann"))
		ws, err := svc.Create(context.Background(), 1, nil)
		require.NoError(tb, err)

		// Workspace ownership is enforced first: user 2 does not own ws (owned
		// by user 1), so attach fails without ever contacting the website svc.
		_, err = svc.Attach(context.Background(), 2, ws.ID, 2)
		assert.ErrorIs(tb, err, ErrWorkspaceNotFound)

		// Website ownership is enforced: user 1 owns the workspace but not
		// website 2; GetWebsite(user 1, website 2) returns (nil, nil).
		mockWS.EXPECT().GetWebsite(mock.Anything, uint(1), uint(2)).Return(nil, nil)
		_, err = svc.Attach(context.Background(), 1, ws.ID, 2)
		assert.ErrorIs(tb, err, ErrWorkspaceNotFound)
	}, workspaceTestOptions)
}

// TestWorkspaceService_ResolveRuntime_Attached verifies a runtime can resolve
// its workspace by the Coolify-injected application resource UUID plus the API
// key owner, and that the optional Website publish relationship is returned.
func TestWorkspaceService_ResolveRuntime_Attached(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 50, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := &pluginDb.Workspace{
			UserID:                1,
			WebsiteID:             new(uint(50)),
			PlatformDomainID:      10,
			Label:                 "ws-test",
			Status:                pluginDb.WorkspaceStatusReady,
			ApplicationResourceID: strptr("coolify-resource-abc"),
		}
		require.NoError(tb, db.Create(ws).Error)

		mockWS := mocks.NewMockWebsiteService(tb)
		mockWS.EXPECT().GetWebsite(mock.Anything, uint(1), uint(50)).
			Return(&pluginDb.Website{ID: 50, UserID: 1, TargetType: string(pluginDb.WebsiteTargetTypeIPFS), Status: "active"}, nil)

		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "build.example.com", "icann"))

		resolved, website, err := svc.ResolveRuntime(context.Background(), 1, "coolify-resource-abc")
		require.NoError(tb, err)
		require.NotNil(tb, resolved)
		assert.Equal(tb, ws.ID, resolved.ID)
		assert.Equal(tb, "ws-test.build.example.com", resolved.Hostname())
		require.NotNil(tb, website)
		assert.Equal(tb, uint(50), website.ID)
	}, workspaceTestOptions)
}

// TestWorkspaceService_ResolveRuntime_Unattached verifies an unattached
// workspace resolves with no Website relationship.
func TestWorkspaceService_ResolveRuntime_Unattached(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := &pluginDb.Workspace{
			UserID:                1,
			WebsiteID:             nil,
			PlatformDomainID:      10,
			Label:                 "ws-test",
			Status:                pluginDb.WorkspaceStatusReady,
			ApplicationResourceID: strptr("coolify-resource-unattached"),
		}
		require.NoError(tb, db.Create(ws).Error)

		mockWS := mocks.NewMockWebsiteService(tb)
		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "build.example.com", "icann"))

		resolved, website, err := svc.ResolveRuntime(context.Background(), 1, "coolify-resource-unattached")
		require.NoError(tb, err)
		require.NotNil(tb, resolved)
		assert.Nil(tb, website)
	}, workspaceTestOptions)
}

// TestWorkspaceService_ResolveRuntime_RejectsMismatch verifies a mismatch
// between the API-key owner and the workspace owner, or a resource UUID that
// does not match, resolves to not-found (no existence/ownership leak).
func TestWorkspaceService_ResolveRuntime_RejectsMismatch(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := &pluginDb.Workspace{
			UserID:                2, // owned by user 2
			WebsiteID:             nil,
			PlatformDomainID:      10,
			Label:                 "ws-test",
			Status:                pluginDb.WorkspaceStatusReady,
			ApplicationResourceID: strptr("coolify-resource-owned-by-2"),
		}
		require.NoError(tb, db.Create(ws).Error)

		mockWS := mocks.NewMockWebsiteService(tb)
		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "build.example.com", "icann"))

		// Wrong owner (user 1 resolves a workspace owned by user 2).
		resolved, website, err := svc.ResolveRuntime(context.Background(), 1, "coolify-resource-owned-by-2")
		require.NoError(tb, err)
		assert.Nil(tb, resolved)
		assert.Nil(tb, website)

		// Correct owner, wrong resource UUID.
		resolved, website, err = svc.ResolveRuntime(context.Background(), 2, "bogus-resource-uuid")
		require.NoError(tb, err)
		assert.Nil(tb, resolved)
		assert.Nil(tb, website)
	}, workspaceTestOptions)
}

// TestWorkspaceService_Create_RecreateAfterSoftDelete verifies the
// delete-then-recreate cycle releases the strict UNIQUE(website_id) key. Delete
// soft-deletes the workspace (a tombstone that still occupies the key), so a
// subsequent Create for the same attached website must purge the tombstone
// before inserting — otherwise the insert would hit the website_id unique-key
// violation and surface as ErrWorkspaceAlreadyExists.
func TestWorkspaceService_Create_RecreateAfterSoftDelete(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)

		mockWS := mocks.NewMockWebsiteService(tb)
		mockWS.EXPECT().GetWebsite(mock.Anything, uint(1), uint(1)).
			Return(&pluginDb.Website{ID: 1, UserID: 1}, nil).Times(1)
		svc := newTestService(tb, db, mockWS, enabledFakeResolver(10, "build.example.com", "icann"))

		// 1. Create a workspace attached to website 1.
		first, err := svc.Create(context.Background(), 1, new(uint(1)))
		require.NoError(tb, err)
		require.NotNil(tb, first.WebsiteID)
		assert.Equal(tb, uint(1), *first.WebsiteID)

		// 2. Simulate Delete's final step: the workspace row is soft-deleted
		// (tombstoned) AFTER provider cleanup. The tombstone still occupies the
		// website_id strict key.
		require.NoError(tb, db.Delete(first).Error)

		// 3. Recreate a workspace for the SAME attached website. Without the
		// tombstone purge this would violate UNIQUE(website_id); with the purge
		// the key is released and creation succeeds.
		mockWS.EXPECT().GetWebsite(mock.Anything, uint(1), uint(1)).
			Return(&pluginDb.Website{ID: 1, UserID: 1}, nil).Times(1)
		second, err := svc.Create(context.Background(), 1, new(uint(1)))
		require.NoError(tb, err)
		require.NotNil(tb, second.WebsiteID)
		assert.Equal(tb, uint(1), *second.WebsiteID)
		assert.NotEqual(tb, first.ID, second.ID,
			"recreation must produce a fresh workspace row, not reuse the tombstone id")

		// Exactly one live workspace exists for website 1.
		var liveCount int64
		require.NoError(tb, db.Model(&pluginDb.Workspace{}).
			Where("website_id = ? AND deleted_at IS NULL", uint(1)).Count(&liveCount).Error)
		assert.Equal(tb, int64(1), liveCount)
	}, workspaceTestOptions)
}
