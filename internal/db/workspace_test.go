package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestWorkspace_TableName(t *testing.T) {
	ws := Workspace{}
	assert.Equal(t, "workspaces", ws.TableName())
}

func TestWorkspace_Hostname(t *testing.T) {
	ws := Workspace{
		Label: "alice",
		PlatformDomain: PlatformDomain{
			Domain: "build.lumeweb.com",
		},
	}
	assert.Equal(t, "alice.build.lumeweb.com", ws.Hostname())
}

// TestWorkspace_UniqueWebsiteID documents the corrected separation: building in
// a workspace is separate from publishing, WebsiteID is nullable, and exactly
// one live workspace may be attached to a website while any number of
// unattached workspaces (WebsiteID NULL) may coexist.
func TestWorkspace_UniqueWebsiteID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()

		// First (live) workspace attaches to website_id 1.
		assert.NoError(tb, gormDB.Create(&Workspace{
			UserID:           1,
			WebsiteID:        new(uint(1)),
			PlatformDomainID: 10,
			Label:            "alice",
			Status:           WorkspaceStatusReady,
		}).Error)

		// A second live workspace for the same website must be rejected by the
		// strict UNIQUE(website_id) key, even with a different hostname.
		assert.Error(tb, gormDB.Create(&Workspace{
			UserID:           1,
			WebsiteID:        new(uint(1)),
			PlatformDomainID: 11,
			Label:            "bob",
			Status:           WorkspaceStatusReady,
		}).Error)

		// A different website_id with a different hostname is allowed.
		assert.NoError(tb, gormDB.Create(&Workspace{
			UserID:           1,
			WebsiteID:        new(uint(2)),
			PlatformDomainID: 11,
			Label:            "bob",
			Status:           WorkspaceStatusReady,
		}).Error)
	}, dbTestOptions)
}

// TestWorkspace_MultipleUnattachedWorkspaces documents that a workspace may
// exist without any Website: NULL WebsiteID values are distinct inside a SQL
// UNIQUE index for both MySQL and SQLite, so any number of unattached
// workspaces can coexist for one owner.
func TestWorkspace_MultipleUnattachedWorkspaces(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()

		// Several unattached workspaces (WebsiteID nil) for the same owner are
		// all allowed.
		for i, l := range []string{"a1", "a2", "a3"} {
			require.NoError(tb, gormDB.Create(&Workspace{
				UserID:           1,
				PlatformDomainID: 10,
				Label:            l,
				Status:           WorkspaceStatusReady,
			}).Error, "unattached workspace %d (%s) should be allowed", i, l)
		}

		// Attaching afterwards is fine for one of them; the others stay
		// unattached.
		require.NoError(tb, gormDB.Model(&Workspace{}).
			Where("id = ?", 1).Update("website_id", 1).Error)
		var attached, unattached int64
		gormDB.Model(&Workspace{}).Where("website_id IS NOT NULL").Count(&attached)
		gormDB.Model(&Workspace{}).Where("website_id IS NULL").Count(&unattached)
		assert.Equal(t, int64(1), attached)
		assert.Equal(t, int64(2), unattached)
	}, dbTestOptions)
}

func TestWorkspace_UniquePlatformDomainLabel(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()

		assert.NoError(tb, gormDB.Create(&Workspace{
			UserID:           1,
			WebsiteID:        new(uint(1)),
			PlatformDomainID: 10,
			Label:            "alice",
			Status:           WorkspaceStatusReady,
		}).Error)

		// Same (platform_domain_id, label) is rejected even for a different website.
		assert.Error(tb, gormDB.Create(&Workspace{
			UserID:           1,
			WebsiteID:        new(uint(2)),
			PlatformDomainID: 10,
			Label:            "alice",
			Status:           WorkspaceStatusReady,
		}).Error)

		// Same label under a different platform domain is allowed.
		assert.NoError(tb, gormDB.Create(&Workspace{
			UserID:           1,
			WebsiteID:        new(uint(2)),
			PlatformDomainID: 11,
			Label:            "alice",
			Status:           WorkspaceStatusReady,
		}).Error)

		// Same platform domain with a different label is allowed.
		assert.NoError(tb, gormDB.Create(&Workspace{
			UserID:           1,
			WebsiteID:        new(uint(3)),
			PlatformDomainID: 10,
			Label:            "carol",
			Status:           WorkspaceStatusReady,
		}).Error)
	}, dbTestOptions)
}

func TestWorkspace_UniqueNullableProviderIDs(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()

		// Multiple workspaces may have NULL application_resource_id (nulls are
		// distinct in a SQL UNIQUE index for both MySQL and SQLite).
		assert.NoError(tb, gormDB.Create(&Workspace{
			UserID: 1, WebsiteID: new(uint(1)), PlatformDomainID: 10, Label: "alice", Status: WorkspaceStatusReady,
		}).Error)
		assert.NoError(tb, gormDB.Create(&Workspace{
			UserID: 1, WebsiteID: new(uint(2)), PlatformDomainID: 11, Label: "bob", Status: WorkspaceStatusReady,
		}).Error)

		// A non-NULL application_resource_id is unique: reassigning it to
		// another workspace is rejected.
		appID := "app-uuid-1"
		assert.NoError(tb, gormDB.Model(&Workspace{}).
			Where("id = ?", 1).Update("application_resource_id", appID).Error)
		assert.Error(tb, gormDB.Model(&Workspace{}).
			Where("id = ?", 2).Update("application_resource_id", appID).Error)
	}, dbTestOptions)
}

// TestWorkspace_LogicalDBIdentifiersAreNotUnique documents the corrected
// architecture: the shared DB resource is NOT a per-workspace resource, so there
// is no database_resource_id column and logical database_name/database_user are
// legally shared/duplicated identifiers (they are derived deterministically and
// are not UNIQUE-constrained).
func TestWorkspace_LogicalDBIdentifiersAreNotUnique(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()

		name, user := "workspace_1", "workspace_1"
		assert.NoError(tb, gormDB.Create(&Workspace{
			UserID: 1, WebsiteID: new(uint(1)), PlatformDomainID: 10, Label: "alice", Status: WorkspaceStatusReady,
			DatabaseName: &name, DatabaseUser: &user,
		}).Error)
		// Another workspace may reuse the same logical identifier strings; no
		// UNIQUE key prevents it (uniqueness is enforced by the strict
		// website_id / label keys, not by the shared DB identifiers).
		assert.NoError(tb, gormDB.Create(&Workspace{
			UserID: 1, WebsiteID: new(uint(2)), PlatformDomainID: 11, Label: "bob", Status: WorkspaceStatusReady,
			DatabaseName: &name, DatabaseUser: &user,
		}).Error)
	}, dbTestOptions)
}

// TestWorkspace_SoftDeleteStrictKeyContract documents the live-uniqueness
// trade-off chosen for Workspace: unique keys are STRICT (no deleted_at), so a
// soft-deleted row still occupies its keys until the tombstone is purged.
// Re-provisioning after a soft delete therefore must purge the tombstone with
// Unscoped before re-inserting, mirroring the CreatePlatformDomain contract.
func TestWorkspace_SoftDeleteStrictKeyContract(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()

		ws := &Workspace{
			UserID: 1, WebsiteID: new(uint(1)), PlatformDomainID: 10, Label: "alice",
			Status: WorkspaceStatusReady,
		}
		require.NoError(tb, gormDB.Create(ws).Error)

		// Soft delete: the row is tombstoned but the strict website_id key is
		// still occupied, so a naive re-insert for the same website fails.
		require.NoError(tb, gormDB.Delete(ws).Error)
		assert.Error(tb, gormDB.Create(&Workspace{
			UserID: 1, WebsiteID: new(uint(1)), PlatformDomainID: 10, Label: "alice",
			Status: WorkspaceStatusReady,
		}).Error)

		// Purge the tombstone; the strict key is reclaimed and re-insert succeeds.
		require.NoError(tb, gormDB.Unscoped().Delete(ws).Error)
		assert.NoError(tb, gormDB.Create(&Workspace{
			UserID: 1, WebsiteID: new(uint(1)), PlatformDomainID: 10, Label: "alice",
			Status: WorkspaceStatusReady,
		}).Error)
	}, dbTestOptions)
}
