package core

import (
	"context"

	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

// WORKSPACE_SERVICE identifies the workspace provisioning service in this
// plugin. The workspace domain model and service remain runtime-agnostic; only
// the service configuration encodes the selected runtime.
const WORKSPACE_SERVICE = "ipfs.workspace"

// WorkspaceService manages persistent authoring workspaces, with owner-only
// access and local persistence. Building in a Workspace is separate from
// publishing: a Workspace may exist without any Website record or domain, so
// creation does not require a website. Ownership is stored on the Workspace
// itself (UserID). A created workspace is persisted with status provisioning
// and returned immediately; external provisioning is advanced asynchronously
// by a reconciler, so no Coolify or API-key work happens here.
type WorkspaceService interface {
	core.Service
	core.Configurable

	// Create records a new workspace owned by userID. websiteID is the
	// OPTIONAL website to attach the workspace to (the publish link); pass nil
	// to create a standalone (unattached) workspace that needs no Website
	// record or domain. It performs only local work:
	//   1. when websiteID is set, loads the website through
	//      WebsiteService.GetWebsite (ownership is enforced by that call, so a
	//      website the user does not own resolves to a not-found result) and
	//      enforces one live workspace per attached website;
	//   2. resolves the configured enabled PlatformDomain;
	//   3. generates a DNS-safe opaque label (collision-safe via the
	//      (platform_domain_id, label) unique key);
	//   4. inserts a Workspace with status `provisioning`.
	// It returns immediately; the unique constraints settle concurrent creates
	// and label collisions.
	Create(ctx context.Context, userID uint, websiteID *uint) (*pluginDb.Workspace, error)

	// Attach links an existing workspace to a website owned by userID (the
	// publish link). It verifies the user owns both the workspace and the
	// website, and prevents duplicate attachment (a website can be attached to
	// at most one live workspace). It fails clearly when the workspace is
	// already attached to a different website. Attaching is a separate
	// operation from publishing website domains; it never changes the
	// workspace's authoring hostname.
	Attach(ctx context.Context, userID uint, workspaceID uint, websiteID uint) (*pluginDb.Workspace, error)

	// Get returns the Workspace identified by workspaceID only if it is owned
	// by userID (Workspace.UserID). It returns (nil, nil) when the workspace
	// does not exist or is not owned by the user, matching the
	// WebsiteService.GetWebsite convention so a caller cannot distinguish a
	// missing row from a forbidden one (no existence leak).
	Get(ctx context.Context, userID uint, workspaceID uint) (*pluginDb.Workspace, error)

	// ResolveRuntime resolves the live workspace that owns the given Coolify
	// application resource UUID (the COOLIFY_RESOURCE_UUID Coolify injects into
	// the container) when it is owned by userID (the authenticated workspace
	// API-key owner). The runtime sends its COOLIFY_RESOURCE_UUID plus its
	// PORTAL_API_KEY; both the API-key owner and the application resource ID
	// MUST match, so a mismatched UUID/key pair returns (nil, nil, nil) and
	// never leaks existence or ownership. It eagerly loads the workspace's
	// platform domain (for the authoring hostname) and, when attached, the
	// optional owning Website (the publish relationship). No workspace numeric
	// ID is required from runtime input.
	ResolveRuntime(ctx context.Context, userID uint, appResourceID string) (*pluginDb.Workspace, *pluginDb.Website, error)

	// List returns the workspaces owned by userID, filtered, sorted, and
	// paginated, along with the total count. Ownership is enforced on
	// Workspace.UserID, so unattached workspaces are included and visible to
	// their owner without any Website join.
	List(ctx context.Context, userID uint, filter []queryutil.CrudFilter, sort []filter.Sort, pagination queryutil.Pagination) ([]*pluginDb.Workspace, int64, error)

	// Suspend stops a ready workspace's application. The shared MySQL/MariaDB
	// resource is a portal dependency, never a workspace-owned resource, so
	// suspend never stops it: the workspace's logical database and data remain
	// intact and available. State is marked `suspended` only after the provider
	// call succeeds. All application storage is preserved by the provider's
	// stop. Ownership is enforced: a workspace whose owning website is not
	// controlled by userID resolves to not-found. Returns
	// ErrWorkspaceInvalidState when the workspace is not ready.
	Suspend(ctx context.Context, userID uint, workspaceID uint) (*pluginDb.Workspace, error)

	// Resume starts a suspended workspace: it re-provisions (idempotently) the
	// workspace's logical database/user on the shared MySQL/MariaDB resource,
	// refreshes the portal API key and database credentials in the application
	// environment, starts the application, waits for readiness, and marks the
	// workspace `ready`. It never starts/stops the shared resource itself.
	// Ownership is enforced. Returns ErrWorkspaceInvalidState when the
	// workspace is not suspended.
	Resume(ctx context.Context, userID uint, workspaceID uint) (*pluginDb.Workspace, error)

	// Delete removes a workspace and its resources. It marks the row
	// `deleting`, revokes the workspace portal API key through the exported
	// dashboard core.APIKeyService, deletes the application (and its storage),
	// then drops ONLY the workspace's logical database/user from the shared
	// MySQL/MariaDB resource (never the shared resource itself), treats provider
	// 404 as already deleted, and finally soft-deletes the workspace. Ownership
	// is enforced. Returns ErrWorkspaceInvalidState when the workspace is
	// already being deleted.
	Delete(ctx context.Context, userID uint, workspaceID uint) (*pluginDb.Workspace, error)

	// RotateAccessCredentials returns the workspace's proxy Basic Auth
	// credentials to the owner (never the portal API key or the database
	// password). When rotate is true it first generates new proxy credentials,
	// persists them, and applies them to the provider application so the new
	// value takes effect. Ownership is enforced.
	RotateAccessCredentials(ctx context.Context, userID uint, workspaceID uint, rotate bool) (*pluginDb.AccessCredentials, error)
}
