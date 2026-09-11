// This file implements workspace suspend, resume, delete, and owner-authorized
// access credentials. It builds on the database phase (ReconcileDatabase,
// DropLogicalDatabase), the application phase (SetApplicationEnvironment,
// StartAndObserveApplication), and the API-key issuance/revocation boundary
// (ReconcileAPIKey, RevokeAPIKey).
//
// Lifecycle principles:
//   - ownership is enforced by loading the workspace through Get (which joins
//     the owning website on user_id), so an idempotent unauthorized caller sees
//     not-found rather than a row;
//   - state transitions are validated before any provider side-effect: suspend
//     requires `ready`, resume requires `suspended`, and delete rejects an
//     already-`deleting` row;
//   - the shared MySQL/MariaDB resource is a portal dependency, NOT a
//     workspace-owned resource: suspend/resume never stop or start it, and
//     delete never drops it. Suspend only stops the workspace application;
//     resume re-provisions (idempotent) the workspace's logical database/user
//     on the shared server and refreshes the runtime environment;
//   - delete marks `deleting`, revokes the portal API key through the exported
//     dashboard core.APIKeyService, deletes the application (including its
//     storage), then drops only the workspace's logical database/user on the
//     shared resource (never the shared resource itself), treats provider 404
//     as already deleted, and finally soft-deletes the workspace;
//   - the access route returns only the proxy Basic Auth credential, never the
//     portal API key or the database password; rotation persists and applies a
//     fresh credential to the provider application.
package workspace

import (
	"context"
	"errors"
	"fmt"

	"go.lumeweb.com/portal-plugin-ipfs/internal/coolify"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/db"
	"gorm.io/gorm"
)

// Sentinel errors for the workspace lifecycle (suspend/resume/delete/access).
var (
	// ErrWorkspaceInvalidState is returned when a lifecycle operation is
	// attempted from a status the transition does not allow (e.g. suspending a
	// workspace that is not ready, resuming one that is not suspended, or
	// deleting a row that is already `deleting`).
	ErrWorkspaceInvalidState = errors.New("workspace: workspace is not in a valid state for this operation")
)

// Suspend stops a ready workspace's application. The shared MySQL/MariaDB
// resource is a portal dependency, not a workspace-owned resource, so suspend
// never stops it: the workspace's logical database and its data remain intact
// and available. State is marked `suspended` only after the provider call
// succeeds. All application storage is preserved by the provider's stop.
func (s *WorkspaceService) Suspend(ctx context.Context, userID uint, workspaceID uint) (*pluginDb.Workspace, error) {
	if s.config == nil || !s.config.Enabled {
		return nil, ErrWorkspaceNotEnabled
	}
	if s.provider == nil {
		return nil, ErrWorkspaceProviderUnavailable
	}
	ws, err := s.ownedWorkspace(ctx, userID, workspaceID)
	if err != nil {
		return nil, err
	}
	if ws == nil {
		return nil, ErrWorkspaceNotFound
	}

	// Validate the transition: only a ready workspace can be suspended.
	if ws.Status != pluginDb.WorkspaceStatusReady {
		return nil, fmt.Errorf("%w: status is %s (want ready)", ErrWorkspaceInvalidState, ws.Status)
	}

	// 1. Stop the application. Storage is preserved by the provider's stop. The
	// shared database is intentionally left running (never stopped per
	// workspace).
	if ws.ApplicationResourceID != nil {
		if err := s.provider.StopApplication(ctx, *ws.ApplicationResourceID); err != nil {
			return nil, fmt.Errorf("workspace: failed to stop application: %w", err)
		}
	}

	// 2. Mark suspended only after the provider call succeeds.
	if err := s.setStatus(ctx, ws, pluginDb.WorkspaceStatusSuspended); err != nil {
		return nil, err
	}
	return ws, nil
}

// Resume starts a suspended workspace: it re-provisions (idempotently) the
// workspace's logical database/user on the shared MySQL/MariaDB resource,
// refreshes the portal API key and database credentials in the application
// environment, starts the application, waits for the Coolify deployment /
// application status to report ready (no portal HTTP probe), and marks the
// workspace `ready`. It never starts/stops the shared resource itself.
func (s *WorkspaceService) Resume(ctx context.Context, userID uint, workspaceID uint) (*pluginDb.Workspace, error) {
	if s.config == nil || !s.config.Enabled {
		return nil, ErrWorkspaceNotEnabled
	}
	if s.provider == nil {
		return nil, ErrWorkspaceProviderUnavailable
	}
	ws, err := s.ownedWorkspace(ctx, userID, workspaceID)
	if err != nil {
		return nil, err
	}
	if ws == nil {
		return nil, ErrWorkspaceNotFound
	}

	// Validate the transition: only a suspended workspace can be resumed.
	if ws.Status != pluginDb.WorkspaceStatusSuspended {
		return nil, fmt.Errorf("%w: status is %s (want suspended)", ErrWorkspaceInvalidState, ws.Status)
	}

	// 1. Re-provision the workspace's logical database/user on the shared
	// resource (idempotent CREATE/GRANT ... IF EXISTS + ALTER USER to converge
	// the derived password). The shared resource itself is never started or
	// stopped here.
	dbCreds, err := s.ReconcileDatabase(ctx, ws)
	if err != nil {
		return nil, fmt.Errorf("workspace: database not healthy: %w", err)
	}

	// 2. Refresh the portal API key and database credentials in the
	// application environment (reissue a fresh JWT; the token is held only long
	// enough to write it into the environment and then discarded).
	apiKey, err := s.ReconcileAPIKey(ctx, ws)
	if err != nil {
		return nil, err
	}
	if ws.ApplicationResourceID != nil {
		if err := s.SetApplicationEnvironment(ctx, ws, *ws.ApplicationResourceID, dbCreds, apiKey); err != nil {
			return nil, fmt.Errorf("workspace: failed to refresh application environment: %w", err)
		}
	}

	// 3. Start the application and wait for the Coolify application status to
	// report ready; marks ready only on success (no portal HTTP probe).
	if ws.ApplicationResourceID != nil {
		if err := s.StartAndObserveApplication(ctx, ws, *ws.ApplicationResourceID); err != nil {
			return nil, err
		}
	}
	return ws, nil
}

// Delete removes a workspace and its resources. It marks the row `deleting`,
// revokes the workspace portal API key through the exported dashboard
// core.APIKeyService, deletes the application (and its storage), then drops
// ONLY the workspace's logical database/user from the shared MySQL/MariaDB
// resource via DropLogicalDatabase — never the shared resource itself. A
// provider 404 is treated as already deleted, and finally the workspace is
// soft-deleted.
func (s *WorkspaceService) Delete(ctx context.Context, userID uint, workspaceID uint) (*pluginDb.Workspace, error) {
	if s.config == nil || !s.config.Enabled {
		return nil, ErrWorkspaceNotEnabled
	}
	if s.provider == nil {
		return nil, ErrWorkspaceProviderUnavailable
	}
	ws, err := s.ownedWorkspace(ctx, userID, workspaceID)
	if err != nil {
		return nil, err
	}
	if ws == nil {
		return nil, ErrWorkspaceNotFound
	}

	// Validate the transition: an already-deleting workspace is not deleted
	// again (the Delete API is idempotent at the DB level: a second call sees
	// the soft-deleted row excluded from the ownership join).
	if ws.Status == pluginDb.WorkspaceStatusDeleting {
		return nil, fmt.Errorf("%w: workspace is already being deleted", ErrWorkspaceInvalidState)
	}

	// 1. Mark deleting first so the row signals an in-progress teardown (and a
	// concurrent reconciler/delete does not double-run provider deletes).
	if err := s.setStatus(ctx, ws, pluginDb.WorkspaceStatusDeleting); err != nil {
		return nil, err
	}

	// 2. Revoke the workspace portal API key. The key row ID is a revocable
	// identity; the raw JWT was never persisted. Treated as best-effort but an
	// unexpected failure aborts the teardown so it can be retried.
	if ws.APIKeyID != nil {
		if s.apiKeySvc == nil {
			return nil, errors.New("workspace: api key service not available")
		}
		// Ownership is authoritative on the workspace (UserID).
		if err := s.apiKeySvc.RevokeAPIKey(ctx, ws.UserID, *ws.APIKeyID); err != nil {
			return nil, fmt.Errorf("workspace: failed to revoke api key: %w", err)
		}
	}

	// 3. Delete the application first (including its storage and volumes). A
	// provider 404 means it is already deleted.
	if ws.ApplicationResourceID != nil {
		if err := s.provider.DeleteApplication(ctx, *ws.ApplicationResourceID); err != nil && !coolify.IsNotFound(err) {
			return nil, fmt.Errorf("workspace: failed to delete application: %w", err)
		}
	}

	// 4. Drop only the workspace's logical database/user from the shared
	// MySQL/MariaDB resource. The shared resource itself is never touched.
	// DropLogicalDatabase is a no-op when no logical identifiers were
	// provisioned, so deleting a partially-provisioned workspace is safe.
	if err := s.DropLogicalDatabase(ctx, ws); err != nil {
		return nil, err
	}

	// 5. Soft-delete the workspace. The strict unique keys are intentionally
	// left STRICT; the tombstone must be purged by a later re-provision before
	// the website_id / label / provider-ID keys are reclaimed (see the model
	// comment).
	if err := s.softDelete(ctx, ws); err != nil {
		return nil, err
	}
	return ws, nil
}

// RotateAccessCredentials returns the workspace's proxy Basic Auth credentials
// to the owner. It never exposes the portal API key or the database password.
// When rotate is true it first generates fresh proxy credentials, persists
// them (overwriting the previous value), and applies them to the provider
// application so they take effect.
func (s *WorkspaceService) RotateAccessCredentials(ctx context.Context, userID uint, workspaceID uint, rotate bool) (*pluginDb.AccessCredentials, error) {
	if s.config == nil || !s.config.Enabled {
		return nil, ErrWorkspaceNotEnabled
	}
	ws, err := s.ownedWorkspace(ctx, userID, workspaceID)
	if err != nil {
		return nil, err
	}
	if ws == nil {
		return nil, ErrWorkspaceNotFound
	}

	var username, password string
	if rotate {
		username, password, err = generateProxyCredentials()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrProxyCredentialsUnavailable, err)
		}
		// Apply the new credential to the provider application first so it is
		// active in the running runtime. Persistence happens only AFTER the
		// provider update succeeds: if the provider rejects the rotation, the
		// old persisted credential is preserved so a retry re-applies the same
		// value to a consistent provider/DB state.
		if ws.ApplicationResourceID != nil {
			if s.provider == nil {
				return nil, ErrWorkspaceProviderUnavailable
			}
			if err := s.provider.SetApplicationBasicAuth(ctx, *ws.ApplicationResourceID, username, password); err != nil {
				return nil, fmt.Errorf("workspace: failed to apply rotated proxy credentials: %w", err)
			}
		}
		if err := s.overwriteProxyCredentials(ctx, ws, username, password); err != nil {
			return nil, err
		}
	} else {
		// Return the persisted credential, generating/persisting on first use
		// (defensive — proxyCredentials creates them at application creation).
		username, password, err = s.proxyCredentials(ctx, ws)
		if err != nil {
			return nil, err
		}
	}

	return &pluginDb.AccessCredentials{Username: username, Password: password}, nil
}

// ownedWorkspace loads a workspace scoped to the requesting user. It uses Get
// so ownership is enforced by the website join and an unauthorized or missing
// row resolves to (nil, nil).
func (s *WorkspaceService) ownedWorkspace(ctx context.Context, userID uint, workspaceID uint) (*pluginDb.Workspace, error) {
	return s.Get(ctx, userID, workspaceID)
}

// setStatus transitions the workspace to status and updates the in-memory
// pointer.
func (s *WorkspaceService) setStatus(ctx context.Context, ws *pluginDb.Workspace, status pluginDb.WorkspaceStatus) error {
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&pluginDb.Workspace{}).
			Where("id = ?", ws.ID).
			Update("status", status)
	})
	if err != nil {
		return fmt.Errorf("workspace: failed to set workspace status %s: %w", status, err)
	}
	ws.Status = status
	return nil
}

// overwriteProxyCredentials persists new proxy Basic Auth credentials on the
// workspace unconditionally (rotation) and updates the in-memory pointers.
func (s *WorkspaceService) overwriteProxyCredentials(ctx context.Context, ws *pluginDb.Workspace, username, password string) error {
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&pluginDb.Workspace{}).
			Where("id = ?", ws.ID).
			Updates(map[string]any{
				"proxy_username": username,
				"proxy_password": password,
			})
	})
	if err != nil {
		return fmt.Errorf("workspace: failed to persist rotated proxy credentials: %w", err)
	}
	ws.ProxyUsername = &username
	ws.ProxyPassword = &password
	return nil
}

// softDelete soft-deletes the workspace row (sets deleted_at) using GORM's
// Delete. The in-memory pointer is left intact so the caller can return it.
func (s *WorkspaceService) softDelete(ctx context.Context, ws *pluginDb.Workspace) error {
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.Delete(ws).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		return fmt.Errorf("workspace: failed to soft-delete workspace: %w", err)
	}
	return nil
}
