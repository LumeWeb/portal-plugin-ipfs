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
	"go.uber.org/zap"
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

	// First load establishes ownership only; the transition guard is checked
	// from the state re-read under the workspace lock (see Delete).
	owned, err := s.ownedWorkspace(ctx, userID, workspaceID)
	if err != nil {
		return nil, err
	}
	if owned == nil {
		return nil, ErrWorkspaceNotFound
	}

	unlock := s.lockWorkspace(workspaceID)
	defer unlock()

	ws, err := s.reloadWorkspaceLocked(ctx, workspaceID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrWorkspaceNotFound
		}
		return nil, err
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

	// First load establishes ownership only; the transition guard is checked
	// from the state re-read under the workspace lock (see Delete).
	owned, err := s.ownedWorkspace(ctx, userID, workspaceID)
	if err != nil {
		return nil, err
	}
	if owned == nil {
		return nil, ErrWorkspaceNotFound
	}

	unlock := s.lockWorkspace(workspaceID)
	defer unlock()

	ws, err := s.reloadWorkspaceLocked(ctx, workspaceID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrWorkspaceNotFound
		}
		return nil, err
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

// Delete removes a workspace and its resources. Serialized by the per-
// workspace lock, it marks the row `deleting`, revokes the workspace portal
// API key through the exported dashboard core.APIKeyService, deletes the
// authoring hostname's records from the platform zone (before anything
// destructive, so a partial teardown never leaves a live hostname), deletes
// the application (and its storage), then drops ONLY the workspace's logical
// database/user from the shared MySQL/MariaDB resource via
// DropLogicalDatabase — never the shared resource itself. A provider 404 is
// treated as already deleted, and finally the workspace is soft-deleted.
// A delete that previously failed mid-teardown may be retried: every step is
// idempotent, so the retry resumes where it stopped.
func (s *WorkspaceService) Delete(ctx context.Context, userID uint, workspaceID uint) (*pluginDb.Workspace, error) {
	if s.config == nil || !s.config.Enabled {
		return nil, ErrWorkspaceNotEnabled
	}
	if s.provider == nil {
		return nil, ErrWorkspaceProviderUnavailable
	}

	// Serialize against the reconciler and the other lifecycle mutators. The
	// FIRST load only establishes ownership (and rejects a not-found row
	// cheaply); the state the teardown acts on is re-read under the lock.
	owned, err := s.ownedWorkspace(ctx, userID, workspaceID)
	if err != nil {
		return nil, err
	}
	if owned == nil {
		return nil, ErrWorkspaceNotFound
	}

	unlock := s.lockWorkspace(workspaceID)
	defer unlock()

	ws, err := s.reloadWorkspaceLocked(ctx, workspaceID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Soft-deleted between the two loads by the concurrent delete that
			// held the lock: completed, so surface the API's not-found.
			return nil, ErrWorkspaceNotFound
		}
		return nil, err
	}

	// A delete that previously failed mid-teardown (status `deleting`) may be
	// re-run: every step below is idempotent — revoking a revoked key is a
	// no-op, provider deletes treat 404 as already deleted, logical drops use
	// DROP ... IF EXISTS, RRSet deletes are PowerDNS no-ops — so a retry
	// simply resumes where it stopped. A COMPLETED delete surfaces as
	// not-found instead: the row is soft-deleted and excluded from Get, so
	// the Delete API stays idempotent at the DB level without a status guard.

	// 1. Mark deleting first so the row signals an in-progress teardown. The
	// workspace lock and this marker together serialize the teardown; a retry
	// after a failed step re-runs the marker (a no-op) and resumes.
	if err := s.setStatus(ctx, ws, pluginDb.WorkspaceStatusDeleting); err != nil {
		return nil, err
	}

	// 2. Revoke the workspace portal API key. The key row ID is a revocable
	// identity; the raw JWT was never persisted. An unexpected failure aborts
	// the teardown so it can be retried (nothing destructive has happened
	// yet); a missing/already-revoked key is treated as success by the
	// dashboard service, so the retry resumes past it.
	if ws.APIKeyID != nil {
		if s.apiKeySvc == nil {
			return nil, errors.New("workspace: api key service not available")
		}
		// Ownership is authoritative on the workspace (UserID).
		if err := s.apiKeySvc.RevokeAPIKey(ctx, ws.UserID, *ws.APIKeyID); err != nil {
			return nil, fmt.Errorf("workspace: failed to revoke api key: %w", err)
		}
	}

	// 3. Delete the authoring hostname's record from the platform root's zone
	// BEFORE any destructive provider work, so a partial teardown never leaves
	// a published hostname pointing at a deleted application. A failure here
	// aborts while everything is still intact (retryable); a missing zone is a
	// no-op. Like the DNS write, the delete is an idempotent no-op-able RRSet
	// OPERATION, so a later retry re-running it is harmless.
	if err := s.DeleteDNSRecords(ctx, ws); err != nil {
		return nil, fmt.Errorf("workspace: failed to delete workspace dns record: %w", err)
	}

	// 4. Delete the application (including its storage and volumes). A
	// provider 404 means it is already deleted.
	if ws.ApplicationResourceID != nil {
		if err := s.provider.DeleteApplication(ctx, *ws.ApplicationResourceID); err != nil && !coolify.IsNotFound(err) {
			return nil, fmt.Errorf("workspace: failed to delete application: %w", err)
		}
	}

	// 5. Drop only the workspace's logical database/user from the shared
	// MySQL/MariaDB resource. The shared resource itself is never touched.
	// DropLogicalDatabase is a no-op when no logical identifiers were
	// provisioned, so deleting a partially-provisioned workspace is safe.
	if err := s.DropLogicalDatabase(ctx, ws); err != nil {
		return nil, err
	}

	// 6. Soft-delete the workspace. The strict unique keys are intentionally
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
	// First load establishes ownership only; the workspace state below is
	// re-read under the lock so a concurrent reconcile/teardown cannot race
	// the env upsert and redeploy (see Delete).
	owned, err := s.ownedWorkspace(ctx, userID, workspaceID)
	if err != nil {
		return nil, err
	}
	if owned == nil {
		return nil, ErrWorkspaceNotFound
	}

	unlock := s.lockWorkspace(workspaceID)
	defer unlock()

	ws, err := s.reloadWorkspaceLocked(ctx, workspaceID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrWorkspaceNotFound
		}
		return nil, err
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
		//
		// The auth is enforced by the workspace image from the WORKSPACE_AUTH_*
		// secret env vars, so applying the rotation means upserting those vars
		// and triggering a redeploy — Coolify only reads environment changes
		// at deploy time. Only the deployment is refreshed; storage/volumes
		// are never touched here.
		//
		// The whole sequence (env upsert → redeploy → wait → row persist) is
		// treated as atomic: the rotation is guarded by a rollback that undoes
		// any partial application (provider env + in-memory pointers) back to
		// the persisted values, and the rollback is disarmed only after the
		// row has been persisted.
		var rb *deployRollback
		if ws.ApplicationResourceID != nil {
			if s.provider == nil {
				return nil, ErrWorkspaceProviderUnavailable
			}
			rb = newDeployRollback()
			defer rb.run(ctx)

			// Snapshot the current (old) in-memory pointers so the rollback can
			// restore both the workspace object and the provider environment to
			// the persisted state.
			oldUser, oldPass := ws.ProxyUsername, ws.ProxyPassword
			envApplied := false
			restore := func(ctx context.Context) {
				// Always drop the unpersisted staged values from the workspace
				// object; re-upsert the provider env only if the rotated values
				// actually reached Coolify.
				ws.ProxyUsername, ws.ProxyPassword = oldUser, oldPass
				if !envApplied || oldUser == nil || oldPass == nil || ws.ApplicationResourceID == nil {
					return
				}
				authEnv, err := s.proxyAuthEnvironment(ws)
				if err != nil {
					s.Logger().Error("workspace: rotation rollback could not build old auth env", zap.Error(err))
					return
				}
				if err := s.provider.SetApplicationEnvironment(ctx, *ws.ApplicationResourceID, authEnv); err != nil {
					s.Logger().Error("workspace: rotation rollback env restore failed",
						zap.String("app_id", *ws.ApplicationResourceID), zap.Error(err))
					return
				}
				// The abandoned rotation deploy may still have gone out
				// (ambiguously), so trigger a best-effort redeploy so the
				// running container is guaranteed to enforce the restored,
				// row-consistent credentials. A failed restore deploy leaves
				// the next reconcile/rotation to converge.
				if _, err := s.provider.StartApplication(ctx, *ws.ApplicationResourceID); err != nil {
					s.Logger().Error("workspace: rotation rollback redeploy failed",
						zap.String("app_id", *ws.ApplicationResourceID), zap.Error(err))
				}
			}

			// Stage the rotated credentials in memory so the environment build
			// injects the NEW values; persistence still happens only after the
			// provider calls succeed.
			ws.ProxyUsername, ws.ProxyPassword = &username, &password
			authEnv, err := s.proxyAuthEnvironment(ws)
			if err != nil {
				restore(ctx)
				return nil, fmt.Errorf("%w: %v", ErrProxyCredentialsUnavailable, err)
			}
			if err := s.provider.SetApplicationEnvironment(ctx, *ws.ApplicationResourceID, authEnv); err != nil {
				restore(ctx)
				return nil, fmt.Errorf("workspace: failed to apply rotated proxy credentials: %w", err)
			}
			envApplied = true
			rb.push(restore)
			dep, err := s.provider.StartApplication(ctx, *ws.ApplicationResourceID)
			if err != nil {
				return nil, fmt.Errorf("workspace: failed to redeploy workspace for rotated credentials: %w", err)
			}
			// Wait for the redeploy to reach a terminal state before returning
			// (and persisting) the new credential: Coolify only reads the env
			// upsert at deploy time, so an early success report would hand the
			// owner credentials the running container is not enforcing yet.
			// A failed/slow deployment errors out here with the old persisted
			// credential intact, so a retry converges. Volumes are unaffected.
			if dep.ID != "" {
				if err := s.waitDeploymentFinished(ctx, dep.ID); err != nil {
					return nil, fmt.Errorf("workspace: redeploy for rotated credentials did not finish: %w", err)
				}
			}
		}
		if err := s.overwriteProxyCredentials(ctx, ws, username, password); err != nil {
			return nil, err
		}
		if rb != nil {
			// The full sequence (env + redeploy + row) succeeded; disarm the
			// rollback.
			rb.commit()
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

// deployRollback collects compensating actions for a multi-step provider
// mutation (env upsert, redeploy, storage) so a failure part-way through
// cannot leave the provider holding a half-applied state. It is used like a
// mutex: defer the release immediately, and only commit() after the LAST
// side-effect (including the workspace-row persistence) succeeds — otherwise
// the deferred release runs every compensating action in LIFO order. Steps run
// best-effort; failures are logged and never mask the original error, since
// the next reconcile pass re-asserts the persisted environment anyway.
type deployRollback struct {
	steps []func(context.Context)
	done  bool
}

func newDeployRollback() *deployRollback {
	return &deployRollback{}
}

// push registers a compensating action. Actions run in reverse order of
// registration.
func (d *deployRollback) push(step func(context.Context)) {
	d.steps = append(d.steps, step)
}

// commit marks the operation successful and disarms the deferred rollback.
func (d *deployRollback) commit() {
	d.done = true
}

// run executes and clears the rollback when the operation was not committed.
// Steps are best-effort: each one logs its own failures via the bound logger.
func (d *deployRollback) run(ctx context.Context) {
	if d.done {
		return
	}
	for i := len(d.steps) - 1; i >= 0; i-- {
		d.steps[i](ctx)
	}
	d.steps = nil
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
