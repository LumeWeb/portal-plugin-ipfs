// This file implements the database provisioning and credential retrieval
// reconciliation for Workspace, plus the issuance/reissuance of the workspace
// portal API key through the dashboard plugin's exported core.APIKeyService
// interface. The methods here are the building blocks the reconciler drives.
//
// Architecture (corrected): there is ONE shared, Coolify-managed MySQL/MariaDB
// resource (WorkspaceConfig.Database.ResourceID). Reconciliation never creates,
// starts, stops, or deletes that resource. Instead the portal fetches the
// shared resource through Coolify's existing GET /databases/{uuid} API, derives
// its admin host, port, and root password from the response (no admin
// credentials are configured; no Coolify modifications are required), and uses
// a MySQL root connection (mysqlprovision.Engineer) to provision a *logical*
// database + user for each workspace: CREATE DATABASE / CREATE USER / GRANT.
// The runtime receives ONLY the logical connection values (host/port/name/
// user/password); no remote Coolify database/resource ID is ever passed into a
// workspace runtime or stored on the workspace.
//
// Secret safety principles:
//   - the generated logical database password is derived deterministically
//     with HKDF-SHA256 keyed by the portal identity key plus a per-workspace
//     random salt (see password.go); it is held in memory for the current
//     reconcile call only and is NEVER persisted, serialized, or logged;
//   - the shared resource's root password is fetched from Coolify per
//     operation, held in memory for the current call only, and is never
//     logged, persisted, or returned from an API;
//   - the portal API key JWT is held only long enough to write it into the
//     application environment and is then discarded;
//   - Coolify errors are already secret-safe (StatusCodeError never retains a
//     raw response body), and LastError is bounded and redacted.
//
// Provisioning is retryable and idempotent: the shared resource is only looked
// up (never recreated), logical provisioning uses CREATE/GRANT ... IF EXISTS
// semantics and ALTER USER to converge the password, and only the logical
// name/user are persisted on the workspace row.
package workspace

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	dashboardCore "go.lumeweb.com/portal-plugin-dashboard/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/coolify"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/mysqlprovision"
	"go.lumeweb.com/portal/db"
	"gorm.io/gorm"
)

// Sentinel errors for the database provisioning phase.
var (
	// ErrSharedDatabaseNotReady is returned when the shared MySQL/MariaDB
	// resource is not in a running/ready state (or cannot be resolved), so
	// logical provisioning cannot proceed. It is a shared-resource condition,
	// not a per-workspace failure.
	ErrSharedDatabaseNotReady = errors.New("workspace: shared database resource is not ready")
	// ErrDatabaseProvisionFailed is returned when logical database/user
	// provisioning on the shared resource fails.
	ErrDatabaseProvisionFailed = errors.New("workspace: failed to provision logical database")
	// ErrDatabaseDropFailed is returned when dropping a workspace's logical
	// database/user on deletion fails.
	ErrDatabaseDropFailed = errors.New("workspace: failed to drop logical database")
	// ErrDatabaseCredentialFieldMissing is retained for the shared-resource
	// lookup: Coolify reports the resource but omits the sensitive internal
	// URL/root-password fields (token may lack read:sensitive).
	ErrDatabaseCredentialFieldMissing = errors.New("workspace: database credential fields are missing (token may lack read:sensitive)")
	// ErrDatabaseURLMalformed is returned when the shared DB internal URL cannot
	// be parsed with net/url.
	ErrDatabaseURLMalformed = errors.New("workspace: database internal url is malformed")
	// ErrDatabaseServerFailed is returned when the shared database reports a
	// terminal status (e.g. failed).
	ErrDatabaseServerFailed = errors.New("workspace: database server reported a terminal failure")
)

// sharedDatabaseAdminUser is the admin user the portal uses to connect to the
// shared MySQL/MariaDB resource to provision logical databases/users. Coolify
// always provisions a root account on the shared resource; the root password is
// fetched from Coolify per operation (never configured or persisted).
const sharedDatabaseAdminUser = "root"

// apiKeyTTL is how long each issued/reissued workspace portal API key is
// valid. Reissue before expiry (or after a portal restart) is handled by the
// reconciler calling ReconcileAPIKey on later passes.
const apiKeyTTL = 90 * 24 * time.Hour

// maxLastErrorLen bounds the persisted LastError string so a pathological
// provider error cannot bloat the workspace row.
const maxLastErrorLen = 512

// DatabaseCredentials carries the generated logical database connection
// details for one workspace. Host/Port point at the shared MySQL/MariaDB
// resource (its internal host/alias) and Database/Username/Password are the
// logical database/user created for this workspace. Password is ephemeral: it
// lives only for the current reconcile call, is never persisted or logged, and
// is discarded when the function returns.
type DatabaseCredentials struct {
	Host     string
	Port     uint16
	Database string
	Username string
	Password string
}

// ReconcileDatabase advances the logical-database provisioning phase for ws:
//
//   - fetches the shared MySQL/MariaDB resource via Coolify's existing
//     GET /databases/{uuid} API and derives its admin host, port, and root
//     password from the response (no config admin connection, no Coolify
//     modifications);
//   - derives this workspace's logical database name, user, and password;
//   - provisions (idempotently) the logical database/user on the shared server
//     through the MySQL root connection;
//   - persists the logical name/user on the workspace;
//   - returns the logical connection credentials for the current call.
//
// It never creates, starts, stops, or deletes the shared Coolify resource and
// never calls POST /databases/mariadb. The returned password and the shared
// root password are held in memory for the current call only and are never
// persisted, logged, or passed into the runtime environment.
func (s *WorkspaceService) ReconcileDatabase(ctx context.Context, ws *pluginDb.Workspace) (*DatabaseCredentials, error) {
	if s.provider == nil {
		return nil, fmt.Errorf("workspace: provider not available")
	}
	if s.config == nil {
		return nil, fmt.Errorf("workspace: config not available")
	}

	// 1. Fetch the shared resource and derive its admin host, port, and root
	// password from Coolify (no AdminHost/AdminPort override is configured).
	host, port, rootPassword, err := s.resolveSharedDatabase(ctx)
	if err != nil {
		return nil, err
	}

	// 2. Deterministic logical identifiers and password. The password is
	// derived from the portal identity key plus this workspace's random salt
	// (generated/persisted lazily); it is held in memory only.
	dbName := deterministicDatabaseName(ws.ID)
	dbUser := deterministicDatabaseUser(ws.ID)
	password, err := s.databasePassword(ctx, ws)
	if err != nil {
		return nil, err
	}

	// 3. Provision the logical database/user (idempotent, safe on retry) using
	// the shared resource's root account and the transient root password.
	eng, cleanup, err := s.dbEngineer(sharedDatabaseAdminUser, rootPassword, host, port)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	if err := eng.EnsureDatabase(ctx, mysqlprovision.EnsureRequest{
		Database: dbName,
		User:     dbUser,
		Password: password,
	}); err != nil {
		err = fmt.Errorf("%w: %v", ErrDatabaseProvisionFailed, err)
		_ = s.recordLastError(ctx, ws, err)
		return nil, err
	}

	// 4. Persist the logical name/user (idempotent) so reconciliation and
	// deletion can resolve them across passes.
	if err := s.setDatabaseIdentifiers(ctx, ws, dbName, dbUser); err != nil {
		return nil, err
	}

	// 5. Return only logical connection values. No remote DB ID is ever here.
	return &DatabaseCredentials{
		Host:     host,
		Port:     port,
		Database: dbName,
		Username: dbUser,
		Password: password,
	}, nil
}

// resolveSharedDatabase fetches the shared MySQL/MariaDB resource through
// Coolify's existing GET /databases/{uuid} API and returns its admin host,
// port, and root password. Host/port are parsed from the resource's internal
// DB URL; the root password comes from the engine-specific root-password field
// selected by the resource type. It fails closed when the resource is not in a
// running/ready status or when the internal URL or root password is absent
// (token may lack read:sensitive).
func (s *WorkspaceService) resolveSharedDatabase(ctx context.Context) (string, uint16, string, error) {
	dbCfg := s.config.Database
	if dbCfg.ResourceID == "" {
		return "", 0, "", errors.New("workspace: database.resource_id is required to resolve the shared database")
	}
	res, err := s.provider.ResolveDatabaseResource(ctx, dbCfg.ResourceID)
	if err != nil {
		return "", 0, "", fmt.Errorf("workspace: failed to resolve shared database resource: %w", err)
	}
	switch res.Status {
	case coolify.ResourceStatusRunning, coolify.ResourceStatusReady:
		// ok
	case coolify.ResourceStatusFailed:
		// A terminal failure is an operator problem, not a transient/retryable
		// condition: fail closed with the terminal sentinel so the reconciler
		// does not hot-loop provisioning work against a dead shared server.
		return "", 0, "", fmt.Errorf("%w: resource %s status %s", ErrDatabaseServerFailed, dbCfg.ResourceID, res.Status)
	default:
		// Starting/stopping/exited/etc. — not ready yet; retryable.
		return "", 0, "", fmt.Errorf("%w: resource %s status %s", ErrSharedDatabaseNotReady, dbCfg.ResourceID, res.Status)
	}
	if res.InternalURL == "" {
		err := fmt.Errorf("%w: internal_db_url", ErrDatabaseCredentialFieldMissing)
		_ = s.recordLastErrorField(ctx, 0, err) // no workspace row yet; surface only
		return "", 0, "", err
	}
	if res.RootPassword == "" {
		err := fmt.Errorf("%w: root password", ErrDatabaseCredentialFieldMissing)
		_ = s.recordLastErrorField(ctx, 0, err) // no workspace row yet; surface only
		return "", 0, "", err
	}
	host, port, err := parseDatabaseURL(res.InternalURL)
	if err != nil {
		return "", 0, "", fmt.Errorf("%w: %v", ErrDatabaseURLMalformed, err)
	}
	return host, port, res.RootPassword, nil
}

// dbEngineer returns the MySQL admin Engineer for the shared resource's root
// account. When a fake is injected (tests) it is returned with a no-op cleanup;
// otherwise a real connection is opened against the shared resource using the
// fetched root credentials and cleaned up when the caller finishes. The root
// password is held in memory for the current call only.
func (s *WorkspaceService) dbEngineer(adminUser, adminPassword string, host string, port uint16) (mysqlprovision.Engineer, func(), error) {
	if s.mysqlProv != nil {
		return s.mysqlProv, func() {}, nil
	}
	if adminUser == "" {
		return nil, nil, fmt.Errorf("workspace: shared database admin user is missing")
	}
	eng, err := mysqlprovision.NewEngineer(adminUser, adminPassword, host, port)
	if err != nil {
		return nil, nil, fmt.Errorf("workspace: failed to open shared database admin connection: %w", err)
	}
	return eng, func() { _ = eng.Close() }, nil
}

// setDatabaseIdentifiers persists the workspace's logical database name/user and
// updates the in-memory pointers.
func (s *WorkspaceService) setDatabaseIdentifiers(ctx context.Context, ws *pluginDb.Workspace, name, user string) error {
	if ws.DatabaseName != nil && ws.DatabaseUser != nil && *ws.DatabaseName == name && *ws.DatabaseUser == user {
		ws.DatabaseName = &name
		ws.DatabaseUser = &user
		return nil
	}
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&pluginDb.Workspace{}).
			Where("id = ?", ws.ID).
			Updates(map[string]any{
				"database_name": name,
				"database_user": user,
			})
	})
	if err != nil {
		return fmt.Errorf("workspace: failed to persist logical database identifiers: %w", err)
	}
	ws.DatabaseName = &name
	ws.DatabaseUser = &user
	return nil
}

// DropLogicalDatabase drops only the workspace's logical database and user on
// the shared resource during deletion. It resolves the shared host/port (via
// config overrides), uses the persisted logical name/user, and never touches
// the shared resource itself. Idempotent: DROP ... IF EXISTS.
func (s *WorkspaceService) DropLogicalDatabase(ctx context.Context, ws *pluginDb.Workspace) error {
	if s.config == nil {
		return fmt.Errorf("workspace: config not available")
	}
	if ws.DatabaseName == nil || ws.DatabaseUser == nil {
		// Nothing provisioned; treat as already dropped.
		return nil
	}
	host, port, rootPassword, err := s.resolveSharedDatabase(ctx)
	if err != nil {
		return err
	}
	eng, cleanup, err := s.dbEngineer(sharedDatabaseAdminUser, rootPassword, host, port)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := eng.DropDatabase(ctx, mysqlprovision.EnsureRequest{
		Database: *ws.DatabaseName,
		User:     *ws.DatabaseUser,
	}); err != nil {
		return fmt.Errorf("%w: %v", ErrDatabaseDropFailed, err)
	}
	return nil
}

// provisionTimeout returns the configured provisioning timeout with a sane
// fallback.
func (s *WorkspaceService) provisionTimeout() time.Duration {
	if s.config != nil && s.config.ProvisionTimeout > 0 {
		return s.config.ProvisionTimeout
	}
	return 15 * time.Minute
}

// databasePassword derives this workspace's logical database password from the
// portal identity key and this workspace's random salt (generated and persisted
// lazily on first provisioning, so each workspace can rotate independently). It
// returns the derived password for the current in-memory use only; nothing is
// persisted or logged.
func (s *WorkspaceService) databasePassword(ctx context.Context, ws *pluginDb.Workspace) (string, error) {
	if len(s.identityKey) == 0 {
		return "", fmt.Errorf("workspace: failed to generate database password: %w", errEmptyIdentityKey)
	}
	saltB64, err := s.ensureDatabasePasswordSalt(ctx, ws)
	if err != nil {
		return "", err
	}
	salt, err := base64.RawURLEncoding.DecodeString(saltB64)
	if err != nil {
		return "", fmt.Errorf("workspace: failed to decode database password salt: %w", err)
	}
	password, err := deriveDatabasePassword(s.identityKey, ws.ID, salt)
	if err != nil {
		return "", fmt.Errorf("workspace: failed to generate database password: %w", err)
	}
	return password, nil
}

// ensureDatabasePasswordSalt returns the workspace's persisted salt, generating
// and persisting one (idempotently) on first use. Only the non-secret salt is
// stored; the derived password is never persisted.
func (s *WorkspaceService) ensureDatabasePasswordSalt(ctx context.Context, ws *pluginDb.Workspace) (string, error) {
	if ws.DatabasePasswordSalt != nil && *ws.DatabasePasswordSalt != "" {
		return *ws.DatabasePasswordSalt, nil
	}
	saltB64, err := newDatabasePasswordSalt()
	if err != nil {
		return "", err
	}
	err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		update := tx.Model(&pluginDb.Workspace{}).
			Where("id = ? AND database_password_salt IS NULL", ws.ID).
			Update("database_password_salt", saltB64)
		if update.RowsAffected == 0 {
			_ = tx.First(ws, ws.ID)
		}
		return tx
	})
	if err != nil {
		return "", fmt.Errorf("workspace: failed to persist database password salt: %w", err)
	}
	ws.DatabasePasswordSalt = &saltB64
	return saltB64, nil
}

// parseDatabaseURL parses the Coolify internal DB URL with net/url and returns
// the host and port. The URL is parsed structurally, never split on
// punctuation, so it survives non-alphanumeric characters in the host or
// credentials.
func parseDatabaseURL(raw string) (string, uint16, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", 0, err
	}
	if u.Hostname() == "" {
		return "", 0, errors.New("no host in database url")
	}
	port := uint16(3306) // MariaDB default
	if ps := u.Port(); ps != "" {
		p, cerr := strconv.ParseUint(ps, 10, 16)
		if cerr != nil || p == 0 {
			return "", 0, fmt.Errorf("invalid port %q", ps)
		}
		port = uint16(p)
	}
	return u.Hostname(), port, nil
}

// ReconcileAPIKey ensures the owning user has a workspace portal API key row
// and (re)issues a valid JWT through the dashboard plugin's exported
// core.APIKeyService. It issues when Workspace.APIKeyID is nil and reissues
// otherwise, so a retry after a portal restart or before expiry refreshes the
// token without creating duplicate key rows.
//
// The returned one-time JWT must be held only long enough to write it into the
// runtime environment and then discarded; it is never persisted here. Only the
// API key row ID (a revocable identity, not the token) is stored on the
// workspace.
func (s *WorkspaceService) ReconcileAPIKey(ctx context.Context, ws *pluginDb.Workspace) (*dashboardCore.IssuedAPIKey, error) {
	if s.apiKeySvc == nil {
		return nil, errors.New("workspace: api key service not available")
	}
	// Ownership is authoritative on the workspace (UserID); no Website join is
	// needed, so unattached workspaces can issue API keys too.
	ownerID := ws.UserID
	name := deterministicAPIKeyName(ws.ID)

	if ws.APIKeyID == nil {
		issued, err := s.apiKeySvc.IssueAPIKey(ctx, ownerID, name, apiKeyTTL)
		if err != nil {
			return nil, fmt.Errorf("workspace: failed to issue api key: %w", err)
		}
		if issued.ID == 0 {
			return nil, errors.New("workspace: api key issuance returned no id")
		}
		if err := s.setAPIKeyID(ctx, ws, issued.ID); err != nil {
			return nil, err
		}
		return issued, nil
	}

	// Reissue a valid JWT for the existing key row. Held in memory only.
	issued, err := s.apiKeySvc.ReissueAPIKey(ctx, ownerID, *ws.APIKeyID, apiKeyTTL)
	if err != nil {
		return nil, fmt.Errorf("workspace: failed to reissue api key: %w", err)
	}
	return issued, nil
}

// setAPIKeyID persists the workspace API key row ID and updates the in-memory
// pointer.
func (s *WorkspaceService) setAPIKeyID(ctx context.Context, ws *pluginDb.Workspace, keyID uint) error {
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		update := tx.Model(&pluginDb.Workspace{}).
			Where("id = ? AND api_key_id IS NULL", ws.ID).
			Update("api_key_id", keyID)
		if update.RowsAffected == 0 {
			_ = tx.First(ws, ws.ID)
		}
		return tx
	})
	if err != nil {
		return fmt.Errorf("workspace: failed to persist api key id: %w", err)
	}
	ws.APIKeyID = &keyID
	return nil
}

// recordLastError persists a bounded, redacted LastError on the workspace. The
// Coolify errors are already secret-safe; this bounds length so a pathological
// error cannot bloat the row. Password/token values are never written here.
func (s *WorkspaceService) recordLastError(ctx context.Context, ws *pluginDb.Workspace, err error) error {
	msg := err.Error()
	if len(msg) > maxLastErrorLen {
		msg = msg[:maxLastErrorLen]
	}
	return db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&pluginDb.Workspace{}).
			Where("id = ?", ws.ID).
			Update("last_error", msg)
	})
}

// recordLastErrorField records a bounded error on a workspace-less path
// (shared-resource resolution). It is a no-op carrier so shared-resolution
// errors surface consistently without a row.
func (s *WorkspaceService) recordLastErrorField(_ context.Context, _ uint, _ error) error {
	return nil
}

// isDatabaseFailure reports whether err is a terminal database failure that
// should be recorded, as opposed to a transient/retryable condition.
func isDatabaseFailure(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrSharedDatabaseNotReady) ||
		errors.Is(err, ErrDatabaseProvisionFailed) ||
		errors.Is(err, ErrDatabaseCredentialFieldMissing) ||
		errors.Is(err, ErrDatabaseURLMalformed) ||
		errors.Is(err, ErrDatabaseServerFailed)
}

// sleepContext sleeps for d or returns false when the context is cancelled first.
func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// deterministicDatabaseName is the deterministic logical database name for a
// workspace on the shared MySQL/MariaDB server. It is validated by
// mysqlprovision (strict identifier charset).
func deterministicDatabaseName(id uint) string {
	return fmt.Sprintf("workspace_%d", id)
}

// deterministicDatabaseUser is the deterministic logical user for a workspace.
func deterministicDatabaseUser(id uint) string {
	return fmt.Sprintf("workspace_%d", id)
}

// deterministicAPIKeyName is the dashboard API key name for a workspace's
// portal credential.
func deterministicAPIKeyName(id uint) string {
	return fmt.Sprintf("workspace-%d", id)
}
