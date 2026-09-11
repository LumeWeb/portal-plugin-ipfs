// Package mysqlprovision implements logical database/user provisioning on a
// shared, Coolify-managed MySQL/MariaDB server.
//
// The workspace architecture owns ONE shared MariaDB resource (configured in
// WorkspaceConfig.Database). Per-workspace reconciliation never creates,
// starts, stops, or deletes that shared resource; instead the portal connects
// to it with an admin account and provisions a *logical* database + user for
// each workspace (CREATE DATABASE / CREATE USER / GRANT), and drops only that
// workspace's database + user on workspace deletion.
//
// Identifier safety: every database/user name is validated against a strict
// identifier charset and backtick-quoted before interpolation. Passwords are
// inserted as single-quoted SQL string literals with the relevant characters
// escaped. Callers must supply generated strong passwords (see
// derived by the workspace package (never persisted) and must never
// interpolate untrusted names.
//
// All operations are idempotent and safe to retry: CREATE ... IF NOT EXISTS,
// ALTER USER to converge the password, idempotent GRANT, and DROP ... IF
// EXISTS for cleanup.
package mysqlprovision

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// EnsureRequest is the logical database + user to provision on the shared
// server. Database and User are validated identifiers; Password is a generated
// strong secret.
type EnsureRequest struct {
	Database string
	User     string
	Password string
}

// Engineer is the MySQL database-provisioning boundary consumed by the
// workspace service. It provisions and drops only the logical database/user it
// is asked for; it never creates or destroys the shared server resource.
type Engineer interface {
	EnsureDatabase(ctx context.Context, req EnsureRequest) error
	DropDatabase(ctx context.Context, req EnsureRequest) error
	Close() error
}

// MySQLEngineer is the real Engineer backed by a *sql.DB admin connection to
// the shared MySQL/MariaDB server.
type MySQLEngineer struct {
	db *sql.DB
}

// NewEngineer opens an admin connection (database/sql, MySQL driver) to the
// shared MariaDB server using the configured admin credentials. host may be a
// hostname or Coolify network alias; port is the DB port (default 3306 when 0).
func NewEngineer(adminUser, adminPassword, host string, port uint16) (*MySQLEngineer, error) {
	if adminUser == "" {
		return nil, errors.New("mysqlprovision: admin user is required")
	}
	if host == "" {
		return nil, errors.New("mysqlprovision: host is required")
	}
	if port == 0 {
		port = 3306
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s&readTimeout=10s&writeTimeout=10s&parseTime=true",
		adminUser, adminPassword, host, port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysqlprovision: open: %w", err)
	}
	return &MySQLEngineer{db: db}, nil
}

// NewEngineerFromDB wraps an already-open *sql.DB (used in tests and by
// callers that manage the driver/session themselves).
func NewEngineerFromDB(db *sql.DB) *MySQLEngineer {
	return &MySQLEngineer{db: db}
}

// Close closes the admin connection.
func (e *MySQLEngineer) Close() error {
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

// identRe is the strict charset for MySQL identifiers we manage. Only
// alphanumerics and underscore are allowed, so backtick quoting is always
// safe.
var identRe = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)

// quoteIdent validates and backtick-quotes an identifier. It returns an error
// for any name outside the strict charset, so untrusted names can never inject
// SQL.
func quoteIdent(name string) (string, error) {
	if !identRe.MatchString(name) {
		return "", fmt.Errorf("mysqlprovision: invalid identifier %q (allowed: [A-Za-z0-9_]{1,64})", name)
	}
	return "`" + name + "`", nil
}

// quoteString escapes a value for embedding inside a single-quoted SQL string
// literal.
func quoteString(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `''`, "\x00", "")
	return "'" + r.Replace(v) + "'"
}

// EnsureDatabase idempotently provisions the logical database, user, and grant
// on the shared server. Safe to call repeatedly on retries.
func (e *MySQLEngineer) EnsureDatabase(ctx context.Context, req EnsureRequest) error {
	if e.db == nil {
		return errors.New("mysqlprovision: no database connection")
	}
	if err := validateReq(req); err != nil {
		return err
	}
	db, err := quoteIdent(req.Database)
	if err != nil {
		return err
	}
	pass := quoteString(req.Password)

	stmts := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db),
		fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY %s", req.User, pass),
		// Converge the password so a retried/rotated secret is current even
		// when the user already exists from an earlier partial run.
		fmt.Sprintf("ALTER USER '%s'@'%%' IDENTIFIED BY %s", req.User, pass),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%%'", db, req.User),
		"FLUSH PRIVILEGES",
	}
	for _, s := range stmts {
		if _, err := e.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("mysqlprovision: %s: %w", summarize(s, req), err)
		}
	}
	return nil
}

// DropDatabase idempotently drops the workspace's logical database and user. It
// never touches the shared server or any other database/user.
func (e *MySQLEngineer) DropDatabase(ctx context.Context, req EnsureRequest) error {
	if e.db == nil {
		return errors.New("mysqlprovision: no database connection")
	}
	if err := validateReq(req); err != nil {
		return err
	}
	db, err := quoteIdent(req.Database)
	if err != nil {
		return err
	}
	stmts := []string{
		fmt.Sprintf("DROP DATABASE IF EXISTS %s", db),
		fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", req.User),
		"FLUSH PRIVILEGES",
	}
	for _, s := range stmts {
		if _, err := e.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("mysqlprovision: %s: %w", summarize(s, req), err)
		}
	}
	return nil
}

func validateReq(req EnsureRequest) error {
	if req.Database == "" || req.User == "" {
		return errors.New("mysqlprovision: database and user are required")
	}
	if !identRe.MatchString(req.Database) {
		return fmt.Errorf("mysqlprovision: invalid database name %q", req.Database)
	}
	if !identRe.MatchString(req.User) {
		return fmt.Errorf("mysqlprovision: invalid user name %q", req.User)
	}
	if req.Password == "" {
		return errors.New("mysqlprovision: password is required")
	}
	return nil
}

// summarize returns a secret-free prefix of a statement for error wrapping
// (never includes the password).
func summarize(stmt string, req EnsureRequest) string {
	s := strings.TrimSpace(stmt)
	if i := strings.Index(s, " "); i > 0 {
		s = s[:i]
	}
	return strings.ToUpper(s) + " (" + req.Database + "/" + req.User + ")"
}
