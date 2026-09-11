// Tests for the MySQL logical database/user provisioning abstraction
// (internal/mysqlprovision).
//
// Coverage approach: the real MySQLEngineer is backed by database/sql with the
// go-sql-driver/mysql driver (see filesystem dependency visibility). The shared
// server is configured once by the operator and is never created/deleted by
// workspace reconciliation, so these tests drive the Engineer through a fake
// database/sql driver (DATA-DOG/go-sqlmock) instead of requiring a live MySQL
// server. This proves the exact SQL issued (CREATE DATABASE / CREATE USER /
// GRANT / ALTER USER / DROP ... IF EXISTS), idempotency, password escaping, and
// strict identifier validation without any network/database dependency.
//
// A live, opt-in MySQL/MariaDB integration test is intentionally NOT added
// here: the repository does not currently vendor a testcontainers driver, so a
// containerised test would add tooling the codebase does not otherwise use.
// The fake-driver coverage below plus the workspace service fake-provider tests
// cover the provisioning contract.
package mysqlprovision

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// newMockEngineer returns an Engineer backed by a sqlmock database and the mock
// handle to set expectations.
func newMockEngineer(t *testing.T) (*MySQLEngineer, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewEngineerFromDB(db), mock
}

// finalizeExpectations fails the test if any set expectation was not met.
func finalizeExpectations(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled expectations: %v", err)
	}
}

func TestEnsureDatabase_IssuesProvisionStatements(t *testing.T) {
	eng, mock := newMockEngineer(t)
	ctx := context.Background()

	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `workspace_5`").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("CREATE USER IF NOT EXISTS 'workspace_5'@'%' IDENTIFIED BY").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("ALTER USER 'workspace_5'@'%' IDENTIFIED BY").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("GRANT ALL PRIVILEGES ON `workspace_5`\\.\\* TO 'workspace_5'@'%'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("FLUSH PRIVILEGES").WillReturnResult(sqlmock.NewResult(0, 1))

	err := eng.EnsureDatabase(ctx, EnsureRequest{
		Database: "workspace_5",
		User:     "workspace_5",
		Password: "ws_secret",
	})
	if err != nil {
		t.Fatalf("EnsureDatabase: %v", err)
	}
	finalizeExpectations(t, mock)
}

func TestEnsureDatabase_ConvergesPasswordOnRetry(t *testing.T) {
	// A second call (e.g. reconciliation retry) must re-run ALTER USER so the
	// derived password is always current; calls are idempotent (IF EXISTS).
	eng, mock := newMockEngineer(t)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `workspace_9`").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("CREATE USER IF NOT EXISTS 'workspace_9'@'%'").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("ALTER USER 'workspace_9'@'%'").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("GRANT ALL PRIVILEGES ON `workspace_9`\\.\\*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("FLUSH PRIVILEGES").WillReturnResult(sqlmock.NewResult(0, 1))
	}

	for i := 0; i < 2; i++ {
		if err := eng.EnsureDatabase(ctx, EnsureRequest{
			Database: "workspace_9",
			User:     "workspace_9",
			Password: "ws_secret",
		}); err != nil {
			t.Fatalf("EnsureDatabase attempt %d: %v", i, err)
		}
	}
	finalizeExpectations(t, mock)
}

func TestEnsureDatabase_EscapesQuoteInPasswordLiteral(t *testing.T) {
	// A generated password containing a single quote must be escaped as '' so
	// it can never break out of the SQL string literal. Assert the CREATE USER
	// / ALTER USER statements carry the escaped literal. (Backslash escaping is
	// covered directly by TestQuoteString below.)
	eng, mock := newMockEngineer(t)
	ctx := context.Background()
	password := `p'w`

	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `workspace_1`").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("CREATE USER IF NOT EXISTS 'workspace_1'@'%' IDENTIFIED BY 'p''w'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("ALTER USER 'workspace_1'@'%' IDENTIFIED BY 'p''w'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("GRANT ALL PRIVILEGES ON `workspace_1`\\.\\*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("FLUSH PRIVILEGES").WillReturnResult(sqlmock.NewResult(0, 1))

	if err := eng.EnsureDatabase(ctx, EnsureRequest{
		Database: "workspace_1",
		User:     "workspace_1",
		Password: password,
	}); err != nil {
		t.Fatalf("EnsureDatabase: %v", err)
	}
	finalizeExpectations(t, mock)
}

func TestQuoteString_EscapesQuoteAndBackslash(t *testing.T) {
	// The escaping helper must neutralize both single quotes (SQL standard)
	// and backslashes (MySQL string escaping) so a secret can never break out
	// of a quoted literal. This is the direct, deterministic test for the
	// backslash path that is awkward to assert through the sqlmock regexp
	// matcher.
	got := quoteString(`p'a\ss`)
	want := `'p''a\\ss'`
	if got != want {
		t.Fatalf("quoteString = %q, want %q", got, want)
	}
	// Control characters that could break out of a literal are removed.
	if q := quoteString("abc\x00def"); q != `'abcdef'` {
		t.Fatalf("quoteString with NUL = %q, want %q", q, `'abcdef'`)
	}
}

func TestDropDatabase_IssuesCleanupStatements(t *testing.T) {
	eng, mock := newMockEngineer(t)
	ctx := context.Background()

	mock.ExpectExec("DROP DATABASE IF EXISTS `workspace_5`").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DROP USER IF EXISTS 'workspace_5'@'%'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("FLUSH PRIVILEGES").WillReturnResult(sqlmock.NewResult(0, 1))

	if err := eng.DropDatabase(ctx, EnsureRequest{
		Database: "workspace_5",
		User:     "workspace_5",
		Password: "irrelevant",
	}); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}
	finalizeExpectations(t, mock)
}

func TestEnsureDatabase_InvalidIdentifierRejectedBeforeSQL(t *testing.T) {
	eng, mock := newMockEngineer(t)
	ctx := context.Background()
	// No expectations: an invalid identifier must be rejected without issuing
	// any SQL, so untrusted names can never be interpolated into the shared
	// admin connection.

	cases := []EnsureRequest{
		{Database: "workspace; DROP TABLE x", User: "workspace_1", Password: "x"},
		{Database: "workspace_1", User: "user with spaces", Password: "x"},
		{Database: "workspace`exploit", User: "workspace_1", Password: "x"},
		{Database: "", User: "workspace_1", Password: "x"},
		{Database: "workspace_1", User: "", Password: "x"},
		{Database: "workspace_1", User: "workspace_1", Password: ""},
	}
	for _, c := range cases {
		err := eng.EnsureDatabase(ctx, c)
		if err == nil {
			t.Fatalf("expected error for %+v", c)
		}
		if !strings.Contains(err.Error(), "invalid") && !strings.Contains(err.Error(), "required") {
			t.Fatalf("unexpected error message for %+v: %v", c, err)
		}
	}
	finalizeExpectations(t, mock)
}

func TestEnsureDatabase_IdentifierLimit(t *testing.T) {
	// mysqlprovision identifiers are capped at 64 chars; workspace_<id> fits,
	// but a longer name must be rejected before SQL is issued.
	eng, mock := newMockEngineer(t)
	ctx := context.Background()
	long := strings.Repeat("a", 65)
	if err := eng.EnsureDatabase(ctx, EnsureRequest{Database: long, User: long, Password: "x"}); err == nil {
		t.Fatal("expected identifier-length error")
	}
	finalizeExpectations(t, mock)
}

func TestEnsureDatabase_NoConnection(t *testing.T) {
	eng := &MySQLEngineer{db: nil}
	err := eng.EnsureDatabase(context.Background(), EnsureRequest{
		Database: "workspace_1", User: "workspace_1", Password: "x",
	})
	if err == nil {
		t.Fatal("expected error on nil connection")
	}
	if !errors.Is(err, nil) && !strings.Contains(err.Error(), "no database connection") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureDatabase_StatementErrorWrappedSecretFree(t *testing.T) {
	eng, mock := newMockEngineer(t)
	ctx := context.Background()

	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `workspace_7`").WillReturnError(errors.New("boom"))
	err := eng.EnsureDatabase(ctx, EnsureRequest{
		Database: "workspace_7",
		User:     "workspace_7",
		Password: "super-secret-value",
	})
	if err == nil {
		t.Fatal("expected wrapped error")
	}
	// The wrapped error must not embed the password; it summarizes to the
	// statement verb + identifiers only.
	msg := err.Error()
	if strings.Contains(msg, "super-secret-value") {
		t.Fatalf("error leaked the password: %v", err)
	}
	if !strings.Contains(msg, "CREATE") || !strings.Contains(msg, "workspace_7") {
		t.Fatalf("expected a secret-free summarized error, got: %v", err)
	}
}
