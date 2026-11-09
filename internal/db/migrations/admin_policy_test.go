package migrations

import (
	"database/sql"
	"io/fs"
	"strings"
	"testing"

	_ "github.com/glebarez/go-sqlite"
)

func TestAdminRoutePolicyMigration(t *testing.T) {
	for _, dialect := range []struct {
		name string
		fs   fs.FS
	}{{"sqlite", GetSQLite()}, {"mysql", GetMySQL()}} {
		t.Run(dialect.name, func(t *testing.T) {
			migration, err := fs.ReadFile(dialect.fs, "20260922120000_align_admin_route_policies.sql")
			if err != nil {
				t.Fatal(err)
			}
			up, down, ok := strings.Cut(string(migration), "-- +goose Down")
			if !ok {
				t.Fatal("missing down section")
			}
			db, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			if _, err := db.Exec(`CREATE TABLE access_rules (id INTEGER PRIMARY KEY, ptype TEXT, v0 TEXT, v1 TEXT, v2 TEXT, v3 TEXT)`); err != nil {
				t.Fatal(err)
			}
			cases := []struct {
				ptype, role, host, path, method string
				keep                            bool
			}{
				{"p", "user", "ipfs.example.com", "/api/ipfs/websites/:id/block", "POST", false},
				{"p", "user", "ipfs.example.com", "/api/ipfs/websites/:id/unblock", "POST", false},
				{"p", "user", "admin.example.com", "/api/ipfs/ipns/republish", "POST", false},
				{"p", "admin", "ipfs.example.com", "/api/ipfs/platform-domains", "POST", false},
				{"p", "admin", "ipfs.example.com", "/api/ipfs/platform-domains/:id/bind", "POST", false},
				{"p", "admin", "ipfs.other.example", "/api/ipfs/platform-domains", "GET", false},
				{"p", "admin", "ipfs.example.com", "/api/ipfs/platform-domains/:id", "PATCH", false},
				{"p", "admin", "ipfs.example.com", "/api/ipfs/platform-domains/:id", "DELETE", false},
				{"p", "user", "admin.example.com", "/api/ipfs/platform-domains", "GET", false},
				{"p", "admin", "admin.example.com", "/api/ipfs/websites/:id/block", "POST", true},
				{"p", "admin", "admin.example.com", "/api/ipfs/platform-domains", "GET", true},
				{"p", "user", "ipfs.example.com", "/api/websites", "GET", true},
				{"p", "user", "ipfs.example.com", "/api/ipfs/websites/:id/block", "GET", true},
				{"p", "user", "account.example.com", "/api/ipfs/websites/:id/block", "POST", true},
				{"p", "custom", "admin.example.com", "/api/ipfs/platform-domains", "GET", true},
				{"g", "user", "ipfs.example.com", "/api/ipfs/websites/:id/block", "POST", true},
			}
			for i, tc := range cases {
				if _, err := db.Exec(`INSERT INTO access_rules VALUES (?, ?, ?, ?, ?, ?)`, i, tc.ptype, tc.role, tc.host, tc.path, tc.method); err != nil {
					t.Fatal(err)
				}
			}
			// Repeating the update and rolling back must not add policy rows.
			for _, statement := range []string{up, up, down} {
				if _, err := db.Exec(statement); err != nil {
					t.Fatal(err)
				}
				for i, tc := range cases {
					var count int
					if err := db.QueryRow(`SELECT COUNT(*) FROM access_rules WHERE id = ?`, i).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if (count == 1) != tc.keep {
						t.Errorf("policy %d (%s %s %s): retained=%v, want %v", i, tc.host, tc.method, tc.path, count == 1, tc.keep)
					}
				}
			}
		})
	}
}
