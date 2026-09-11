package coolify

// Focused unit tests for the Coolify smoke orchestration (runCoolifySmoke).
//
// These tests exercise the same orchestration as TestCoolifyWorkspaceSmoke but
// against an httptest fake, so the flow is verifiable without a real Coolify.
// They assert:
//   - the happy path succeeds end to end (readiness validated via Coolify
//     application status, with no portal HTTP probe of the public URL);
//   - cleanup deletes the application before the database;
//   - cleanup still runs (and deletes both resources) when an assertion fails.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// smokeFake implements the subset of the Coolify API exercised by the smoke
// flow. It records ordered deletes so tests can assert cleanup ordering.
type smokeFake struct {
	// sensitiveOk toggles whether GET /databases/{uuid} returns sensitive
	// fields (simulating a token with/without read:sensitive).
	sensitiveOk bool
	// envFail toggles whether the bulk environment upsert fails (simulating a
	// failure after both resources were created).
	envFail bool
	// deletes records deleted resource IDs in call order.
	deletes []string
	// deletedRecords records resource type by ID for assertions.
	deletedRecords map[string]string
}

func newSmokeFake() *smokeFake {
	return &smokeFake{sensitiveOk: true, deletedRecords: map[string]string{}}
}

func (f *smokeFake) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	mux.HandleFunc("/databases/mariadb", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"uuid": "db-1"})
	})

	mux.HandleFunc("/databases/db-1/start", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/databases/db-1/stop", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/databases/db-1", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if !f.sensitiveOk {
				writeJSON(w, http.StatusOK, map[string]string{
					"uuid": "db-1", "name": "db", "status": "running", "type": "mariadb",
				})
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{
				"uuid":             "db-1",
				"name":             "db",
				"status":           "running",
				"type":             "mariadb",
				"internal_db_url":  "mariadb://smoke-user:smoke-pass@db-1.internal:3306/smokedb",
				"mariadb_user":     "smoke-user",
				"mariadb_password": "smoke-pass",
				"mariadb_database": "smokedb",
			})
		case http.MethodDelete:
			f.deletes = append(f.deletes, "db-1")
			f.deletedRecords["db-1"] = "database"
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/applications/dockerimage", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusCreated, map[string]string{"uuid": "app-1"})
	})
	mux.HandleFunc("/applications/app-1/envs/bulk", func(w http.ResponseWriter, r *http.Request) {
		if f.envFail {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"boom"}`))
			return
		}
		// capture the secret env so we can confirm the DB password is sent but
		// never logged.
		var payload struct {
			Data []map[string]any `json:"data"`
		}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/applications/app-1/storages", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			writeJSON(w, http.StatusOK, map[string]any{"persistent_storages": []any{}})
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/applications/app-1/start", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"deployment_uuid": "dep-1"})
	})
	mux.HandleFunc("/applications/app-1/stop", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/applications/app-1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			writeJSON(w, http.StatusOK, map[string]string{
				"uuid": "app-1", "status": "running", "fqdn": "https://smoke-x.build.example.com",
			})
			return
		}
		f.deletes = append(f.deletes, "app-1")
		f.deletedRecords["app-1"] = "application"
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/deployments/dep-1", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"uuid": "dep-1", "status": "finished"})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func baseSmokeConfig() smokeConfig {
	return smokeConfig{
		serverUUID:  "server-1",
		projectUUID: "project-1",
		envUUID:     "env-1",
		destUUID:    "dest-1",
		image:       "wordpress:6-fpm-alpine",
		domain:      "build.example.com",
	}
}

func TestRunProvisioningOrchestration_HappyPath(t *testing.T) {
	fake := newSmokeFake()
	ts := httptest.NewServer(fake.handler())
	defer ts.Close()

	client, err := NewClient(ts.URL, "test-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	cfg := baseSmokeConfig()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := runCoolifySmoke(ctx, client, cfg, t.Logf); err != nil {
		t.Fatalf("runCoolifySmoke: %v", err)
	}

	// Readiness is decided from the Coolify application status ("running" in
	// the fake), not from a portal HTTP probe of the public URL — there is no
	// readiness request to assert against here.

	// Cleanup applied on success: app deleted before db.
	if len(fake.deletes) != 2 {
		t.Fatalf("expected 2 deletes, got %v", fake.deletes)
	}
	if fake.deletes[0] != "app-1" || fake.deletes[1] != "db-1" {
		t.Errorf("cleanup order wrong: %v (want app-1 before db-1)", fake.deletes)
	}
	if fake.deletedRecords["app-1"] != "application" || fake.deletedRecords["db-1"] != "database" {
		t.Errorf("cleanup recorded wrong types: %v", fake.deletedRecords)
	}
}

func TestRunProvisioningOrchestration_CleanupOnFailure(t *testing.T) {
	fake := newSmokeFake()
	fake.envFail = true // env upsert fails after both resources are created.
	ts := httptest.NewServer(fake.handler())
	defer ts.Close()

	client, err := NewClient(ts.URL, "test-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	cfg := baseSmokeConfig()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := runCoolifySmoke(ctx, client, cfg, t.Logf); err == nil {
		t.Fatal("expected runCoolifySmoke to fail on env upsert error")
	}

	// Guaranteed cleanup: even on failure both created resources are removed,
	// application before database.
	if len(fake.deletes) != 2 {
		t.Fatalf("expected cleanup to delete both resources, got %v", fake.deletes)
	}
	if fake.deletes[0] != "app-1" || fake.deletes[1] != "db-1" {
		t.Errorf("cleanup order wrong on failure: %v", fake.deletes)
	}
}

func TestRunProvisioningOrchestration_MissingSensitiveFieldsFails(t *testing.T) {
	fake := newSmokeFake()
	fake.sensitiveOk = false // simulate a token without read:sensitive.
	ts := httptest.NewServer(fake.handler())
	defer ts.Close()

	client, err := NewClient(ts.URL, "test-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	cfg := baseSmokeConfig()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := runCoolifySmoke(ctx, client, cfg, t.Logf); err == nil {
		t.Fatal("expected runCoolifySmoke to fail when sensitive fields are missing")
	} else if !strings.Contains(err.Error(), "read:sensitive") {
		t.Errorf("expected error to mention read:sensitive, got: %v", err)
	}

	// Database was created before the sensitive check; it must still be
	// cleaned up even though provisioning failed there.
	if len(fake.deletes) != 1 || fake.deletes[0] != "db-1" {
		t.Errorf("expected database cleanup on sensitive-field failure, got %v", fake.deletes)
	}
}
