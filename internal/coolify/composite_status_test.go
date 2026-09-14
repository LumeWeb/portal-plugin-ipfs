package coolify

import (
	"context"
	"net/http"
	"testing"
)

// CompositeStatusNormalizationTest pins the client's mapping of Coolify's
// composite "state:health" container statuses (the colon format emitted
// server-side by ContainerStatusAggregator) onto the coarse resource
// vocabulary BEFORE they reach the workspace service, whose readiness
// switches only accept the base states. Before normalization, a healthy
// shared database reporting "running:healthy" was classified as not-ready
// and its workspaces false-cycled to failed.
func TestCompositeStatusNormalization(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/databases/db-uuid-1":
			_, _ = w.Write([]byte(`{` +
				`"uuid":"db-uuid-1","name":"db","status":"running:healthy","type":"mariadb",` +
				`"internal_db_url":"mysql://user:pw@mariadb.internal:3306/db1",` +
				`"mariadb_user":"user","mariadb_password":"pw","mariadb_database":"db1"}`))
		case "/applications/app-1":
			_, _ = w.Write([]byte(`{"uuid":"app-1","status":"running:unknown"}`))
		case "/applications/app-2":
			_, _ = w.Write([]byte(`{"uuid":"app-2","status":"running:unhealthy"}`))
		}
	})
	ctx := context.Background()

	// A health-checked healthy database is ready, and so is a database
	// without any healthcheck (the "unknown"/"excluded" suffixes are not
	// failure states — see ContainerHealthStatusTest in Coolify).
	db, err := client.GetDatabase(ctx, "db-uuid-1")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if db.Status != ResourceStatusRunning {
		t.Errorf("database status = %q, want %q", db.Status, ResourceStatusRunning)
	}

	// A running application whose healthcheck is still starting/absent is
	// reported as base "running".
	app, err := client.GetApplication(ctx, "app-1")
	if err != nil {
		t.Fatalf("GetApplication: %v", err)
	}
	if app.Status != ResourceStatusRunning {
		t.Errorf("application status = %q, want %q", app.Status, ResourceStatusRunning)
	}

	// An explicit "unhealthy" health suffix wins over the base state so a
	// failing health check terminates the readiness wait immediately.
	app, err = client.GetApplication(ctx, "app-2")
	if err != nil {
		t.Fatalf("GetApplication (unhealthy): %v", err)
	}
	if app.Status != ResourceStatusUnhealthy {
		t.Errorf("unhealthy application status = %q, want %q", app.Status, ResourceStatusUnhealthy)
	}
}
