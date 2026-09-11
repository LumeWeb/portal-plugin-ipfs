package coolify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestServer(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	ts := httptest.NewServer(handler)
	client, err := NewClient(ts.URL, "test-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(ts.Close)
	return client, ts
}

func readReqBody(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("parse body: %v (body=%s)", err, b)
	}
	return m
}

func TestCreateDatabaseSendsBearerAndPlacement(t *testing.T) {
	var gotAuth, gotMethod, gotPath string
	var gotBody map[string]any
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotBody = readReqBody(t, r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"uuid":"db-uuid-1"}`))
	})

	_, err := client.CreateDatabase(context.Background(), CreateDatabaseRequest{
		ServerUUID: "server-1", ProjectUUID: "proj-1",
		EnvironmentName: "production", EnvironmentUUID: "env-1",
		DestinationUUID: "dest-1", Name: "workspace-1-db",
		MemoryLimit: "512m", Tags: []string{"workspace-1"},
	})
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if gotAuth != "Bearer test-token" {
		t.Errorf("auth header = %q", gotAuth)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %s, want POST", gotMethod)
	}
	if gotPath != "/databases/mariadb" {
		t.Errorf("path = %s", gotPath)
	}
	if gotBody["project_uuid"] != "proj-1" || gotBody["server_uuid"] != "server-1" {
		t.Errorf("placement not sent: %v", gotBody)
	}
}

func TestCreateDatabaseReturnsUUID(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"uuid":"db-uuid-42"}`))
	})
	got, err := client.CreateDatabase(context.Background(), CreateDatabaseRequest{
		ServerUUID: "s", ProjectUUID: "p", EnvironmentName: "prod", EnvironmentUUID: "e",
	})
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if got.UUID != "db-uuid-42" {
		t.Errorf("uuid = %q", got.UUID)
	}
}

func TestGetDatabaseTypedSensitiveResponse(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"uuid":"db-uuid-1","name":"workspace-1-db","status":"running","type":"mariadb",
			"internal_db_url":"mysql://user:pw@mariadb.internal:3306/db1",
			"mariadb_user":"user","mariadb_password":"pw","mariadb_database":"db1"
		}`))
	})
	got, err := client.GetDatabase(context.Background(), "db-uuid-1")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if got.ID != "db-uuid-1" || got.Status != "running" {
		t.Errorf("got %+v", got)
	}
	if got.Username != "user" || got.Password != "pw" || got.Database != "db1" {
		t.Errorf("sensitive fields: %+v", got)
	}
	if !strings.Contains(got.InternalURL, "mariadb.internal") {
		t.Errorf("internal url = %q", got.InternalURL)
	}
}

func TestGetDatabaseMissingSensitiveIsNotErrorButEmpty(t *testing.T) {
	// Simulates a token without read:sensitive — fields are absent.
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"uuid":"db-uuid-1","name":"x","status":"running","type":"mariadb"}`))
	})
	got, err := client.GetDatabase(context.Background(), "db-uuid-1")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if got.Password != "" || got.Username != "" {
		t.Errorf("expected empty credentials, got %+v", got)
	}
}

// TestGetDatabase_EngineCredentialSelection verifies that GetDatabase selects
// the engine-specific credential set (including the root password) by the
// resource Type, so both MySQL and MariaDB shared resources resolve correctly
// and fields from the other engine are ignored.
func TestGetDatabase_EngineCredentialSelection(t *testing.T) {
	tests := []struct {
		name     string
		typ      string
		body     string
		wantUser string
		wantPass string
		wantDB   string
		wantRoot string
	}{
		{
			name: "mariadb selects mariadb_* fields",
			typ:  "mariadb",
			body: `{"uuid":"db","status":"running","type":"mariadb",
				"mariadb_user":"muser","mariadb_password":"mpw","mariadb_database":"mdb","mariadb_root_password":"mroot",
				"mysql_user":"xuser","mysql_password":"xpw","mysql_database":"xdb","mysql_root_password":"xroot"}`,
			wantUser: "muser", wantPass: "mpw", wantDB: "mdb", wantRoot: "mroot",
		},
		{
			name: "mysql selects mysql_* fields",
			typ:  "mysql",
			body: `{"uuid":"db","status":"running","type":"mysql",
				"mariadb_user":"xuser","mariadb_password":"xpw","mariadb_database":"xdb","mariadb_root_password":"xroot",
				"mysql_user":"suser","mysql_password":"spw","mysql_database":"sdb","mysql_root_password":"sroot"}`,
			wantUser: "suser", wantPass: "spw", wantDB: "sdb", wantRoot: "sroot",
		},
		{
			name: "unknown type leaves credentials empty",
			typ:  "postgres",
			body: `{"uuid":"db","status":"running","type":"postgres",
				"mariadb_root_password":"mroot","mysql_root_password":"sroot"}`,
			wantRoot: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.body))
			})
			got, err := client.GetDatabase(context.Background(), "db-uuid-1")
			if err != nil {
				t.Fatalf("GetDatabase: %v", err)
			}
			if got.Type != tc.typ {
				t.Errorf("Type = %q, want %q", got.Type, tc.typ)
			}
			if got.Username != tc.wantUser || got.Password != tc.wantPass || got.Database != tc.wantDB || got.RootPassword != tc.wantRoot {
				t.Errorf("credentials = user %q pass %q db %q root %q; want user %q pass %q db %q root %q",
					got.Username, got.Password, got.Database, got.RootPassword,
					tc.wantUser, tc.wantPass, tc.wantDB, tc.wantRoot)
			}
		})
	}
}

func TestStartApplicationReturnsDeploymentUUID(t *testing.T) {
	var method, path string
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"deployment_uuid":"deploy-9","message":"ok"}`))
	})
	got, err := client.StartApplication(context.Background(), "app-uuid-1")
	if err != nil {
		t.Fatalf("StartApplication: %v", err)
	}
	if method != http.MethodPost || path != "/applications/app-uuid-1/start" {
		t.Errorf("start called %s %s", method, path)
	}
	if got.ID != "deploy-9" {
		t.Errorf("deployment id = %q", got.ID)
	}
}

func TestBulkEnvUsesPatchAndDataArray(t *testing.T) {
	var method, path string
	var body map[string]any
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		body = readReqBody(t, r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`[]`))
	})
	err := client.SetApplicationEnvironment(context.Background(), "app-1", []EnvironmentVariable{
		{Key: "PORTAL_API_KEY", Value: "secret", Secret: true},
		{Key: "PORTAL_API_URL", Value: "https://api.example.com"},
	})
	if err != nil {
		t.Fatalf("SetApplicationEnvironment: %v", err)
	}
	if method != http.MethodPatch || path != "/applications/app-1/envs/bulk" {
		t.Errorf("env update called %s %s", method, path)
	}
	data, ok := body["data"].([]any)
	if !ok || len(data) != 2 {
		t.Fatalf("data not array: %v", body)
	}
	first := data[0].(map[string]any)
	if first["key"] != "PORTAL_API_KEY" || first["is_shown_once"] != true {
		t.Errorf("secret env not marked: %v", first)
	}
}

func TestCreateApplicationUsesPOSTAndReturnsUUID(t *testing.T) {
	var method, path, rawBody string
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		b, _ := io.ReadAll(r.Body)
		rawBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"uuid":"app-uuid-7"}`))
	})
	got, err := client.CreateApplication(context.Background(), CreateApplicationRequest{
		ServerUUID: "s", ProjectUUID: "p", EnvironmentName: "prod", EnvironmentUUID: "e",
		Image: "wordpress", Tag: "latest", Port: "80",
		Tags: []string{"portalx-workspaces"},
	})
	if err != nil {
		t.Fatalf("CreateApplication: %v", err)
	}
	if method != http.MethodPost || path != "/applications/dockerimage" {
		t.Errorf("create called %s %s", method, path)
	}
	if got.UUID != "app-uuid-7" {
		t.Errorf("uuid = %q", got.UUID)
	}
	// The installation-scoped tag is persisted on create (v4.3.19 HandlesTagsApi).
	body := readReqBodyFromString(t, rawBody)
	if !assertTagsInBody(t, body, "portalx-workspaces") {
		t.Errorf("create body tags missing: %s", rawBody)
	}
}

// assertTagsInBody checks that the request body carries the given application
// tag and returns whether it does.
func assertTagsInBody(t *testing.T, body map[string]any, want string) bool {
	t.Helper()
	tags, ok := body["tags"].([]any)
	if !ok || len(tags) != 1 {
		return false
	}
	return tags[0] == want
}

// readReqBodyFromString parses an already-read raw JSON body.
func readReqBodyFromString(t *testing.T, raw string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("parse body: %v (body=%s)", err, raw)
	}
	return m
}

func TestStorageUpsertSkipsExisting(t *testing.T) {
	var creates int
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/applications/app-1/storages" && r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"persistent_storages":[{"name":"wp-vol","mount_path":"/var/www/html"}]}`))
			return
		}
		creates++
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	})
	err := client.EnsureApplicationStorage(context.Background(), "app-1", []StorageMount{
		{Name: "wp-vol", MountPath: "/var/www/html"},
		{Name: "upload-vol", MountPath: "/var/www/html/wp-content/uploads"},
	})
	if err != nil {
		t.Fatalf("EnsureApplicationStorage: %v", err)
	}
	if creates != 1 {
		t.Errorf("expected 1 storage create, got %d", creates)
	}
}

func Test409ParsesConflict(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"message":"Domain conflicts detected.","conflicts":[{"domain":"build.example.com","resource_name":"other","resource_type":"application"}]}`))
	})
	_, err := client.CreateApplication(context.Background(), CreateApplicationRequest{
		ServerUUID: "s", ProjectUUID: "p", EnvironmentName: "prod", EnvironmentUUID: "e",
		Image: "wordpress", Tag: "latest", Domain: "build.example.com",
	})
	if err == nil {
		t.Fatal("expected conflict error")
	}
	if !IsConflict(err) {
		t.Errorf("expected conflict, got %v", err)
	}
	var ce *Error
	if !asError(err, &ce) || len(ce.Conflicts) != 1 {
		t.Errorf("conflicts not parsed: %+v", err)
	}
}

func Test422ParsesValidation(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"message":"Validation error.","errors":{"name":["The name field is required."]}}`))
	})
	_, err := client.CreateDatabase(context.Background(), CreateDatabaseRequest{})
	if err == nil {
		t.Fatal("expected 422 error")
	}
	if !IsUnprocessable(err) {
		t.Errorf("expected unprocessable, got %v", err)
	}
}

func TestGetDeploymentStatus(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"deployment_uuid":"deploy-1","status":"in_progress"}`))
	})
	got, err := client.GetDeployment(context.Background(), "deploy-1")
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if got.ID != "deploy-1" || got.Status != "in_progress" {
		t.Errorf("got %+v", got)
	}
}

func TestHealth(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestDeleteDatabaseSendsVolumesParam(t *testing.T) {
	var q string
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		q = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	})
	if err := client.DeleteDatabase(context.Background(), "db-1", true, true); err != nil {
		t.Fatalf("DeleteDatabase: %v", err)
	}
	if !strings.Contains(q, "delete_volumes=true") {
		t.Errorf("missing delete_volumes param, query=%q", q)
	}
}

func TestFindApplicationByName(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/applications" {
			t.Errorf("path = %s", r.URL.Path)
		}
		// FindApplicationByName is the deterministic-name fallback: it lists
		// applications without relying on a tag filter.
		if r.URL.Query().Get("tag") != "" {
			t.Errorf("tag = %q (name fallback should not send a tag filter)", r.URL.Query().Get("tag"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[` +
			`{"uuid":"app-1","name":"workspace-1-app","status":"running"},` +
			`{"uuid":"app-2","name":"another-app","status":"exited"}` +
			`]`))
	})
	got, err := client.FindApplicationByName(context.Background(), "workspace-1-app")
	if err != nil {
		t.Fatalf("FindApplicationByName: %v", err)
	}
	// Only the deterministic name match is returned; the non-matching name is
	// filtered out client-side.
	if len(got) != 1 || got[0].ID != "app-1" || got[0].Name != "workspace-1-app" {
		t.Errorf("got %+v", got)
	}
}

func TestFindApplicationsByTag(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/applications" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("tag"); got != "portalx-workspaces" {
			t.Errorf("tag = %q, want portalx-workspaces", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[` +
			`{"uuid":"app-1","name":"portalx-workspace-1-app","status":"running"},` +
			`{"uuid":"app-2","name":"otherx-workspace-1-app","status":"exited"}` +
			`]`))
	})
	got, err := client.FindApplicationsByTag(context.Background(), "portalx-workspaces")
	if err != nil {
		t.Fatalf("FindApplicationsByTag: %v", err)
	}
	// Tag-scoped lookup returns every tagged application; name filtering is the
	// caller's responsibility (it matches the exact deterministic name).
	if len(got) != 2 {
		t.Errorf("len = %d, want 2", len(got))
	}
	if got[0].ID != "app-1" || got[0].Name != "portalx-workspace-1-app" {
		t.Errorf("got %+v", got)
	}
}

func asError(err error, target **Error) bool {
	e, ok := err.(*Error)
	if ok {
		*target = e
	}
	return ok
}
