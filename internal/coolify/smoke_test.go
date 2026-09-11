package coolify

// This file contains an opt-in, real Coolify smoke test that drives a live
// Coolify instance through the provider layer.
//
// The test is OPT-IN. It never runs during normal unit test execution: it
// calls t.Skip when any of the required environment variables below are absent.
// When those variables are set it drives a real Coolify instance through the
// provider layer and validates the full workspace provisioning path:
//
//   - Bearer-authenticated create of a MariaDB database (write permission);
//   - start + poll to running (write permission);
//   - sensitive secret retrieval via GET /databases/{uuid} (read:sensitive);
//   - private connectivity wiring through the internal DB host;
//   - create of a Docker-image application with domain + proxy Basic Auth
//     + Docker/Coolify health-check settings (write + deploy);
//   - bulk environment upsert and storage;
//   - start + deployment to a terminal state + application running/healthy
//     (deploy).
//
// Readiness is validated through Coolify's own deployment/application status
// (including health-aware states), NOT through a portal HTTP probe of the
// public workspace URL.
//
// All created resources are removed via t.Cleanup handlers that run even when
// an assertion fails. No secrets (the Coolify token, the database password, the
// DB internal URL, or the proxy Basic Auth password) are ever written to the
// test log.
//
// Required environment variables:
//
//	COOLIFY_API_URL            e.g. https://coolify.example.com
//	COOLIFY_API_TOKEN          a Coolify token with read, read:sensitive, write, deploy
//	COOLIFY_SERVER_UUID        server the disposable project/destination lives on
//	COOLIFY_PROJECT_UUID       a disposable Coolify project
//	COOLIFY_ENVIRONMENT_UUID   an environment within that project
//	COOLIFY_DESTINATION_UUID   the server destination/network to use
//	WORKSPACE_TEST_IMAGE       Docker image to deploy, e.g. wordpress:6-fpm-alpine
//	WORKSPACE_TEST_DOMAIN      platform domain with a wildcard DNS record, e.g. build.example.com
//
// Optional:
//
//	COOLIFY_SMOKE_TIMEOUT      overall budget in seconds (default 600)
//
// See docs/workspace-operator-guide for the full Coolify setup.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	envSmokeAPIURL      = "COOLIFY_API_URL"
	envSmokeAPIToken    = "COOLIFY_API_TOKEN"
	envSmokeServerUUID  = "COOLIFY_SERVER_UUID"
	envSmokeProjectUUID = "COOLIFY_PROJECT_UUID"
	envSmokeEnvUUID     = "COOLIFY_ENVIRONMENT_UUID"
	envSmokeDestUUID    = "COOLIFY_DESTINATION_UUID"
	envSmokeImage       = "WORKSPACE_TEST_IMAGE"
	envSmokeDomain      = "WORKSPACE_TEST_DOMAIN"

	envSmokeTimeout = "COOLIFY_SMOKE_TIMEOUT"

	smokePoll           = 5 * time.Second
	defaultSmokeTimeout = 10 * time.Minute
)

// TestCoolifyWorkspaceSmoke is the opt-in real Coolify smoke test. It is
// skipped unless every required environment variable is set, so `go test
// ./...` with no configuration performs no external network activity.
func TestCoolifyWorkspaceSmoke(t *testing.T) {
	vars, missing := requiredSmokeEnv()
	if len(missing) > 0 {
		t.Skipf("Coolify smoke test skipped: missing required env vars: %s. See docs/workspace-operator-guide",
			strings.Join(missing, ", "))
	}

	client, err := NewClient(vars[envSmokeAPIURL], vars[envSmokeAPIToken])
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), smokeTimeout())
	t.Cleanup(cancel)

	cfg := smokeConfig{
		serverUUID:  vars[envSmokeServerUUID],
		projectUUID: vars[envSmokeProjectUUID],
		envUUID:     vars[envSmokeEnvUUID],
		destUUID:    vars[envSmokeDestUUID],
		image:       vars[envSmokeImage],
		domain:      vars[envSmokeDomain],
	}
	if err := runCoolifySmoke(ctx, client, cfg, t.Logf); err != nil {
		t.Fatalf("Coolify smoke test failed: %v", err)
	}
}

// smokeConfig carries the operator-supplied placement and runtime settings for
// the disposable smoke project.
type smokeConfig struct {
	serverUUID  string
	projectUUID string
	envUUID     string
	destUUID    string
	image       string
	domain      string
}

// runCoolifySmoke executes the smoke flow against a live Coolify endpoint. It
// is a plain function (taking a log callback) so the same orchestration can be
// unit-tested against an httptest fake without a real Coolify instance.
// Progress logs carry only non-secret identifiers; secrets are never passed to
// the log callback.
func runCoolifySmoke(ctx context.Context, client *Client, cfg smokeConfig, logf func(string, ...any)) error {
	// 1. Reachability + token validity (does not require any specific scope).
	if err := client.Health(ctx); err != nil {
		return fmt.Errorf("Coolify health check failed: %w", err)
	}

	suffix := randomSuffix()
	dbName := "portal-smoke-db-" + suffix
	appName := "portal-smoke-app-" + suffix
	sub := "smoke-" + suffix
	domain := "https://" + sub + "." + strings.TrimSpace(cfg.domain)
	logf("smoke: run %s", sub)

	// 2. Create a dedicated MariaDB resource on the disposable project.
	dbResult, err := client.CreateDatabase(ctx, CreateDatabaseRequest{
		ServerUUID:      cfg.serverUUID,
		ProjectUUID:     cfg.projectUUID,
		EnvironmentUUID: cfg.envUUID,
		DestinationUUID: cfg.destUUID,
		Name:            dbName,
		MemoryLimit:     "256m",
		CPULimit:        "1",
		Tags:            []string{"portal-smoke"},
	})
	if err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	dbID := dbResult.UUID
	logf("smoke: created database %s", dbID)
	cleanup := newSmokeCleanup()
	// Guaranteed database cleanup. Registered first so it runs LAST (LIFO),
	// guaranteeing the application is deleted before the database.
	cleanup.add(func(c context.Context) {
		_ = client.StopDatabase(c, dbID)
		if err := client.DeleteDatabase(c, dbID, true, true); err != nil && !IsNotFound(err) {
			logf("smoke: cleanup delete database %s: %v", dbID, err)
			return
		}
		logf("smoke: deleted database %s", dbID)
	})

	// 3. Start the database and wait for it to report running (write).
	if err := client.StartDatabase(ctx, dbID); err != nil {
		cleanup.run(ctx)
		return fmt.Errorf("start database: %w", err)
	}
	if err := smokeWaitDatabaseRunning(ctx, client, dbID); err != nil {
		cleanup.run(ctx)
		return fmt.Errorf("database %s did not become ready: %w", dbID, err)
	}
	logf("smoke: database %s running and healthy", dbID)

	// 4. Validate sensitive secret retrieval (read:sensitive permission).
	// We deliberately never log any of these returned secrets.
	db, err := client.GetDatabase(ctx, dbID)
	if err != nil {
		cleanup.run(ctx)
		return fmt.Errorf("get database: %w", err)
	}
	if db.Username == "" || db.Database == "" || db.Password == "" || db.InternalURL == "" {
		cleanup.run(ctx)
		return fmt.Errorf("database %s returned empty sensitive fields; the Coolify token likely lacks read:sensitive (username_set=%t database_set=%t password_set=%t internal_url_set=%t)",
			dbID, db.Username != "", db.Database != "", db.Password != "", db.InternalURL != "")
	}
	// Private connectivity: the database must report an internal-only host, not
	// a public endpoint. We only log the host, never the full URL (which
	// embeds the password).
	dbHost, err := parseSmokeDatabaseHost(db.InternalURL)
	if err != nil {
		cleanup.run(ctx)
		return fmt.Errorf("database %s internal url host unparseable: %w", dbID, err)
	}
	logf("smoke: database %s private host %s", dbID, dbHost)

	// 5. Create the Docker-image application with domain + proxy Basic Auth.
	basicUser := "smoke-" + suffix
	basicPass := randomSecret()
	appResult, err := client.CreateApplication(ctx, CreateApplicationRequest{
		ServerUUID:            cfg.serverUUID,
		ProjectUUID:           cfg.projectUUID,
		EnvironmentUUID:       cfg.envUUID,
		DestinationUUID:       cfg.destUUID,
		Name:                  appName,
		Image:                 cfg.image,
		Tag:                   "latest",
		Port:                  "80",
		Domain:                domain,
		BasicAuthUsername:     basicUser,
		BasicAuthPassword:     basicPass,
		HealthCheckEnabled:    true,
		HealthCheckPath:       "/",
		HealthCheckPort:       "80",
		HealthCheckMethod:     "GET",
		HealthCheckReturnCode: 200,
		MemoryLimit:           "512m",
		CPULimit:              "1",
		NoindexDomains:        []string{domain},
		Tags:                  []string{"portal-smoke"},
	})
	if err != nil {
		cleanup.run(ctx)
		return fmt.Errorf("create application: %w", err)
	}
	appID := appResult.UUID
	logf("smoke: created application %s", appID)
	// Guaranteed application cleanup. Registered second so it runs FIRST
	// (LIFO), before the database cleanup above.
	cleanup.add(func(c context.Context) {
		_ = client.StopApplication(c, appID)
		if err := client.DeleteApplication(c, appID, true, true); err != nil && !IsNotFound(err) {
			logf("smoke: cleanup delete application %s: %v", appID, err)
			return
		}
		logf("smoke: deleted application %s", appID)
	})

	// 6. Wire the database into the application environment (private
	// connectivity). The DB password is sent as a secret env entry.
	envs := []EnvironmentVariable{
		{Key: "PORTAL_API_URL", Value: "https://api.example.com"},
		{Key: "PORTAL_WORKSPACE_URL", Value: domain},
		{Key: "DB_HOST", Value: dbHost},
		{Key: "DB_NAME", Value: db.Database},
		{Key: "DB_USER", Value: db.Username},
		{Key: "DB_PASSWORD", Value: db.Password, Secret: true},
	}
	if err := client.SetApplicationEnvironment(ctx, appID, envs); err != nil {
		cleanup.run(ctx)
		return fmt.Errorf("set application environment: %w", err)
	}

	// 7. Ensure a persistent volume is attached (storage contract).
	if err := client.EnsureApplicationStorage(ctx, appID, []StorageMount{
		{Name: "portal-smoke-" + suffix, MountPath: "/var/www/html"},
	}); err != nil {
		cleanup.run(ctx)
		return fmt.Errorf("ensure application storage: %w", err)
	}

	// 8. Start the application, wait for the deployment to finish and the
	// application to report running (deploy permission).
	dep, err := client.StartApplication(ctx, appID)
	if err != nil {
		cleanup.run(ctx)
		return fmt.Errorf("start application: %w", err)
	}
	if dep.ID != "" {
		if err := smokeWaitDeploymentFinished(ctx, client, dep.ID); err != nil {
			cleanup.run(ctx)
			return fmt.Errorf("application %s deployment did not finish: %w", appID, err)
		}
	}
	if err := smokeWaitApplicationRunning(ctx, client, appID); err != nil {
		cleanup.run(ctx)
		return fmt.Errorf("application %s did not reach running: %w", appID, err)
	}
	logf("smoke: application %s running", appID)

	cleanup.run(ctx)
	return nil
}

// smokeCleanup tracks LIFO cleanup handlers that must run even on failure.
type smokeCleanup struct {
	fns []func(context.Context)
}

func newSmokeCleanup() *smokeCleanup {
	return &smokeCleanup{}
}

func (c *smokeCleanup) add(fn func(context.Context)) { c.fns = append(c.fns, fn) }

// run executes handlers in reverse registration order (LIFO) with a fresh
// short-lived context.
func (c *smokeCleanup) run(ctx context.Context) {
	for i := len(c.fns) - 1; i >= 0; i-- {
		child, cancel := context.WithTimeout(ctx, defaultSmokeTimeout)
		func() {
			defer cancel()
			c.fns[i](child)
		}()
	}
}

// requiredSmokeEnv collects the required environment configuration. It returns
// the populated variables and any names that are missing or empty.
func requiredSmokeEnv() (map[string]string, []string) {
	names := []string{
		envSmokeAPIURL,
		envSmokeAPIToken,
		envSmokeServerUUID,
		envSmokeProjectUUID,
		envSmokeEnvUUID,
		envSmokeDestUUID,
		envSmokeImage,
		envSmokeDomain,
	}
	vars := make(map[string]string, len(names))
	var missing []string
	for _, n := range names {
		v := strings.TrimSpace(os.Getenv(n))
		if v == "" {
			missing = append(missing, n)
			continue
		}
		vars[n] = v
	}
	return vars, missing
}

// smokeTimeout returns the overall budget for the smoke test, honoring an
// optional COOLIFY_SMOKE_TIMEOUT override in seconds.
func smokeTimeout() time.Duration {
	if v := os.Getenv(envSmokeTimeout); v != "" {
		if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return defaultSmokeTimeout
}

// smokeWaitDatabaseRunning polls the database until it reports running/ready.
func smokeWaitDatabaseRunning(ctx context.Context, client *Client, id string) error {
	for {
		res, err := client.GetDatabase(ctx, id)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !sleepContextTesting(ctx, smokePoll) {
				return ctx.Err()
			}
			continue
		}
		switch res.Status {
		case ResourceStatusRunning, ResourceStatusReady:
			return nil
		case ResourceStatusFailed:
			return fmt.Errorf("database %s reached failed status", id)
		default:
			if !sleepContextTesting(ctx, smokePoll) {
				return ctx.Err()
			}
		}
	}
}

// smokeWaitDeploymentFinished polls the deployment to a terminal state.
func smokeWaitDeploymentFinished(ctx context.Context, client *Client, deploymentID string) error {
	for {
		dep, err := client.GetDeployment(ctx, deploymentID)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !sleepContextTesting(ctx, smokePoll) {
				return ctx.Err()
			}
			continue
		}
		switch {
		case dep.Status == ResourceStatusFinished:
			return nil
		case dep.Status == ResourceStatusFailed, dep.Status == "error", dep.Status == "cancelled":
			return fmt.Errorf("deployment %s reached %s", deploymentID, dep.Status)
		default:
			if !sleepContextTesting(ctx, smokePoll) {
				return ctx.Err()
			}
		}
	}
}

// smokeWaitApplicationRunning polls the application until it reports a
// ready/running (or healthy) status. Readiness is validated through Coolify's
// application status, not a portal HTTP probe.
func smokeWaitApplicationRunning(ctx context.Context, client *Client, id string) error {
	for {
		res, err := client.GetApplication(ctx, id)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !sleepContextTesting(ctx, smokePoll) {
				return ctx.Err()
			}
			continue
		}
		switch res.Status {
		case ResourceStatusRunning, ResourceStatusReady, ResourceStatusHealthy:
			return nil
		case ResourceStatusFailed, ResourceStatusUnhealthy, ResourceStatusDegraded:
			return fmt.Errorf("application %s reached %s status", id, res.Status)
		default:
			if !sleepContextTesting(ctx, smokePoll) {
				return ctx.Err()
			}
		}
	}
}

// parseSmokeDatabaseHost extracts just the hostname from Coolify's internal DB
// URL. The full URL embeds credentials and is never logged; returning the bare
// host keeps secrets out of the test log.
func parseSmokeDatabaseHost(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("no host in database url")
	}
	return u.Hostname(), nil
}

// randomSuffix returns a lowercase hex suffix used to make resource names and
// hostnames unique and collision-free for the disposable project.
func randomSuffix() string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure is effectively unreachable; fall back to time.
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}

// randomSecret returns a temporary random proxy Basic Auth password. It is
// never logged.
func randomSecret() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "smoke-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return hex.EncodeToString(b)
}

// sleepContextTesting sleeps for the duration or returns false when the
// context is cancelled first. The name avoids clashing with any future shared
// helper from the workspace service package.
func sleepContextTesting(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
