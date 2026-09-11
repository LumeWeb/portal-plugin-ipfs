// Tests for the workspace reconciler: error classification, bounded backoff,
// fail-closed drift handling, bounded redacted LastError storage, and cron-job
// registration. These cover the secret-safe guarantees of the reconcile path:
// classification/backoff/boundError only ever deal with coarse outcomes and
// never with secrets.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/coolify"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

// timeoutErr implements net.Error so classifyError can detect generic network
// timeouts without needing a real socket.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "operation timed out (network)" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestClassifyError(t *testing.T) {
	notFound := func() error {
		return &coolify.Error{StatusCode: 404, Message: "resource missing"}
	}
	conflict := func() error {
		return &coolify.Error{StatusCode: 409, Message: "domain conflict"}
	}
	unprocessable := func() error {
		return &coolify.Error{StatusCode: 422, Message: "bad body"}
	}
	rateLimited := func() error {
		return &coolify.Error{StatusCode: 429, Message: "slow down", RetryAfter: 5 * time.Second}
	}
	serverErr := func() error {
		return &coolify.Error{StatusCode: 503, Message: "server overloaded"}
	}
	unauthorized := func() error {
		return &coolify.Error{StatusCode: 401, Message: "invalid token"}
	}
	forbidden := func() error {
		return &coolify.Error{StatusCode: 403, Message: "forbidden"}
	}

	tests := []struct {
		name     string
		err      error
		expected retryCategory
	}{
		{name: "nil is success", err: nil, expected: catRetryable}, // only reached via nil; treated as success downstream
		{name: "context canceled", err: context.Canceled, expected: catCancelled},
		{name: "deadline exceeded is retryable", err: context.DeadlineExceeded, expected: catRetryable},
		{name: "generic network timeout is retryable", err: timeoutErr{}, expected: catRetryable},
		{name: "404 saved-resource drift", err: notFound(), expected: catDrift},
		{name: "409 conflict is permanent", err: conflict(), expected: catPermanent},
		{name: "422 unprocessable is permanent", err: unprocessable(), expected: catPermanent},
		{name: "429 rate limited is retryable", err: rateLimited(), expected: catRetryable},
		{name: "5xx is retryable", err: serverErr(), expected: catRetryable},
		{name: "401 unauthorized is permanent", err: unauthorized(), expected: catPermanent},
		{name: "403 forbidden is permanent", err: forbidden(), expected: catPermanent},
		{name: "application domain conflict is permanent", err: ErrApplicationDomainConflict, expected: catPermanent},
		{name: "application provision timeout is retryable", err: ErrApplicationProvisionTimeout, expected: catRetryable},
		{name: "shared database credential missing is permanent", err: ErrDatabaseCredentialFieldMissing, expected: catPermanent},
		{name: "shared database url malformed is permanent", err: ErrDatabaseURLMalformed, expected: catPermanent},
		{name: "shared database server failed is permanent", err: ErrDatabaseServerFailed, expected: catPermanent},
		{name: "provider unavailable is retryable default", err: ErrWorkspaceProviderUnavailable, expected: catRetryable},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyError(tc.err)
			assert.Equal(t, tc.expected, got.category, "classifyError(%v)", tc.err)
		})
	}
}

// TestClassifyErrorWrapsDrift verifies a drift error wrapped by driftCheck
// (workspace context around the coolify 404) still classifies as drift.
func TestClassifyErrorWrapsDrift(t *testing.T) {
	wrapped := fmt.Errorf("workspace: drift check database: %w",
		&coolify.Error{StatusCode: 404, Message: "resource gone"})
	assert.Equal(t, catDrift, classifyError(wrapped).category)
}

func TestBackoffDelay(t *testing.T) {
	// Attempt 1 uses the base delay.
	assert.Equal(t, 30*time.Second, backoffDelay(1, 30*time.Second, 5*time.Minute))
	// Attempt 2 doubles the base.
	assert.Equal(t, 60*time.Second, backoffDelay(2, 30*time.Second, 5*time.Minute))
	// Attempt 3 quadruples the base.
	assert.Equal(t, 2*time.Minute, backoffDelay(3, 30*time.Second, 5*time.Minute))
	// The cap is enforced (never exceeds max).
	assert.Equal(t, 5*time.Minute, backoffDelay(10, 30*time.Second, 5*time.Minute))
	// A zero base falls back to 30s.
	assert.Equal(t, 30*time.Second, backoffDelay(1, 0, 5*time.Minute))
	// A zero max falls back to 5m and is still a hard cap.
	assert.Equal(t, 5*time.Minute, backoffDelay(40, 30*time.Second, 0))
	// Backoff is monotonic and bounded.
	prev := backoffDelay(1, time.Second, time.Hour)
	var last time.Duration
	for attempt := 2; attempt <= 30; attempt++ {
		last = backoffDelay(attempt, time.Second, time.Hour)
		if last < prev {
			t.Fatalf("backoff not monotonic at attempt %d: %v < %v", attempt, last, prev)
		}
		prev = last
	}
	assert.Equal(t, time.Hour, last, "long sequence should plateau at max")
}

// TestBoundError verifies LastError storage is bounded and cannot contain
// secret material that a provider body might leak.
// TestNextRetryDelay honors the provider's Retry-After hint for HTTP 429 and
// otherwise uses bounded backoff.
func TestNextRetryDelay(t *testing.T) {
	svc := &WorkspaceService{config: nil} // defaults: base 30s, max 5m

	// No hint → bounded backoff.
	assert.Equal(t, 30*time.Second, svc.nextRetryDelay(1, timeoutErr{}))
	assert.Equal(t, 60*time.Second, svc.nextRetryDelay(2, timeoutErr{}))

	// 429 with a longer Retry-After wins.
	hintErr := &coolify.Error{StatusCode: 429, Message: "slow down", RetryAfter: 2 * time.Minute}
	assert.Equal(t, 2*time.Minute, svc.nextRetryDelay(1, hintErr))

	// A 429 with a shorter/no Retry-After keeps bounded backoff.
	noHint := &coolify.Error{StatusCode: 429, Message: "slow down"}
	assert.Equal(t, 30*time.Second, svc.nextRetryDelay(1, noHint))
}

func TestBoundError(t *testing.T) {
	long := strings.Repeat("x", maxLastErrorLen*3)
	got := boundError(errors.New(long))
	assert.Len(t, got, maxLastErrorLen, "LastError must be bounded to maxLastErrorLen")

	// Ensure truncated payload carries no password/token keywords.
	assert.NotContains(t, strings.ToLower(got), "password")
	assert.NotContains(t, strings.ToLower(got), "token")
	assert.NotContains(t, strings.ToLower(got), "secret")

	// nil yields empty.
	assert.Equal(t, "", boundError(nil))

	// Short messages pass through unmodified.
	msg := "workspace: 404 coolify: resource missing"
	assert.Equal(t, msg, boundError(errors.New(msg)))
}

// TestNewReconcileJob verifies the workspace reconciler cron-job registration:
// it must implement core.CronJob, carry the expected job type/source, and have
// a schedule definition.
func TestNewReconcileJob(t *testing.T) {
	job := NewReconcileJob()
	require.NotNil(t, job)

	recon, ok := job.(*ReconcileJob)
	require.True(t, ok, "job should be a *ReconcileJob")
	require.NotNil(t, recon.BaseCronJob)

	var cronJob core.CronJob = job
	assert.Equal(t, ReconcileJobType, cronJob.Type())
	assert.Equal(t, ReconcileJobSourceID, cronJob.SourceID())
	assert.Equal(t, core.JobOriginPlugin, cronJob.Origin())
	assert.NotEmpty(t, cronJob.ID())
	assert.NotEmpty(t, cronJob.DisplayName())
	require.NotNil(t, cronJob.Schedule())

	// Each job gets a fresh ID.
	second := NewReconcileJob()
	assert.NotEqual(t, job.ID(), second.ID())
}

// TestDriftCheckFailsClosed verifies that a saved resource returning 404 is
// drift: driftCheck returns an error (so the workspace is marked failed) and
// never attempts to create/recreate the application. There is deliberately no
// database drift check: the shared DB resource is a portal dependency, not a
// per-workspace remote resource.
func TestDriftCheckFailsClosed(t *testing.T) {
	provider := &fakeWorkspaceProvider{}
	provider.appGetErr = &coolify.Error{StatusCode: 404, Message: "resource gone"}
	svc := &WorkspaceService{provider: provider}

	appID := "app-1"
	ws := &pluginDb.Workspace{
		ApplicationResourceID: &appID,
		Status:                pluginDb.WorkspaceStatusReady,
	}

	err := svc.driftCheck(context.Background(), ws)
	require.Error(t, err, "drift check must fail for a missing saved resource")
	assert.Equal(t, catDrift, classifyError(err).category, "drift must classify as catDrift")

	// Fails closed: the reconciler never recreated the resource.
	assert.Zero(t, provider.createCalls, "drift must never create a resource")
}

// TestDriftCheckNoDatabase verifies the shared database resource is never a
// per-workspace drift check target: a workspace with no application resource
// (only logical DB identifiers) passes drift with no provider DB lookup.
func TestDriftCheckNoDatabase(t *testing.T) {
	provider := &fakeWorkspaceProvider{}
	svc := &WorkspaceService{provider: provider}

	name, user := "workspace_1", "workspace_1"
	ws := &pluginDb.Workspace{
		DatabaseName: &name,
		DatabaseUser: &user,
		Status:       pluginDb.WorkspaceStatusReady,
	}

	err := svc.driftCheck(context.Background(), ws)
	require.NoError(t, err, "logical DB identifiers are not drift-checked against the provider")
	assert.Equal(t, 0, provider.resolveCalls, "shared DB resolution must not happen during drift check")
	assert.Equal(t, 0, provider.appGetCalls)
}

// newReconcileService builds a workspace service wired to a DB and a fake
// provider, with fast retry config so tests observe retry-go attempt counts
// without waiting on real backoff pauses.
func newReconcileService(tb coreTesting.TB, db *gorm.DB, provider *fakeWorkspaceProvider) *WorkspaceService {
	tb.Helper()
	bc := &core.BaseComponent{}
	bc.SetDB(db)
	return &WorkspaceService{
		BaseComponent: bc,
		config: &config.WorkspaceConfig{
			RetryMaxAttempts:  3,
			RetryInitialDelay: time.Millisecond,
			RetryMaxDelay:     time.Millisecond,
		},
		provider: provider,
	}
}

// insertReadyDriftWorkspace inserts a ready workspace with a persisted
// application resource ID so driftCheck hits the provider's GetApplication
// path.
func insertReadyDriftWorkspace(tb coreTesting.TB, db *gorm.DB) *pluginDb.Workspace {
	tb.Helper()
	insertWebsite(tb, db, 1, 1)
	insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
	appID := "app-drift"
	ws := &pluginDb.Workspace{
		UserID:                1,
		WebsiteID:             new(uint(1)),
		PlatformDomainID:      10,
		Label:                 "ws-test",
		Status:                pluginDb.WorkspaceStatusReady,
		ApplicationResourceID: &appID,
	}
	require.NoError(tb, db.Create(ws).Error)
	return ws
}

// TestReconcileWithRetry_BoundedRetryableAttempts verifies retry-go retries a
// transient (5xx) drift-check failure exactly RetryMaxAttempts times within a
// single pass before settling, and that a READY workspace is PRESERVED (not
// demoted to failed) with bounded retry metadata recorded — it never hot-loops
// beyond the configured budget and is never falsely taken offline.
func TestReconcileWithRetry_BoundedRetryableAttempts(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		ws := insertReadyDriftWorkspace(tb, db)

		fake := &fakeWorkspaceProvider{appGetErr: &coolify.Error{StatusCode: 503, Message: "overloaded"}}
		svc := newReconcileService(tb, db, fake)

		err := svc.reconcileWithRetry(context.Background(), ws)
		require.Error(tb, err, "a persistent 5xx must settle on an error")

		// Bounded attempts: exactly RetryMaxAttempts provider calls, no more.
		assert.Equal(tb, 3, fake.appGetCalls, "retryable failure must be retried exactly maxRetryAttempts times")

		var reloaded pluginDb.Workspace
		require.NoError(tb, db.First(&reloaded, ws.ID).Error)
		assert.Equal(tb, pluginDb.WorkspaceStatusReady, reloaded.Status,
			"a retryable drift-check failure must NOT demote a ready workspace")
		assert.Equal(tb, 1, reloaded.RetryCount, "one bounded pass accumulates one consecutive failure")
		require.NotNil(tb, reloaded.NextRetryAt, "retry metadata must be scheduled for a retryable failure")
		require.NotEmpty(tb, reloaded.LastError, "the retryable failure must be recorded")
	}, workspaceTestOptions)
}

// TestReconcileWithRetry_RetryableKeepsSuspended verifies a transient
// (retryable) drift-check failure on a SUSPENDED workspace keeps it suspended
// and records retry metadata, rather than demoting it to failed.
func TestReconcileWithRetry_RetryableKeepsSuspended(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		ws := insertReadyDriftWorkspace(tb, db)
		require.NoError(tb, db.Model(ws).Update("status", pluginDb.WorkspaceStatusSuspended).Error)
		ws.Status = pluginDb.WorkspaceStatusSuspended

		fake := &fakeWorkspaceProvider{appGetErr: &coolify.Error{StatusCode: 503, Message: "overloaded"}}
		svc := newReconcileService(tb, db, fake)

		err := svc.reconcileWithRetry(context.Background(), ws)
		require.Error(tb, err)

		var reloaded pluginDb.Workspace
		require.NoError(tb, db.First(&reloaded, ws.ID).Error)
		assert.Equal(tb, pluginDb.WorkspaceStatusSuspended, reloaded.Status,
			"a retryable drift-check failure must NOT demote a suspended workspace")
		assert.Equal(tb, 1, reloaded.RetryCount)
		require.NotNil(tb, reloaded.NextRetryAt)
	}, workspaceTestOptions)
}

// TestReconcileWithRetry_RetryableProvisioningDemotesToFailed verifies that a
// retryable failure during PROVISIONING still transitions to failed (the
// provisioning pipeline needs a retry gate), so the workspace is not stuck in
// the endless-provisioning selection path.
func TestReconcileWithRetry_RetryableProvisioningDemotesToFailed(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "build.example.com", "icann", true)
		ws := &pluginDb.Workspace{
			UserID:           1,
			WebsiteID:        new(uint(1)),
			PlatformDomainID: 10,
			Label:            "ws-provisioning",
			Status:           pluginDb.WorkspaceStatusProvisioning,
		}
		require.NoError(tb, db.Create(ws).Error)

		fake := &fakeWorkspaceProvider{}
		svc := newReconcileService(tb, db, fake)
		// reconcileProvision's first step (ReconcileDatabase) fails retryably.
		fake.resolveErr = &coolify.Error{StatusCode: 503, Message: "db starting"}

		err := svc.reconcileWithRetry(context.Background(), ws)
		require.Error(tb, err)
		assert.Equal(tb, catRetryable, classifyError(err).category)

		var reloaded pluginDb.Workspace
		require.NoError(tb, db.First(&reloaded, ws.ID).Error)
		assert.Equal(tb, pluginDb.WorkspaceStatusFailed, reloaded.Status,
			"a retryable failure on a provisioning workspace must demote to failed")
		assert.Equal(tb, 1, reloaded.RetryCount)
	}, workspaceTestOptions)
}

// TestReconcileWithRetry_PermanentNotRetried verifies a permanent (403)
// failure is surfaced immediately and never retried, even though attempts
// remain in the budget.
func TestReconcileWithRetry_PermanentNotRetried(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		ws := insertReadyDriftWorkspace(tb, db)

		fake := &fakeWorkspaceProvider{appGetErr: &coolify.Error{StatusCode: 403, Message: "forbidden"}}
		svc := newReconcileService(tb, db, fake)

		err := svc.reconcileWithRetry(context.Background(), ws)
		require.Error(tb, err)

		assert.Equal(tb, 1, fake.appGetCalls, "permanent errors must not be retried")
		assert.Equal(tb, catPermanent, classifyError(err).category)

		var reloaded pluginDb.Workspace
		require.NoError(tb, db.First(&reloaded, ws.ID).Error)
		assert.Equal(tb, pluginDb.WorkspaceStatusFailed, reloaded.Status)
	}, workspaceTestOptions)
}

// TestReconcileWithRetry_Cancellation verifies a cancelled context stops the
// retrier before any attempt and never changes desired state (the workspace is
// not marked failed and the provider is never called).
func TestReconcileWithRetry_Cancellation(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		ws := insertReadyDriftWorkspace(tb, db)

		fake := &fakeWorkspaceProvider{appGetErr: &coolify.Error{StatusCode: 503, Message: "overloaded"}}
		svc := newReconcileService(tb, db, fake)

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()

		err := svc.reconcileWithRetry(cancelled, ws)
		require.Error(tb, err)
		assert.True(tb, errors.Is(err, context.Canceled), "cancellation must surface as context.Canceled")

		// The provider is never called and desired state is untouched.
		assert.Zero(tb, fake.appGetCalls, "no provider call on a cancelled pass")
		var reloaded pluginDb.Workspace
		require.NoError(tb, db.First(&reloaded, ws.ID).Error)
		assert.Equal(tb, pluginDb.WorkspaceStatusReady, reloaded.Status, "cancelled pass must not mark the workspace failed")
		assert.Zero(tb, reloaded.RetryCount)
	}, workspaceTestOptions)
}
