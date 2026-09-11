// Workspace reconciliation scheduling, retries, drift handling, and structured
// error classification. This file provides:
//
//   - ReconcileJob, the plugin cron job, running on WorkspaceConfig.ReconcileInterval;
//   - bounded batch selection of provisioning / retry-eligible-failed /
//     ready|suspended drift-check workspaces;
//   - an in-process keyed per-workspace lock (current single-instance setup);
//   - failure classification and bounded exponential backoff for
//     timeout/429/5xx, with permanent handling for 401/403/409/422;
//   - fail-closed drift handling: a saved resource returning 404 is drift and
//     is never recreated automatically;
//   - bounded, redacted LastError storage (secret-safe) and reconcile metrics.
//
// Secret safety: classifyError/backoff and applyReconcileFailure only ever
// record the (already secret-safe) error message — never passwords, API
// tokens, or raw provider response bodies. Logging carries only the workspace
// ID and the coarse classification outcome.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/avast/retry-go/v5"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/coolify"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ReconcileJobType and ReconcileJobSourceID identify the workspace reconciler
// cron job in plugin info.
const (
	ReconcileJobType     = "plugin.ipfs.workspace_reconciler"
	ReconcileJobSourceID = "ipfs"
)

// ReconcileJob is the plugin cron job that advances workspace reconciliation
// on WorkspaceConfig.ReconcileInterval.
type ReconcileJob struct {
	*core.BaseCronJob

	svc    *WorkspaceService
	logger *core.Logger

	// lastRunMu guards lastRun, which enforces the configured ReconcileInterval
	// cadence on top of the coarse per-minute cron expression.
	lastRunMu sync.Mutex
	lastRun   time.Time
}

// NewReconcileJob creates the workspace reconciler cron job.
func NewReconcileJob() core.CronJob {
	job := &ReconcileJob{}
	jobID := uuid.New()
	// The cron expression is a coarse per-minute trigger; the configured
	// ReconcileInterval cadence is enforced in Run (via due).
	scheduleDef := core.NewCronScheduleDefinition(core.CronScheduleTypeCron).
		WithCronExpression("* * * * *")
	job.BaseCronJob = core.NewBaseCronJob(
		jobID,
		core.JobOriginPlugin,
		ReconcileJobSourceID,
		"IPFS Workspace Reconciler",
		scheduleDef,
		nil,
		core.WithExplicitJobType(ReconcileJobType),
	)
	return job
}

// Run executes one reconciler pass: it loads the workspace service, enforces
// the ReconcileInterval cadence, selects a bounded batch, and reconciles each
// workspace under a per-workspace keyed lock.
func (j *ReconcileJob) Run(ctx core.Context, eventCtx context.Context) error {
	if err := j.initialize(ctx); err != nil {
		return err
	}
	svc := j.svc
	if svc.config == nil || !svc.config.Enabled {
		j.logger.Debug("Workspace reconciliation is disabled")
		return nil
	}
	if svc.provider == nil {
		j.logger.Debug("Workspace provider not available; skipping reconcile")
		return nil
	}

	if !j.due() {
		j.logger.Debug("Workspace reconcile not yet due per ReconcileInterval")
		return nil
	}

	batch, err := svc.selectReconcileBatch(eventCtx)
	if err != nil {
		j.logger.Error("Failed to select workspace reconcile batch", zap.Error(err))
		return err
	}
	if len(batch) == 0 {
		return nil
	}
	j.logger.Info("Running workspace reconcile pass",
		zap.Int("batch_size", len(batch)),
		zap.Duration("interval", svc.ReconcileInterval()))

	for _, ws := range batch {
		unlock := svc.lockWorkspace(ws.ID)
		reconcileErr := svc.reconcileWithRetry(eventCtx, ws)
		unlock()

		if reconcileErr != nil && !errors.Is(reconcileErr, context.Canceled) {
			// The classified error is already secret-safe (no tokens/body) and
			// recorded on the row; this log carries only the workspace ID and
			// the coarse outcome.
			j.logger.Warn("Workspace reconcile failed",
				zap.Uint("workspace_id", ws.ID),
				zap.String("outcome", outcomeLabel(reconcileErr)),
				zap.Error(reconcileErr))
		}
	}

	svc.refreshStateGauge(eventCtx)
	return nil
}

// initialize wires the workspace service and logger from the portal context.
func (j *ReconcileJob) initialize(ctx core.Context) error {
	svc := core.GetServiceOptional[*WorkspaceService](ctx, pluginCore.WORKSPACE_SERVICE)
	if svc == nil {
		return errors.New("workspace: workspace service not available")
	}
	j.svc = svc
	j.logger = ctx.Logger()
	if j.logger == nil {
		return errors.New("workspace: logger not available")
	}
	return nil
}

// due reports whether a reconcile pass should run, enforcing the configured
// ReconcileInterval on top of the per-minute cron trigger.
func (j *ReconcileJob) due() bool {
	interval := j.svc.ReconcileInterval()
	j.lastRunMu.Lock()
	defer j.lastRunMu.Unlock()
	now := time.Now()
	if j.lastRun.IsZero() || now.Sub(j.lastRun) >= interval {
		j.lastRun = now
		return true
	}
	return false
}

// ReconcileInterval returns the configured reconcile cadence with a fallback.
func (s *WorkspaceService) ReconcileInterval() time.Duration {
	if s.config != nil && s.config.ReconcileInterval > 0 {
		return s.config.ReconcileInterval
	}
	return time.Minute
}

// batchSize returns the bounded reconcile batch size with a fallback.
func (s *WorkspaceService) batchSize() int {
	if s.config != nil && s.config.ReconcileBatchSize > 0 {
		return s.config.ReconcileBatchSize
	}
	return 50
}

// maxRetryAttempts returns the per-pass retry budget for transient failures.
func (s *WorkspaceService) maxRetryAttempts() int {
	if s.config != nil && s.config.RetryMaxAttempts > 0 {
		return s.config.RetryMaxAttempts
	}
	return 3
}

// driftCheckInterval returns how often ready/suspended workspaces are checked.
func (s *WorkspaceService) driftCheckInterval() time.Duration {
	if s.config != nil && s.config.DriftCheckInterval > 0 {
		return s.config.DriftCheckInterval
	}
	return 24 * time.Hour
}

// nextRetryDelay returns the delay before the next attempt: bounded
// exponential backoff, upgraded to the provider's Retry-After hint (HTTP 429)
// when that asks for a longer wait. The hint is a non-secret duration, so
// failures can still be classified secret-safely while honouring the provider.
func (s *WorkspaceService) nextRetryDelay(attempt int, err error) time.Duration {
	delay := s.retryDelay(attempt)
	if ra := coolify.RetryAfterHint(err); ra > delay {
		return ra
	}
	return delay
}

// retryDelay returns the bounded exponential backoff for a retry count.
func (s *WorkspaceService) retryDelay(count int) time.Duration {
	base := time.Duration(0)
	max := time.Duration(0)
	if s.config != nil {
		base = s.config.RetryInitialDelay
		max = s.config.RetryMaxDelay
	}
	return backoffDelay(count, base, max)
}

// lockWorkspace acquires the in-process keyed lock for one workspace and
// returns a function to release it. It is only safe across one portal instance
// (the current deployment).
func (s *WorkspaceService) lockWorkspace(id uint) func() {
	s.lockMu.Lock()
	if s.locks == nil {
		s.locks = make(map[uint]*sync.Mutex)
	}
	m := s.locks[id]
	if m == nil {
		m = &sync.Mutex{}
		s.locks[id] = m
	}
	s.lockMu.Unlock()
	m.Lock()
	return m.Unlock
}

// selectReconcileBatch selects a bounded batch of workspaces needing work:
//
//   - provisioning workspaces (advance the pipeline),
//   - failed workspaces whose NextRetryAt has elapsed (retry-eligible),
//   - ready/suspended workspaces past DriftCheckInterval (drift check).
func (s *WorkspaceService) selectReconcileBatch(ctx context.Context) ([]*pluginDb.Workspace, error) {
	now := time.Now()
	driftThreshold := now.Add(-s.driftCheckInterval())
	statuses := []pluginDb.WorkspaceStatus{
		pluginDb.WorkspaceStatusReady,
		pluginDb.WorkspaceStatusSuspended,
	}

	var workspaces []*pluginDb.Workspace
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		q := tx.Model(&pluginDb.Workspace{}).
			Where("deleted_at IS NULL").
			Where(`(
				status = ? OR
				(status = ? AND next_retry_at IS NOT NULL AND next_retry_at <= ?) OR
				(status IN ? AND (last_reconcile_at IS NULL OR last_reconcile_at <= ?))
			)`,
				pluginDb.WorkspaceStatusProvisioning,
				pluginDb.WorkspaceStatusFailed, now,
				statuses,
				driftThreshold,
			).
			Order("id ASC").
			Limit(s.batchSize())
		if err := q.Find(&workspaces).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		return nil, fmt.Errorf("workspace: failed to select reconcile batch: %w", err)
	}
	return workspaces, nil
}

// reconcileWithRetry runs the full reconcile for one workspace under its lock,
// using retry-go/v5 for the bounded per-pass retry loop. It preserves the
// original behavior:
//
//   - bounded attempts and bounded exponential backoff (RetryMaxAttempts /
//     RetryInitialDelay / RetryMaxDelay);
//   - context cancellation aborts the loop and never changes desired state;
//   - Retry-After hints (HTTP 429) are honoured when they request a longer
//     wait than the computed backoff;
//   - permanent / drift / cancellation errors are not retried (the retrier is
//     only ever told to retry by classifyError's coarse catRetryable outcome);
//   - the recorded error and the logs stay secret-safe: retry-go only sees a
//     non-secret duration and a coarse classification, never passwords,
//     tokens, or raw provider bodies.
//
// After the retrier settles it persists the reconcile outcome and updates the
// reconcile metrics, returning the (secret-safe) classified error for logging,
// or nil on success/cancellation-skip.
func (s *WorkspaceService) reconcileWithRetry(ctx context.Context, ws *pluginDb.Workspace) error {
	start := time.Now()

	// retry-go treats Attempts(0) as "retry forever"; clamp to the bounded
	// per-pass budget so the reconcile loop can never run unbounded.
	attempts := s.maxRetryAttempts()
	if attempts < 1 {
		attempts = 1
	}

	err := retry.New(
		retry.Attempts(uint(attempts)),
		retry.Context(ctx),
		retry.LastErrorOnly(true),
		retry.RetryIf(func(err error) bool {
			// Only transient failures are retried; permanent, drift and
			// cancellation are surfaced immediately and never re-run.
			return classifyError(err).category == catRetryable
		}),
		// Bounded exponential backoff, upgraded to the provider's Retry-After
		// hint (HTTP 429) when that asks for a longer wait.
		retry.DelayType(func(n uint, err error, _ retry.DelayContext) time.Duration {
			return s.nextRetryDelay(int(n), err)
		}),
	).Do(func() error {
		if err := ctx.Err(); err != nil {
			// Stop before changing desired state on a cancelled pass.
			return err
		}
		return s.reconcileWorkspace(ctx, ws)
	})

	if err == nil {
		err = s.persistReconcileOK(ctx, ws)
	}

	duration := time.Since(start)
	if err == nil {
		observeOutcome(LabelOutcomeSuccess, duration)
		return nil
	}

	cls := classifyError(err)
	if cls.category == catCancelled {
		// Stop the current pass without changing desired state.
		return ctx.Err()
	}
	observeOutcome(outcomeFor(cls.category), duration)
	s.applyReconcileFailure(ctx, ws, err, cls)
	return err
}

// reconcileWorkspace advances one workspace based on its current status:
// provisioning/failed run the provisioning pipeline; ready/suspended only get
// a drift check (never-recreate).
func (s *WorkspaceService) reconcileWorkspace(ctx context.Context, ws *pluginDb.Workspace) error {
	if s.provider == nil {
		return ErrWorkspaceProviderUnavailable
	}
	switch ws.Status {
	case pluginDb.WorkspaceStatusProvisioning, pluginDb.WorkspaceStatusFailed:
		return s.reconcileProvision(ctx, ws)
	case pluginDb.WorkspaceStatusReady, pluginDb.WorkspaceStatusSuspended:
		return s.driftCheck(ctx, ws)
	default:
		// deleting (or unknown): the reconciler does not touch it.
		return nil
	}
}

// reconcileProvision drives the full provisioning pipeline. The building
// blocks are idempotent (create-or-adopt by deterministic name/tag), so a
// retry never duplicates resources.
func (s *WorkspaceService) reconcileProvision(ctx context.Context, ws *pluginDb.Workspace) error {
	dbCreds, err := s.ReconcileDatabase(ctx, ws)
	if err != nil {
		return err
	}
	apiKey, err := s.ReconcileAPIKey(ctx, ws)
	if err != nil {
		return err
	}
	if _, err := s.ReconcileApplication(ctx, ws); err != nil {
		return err
	}
	if err := s.ReconcileApplicationStorage(ctx, ws, *ws.ApplicationResourceID); err != nil {
		return err
	}
	if err := s.SetApplicationEnvironment(ctx, ws, *ws.ApplicationResourceID, dbCreds, apiKey); err != nil {
		return err
	}
	return s.StartAndObserveApplication(ctx, ws, *ws.ApplicationResourceID)
}

// driftCheck verifies that a ready/suspended workspace's persisted provider
// resources still exist. If a saved resource returns 404 it is drift — the
// reconciler fails closed and never recreates the application automatically,
// because recreating would silently lose data/storage.
//
// There is deliberately no database drift check here: every workspace shares
// ONE Coolify-managed MySQL/MariaDB resource that reconciliation never
// creates/deletes. Its liveness is resolved at reconcile time for logical
// provisioning, not tracked per workspace, so there is no per-workspace remote
// DB ID to re-check.
func (s *WorkspaceService) driftCheck(ctx context.Context, ws *pluginDb.Workspace) error {
	// The shared database resource is never per-workspace drift: it is a
	// configured portal dependency, not a workspace-owned remote resource.
	if ws.ApplicationResourceID != nil {
		if _, err := s.provider.GetApplication(ctx, *ws.ApplicationResourceID); err != nil {
			return fmt.Errorf("workspace: drift check application: %w", err)
		}
	}
	return nil
}

// persistReconcileOK records a successful reconcile: refreshed
// LastReconcileAt and cleared retry state. StartAndObserveApplication already
// marked a freshly-provisioned workspace ready.
func (s *WorkspaceService) persistReconcileOK(ctx context.Context, ws *pluginDb.Workspace) error {
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&pluginDb.Workspace{}).
			Where("id = ?", ws.ID).
			Updates(map[string]any{
				"last_reconcile_at": time.Now(),
				"retry_count":       0,
				"next_retry_at":     nil,
			})
	})
	if err != nil {
		return fmt.Errorf("workspace: failed to persist reconcile success: %w", err)
	}
	return nil
}

// applyReconcileFailure records a bounded redacted LastError and schedules
// retry state, and transitions the workspace's status. ready/suspended
// workspaces that fail a transient (retryable) drift check KEEP their
// ready/suspended status: a transient drift failure does not mean the
// workspace is broken, so it must not be demoted. Provisioning workspaces and
// permanent/drift failures DO demote to failed (an operator must intervene, or
// the provisioning pipeline needs a retry gate). Transient failures get a
// backoff NextRetryAt; permanent and drift failures are not auto-retried
// (operator must intervene) and clear NextRetryAt.
func (s *WorkspaceService) applyReconcileFailure(ctx context.Context, ws *pluginDb.Workspace, err error, cls classifyResult) {
	newCount := ws.RetryCount + 1
	var nextRetry *time.Time
	if cls.category == catRetryable {
		t := time.Now().Add(s.retryDelay(newCount))
		nextRetry = &t
	}
	msg := boundError(err)

	// A transient (retryable) drift-check failure on a workspace that is
	// already oper-ready or suspended is not a demotion condition: the runtime
	// is still standing and will simply be drift-checked again after the
	// backoff. Everything else (provisioning, or a permanent/drift failure)
	// transitions to failed as before.
	newStatus := pluginDb.WorkspaceStatusFailed
	if (ws.Status == pluginDb.WorkspaceStatusReady || ws.Status == pluginDb.WorkspaceStatusSuspended) &&
		cls.category == catRetryable {
		newStatus = ws.Status
	}

	_ = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&pluginDb.Workspace{}).
			Where("id = ?", ws.ID).
			Updates(map[string]any{
				"status":            newStatus,
				"last_error":        msg,
				"retry_count":       newCount,
				"next_retry_at":     nextRetry,
				"last_reconcile_at": time.Now(),
			})
	})
	ws.Status = newStatus
	ws.LastError = msg
	ws.RetryCount = newCount
	ws.NextRetryAt = nextRetry
}

// refreshStateGauge updates the WorkspaceState gauge from current DB counts.
func (s *WorkspaceService) refreshStateGauge(ctx context.Context) {
	type stateCount struct {
		Status pluginDb.WorkspaceStatus
		Count  int64
	}
	var rows []stateCount
	_ = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.Model(&pluginDb.Workspace{}).
			Select("status, count(*) as count").
			Where("deleted_at IS NULL").
			Group("status").
			Scan(&rows).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	WorkspaceState.Reset()
	for _, r := range rows {
		WorkspaceState.WithLabelValues(string(r.Status)).Set(float64(r.Count))
	}
}

// observeOutcome increments the reconcile counter and duration histogram for
// an outcome.
func observeOutcome(outcome string, duration time.Duration) {
	ReconcileTotal.WithLabelValues(outcome).Inc()
	ReconcileDurationSeconds.WithLabelValues(outcome).Observe(duration.Seconds())
}

// outcomeLabel derives a coarse outcome label from an error for logging and
// metrics (no secret/error detail).
func outcomeLabel(err error) string {
	if err == nil {
		return LabelOutcomeSuccess
	}
	return outcomeFor(classifyError(err).category)
}

// retryCategory is the coarse classification of a reconcile failure.
type retryCategory int

const (
	// catRetryable: transient (timeout/429/5xx) — retry with backoff.
	catRetryable retryCategory = iota
	// catPermanent: operator/config problem (401/403/409/422) — mark failed.
	catPermanent
	// catDrift: a saved resource is missing (404) — mark failed, never recreate.
	catDrift
	// catCancelled: context cancelled — stop without changing desired state.
	catCancelled
)

// classifyResult is the outcome of classifyError.
type classifyResult struct {
	category retryCategory
}

// classifyError maps a reconcile error to its coarse category.
// Sentinel/typed errors are classified first, then coolify HTTP status codes,
// then generic network timeouts. It never inspects or returns secret values.
func classifyError(err error) classifyResult {
	if err == nil {
		return classifyResult{}
	}
	if errors.Is(err, context.Canceled) {
		return classifyResult{category: catCancelled}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return classifyResult{category: catRetryable}
	}

	// Sentinel terminal errors already wired into the building blocks.
	switch {
	case errors.Is(err, ErrDatabaseCredentialFieldMissing),
		errors.Is(err, ErrDatabaseURLMalformed),
		errors.Is(err, ErrDatabaseServerFailed),
		errors.Is(err, ErrApplicationDomainConflict),
		errors.Is(err, ErrApplicationAdoptionAmbiguous),
		errors.Is(err, ErrApplicationCreateNoID),
		errors.Is(err, ErrDeploymentFailed),
		errors.Is(err, ErrApplicationNotHealthy),
		errors.Is(err, ErrProxyCredentialsUnavailable):
		return classifyResult{category: catPermanent}
	case errors.Is(err, ErrApplicationProvisionTimeout):
		return classifyResult{category: catRetryable}
	}

	// Coolify typed status errors.
	if coolify.IsNotFound(err) {
		return classifyResult{category: catDrift}
	}
	if coolify.IsRateLimited(err) {
		return classifyResult{category: catRetryable}
	}
	if coolify.IsUnauthorized(err) {
		return classifyResult{category: catPermanent}
	}
	if coolify.IsUnprocessable(err) {
		return classifyResult{category: catPermanent}
	}
	if coolify.IsConflict(err) {
		return classifyResult{category: catPermanent}
	}
	if coolify.IsRetryableServerError(err) {
		return classifyResult{category: catRetryable}
	}

	// Generic network timeout.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return classifyResult{category: catRetryable}
	}

	// Default: transient — a provider failure is safe to retry (create paths
	// adopt by deterministic name/tag instead of duplicating).
	return classifyResult{category: catRetryable}
}

// outcomeFor renders a coarse, secret-free metric/log outcome label.
func outcomeFor(c retryCategory) string {
	switch c {
	case catRetryable:
		return LabelOutcomeRetryable
	case catPermanent:
		return LabelOutcomePermanent
	case catDrift:
		return LabelOutcomeDrift
	case catCancelled:
		return LabelOutcomeSuccess // cancelled passes are not failures
	default:
		return LabelOutcomeSuccess
	}
}

// boundError bounds and returns a redacted error string for LastError storage.
// Coolify errors never retain raw bodies, so the message carries no secrets.
func boundError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) > maxLastErrorLen {
		msg = msg[:maxLastErrorLen]
	}
	return msg
}

// backoffDelay returns a bounded exponential backoff for attempt (1-based),
// capped at max. A zero base falls back to 30s; a zero max to 5m.
func backoffDelay(attempt int, base, max time.Duration) time.Duration {
	if max <= 0 {
		max = 5 * time.Minute
	}
	if base <= 0 {
		base = 30 * time.Second
	}
	if attempt <= 1 {
		return base
	}
	exp := uint(attempt - 1)
	if exp > 20 {
		exp = 20
	}
	d := base * time.Duration(1<<exp)
	if d < base { // overflow guard
		return max
	}
	if d > max {
		return max
	}
	return d
}

// settle compile-time interface assertions.
var (
	_ core.CronJob         = (*ReconcileJob)(nil)
	_ prometheus.Collector = nil
)
