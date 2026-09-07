package domain

// DomainPolicyAxesBackfillJob implements core.CronJob for the bounded,
// idempotent application backfill of the persisted policy axes. It follows the WebsiteJanitorJob
// registration pattern (plugin cron registry), but is NOT auto-enabled: the
// run no-ops unless DnsConfig.DomainPolicyAxesBackfillEnabled is true (the
// default) or the delegated-domain service (and with it the row-local job
// config) is unavailable. Production rollback never needs this job at all —
// old readers keep working with the nullable columns in place.

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	// AxesBackfillJobSourceID matches the plugin's cron source id convention.
	AxesBackfillJobSourceID = "ipfs"
	// AxesBackfillJobType is the durable job type identifier.
	AxesBackfillJobType = "plugin.ipfs.domain_policy_axes_backfill"
)

// DomainPolicyAxesBackfillJob implements core.CronJob.
type DomainPolicyAxesBackfillJob struct {
	*core.BaseCronJob
	svc    *DelegatedDomainService
	db     *gorm.DB
	logger *core.Logger
}

// NewDomainPolicyAxesBackfillJob creates the job (never auto-enabled).
func NewDomainPolicyAxesBackfillJob() core.CronJob {
	job := &DomainPolicyAxesBackfillJob{}

	jobID := uuid.New()
	scheduleDef := core.NewCronScheduleDefinition(core.CronScheduleTypeCron).
		WithCronExpression("*/5 * * * *") // every 5 minutes; gated by config

	job.BaseCronJob = core.NewBaseCronJob(
		jobID,
		core.JobOriginPlugin,
		AxesBackfillJobSourceID,
		"IPFS Domain Policy Axes Backfill",
		scheduleDef,
		nil,
		core.WithExplicitJobType(AxesBackfillJobType),
	)

	return job
}

// Run executes one bounded backfill pass when (and only when) the operator
// enabled it. Missing dependencies degrade to a no-op so an unconfigured
// environment never starts the backfill at all.
func (j *DomainPolicyAxesBackfillJob) Run(ctx core.Context, eventCtx context.Context) error {
	if err := j.initialize(ctx); err != nil {
		// Initialization failure means the backfill cannot run safely: skip
		// this pass (the registered job keeps the schedule for retries).
		if j.logger != nil {
			j.logger.Warn("domain policy axes backfill skipped: dependencies unavailable",
				zap.Error(err))
		}
		return nil
	}

	if j.svc == nil {
		j.logger.Debug("domain policy axes backfill skipped: delegated-domain service unavailable")
		return nil
	}
	if !j.svc.axesBackfillEnabled() {
		j.logger.Debug("domain policy axes backfill skipped: not enabled (domain_policy_axes_backfill_enabled=false)")
		return nil
	}

	if _, err := j.svc.BackfillPolicyAxes(eventCtx, backfillDefaultRowBudget); err != nil {
		j.logger.Error("domain policy axes backfill run failed", zap.Error(err))
		return err
	}
	return nil
}

// initialize resolves the job's dependencies from the service registry.
func (j *DomainPolicyAxesBackfillJob) initialize(ctx core.Context) error {
	if dds := core.GetServiceOptional[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE); dds != nil {
		j.svc = dds
	}
	j.db = ctx.DB()
	if j.db == nil {
		j.logger = ctx.Logger()
		return fmt.Errorf("database not available")
	}
	j.logger = ctx.Logger()
	return nil
}
