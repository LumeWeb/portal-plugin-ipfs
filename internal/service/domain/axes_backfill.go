package domain

// axes_backfill.go implements the BOUNDED APPLICATION BACKFILL for the
// persisted policy axes.
//
// Invariants:
//
//   - Locks one row at a time: each row is read under a single-row take lock;
//     derivation happens outside the write transaction and the final UPDATE
//     is guarded on reconciliation_status IS NULL so a concurrent dual-write
//     is never overwritten.
//   - Axes are derived ONLY via the legacy facts/profile mapper plus profile
//     validation (legacyFacts + legacyProfileFor + PlanBinding). A mapping
//     failure writes the persisted ERROR reconciliation status (see
//     db.PolicyReconciliationError) and NEVER a guessed axis value.
//   - Soft-deleted bindings are never processed (GORM's default scope); a
//     re-bind purges its own tombstones before inserting.
//   - There is NO SQL backfill of ambiguous rows: SQL cannot decide
//     coherence; only this validated application path writes axes.
//   - NOT auto-enabled: the registered cron job no-ops while
//     DnsConfig.DomainPolicyAxesBackfillEnabled is false (the default).
//
// Counts are emitted for mapped, already-mapped, and inconsistent rows (the
// returned summary is also logged at the end of a run).

import (
	"context"
	"errors"
	"fmt"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// backfillDefaultRowBudget bounds a single run: an operator turns the flag on
// for a bounded maintenance window, not for an unbounded migration. One row
// is processed per pass; this budget bounds the pass and the cron reruns it.
const backfillDefaultRowBudget = 100

// BackfillSummary reports the outcome of one bounded backfill run.
type BackfillSummary struct {
	// Mapped counts rows this run mapped to a complete, valid axis set.
	Mapped int
	// Inconsistent counts rows whose mapping FAILED this run: they carry the
	// persisted error reconciliation status and no guessed axis values.
	Inconsistent int
	// AlreadyMapped counts rows that carried a mapped axis set before this
	// run started (the idempotence/census signal for dashboards).
	AlreadyMapped int
	// Remaining counts legacy rows (reconciliation_status IS NULL) still
	// unmapped after this run (0 when the backlog drained).
	Remaining int
}

// BackfillPolicyAxes processes up to maxRows unmapped rows, one row at a
// time. It is the runtime entry point behind the registered backfill cron
// job (which no-ops unless the config flag is true) and tests.
func (s *DelegatedDomainService) BackfillPolicyAxes(ctx context.Context, maxRows int) (BackfillSummary, error) {
	summary, err := s.backfillPolicyAxesLoop(ctx, maxRows)
	if err != nil {
		return summary, err
	}
	s.Logger().Info("persisted policy axis backfill run completed",
		zap.Int("mapped", summary.Mapped),
		zap.Int("already_mapped", summary.AlreadyMapped),
		zap.Int("inconsistent", summary.Inconsistent),
		zap.Int("remaining", summary.Remaining))
	return summary, nil
}

func (s *DelegatedDomainService) backfillPolicyAxesLoop(ctx context.Context, maxRows int) (BackfillSummary, error) {
	var summary BackfillSummary
	if s.DB() == nil {
		return summary, fmt.Errorf("database not available")
	}
	if maxRows <= 0 {
		maxRows = backfillDefaultRowBudget
	}

	// Idempotence/census signal: rows already mapped ahead of this pass.

	var already int64
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.Model(&pluginDb.WebsiteDomain{}).
			Where("reconciliation_status = ?", pluginDb.PolicyReconciliationMapped).
			Count(&already).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	}); err != nil {
		return summary, fmt.Errorf("count mapped rows: %w", err)
	}
	summary.AlreadyMapped = int(already)

	for i := 0; i < maxRows; i++ {
		more, outcome, err := s.backfillOneRow(ctx)
		if err != nil {
			return summary, err
		}
		switch outcome {
		case backfillOutcomeMapped:
			summary.Mapped++
		case backfillOutcomeInconsistent:
			summary.Inconsistent++
		case backfillOutcomeTaken:
			summary.AlreadyMapped++
		}
		if !more {
			break
		}
	}

	var unmapped int64
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.Model(&pluginDb.WebsiteDomain{}).
			Where("reconciliation_status IS NULL").
			Count(&unmapped).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	}); err != nil {
		return summary, fmt.Errorf("count unmapped rows: %w", err)
	}
	summary.Remaining = int(unmapped)
	return summary, nil
}

// Backfill outcomes.
const (
	backfillOutcomeMapped       = "mapped"
	backfillOutcomeInconsistent = "inconsistent"
	backfillOutcomeTaken        = "taken"
	backfillOutcomeDrained      = "drained"
)

// backfillOneRow maps and writes ONE row under the guarded procedure:
//
//  1. lock + read the next unmapped row (single-row take lock);
//  2. map that row through the legacy facts/profile chain (no lock held —
//     mapper-side reads are transient values, not control columns);
//  3. write the derived columns guarded on reconciliation_status IS NULL, so
//     a row reconciled concurrently keeps its stored representation.
//
// Returns more=true while the backlog may still hold work, plus the outcome
// for the run counters.
func (s *DelegatedDomainService) backfillOneRow(ctx context.Context) (bool, string, error) {
	var row *pluginDb.WebsiteDomain
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		var wd pluginDb.WebsiteDomain
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("reconciliation_status IS NULL").
			Order("id ASC").
			First(&wd).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return tx // backlog drained
			}
			_ = tx.AddError(err)
			return tx
		}
		row = &wd
		return tx
	}); err != nil {
		return false, backfillOutcomeDrained, fmt.Errorf("lock next unmapped binding row: %w", err)
	}
	if row == nil {
		return false, backfillOutcomeDrained, nil
	}

	website, werr := s.loadBackfillWebsite(ctx, row.WebsiteID)
	if werr != nil {
		// No website row (or unreachable DB): the mapper cannot validate the
		// target facts. Record the inconsistency; never guess axes.
		if err := s.writeBackfillReconciliation(ctx, row.ID, pluginDb.PolicyReconciliationError); err != nil {
			return false, backfillOutcomeDrained, err
		}
		s.Logger().Warn("persisted policy axis backfill: cannot load owning website; row marked inconsistent",
			zap.Uint("id", row.ID), zap.String("domain", row.Domain), zap.Error(werr))
		return true, backfillOutcomeInconsistent, nil
	}

	var updates map[string]any
	var outcome string
	if axes, err := s.derivePolicyAxes(row, website); err == nil {
		updates = axes.Columns()
		outcome = backfillOutcomeMapped
	} else {
		updates = pluginDb.PolicyAxesErrorColumns()
		outcome = backfillOutcomeInconsistent
		s.Logger().Warn("persisted policy axis backfill: mapping rejected the row (fail closed, no axis values guessed)",
			zap.Uint("id", row.ID), zap.String("domain", row.Domain), zap.Error(err))
	}

	applied, err := s.writeBackfillColumns(ctx, row.ID, updates)
	if err != nil {
		return false, backfillOutcomeDrained, err
	}
	if !applied {
		// Guard missed: the row was reconciled concurrently after we read it.
		return true, backfillOutcomeTaken, nil
	}
	return true, outcome, nil
}

// writeBackfillColumns writes the derived column map guarded on
// reconciliation_status IS NULL. Reports whether the guard matched (the write
// was applied).
func (s *DelegatedDomainService) writeBackfillColumns(ctx context.Context, id uint, cols map[string]any) (bool, error) {
	var applied bool
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		res := tx.Model(&pluginDb.WebsiteDomain{}).
			Where("id = ? AND reconciliation_status IS NULL", id).
			Updates(cols)
		if res.Error != nil {
			_ = tx.AddError(res.Error)
			return tx
		}
		applied = res.RowsAffected == 1
		return tx
	})
	if err != nil {
		return false, fmt.Errorf("write policy axes for binding %d: %w", id, err)
	}
	return applied, nil
}

// loadBackfillWebsite loads the owning website for the mapper's target facts.
func (s *DelegatedDomainService) loadBackfillWebsite(ctx context.Context, websiteID uint) (*pluginDb.Website, error) {
	var website pluginDb.Website
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.First(&website, websiteID).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		return nil, err
	}
	return &website, nil
}

// writeBackfillReconciliation persists only the reconciliation status column.
func (s *DelegatedDomainService) writeBackfillReconciliation(ctx context.Context, id uint, status string) error {
	_, err := s.writeBackfillColumns(ctx, id, map[string]any{"reconciliation_status": status})
	return err
}

// axesBackfillEnabled reads the config flag the registered (but not
// auto-enabled) backfill cron job gates on. Default false — the job is
// registered per the plugin cron pattern but never starts automatically.
func (s *DelegatedDomainService) axesBackfillEnabled() bool {
	if s.BaseComponent == nil {
		return false
	}
	dnsCfg := core.GetServiceConfig[*pluginConfig.DnsConfig](s.Context(), pluginCore.DNS_SERVICE)
	if dnsCfg == nil {
		return false
	}
	return dnsCfg.DomainPolicyAxesBackfillEnabled
}
