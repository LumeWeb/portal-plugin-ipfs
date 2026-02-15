package website

import (
	"context"
	"fmt"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	JanitorJobSourceID = "ipfs.website_janitor"
)

// WebsiteJanitorJob implements core.CronJob for periodic website validation
type WebsiteJanitorJob struct {
	*core.BaseCronJob
	config              *config.WebsiteConfig
	pinService          pluginCore.IPFSPinService
	ipnsKeyService      pluginCore.IPNSKeyService
	ipnsPublisherService pluginCore.IPNSPublisherService
	db                  *gorm.DB
	logger              *core.Logger
}

// NewWebsiteJanitorJob creates a new WebsiteJanitorJob instance
func NewWebsiteJanitorJob() core.CronJob {
	job := &WebsiteJanitorJob{}

	// Initialize BaseCronJob with default values
	jobID := uuid.New()
	scheduleDef := core.NewCronScheduleDefinition(core.CronScheduleTypeCron).
		WithCronExpression("*/30 * * * *") // Every 30 minutes (default)

	job.BaseCronJob = core.NewBaseCronJob(
		jobID,
		core.JobOriginPlugin,
		JanitorJobSourceID,
		"IPFS Website Janitor",
		scheduleDef,
		nil,
	)

	return job
}

// Run executes the janitor job logic
func (j *WebsiteJanitorJob) Run(ctx core.Context, eventCtx context.Context) error {
	// Use eventCtx for tracing since it's the context.Context type
	// ctx is core.Context for service access
	eventCtx, span := core.TraceMethod(eventCtx, "WebsiteJanitorJob.Run")
	defer span.End()

	// Initialize job dependencies
	if err := j.initializeJob(ctx); err != nil {
		return fmt.Errorf("failed to initialize website janitor: %w", err)
	}

	if j.config == nil || !j.config.JanitorEnabled {
		j.logger.Debug("Website janitor is disabled, skipping")
		return nil
	}

	j.logger.Info("Starting website janitor run",
		zap.Duration("interval", j.config.JanitorInterval),
		zap.Int("workers", j.config.JanitorWorkerCount),
		zap.Int("batch_size", j.config.JanitorBatchSize))

	// Query websites that need validation
	var websites []*pluginDb.Website
	err := j.db.WithContext(eventCtx).
		Where("deleted_at IS NULL").
		Where("last_checked_at IS NULL OR last_checked_at < ?", time.Now().Add(-j.config.JanitorInterval)).
		Find(&websites).Error

	if err != nil {
		j.logger.Error("Failed to query websites for validation", zap.Error(err))
		return fmt.Errorf("failed to query websites: %w", err)
	}

	if len(websites) == 0 {
		j.logger.Debug("No websites need validation")
		return nil
	}

	j.logger.Info("Processing websites for validation", zap.Int("count", len(websites)))

	// Create worker pool for parallel processing
	wp := workerpool.New(j.config.JanitorWorkerCount)
	defer wp.StopWait()

	// Process websites in batches
	for i := 0; i < len(websites); i += j.config.JanitorBatchSize {
		end := i + j.config.JanitorBatchSize
		if end > len(websites) {
			end = len(websites)
		}
		batch := websites[i:end]

		j.logger.Debug("Processing batch", zap.Int("batch_start", i), zap.Int("batch_end", end))

		for _, website := range batch {
			website := website
			wp.Submit(func() {
				if err := j.validateWebsite(eventCtx, website); err != nil {
					j.logger.Error("Failed to validate website",
						zap.Error(err),
						zap.Uint("website_id", website.ID),
						zap.String("domain", website.Domain))
				}
			})
		}
	}

	// Wait for all workers to complete
	wp.StopWait()

	j.logger.Info("Website janitor run completed", zap.Int("processed", len(websites)))
	return nil
}

// validateWebsite checks if a website's target is still valid and updates its status
func (j *WebsiteJanitorJob) validateWebsite(ctx context.Context, website *pluginDb.Website) error {
	ctx, span := core.TraceMethod(ctx, "WebsiteJanitorJob.validateWebsite")
	defer span.End()

	oldStatus := website.Status
	newStatus := pluginDb.WebsiteStatusActive

	// Check target based on type
	switch website.TargetType {
	case string(pluginDb.WebsiteTargetTypeIPFS):
		valid, err := j.validateCIDTarget(ctx, website.TargetHash())
		if err != nil {
			j.logger.Warn("Failed to validate CID target",
				zap.Error(err),
				zap.String("target", website.TargetHash()))
			newStatus = pluginDb.WebsiteStatusBroken
		} else if !valid {
			j.logger.Debug("CID target is not valid or not pinned",
				zap.String("target", website.TargetHash()))
			newStatus = pluginDb.WebsiteStatusBroken
		}

	case string(pluginDb.WebsiteTargetTypeIPNS):
		err := j.validateIPNSTarget(ctx, website)
		if err != nil {
			j.logger.Warn("Failed to validate IPNS target",
				zap.Error(err),
				zap.String("target", website.TargetHash()))
			newStatus = pluginDb.WebsiteStatusBroken
		}
		// validateIPNSTarget handles status and LastCheckedAt updates internally
		// Save the changes immediately
		if err := j.db.WithContext(ctx).Save(website).Error; err != nil {
			return fmt.Errorf("failed to update website: %w", err)
		}
		return nil

	default:
		j.logger.Warn("Unknown target type", zap.String("target_type", website.TargetType))
		newStatus = pluginDb.WebsiteStatusBroken
	}

	// Update website status if changed
	if string(newStatus) != oldStatus {
		j.logger.Info("Website status changed",
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("old_status", oldStatus),
			zap.String("new_status", string(newStatus)))

		website.Status = string(newStatus)

		// Trigger notification if status changed to broken
		if newStatus == pluginDb.WebsiteStatusBroken {
			// Note: notifyStatusChange requires core.Context, but we have context.Context here
			// Skip notification in janitor context - notifications will be handled in user-facing operations
			j.logger.Debug("Status changed to broken, skipping notification in janitor context",
				zap.Uint("website_id", website.ID),
				zap.String("domain", website.Domain))
		}
	}

	// Update last_checked_at timestamp
	now := time.Now()
	website.LastCheckedAt = &now

	// Save changes
	if err := j.db.WithContext(ctx).Save(website).Error; err != nil {
		return fmt.Errorf("failed to update website: %w", err)
	}

	return nil
}

// validateCIDTarget checks if a CID target is still valid and pinned
func (j *WebsiteJanitorJob) validateCIDTarget(ctx context.Context, targetHash string) (bool, error) {
	parsedCid, err := cid.Decode(targetHash)
	if err != nil {
		return false, fmt.Errorf("invalid CID: %w", err)
	}

	// Check if CID is pinned for any user
	// We need to scan through pins since GetPinByCIDAndUser requires a userID
	var pins []*pluginDb.IPFSPin
	err = j.db.WithContext(ctx).
		Where("cid = ?", parsedCid.Bytes()).
		Where("deleted_at IS NULL").
		Where("status = ?", pluginDb.PinningStatusPinned).
		Limit(1).
		Find(&pins).Error

	if err != nil {
		return false, fmt.Errorf("failed to query pins: %w", err)
	}

	if len(pins) == 0 {
		return false, nil
	}

	return true, nil
}

// validateIPNSTarget checks if an IPNS target is still valid and updates website status
func (j *WebsiteJanitorJob) validateIPNSTarget(ctx context.Context, website *pluginDb.Website) error {
	ctx, span := core.TraceMethod(ctx, "WebsiteJanitorJob.validateIPNSTarget")
	defer span.End()

	// Check if IPNS key exists in database by trying to get the private key
	privKey, userID, err := j.ipnsKeyService.GetPrivateKeyByPeerID(ctx, website.TargetHash())
	if err != nil {
		j.logger.Error("IPNS key not found in database",
			zap.Error(err),
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("peer_id", website.TargetHash()),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		now := time.Now()
		website.LastCheckedAt = &now
		return nil
	}

	// Verify user ownership
	if website.UserID != userID {
		j.logger.Error("IPNS key belongs to different user",
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.Uint("website_user_id", website.UserID),
			zap.Uint("key_user_id", userID),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		now := time.Now()
		website.LastCheckedAt = &now
		return nil
	}

	_ = privKey

	// Resolve IPNS record via IPNSPublisherService
	record, err := j.ipnsPublisherService.GetPublished(ctx, website.TargetHash(), true)
	if err != nil {
		j.logger.Error("Failed to resolve IPNS record",
			zap.Error(err),
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("peer_id", website.TargetHash()),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		now := time.Now()
		website.LastCheckedAt = &now
		return nil
	}

	// Extract CID from record
	recordCID, err := record.Value()
	if err != nil {
		j.logger.Error("Failed to extract CID from IPNS record",
			zap.Error(err),
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("peer_id", website.TargetHash()),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		now := time.Now()
		website.LastCheckedAt = &now
		return nil
	}

	// Validate CID is pinned
	cidValid, err := j.validateCIDTarget(ctx, recordCID.String())
	if err != nil {
		j.logger.Warn("Failed to validate resolved CID",
			zap.Error(err),
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("cid", recordCID.String()),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		now := time.Now()
		website.LastCheckedAt = &now
		return nil
	}

	if !cidValid {
		j.logger.Warn("Resolved CID is not pinned",
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("cid", recordCID.String()),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		now := time.Now()
		website.LastCheckedAt = &now
		return nil
	}

	// Check record validity timestamp
	validity, err := record.Validity()
	if err != nil {
		j.logger.Warn("Failed to get IPNS record validity",
			zap.Error(err),
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("peer_id", website.TargetHash()),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		now := time.Now()
		website.LastCheckedAt = &now
		return nil
	}

	if validity.Before(time.Now()) {
		j.logger.Warn("IPNS record has expired",
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("peer_id", website.TargetHash()),
			zap.Time("validity", validity),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		now := time.Now()
		website.LastCheckedAt = &now
		return nil
	}

	// All validations passed
	website.Status = string(pluginDb.WebsiteStatusActive)
	now := time.Now()
	website.LastCheckedAt = &now

	j.logger.Debug("IPNS target validated successfully",
		zap.Uint("website_id", website.ID),
		zap.String("domain", website.Domain),
		zap.String("peer_id", website.TargetHash()),
	)

	return nil
}

// initializeJob sets up the job dependencies
func (j *WebsiteJanitorJob) initializeJob(ctx core.Context) error {
	// Get configuration
	j.config = &config.WebsiteConfig{}
	// Use defaults since we can't access website-specific config via the API
	// The config will be loaded from the protocol config if needed later

	// Get pin service
	j.pinService = core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
	if j.pinService == nil {
		return fmt.Errorf("pin service not available")
	}

	// Get IPNS key service
	j.ipnsKeyService = core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
	if j.ipnsKeyService == nil {
		return fmt.Errorf("IPNS key service not available")
	}

	// Get IPNS publisher service
	j.ipnsPublisherService = core.GetService[pluginCore.IPNSPublisherService](ctx, pluginCore.IPNS_PUBLISHER_SERVICE)
	if j.ipnsPublisherService == nil {
		return fmt.Errorf("IPNS publisher service not available")
	}

	// Get database
	j.db = ctx.DB()
	if j.db == nil {
		return fmt.Errorf("database not available")
	}

	// Get logger
	j.logger = ctx.Logger()
	if j.logger == nil {
		return fmt.Errorf("logger not available")
	}

	return nil
}
