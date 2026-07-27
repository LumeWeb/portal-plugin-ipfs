package website

import (
	"context"
	"fmt"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
	"gorm.io/gorm"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	domsvc "go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
)

const (
	JanitorJobSourceID = "ipfs"
JanitorJobType     = "plugin.ipfs.website_janitor"
)

// WebsiteJanitorJob implements core.CronJob for periodic website validation
type WebsiteJanitorJob struct {
	*core.BaseCronJob
	config             *config.WebsiteConfig
	pinService         pluginCore.IPFSPinService
	ipnsKeyService     pluginCore.IPNSKeyService
	dnsService         pluginCore.DNSService
	delegatedDomainSvc delegatedDomainService
	db                 *gorm.DB
	logger             *core.Logger
}

// NewWebsiteJanitorJob creates a new WebsiteJanitorJob instance
func NewWebsiteJanitorJob() core.CronJob {
	job := &WebsiteJanitorJob{}

	// Initialize BaseCronJob with default values
	jobID := uuid.New()
	scheduleDef := core.NewCronScheduleDefinition(core.CronScheduleTypeCron).
		WithCronExpression("* * * * *") // Every minute

	job.BaseCronJob = core.NewBaseCronJob(
		jobID,
		core.JobOriginPlugin,
		JanitorJobSourceID,
		"IPFS Website Janitor",
		scheduleDef,
		nil,
		core.WithExplicitJobType(JanitorJobType),
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
		zap.Duration("check_interval", j.config.CheckInterval),
		zap.Int("workers", j.config.JanitorWorkerCount),
		zap.Int("batch_size", j.config.JanitorBatchSize))

	// Query websites that need validation
	var websites []*pluginDb.Website
	err := j.db.WithContext(eventCtx).
		Where("deleted_at IS NULL").
		Where("last_checked_at IS NULL OR last_checked_at < ?", time.Now().Add(-j.config.CheckInterval)).
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

	// Validate DNS zones if DNS service is available
	if j.dnsService != nil {
		if err := j.validateDNSZones(eventCtx); err != nil {
			j.logger.Warn("Failed to validate DNS zones",
				zap.Error(err))
		}
	}

	// Verify pending domain delegations (alt-root NS/TLSA verification)
	if j.delegatedDomainSvc != nil {
		if err := j.verifyPendingDelegations(eventCtx); err != nil {
			j.logger.Warn("Failed to verify pending delegations",
				zap.Error(err))
		}
	}

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
	website.LastCheckedAt = new(time.Now())

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

	// Normalize to match database storage format (pins are stored as normalized CID v1)
	normalizedCid := encoding.NormalizeCid(parsedCid)

	// Check if CID is pinned for any user
	// We need to scan through pins since GetPinByCIDAndUser requires a userID
	var pins []*pluginDb.IPFSPin
	err = j.db.WithContext(ctx).
		Where("cid = ?", normalizedCid.Bytes()).
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

	peerID := website.TargetHash()

	privKey, userID, err := j.ipnsKeyService.GetPrivateKeyByPeerID(ctx, peerID)
	if err != nil {
		j.logger.Error("IPNS key not found in database",
			zap.Error(err),
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("peer_id", peerID),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		website.LastCheckedAt = new(time.Now())
		return nil
	}

	_ = privKey

	if website.UserID != userID {
		j.logger.Error("IPNS key belongs to different user",
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.Uint("website_user_id", website.UserID),
			zap.Uint("key_user_id", userID),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		website.LastCheckedAt = new(time.Now())
		return nil
	}

	var key pluginDb.IPFSIPNSKey
	if err := j.db.WithContext(ctx).Where("user_id = ? AND peer_id_multihash = ?", userID, []byte(website.TargetMultihash)).First(&key).Error; err != nil {
		j.logger.Error("Failed to look up IPNS key record",
			zap.Error(err),
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("peer_id", peerID),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		website.LastCheckedAt = new(time.Now())
		return nil
	}

	if key.LastPublishedCID == "" {
		j.logger.Warn("IPNS key has no published CID",
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("peer_id", peerID),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		website.LastCheckedAt = new(time.Now())
		return nil
	}

	cidValid, err := j.validateCIDTarget(ctx, key.LastPublishedCID)
	if err != nil {
		j.logger.Warn("Failed to validate last published CID",
			zap.Error(err),
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("cid", key.LastPublishedCID),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		website.LastCheckedAt = new(time.Now())
		return nil
	}

	if !cidValid {
		j.logger.Warn("Last published CID is not pinned",
			zap.Uint("website_id", website.ID),
			zap.String("domain", website.Domain),
			zap.String("cid", key.LastPublishedCID),
		)
		website.Status = string(pluginDb.WebsiteStatusBroken)
		website.LastCheckedAt = new(time.Now())
		return nil
	}

	website.Status = string(pluginDb.WebsiteStatusActive)
	website.LastCheckedAt = new(time.Now())

	j.logger.Debug("IPNS target validated successfully",
		zap.Uint("website_id", website.ID),
		zap.String("domain", website.Domain),
		zap.String("peer_id", peerID),
		zap.String("cid", key.LastPublishedCID),
	)

	return nil
}

// validateDNSZones validates DNS zones that are pending nameserver verification
func (j *WebsiteJanitorJob) validateDNSZones(ctx context.Context) error {
	ctx, span := core.TraceMethod(ctx, "WebsiteJanitorJob.validateDNSZones")
	defer span.End()

	if j.dnsService == nil {
		return nil
	}

	// Query DNS zones that are pending nameserver validation
	var zones []*pluginDb.DNSZone
	err := j.db.WithContext(ctx).
		Where("status = ?", pluginDb.DNSZoneStatusPendingNameserver).
		Where("last_nameserver_check_at IS NULL OR last_nameserver_check_at < ?", time.Now().Add(-5*time.Minute)).
		Find(&zones).Error

	if err != nil {
		return fmt.Errorf("failed to query DNS zones: %w", err)
	}

	if len(zones) == 0 {
		j.logger.Debug("No DNS zones need validation")
		return nil
	}

	j.logger.Info("Validating DNS zones", zap.Int("count", len(zones)))

	for _, zone := range zones {
		// Validate nameservers
		validated, err := j.dnsService.ValidateNameservers(ctx, zone.ID)

		// Always update the timestamp to avoid re-checking immediately

		if err != nil {
			j.logger.Warn("Failed to validate DNS zone nameservers",
				zap.Error(err),
				zap.Uint("zone_id", zone.ID),
				zap.String("domain", zone.Domain))
			// Still save the timestamp to prevent a fast retry loop
			zone.LastNameserverCheckAt = new(time.Now())
			if err := j.db.WithContext(ctx).Model(&zone).Select("LastNameserverCheckAt").Updates(&zone).Error; err != nil {
				j.logger.Error("Failed to update zone timestamp", zap.Error(err), zap.Uint("zone_id", zone.ID))
			}
			continue
		}

		zone.LastNameserverCheckAt = new(time.Now())
		updateCols := []string{"LastNameserverCheckAt"}

		if validated {
			j.logger.Info("DNS zone nameservers validated",
				zap.Uint("zone_id", zone.ID),
				zap.String("domain", zone.Domain))
			zone.Status = string(pluginDb.DNSZoneStatusActive)
			updateCols = append(updateCols, "Status")
		}

		if err := j.db.WithContext(ctx).Model(&zone).Select(updateCols).Updates(&zone).Error; err != nil {
			j.logger.Error("Failed to update DNS zone", zap.Error(err), zap.Uint("zone_id", zone.ID))
		}
	}

	return nil
}

// verifyPendingDelegations checks if pending website_domains have their
// delegation NS records published and updates their status.
func (j *WebsiteJanitorJob) verifyPendingDelegations(ctx context.Context) error {
	ctx, span := core.TraceMethod(ctx, "WebsiteJanitorJob.verifyPendingDelegations")
	defer span.End()

	pendingStatuses := []pluginDb.DomainStatus{
		pluginDb.DomainStatusWaitingDelegation,
		pluginDb.DomainStatusRecordsGenerated,
	}

	const batchSize = 100

	for _, status := range pendingStatuses {
		var lastID int
		for {
			wds, err := j.delegatedDomainSvc.GetPendingWebsiteDomainsPaginated(ctx, status, batchSize, lastID)
			if err != nil {
				j.logger.Error("failed to fetch pending domain delegations",
					zap.String("status", string(status)),
					zap.Error(err))
				break
			}
			if len(wds) == 0 {
				break
			}

			for i := range wds {
				wd := &wds[i]
				verifyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				_, err := j.delegatedDomainSvc.VerifyDomain(verifyCtx, wd)
				cancel()
				if err != nil {
					j.logger.Warn("delegation verification failed",
						zap.String("domain", wd.Domain),
						zap.String("namespace", string(wd.Namespace)),
						zap.Error(err))
					continue
				}

				j.logger.Info("delegation verified",
					zap.String("domain", wd.Domain),
					zap.String("namespace", string(wd.Namespace)),
					zap.String("status", string(wd.Status)))
			}

			lastID = int(wds[len(wds)-1].ID)
			if len(wds) < batchSize {
				break
			}
		}
	}

	return nil
}

// initializeJob sets up the job dependencies
func (j *WebsiteJanitorJob) initializeJob(ctx core.Context) error {
	// Get configuration from service config
	j.config = core.GetServiceConfig[*config.WebsiteConfig](ctx, pluginCore.WEBSITE_SERVICE)

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

	// Get DNS service (optional - for DNS hosting validation)
	j.dnsService = core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
	if j.dnsService == nil {
		j.logger.Debug("DNS service not available, skipping DNS validation")
	}

	// Get delegated domain service (optional - for alt-root delegation verification)
	if dds := core.GetServiceOptional[*domsvc.DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE); dds != nil {
		j.delegatedDomainSvc = dds
	} else if j.logger != nil {
		j.logger.Debug("Delegated domain service not available, skipping delegation verification")
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
