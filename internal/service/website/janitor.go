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
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	domsvc "go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
	"gorm.io/gorm"
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

// primaryDomainName resolves a website's primary domain name for logging.
// The Website record no longer stores a domain string; it points at a primary
// WebsiteDomain binding. Resolution is best-effort for log fields only, so a
// lookup failure yields an empty string (never an error).
func (j *WebsiteJanitorJob) primaryDomainName(ctx context.Context, website *pluginDb.Website) string {
	if website == nil || website.PrimaryDomainID == nil || j.db == nil {
		return ""
	}
	var wd pluginDb.WebsiteDomain
	if err := j.db.WithContext(ctx).Where("id = ?", *website.PrimaryDomainID).First(&wd).Error; err != nil {
		return ""
	}
	return wd.Domain
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
		Where("status != ?", string(pluginDb.WebsiteStatusPendingValidation)).
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
						zap.String("domain", j.primaryDomainName(eventCtx, website)))
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

	sm := NewWebsiteStateMachine(website)

	// Websites awaiting DNS validation must not be subjected to CID-only
	// liveness checks. They transition to active solely via ValidateDNS. Skip
	// them here (refreshing last_checked_at so they aren't re-picked every
	// minute) instead of risking an incorrect broken status. The FSM also
	// enforces this: revalidate_ok/cid_unpinned are not legal from
	// pending_validation.
	if website.Status == string(pluginDb.WebsiteStatusPendingValidation) {
		website.LastCheckedAt = new(time.Now())
		return j.db.WithContext(ctx).Save(website).Error
	}

	oldStatus := website.Status

	// Determine the health-driven outcome for the target. The FSM turns this
	// into a legal status transition (and is a no-op when the website is
	// already in the target state).
	var targetStatus pluginDb.WebsiteStatus
	switch website.TargetType {
	case string(pluginDb.WebsiteTargetTypeIPFS):
		valid, err := j.validateCIDTarget(ctx, website.TargetHash())
		if err != nil {
			j.logger.Warn("Failed to validate CID target",
				zap.Error(err),
				zap.String("target", website.TargetHash()))
			targetStatus = pluginDb.WebsiteStatusBroken
		} else if !valid {
			j.logger.Debug("CID target is not valid or not pinned",
				zap.String("target", website.TargetHash()))
			targetStatus = pluginDb.WebsiteStatusBroken
		} else {
			targetStatus = pluginDb.WebsiteStatusActive
		}

	case string(pluginDb.WebsiteTargetTypeIPNS):
		err := j.validateIPNSTarget(ctx, website)
		if err != nil {
			j.logger.Warn("Failed to validate IPNS target",
				zap.Error(err),
				zap.String("target", website.TargetHash()))
			targetStatus = pluginDb.WebsiteStatusBroken
		} else {
			targetStatus = pluginDb.WebsiteStatusActive
		}
		// validateIPNSTarget handles status and LastCheckedAt updates internally.
		// Save the changes immediately.
		if err := j.db.WithContext(ctx).Save(website).Error; err != nil {
			return fmt.Errorf("failed to update website: %w", err)
		}
		return nil

	default:
		j.logger.Warn("Unknown target type", zap.String("target_type", website.TargetType))
		targetStatus = pluginDb.WebsiteStatusBroken
	}

	// Grace period: a freshly created website may still be deploying (pinning
	// the CID / publishing the IPNS record). Don't declare it broken during the
	// grace period — just refresh last_checked_at so it is reconsidered on the
	// next run instead of being left in a wrong state.
	if targetStatus == pluginDb.WebsiteStatusBroken && j.withinGracePeriod(ctx, website) {
		j.logger.Debug("Website within creation grace period; deferring broken status",
			zap.Uint("website_id", website.ID),
			zap.Time("created_at", website.CreatedAt))
		website.LastCheckedAt = new(time.Now())
		return j.db.WithContext(ctx).Save(website).Error
	}

	// Fire the health-driven transition via the FSM (a no-op when the website
	// is already in the desired state):
	//   valid   → revalidate_ok (broken → active; already-active is a no-op
	//              guarded by Can)
	//   invalid → cid_unpinned  (active → broken; an already-broken website
	//              that fails again is a no-op, still refreshing the timestamp)
	var transitionErr error
	if targetStatus == pluginDb.WebsiteStatusActive {
		if sm.Can(EventWebsiteRevalidateOK) {
			transitionErr = sm.Fire(ctx, EventWebsiteRevalidateOK)
		}
	} else if sm.Can(EventWebsiteCIDUnpinned) {
		transitionErr = sm.Fire(ctx, EventWebsiteCIDUnpinned)
	}
	if transitionErr != nil {
		j.logger.Warn("Failed to apply website status transition",
			zap.Error(transitionErr),
			zap.Uint("website_id", website.ID),
			zap.String("event", "revalidate_ok/cid_unpinned"))
		return fmt.Errorf("failed to transition website status: %w", transitionErr)
	}

	// Log status changes
	if website.Status != oldStatus {
		j.logger.Info("Website status changed",
			zap.Uint("website_id", website.ID),
			zap.String("domain", j.primaryDomainName(ctx, website)),
			zap.String("old_status", oldStatus),
			zap.String("new_status", website.Status))
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

	sm := NewWebsiteStateMachine(website)
	peerID := website.TargetHash()

	privKey, userID, err := j.ipnsKeyService.GetPrivateKeyByPeerID(ctx, peerID)
	if err != nil {
		j.logger.Error("IPNS key not found in database",
			zap.Error(err),
			zap.Uint("website_id", website.ID),
			zap.String("domain", j.primaryDomainName(ctx, website)),
			zap.String("peer_id", peerID),
		)
		j.markBroken(ctx, sm, website)
		return nil
	}

	_ = privKey

	if website.UserID != userID {
		j.logger.Error("IPNS key belongs to different user",
			zap.Uint("website_id", website.ID),
			zap.String("domain", j.primaryDomainName(ctx, website)),
			zap.Uint("website_user_id", website.UserID),
			zap.Uint("key_user_id", userID),
		)
		j.markBroken(ctx, sm, website)
		return nil
	}

	var key pluginDb.IPFSIPNSKey
	if err := j.db.WithContext(ctx).Where("user_id = ? AND peer_id_multihash = ?", userID, []byte(website.TargetMultihash)).First(&key).Error; err != nil {
		j.logger.Error("Failed to look up IPNS key record",
			zap.Error(err),
			zap.Uint("website_id", website.ID),
			zap.String("domain", j.primaryDomainName(ctx, website)),
			zap.String("peer_id", peerID),
		)
		j.markBroken(ctx, sm, website)
		return nil
	}

	if key.LastPublishedCID == "" {
		j.logger.Warn("IPNS key has no published CID",
			zap.Uint("website_id", website.ID),
			zap.String("domain", j.primaryDomainName(ctx, website)),
			zap.String("peer_id", peerID),
		)
		j.markBroken(ctx, sm, website)
		return nil
	}

	cidValid, err := j.validateCIDTarget(ctx, key.LastPublishedCID)
	if err != nil {
		j.logger.Warn("Failed to validate last published CID",
			zap.Error(err),
			zap.Uint("website_id", website.ID),
			zap.String("domain", j.primaryDomainName(ctx, website)),
			zap.String("cid", key.LastPublishedCID),
		)
		j.markBroken(ctx, sm, website)
		return nil
	}

	if !cidValid {
		j.logger.Warn("Last published CID is not pinned",
			zap.Uint("website_id", website.ID),
			zap.String("domain", j.primaryDomainName(ctx, website)),
			zap.String("cid", key.LastPublishedCID),
		)
		j.markBroken(ctx, sm, website)
		return nil
	}

	j.markActive(ctx, sm, website)

	j.logger.Debug("IPNS target validated successfully",
		zap.Uint("website_id", website.ID),
		zap.String("domain", j.primaryDomainName(ctx, website)),
		zap.String("peer_id", peerID),
		zap.String("cid", key.LastPublishedCID),
	)

	return nil
}

// withinGracePeriod reports whether the website is still inside the configured
// creation grace period. It returns false when the grace period is disabled
// (<= 0) so that behavior is unchanged unless explicitly configured.
func (j *WebsiteJanitorJob) withinGracePeriod(_ context.Context, website *pluginDb.Website) bool {
	if j.config == nil || j.config.JanitorGracePeriod <= 0 || website == nil {
		return false
	}
	return time.Since(website.CreatedAt) < j.config.JanitorGracePeriod
}

// markBroken transitions the website to broken via the state machine when that
// transition is legal, and refreshes its last_checked_at. It is a no-op for a
// website already in broken state. If the website is still inside the creation
// grace period, the transition is skipped (only last_checked_at is refreshed)
// so freshly deployed sites aren't flagged as bad prematurely.
func (j *WebsiteJanitorJob) markBroken(ctx context.Context, sm *WebsiteStateMachine, website *pluginDb.Website) {
	if j.withinGracePeriod(ctx, website) {
		j.logger.Debug("Website within creation grace period; deferring broken status",
			zap.Uint("website_id", website.ID),
			zap.Time("created_at", website.CreatedAt))
		website.LastCheckedAt = new(time.Now())
		return
	}
	if sm.Can(EventWebsiteCIDUnpinned) {
		if err := sm.Fire(ctx, EventWebsiteCIDUnpinned); err != nil {
			j.logger.Warn("failed to mark website broken",
				zap.Error(err),
				zap.Uint("website_id", website.ID))
		}
	}
	website.LastCheckedAt = new(time.Now())
}

// markActive transitions the website to active via the state machine when that
// transition is legal, and refreshes its last_checked_at.
func (j *WebsiteJanitorJob) markActive(ctx context.Context, sm *WebsiteStateMachine, website *pluginDb.Website) {
	if sm.Can(EventWebsiteRevalidateOK) {
		if err := sm.Fire(ctx, EventWebsiteRevalidateOK); err != nil {
			j.logger.Warn("failed to mark website active",
				zap.Error(err),
				zap.Uint("website_id", website.ID))
		}
	}
	website.LastCheckedAt = new(time.Now())
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
