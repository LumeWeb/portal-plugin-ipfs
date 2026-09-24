// Package workspace provides the workspace provisioning service.
//
// This package owns the workspace service configuration contract
// (WorkspaceConfig), startup validation, and the provider-neutral wiring for
// the Coolify provider. Workspace CRUD, reconciliation, and lifecycle handling
// live in this package too; the constructor here registers the config and runs
// startup validation.
package workspace

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	dashboardCore "go.lumeweb.com/portal-plugin-dashboard/core"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/coolify"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/mysqlprovision"
	domsvc "go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Sentinel errors for the workspace service's local (create/get/list) surface.
var (
	// ErrWorkspaceNotEnabled is returned when the workspace service is disabled
	// in configuration.
	ErrWorkspaceNotEnabled = errors.New("workspace: workspace service is not enabled")
	// ErrWorkspaceProviderUnavailable is returned when the provider boundary is
	// not wired (service not started) so a lifecycle operation cannot reach the
	// Coolify backend.
	ErrWorkspaceProviderUnavailable = errors.New("workspace: provider not available")
	// ErrWorkspaceNotFound is returned when a workspace cannot be found for the
	// requesting user. Ownership and existence are deliberately conflated so
	// callers cannot enumerate other users' rows.
	ErrWorkspaceNotFound = errors.New("workspace: workspace not found")
	// ErrWorkspaceAlreadyExists is returned when an attached website already has
	// a live workspace (one workspace per attached website). The strict
	// UNIQUE(website_id) key is the DB-enforced backstop; this error is surfaced
	// for the common, non-racing case and when a concurrent create/attach loses
	// the website_id race.
	ErrWorkspaceAlreadyExists = errors.New("workspace: website already has a workspace")
	// ErrWorkspaceAlreadyAttached is returned when an attach operation targets a
	// workspace that is already attached to a website, or would attach a website
	// that already has a live workspace.
	ErrWorkspaceAlreadyAttached = errors.New("workspace: workspace is already attached to a website")
	// ErrWorkspacePlatformDomainUnavailable is returned when no enabled
	// platform domain is registered (or the look-up service is not wired).
	ErrWorkspacePlatformDomainUnavailable = errors.New("workspace: no enabled platform domain is available")
	// ErrWorkspacePlatformDomainAmbiguous is returned when more than one
	// enabled platform domain is registered: the workspace authoring hostname
	// root is ambiguous and must be resolved by enabling exactly one.
	ErrWorkspacePlatformDomainAmbiguous = errors.New("workspace: multiple enabled platform domains; enable exactly one for the workspace hostname root")
	// ErrWorkspaceLabelExhausted is returned when the label generator could not
	// produce a label that satisfies the (platform_domain_id, label) unique key
	// within the retry budget.
	ErrWorkspaceLabelExhausted = errors.New("workspace: failed to generate a unique label")
)

// maxLabelAttempts bounds label-generation retries on duplicate-key collision.
// Collisions are rare (opaque 8-char suffix), so a small budget is sufficient.
const maxLabelAttempts = 100

// WorkspaceService is the workspace provisioning service. It wires the
// provider-neutral Coolify adapter and the portal services it needs during
// startup, and implements the create/get/list surface on top of them.
type WorkspaceService struct {
	*core.BaseComponent

	config *pluginConfig.WorkspaceConfig
	// portalAPIURL is the portal API base URL injected into every runtime as
	// PORTAL_API_URL. It is derived once at startup from the portal core config
	// (this plugin's API subdomain on the core domain/secure/external-port), so
	// it is never separately configurable.
	portalAPIURL string
	// provider is the provider-neutral Coolify adapter, built during startup
	// when the service is enabled. It stays nil while disabled.
	provider coolify.WorkspaceProvider

	// websiteSvc loads the owning website during Create and enforces ownership.
	websiteSvc pluginCore.WebsiteService
	// platformSvc lists the enabled PlatformDomain rows so Create can require
	// exactly one for the workspace hostname root. It is a narrow interface so
	// tests can inject a fake; in production it is the concrete
	// *domsvc.DelegatedDomainService.
	platformSvc platformDomainResolver
	// apiKeySvc is the dashboard plugin's exported core.APIKeyService, used to
	// issue/reissue the workspace portal API key. It is wired from the portal
	// context at startup when the workspace service is enabled. IPFS imports
	// only dashboard's exported core package, never dashboard internals.
	apiKeySvc dashboardCore.APIKeyService
	// dnsSvc publishes the per-workspace authoring-hostname records into the
	// platform root's PowerDNS zone (ReconcileDNS). It is wired from the
	// portal context at startup when the workspace service is enabled.
	dnsSvc pluginCore.DNSService
	// identityKey is the portal identity private key (Core.Identity.PrivateKey),
	// the single source of secrecy for deriving each workspace's logical
	// database password. It is wired at startup when the service is enabled
	// (never persisted/logged) and is empty when the service is disabled or the
	// identity is unavailable.
	identityKey ed25519.PrivateKey
	// mysqlProv is an optional injected mysqlprovision.Engineer. When set
	// (tests) it is used for logical database/user provisioning on the shared
	// MySQL/MariaDB resource; when nil a real connection is opened per call
	// using the shared resource's root credentials fetched from Coolify. It is
	// never the shared resource itself and never creates/deletes it.
	mysqlProv mysqlprovision.Engineer
	// slugGen produces a DNS-safe opaque workspace label. It is a field so
	// tests can pin/override generation; defaults to generateOpaqueLabel.
	slugGen func() (string, error)

	// lockMu guards locks. Reconcile and every mutating lifecycle operation
	// (create is serialized by its unique keys, attach by the UNIQUE
	// (website_id) backstop) use an in-process keyed lock per workspace so a
	// single portal instance never runs two of them for the same workspace
	// concurrently. The lock must be held across "load workspace state →
	// validate transition → provider side effects → row updates": because
	// every participant loads its row BEFORE it can know it needs the lock,
	// the state must be re-read under the lock (see reloadWorkspaceLocked).
	// The current deployment is single-instance; a DB claim/lease would be
	// needed before running multiple replicas.
	lockMu sync.Mutex
	locks  map[uint]*sync.Mutex
}

// Ensure WorkspaceService satisfies the core service contract and the
// WorkspaceService interface.
var _ core.Service = (*WorkspaceService)(nil)
var _ pluginCore.WorkspaceService = (*WorkspaceService)(nil)

// platformDomainResolver is the subset of *domsvc.DelegatedDomainService used
// by the workspace service to list enabled platform domains so it can require
// exactly one for the workspace hostname root.
type platformDomainResolver interface {
	ListEnabledPlatformDomains(ctx context.Context, pagination queryutil.Pagination) ([]*pluginDb.PlatformDomain, int64, error)
}

// NewWorkspaceService creates the workspace service. Startup loads the service
// config, wires the portal services it depends on, and validates it; when
// enabled, it verifies basic reachability of the configured Coolify endpoint.
func NewWorkspaceService() (core.Service, []core.ContextBuilderOption, error) {
	svc := &WorkspaceService{
		slugGen: generateOpaqueLabel,
	}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			svc.websiteSvc = core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
			if dds := core.GetServiceOptional[*domsvc.DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE); dds != nil {
				svc.platformSvc = dds
			}
			svc.config = core.GetServiceConfig[*pluginConfig.WorkspaceConfig](ctx, pluginCore.WORKSPACE_SERVICE)
			if svc.config == nil {
				svc.config = &pluginConfig.WorkspaceConfig{}
			}
			// Wire the dashboard API-key issuance boundary from the portal
			// context. It is required only when the workspace service is
			// enabled (startupValidate guards on Enabled), keeping the disabled
			// path free of cross-plugin coupling.
			svc.apiKeySvc = core.GetServiceOptional[dashboardCore.APIKeyService](ctx, dashboardCore.API_KEY_SERVICE)
			// Wire the DNS service so provisioning can publish the workspace
			// authoring-hostname record into the platform root's zone.
			// Required only when the workspace service is enabled
			// (startupValidate guards on Enabled).
			svc.dnsSvc = core.GetServiceOptional[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
			return svc.startupValidate(ctx)
		}),
	)

	return svc, opts, nil
}

func (s *WorkspaceService) ID() string {
	return pluginCore.WORKSPACE_SERVICE
}

// GetConfig registers the workspace service config with the portal config
// manager so operators can set it per plugin/service.
func (s *WorkspaceService) GetConfig() (any, error) {
	return &pluginConfig.WorkspaceConfig{}, nil
}

// WorkspaceConfig returns the loaded workspace service config, used by
// reconciliation to schedule and grant retries. Nil until startup completes
// and only non-nil when the service is enabled.
func (s *WorkspaceService) WorkspaceConfig() *pluginConfig.WorkspaceConfig {
	return s.config
}

// Provider returns the provider-neutral Coolify adapter. It is nil while the
// service is disabled and is built at startup when enabled.
func (s *WorkspaceService) Provider() coolify.WorkspaceProvider {
	return s.provider
}

// startupValidate performs startup validation: structural config validation
// plus, when enabled, a reachability check against the Coolify endpoint.
func (s *WorkspaceService) startupValidate(ctx core.Context) error {
	if s.config == nil {
		return fmt.Errorf("workspace: config not registered")
	}

	// Structural validation of the workspace service's own fields. Live platform
	// domain resolution and the sensitive-database reachability check are left
	// to the reconciliation path that owns those dependencies.
	if err := s.config.Validate(); err != nil {
		return err
	}

	if !s.config.Enabled {
		return nil
	}

	// Reach each nested config struct's OWN validator independently. The parent
	// WorkspaceConfig deliberately does not orchestrate child validation, so the
	// service assembles the enabled-path validation across the independently
	// owning config structs (Coolify placement/auth, runtime image/tag/port and
	// storage/env leaves, and the shared database resource).
	if err := s.config.Coolify.Validate(); err != nil {
		return err
	}
	if err := s.config.Runtime.Validate(); err != nil {
		return err
	}
	if err := s.config.Database.Validate(); err != nil {
		return err
	}

	// The dashboard API-key issuance boundary is a hard dependency of
	// provisioning and must be present when the workspace service is enabled.
	if s.apiKeySvc == nil {
		return fmt.Errorf("workspace: api key service not available")
	}

	// The DNS service is a hard dependency of provisioning too: every
	// workspace's authoring hostname gets an authoritative record in the
	// platform root's zone, so without it the hostnames would never resolve.
	if s.dnsSvc == nil {
		return fmt.Errorf("workspace: dns service not available")
	}

	// The portal identity key is the single source of secrecy for deriving each
	// workspace's logical database password. It must be present when the
	// service is enabled; it is wired here (never persisted/logged).
	if ctx == nil {
		return errors.New("workspace: portal context is unavailable")
	}
	if len(ctx.Config().Config().Core.Identity.PrivateKey()) == 0 {
		return errors.New("workspace: portal identity private key is not available")
	}
	s.identityKey = ctx.Config().Config().Core.Identity.PrivateKey()

	// Derive the portal API URL from the portal core config once, so the
	// runtime's PORTAL_API_URL always matches portal HTTP routing with no
	// duplicate configuration to drift. The root core domain is used — no
	// API subdomain is prefixed — because the workspace runtime's
	// key-exchange and resolve routes are served on the portal root host.
	s.portalAPIURL = derivePortalAPIURL(ctx.Config().Config().Core)
	// Fail fast when the derivation produced no host (an empty/unconfigured
	// core domain), so an empty PORTAL_API_URL can never be injected into a
	// runtime. This must be validated here at startup, before any provisioning,
	// rather than silently proxying an empty URL to every deployed workspace.
	if err := validatePortalAPIURL(s.portalAPIURL); err != nil {
		return err
	}

	client, err := coolify.NewClient(s.config.Coolify.APIURL, s.config.Coolify.APIToken)
	if err != nil {
		return err
	}
	s.provider = coolify.NewProvider(client)

	// RequestTimeout is owned by the config-manager defaults (see
	// WorkspaceConfig.Defaults) so it is always set; there is no service-side
	// fallback to duplicate that default.
	ctxTimeout, cancel := context.WithTimeout(ctx, s.config.RequestTimeout)
	defer cancel()
	if err := client.Health(ctxTimeout); err != nil {
		return fmt.Errorf("workspace: coolify health check failed: %w", err)
	}

	return nil
}

// Create records a new workspace owned by userID using only local work. A
// workspace may be created without any website (websiteID == nil): building in
// a workspace is separate from publishing. See pluginCore.WorkspaceService.Create
// for the step order.
func (s *WorkspaceService) Create(ctx context.Context, userID uint, websiteID *uint) (*pluginDb.Workspace, error) {
	if s.config == nil || !s.config.Enabled {
		return nil, ErrWorkspaceNotEnabled
	}

	// 1. Resolve the configured enabled PlatformDomain for the authoring
	// hostname. Independent of any website/published domain.
	pd, err := s.resolveEnabledPlatformDomain(ctx)
	if err != nil {
		return nil, err
	}
	if pd == nil {
		return nil, ErrWorkspacePlatformDomainUnavailable
	}

	// 2. When attaching at creation, load the website. WebsiteService.GetWebsite
	// filters by user_id, so a website the user does not own resolves to
	// (nil, nil).
	var websiteIDVal *uint
	if websiteID != nil {
		if s.websiteSvc == nil {
			return nil, fmt.Errorf("workspace: website service not available")
		}
		website, err := s.websiteSvc.GetWebsite(ctx, userID, *websiteID)
		if err != nil {
			return nil, fmt.Errorf("workspace: failed to load website: %w", err)
		}
		if website == nil {
			return nil, ErrWorkspaceNotFound
		}

		// 3. Enforce one live workspace per attached website. The strict
		// UNIQUE(website_id) key is the atomic backstop (handled below on the
		// racing duplicate); this pre-check returns a clean error for the
		// common, non-racing case.
		existing, err := s.getByWebsite(ctx, website.ID)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			return nil, ErrWorkspaceAlreadyExists
		}
		websiteIDVal = &website.ID
	}

	// 4. Purge any prior soft-deleted WORKSPACE tombstone that still occupies
	// the strict UNIQUE(website_id) key for the website being attached, so a
	// deleted workspace can be re-created for the same website. Delete only
	// soft-deletes (leaving a tombstone); that tombstone is created only AFTER
	// all provider cleanup succeeds, so purging it here is data-loss safe. Only
	// tombstones (deleted_at IS NOT NULL) are removed; a live same-website row
	// would have already been caught by the getByWebsite check above and left to
	// the unique key to reject — this mirrors the tombstone-purge-before-insert
	// contract used by CreatePlatformDomain/CreateDomain.
	if websiteIDVal != nil {
		if err := s.purgeWorkspaceTombstone(ctx, *websiteIDVal); err != nil {
			return nil, err
		}
	}

	// 5. Generate a DNS-safe opaque label and insert a `provisioning` row.
	// The (platform_domain_id, label) unique key is the ground truth for label
	// collisions: a concurrent create that wins the key surfaces as a
	// duplicate-key error and is retried with a fresh label instead of a 500. A
	// duplicate on website_id (a concurrent create/attach for the same website)
	// is reported as ErrWorkspaceAlreadyExists rather than retried as a label
	// collision.
	for attempt := 0; attempt < maxLabelAttempts; attempt++ {
		label, err := s.newLabel()
		if err != nil {
			return nil, err
		}
		ws := &pluginDb.Workspace{
			UserID:           userID,
			WebsiteID:        websiteIDVal,
			PlatformDomainID: pd.ID,
			Label:            label,
			Status:           pluginDb.WorkspaceStatusProvisioning,
		}
		created, err := s.insert(ctx, ws)
		if err == nil {
			// Populate the workspace's PlatformDomain (the authoring-domain root)
			// from the domain already resolved above, so the returned model and
			// its DTO include the hostname without a second query.
			created.PlatformDomain = *pd
			return created, nil
		}
		if isDuplicateWorkspaceError(err) {
			return nil, ErrWorkspaceAlreadyExists
		}
		if !isDuplicateKeyError(err) {
			return nil, err
		}
	}

	return nil, ErrWorkspaceLabelExhausted
}

// Attach links an existing workspace (owned by userID) to a website (owned by
// userID). It prevents duplicate attachment: a website can be attached to at
// most one live workspace, and a workspace already attached to a website
// cannot be re-attached to another. It records only the publish link; it never
// changes the workspace's authoring hostname or platform domain.
func (s *WorkspaceService) Attach(ctx context.Context, userID uint, workspaceID uint, websiteID uint) (*pluginDb.Workspace, error) {
	if s.config == nil || !s.config.Enabled {
		return nil, ErrWorkspaceNotEnabled
	}
	if s.websiteSvc == nil {
		return nil, fmt.Errorf("workspace: website service not available")
	}

	// 1. Load the workspace scoped to the requesting user (Workspace.UserID).
	ws, err := s.ownedWorkspace(ctx, userID, workspaceID)
	if err != nil {
		return nil, err
	}
	if ws == nil {
		return nil, ErrWorkspaceNotFound
	}
	// 2. A workspace already attached to a website cannot be re-attached.
	if ws.WebsiteID != nil {
		return nil, ErrWorkspaceAlreadyAttached
	}

	// 3. Load the website and verify the user owns it.
	website, err := s.websiteSvc.GetWebsite(ctx, userID, websiteID)
	if err != nil {
		return nil, fmt.Errorf("workspace: failed to load website: %w", err)
	}
	if website == nil {
		return nil, ErrWorkspaceNotFound
	}

	// 4. Prevent duplicate attachment: the target website must not already have
	// a live workspace.
	existing, err := s.getByWebsite(ctx, website.ID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, ErrWorkspaceAlreadyExists
	}

	// 5. Persist the publish link and purge any prior soft-deleted WORKSPACE
	// tombstone atomically in a single transaction. The tombstone-purging and
	// the website_id update must commit together: a concurrent Create/Attach
	// could otherwise claim the strict UNIQUE(website_id) key in the window
	// between two separate transactions. The tombstone is created only AFTER all
	// provider cleanup succeeds in Delete, so purging it here is data-loss safe;
	// only tombstones (deleted_at IS NOT NULL) are removed and a live
	// same-website row is left to the unique key to reject (mirrors Create's
	// purge-before-insert contract).
	err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		// 5a. Lock the workspace row and re-verify ownership and the
		// unattached state under the same transaction that mutates it, so no
		// concurrent attach can observe/race a stale website_id. The row-lock
		// serializes attaches to the same workspace; a website that is already
		// attached here returns the same error the pre-check above would.
		var locked pluginDb.Workspace
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND user_id = ?", workspaceID, userID).
			First(&locked).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				_ = tx.AddError(ErrWorkspaceNotFound)
			} else {
				_ = tx.AddError(fmt.Errorf("workspace: failed to lock workspace: %w", err))
			}
			return tx
		}
		if locked.WebsiteID != nil {
			_ = tx.AddError(ErrWorkspaceAlreadyAttached)
			return tx
		}

		// 5b. Purge any prior soft-deleted workspace tombstone still holding the
		// strict UNIQUE(website_id) key for the target website, so an
		// attach-after-delete can re-link the same website.
		if err := tx.
			Where("website_id = ? AND deleted_at IS NOT NULL", website.ID).
			Unscoped().Delete(&pluginDb.Workspace{}).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("workspace: failed to purge stale workspace tombstone: %w", err))
			return tx
		}

		// 5c. Persist the publish link and verify a row was actually updated.
		// The predicate re-checks website_id IS NULL, so if a concurrent attach
		// slipped in, RowsAffected is zero and we must fail instead of silently
		// claiming the key.
		upd := tx.Model(&pluginDb.Workspace{}).
			Where("id = ? AND website_id IS NULL", workspaceID).
			Update("website_id", website.ID)
		if upd.Error != nil {
			if isDuplicateWorkspaceError(upd.Error) {
				_ = tx.AddError(ErrWorkspaceAlreadyExists)
			} else {
				_ = tx.AddError(fmt.Errorf("workspace: failed to attach website: %w", upd.Error))
			}
			return tx
		}
		if upd.RowsAffected == 0 {
			// The workspace row vanished or was attached concurrently under the
			// lock; do not claim the key without a matching live row.
			_ = tx.AddError(ErrWorkspaceNotFound)
			return tx
		}
		return tx
	})
	if err != nil {
		return nil, err
	}
	ws.WebsiteID = &website.ID
	return ws, nil
}

// Get returns the workspace identified by workspaceID if and only if it is
// owned by userID (Workspace.UserID). It returns (nil, nil) when the row is
// missing or not owned (no existence leak).
func (s *WorkspaceService) Get(ctx context.Context, userID uint, workspaceID uint) (*pluginDb.Workspace, error) {
	var ws pluginDb.Workspace
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		// Eagerly preload the PlatformDomain so the returned model (and its
		// DTO) carries the authoring hostname without a follow-up query.
		if err := s.ownedWorkspaceQuery(tx, userID).
			Where("workspaces.id = ?", workspaceID).
			Preload("PlatformDomain").
			First(&ws).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("workspace: failed to get workspace: %w", err)
	}
	return &ws, nil
}

// List returns the workspaces owned by userID, filtered, sorted, and
// paginated, along with the total count.
func (s *WorkspaceService) List(ctx context.Context, userID uint, filters []queryutil.CrudFilter, sort []filter.Sort, pagination queryutil.Pagination) ([]*pluginDb.Workspace, int64, error) {
	var workspaces []*pluginDb.Workspace
	var total int64

	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		// Eagerly preload the PlatformDomain so each listed workspace (and its
		// DTO) carries the authoring hostname without a follow-up query.
		query := s.ownedWorkspaceQuery(tx, userID).Preload("PlatformDomain")
		query = queryutil.ApplyFilters(query, filters, nil)
		query = queryutil.ApplySort(query, sort)
		query = queryutil.ApplyPagination(query, pagination)

		if err := query.Count(&total).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("workspace: failed to count workspaces: %w", err))
			return tx
		}
		if err := query.Find(&workspaces).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("workspace: failed to list workspaces: %w", err))
			return tx
		}
		return tx
	})
	if err != nil {
		return nil, 0, err
	}
	return workspaces, total, nil
}

// ResolveRuntime resolves the live workspace that owns the given Coolify
// application resource UUID (the COOLIFY_RESOURCE_UUID Coolify injects into the
// container) when it is owned by userID (the authenticated workspace API-key
// owner). The runtime sends its COOLIFY_RESOURCE_UUID plus its PORTAL_API_KEY;
// the API-key owner (userID) and the application resource ID MUST both match,
// so a mismatched UUID/key pair resolves to (nil, nil, nil) and never leaks
// existence or ownership. It eagerly loads the workspace's platform domain
// (for the authoring hostname) and, when the workspace is attached, the
// optional owning Website (the publish relationship), which is owned by the
// same user. No workspace numeric ID is required from runtime input.
func (s *WorkspaceService) ResolveRuntime(ctx context.Context, userID uint, appResourceID string) (*pluginDb.Workspace, *pluginDb.Website, error) {
	if s.config == nil || !s.config.Enabled {
		return nil, nil, ErrWorkspaceNotEnabled
	}
	if appResourceID == "" {
		return nil, nil, errors.New("workspace: application resource id is required to resolve a runtime")
	}

	var ws pluginDb.Workspace
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := s.ownedWorkspaceQuery(tx, userID).
			Where("workspaces.application_resource_id = ?", appResourceID).
			Preload("PlatformDomain").
			First(&ws).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("workspace: failed to resolve runtime: %w", err)
	}

	// Resolve the optional Website (publish relationship) owned by the same
	// user. GetWebsite filters by user_id, so this is always the owner's site.
	var website *pluginDb.Website
	if ws.WebsiteID != nil {
		if s.websiteSvc == nil {
			return nil, nil, fmt.Errorf("workspace: website service not available")
		}
		w, err := s.websiteSvc.GetWebsite(ctx, userID, *ws.WebsiteID)
		if err != nil {
			return nil, nil, fmt.Errorf("workspace: failed to load attached website: %w", err)
		}
		website = w
	}
	return &ws, website, nil
}

// ownedWorkspaceQuery returns a query scoped to the workspaces owned by userID.
// Ownership is stored on Workspace (UserID), so unattached workspaces are
// included and visible to their owner without any Website join.
func (s *WorkspaceService) ownedWorkspaceQuery(tx *gorm.DB, userID uint) *gorm.DB {
	return tx.Model(&pluginDb.Workspace{}).Where("workspaces.user_id = ?", userID)
}

// resolveEnabledPlatformDomain returns the workspaces' authoring hostname root
// from the enabled PlatformDomain rows. The hostname root must be unambiguous,
// so exactly one enabled platform domain is required: no enabled root yields
// (nil, nil) → ErrWorkspacePlatformDomainUnavailable, and more than one yields
// ErrWorkspacePlatformDomainAmbiguous. The namespace comes from the DB row's
// typed value, never from config.
func (s *WorkspaceService) resolveEnabledPlatformDomain(ctx context.Context) (*pluginDb.PlatformDomain, error) {
	if s.config == nil {
		return nil, ErrWorkspaceNotEnabled
	}
	if s.platformSvc == nil {
		return nil, fmt.Errorf("workspace: platform domain service not available")
	}
	roots, _, err := s.platformSvc.ListEnabledPlatformDomains(ctx, queryutil.Pagination{})
	if err != nil {
		return nil, err
	}
	if len(roots) == 0 {
		return nil, nil
	}
	if len(roots) > 1 {
		return nil, ErrWorkspacePlatformDomainAmbiguous
	}
	return roots[0], nil
}

// derivePortalAPIURL builds the portal API base URL from the portal core
// config's trimmed root domain (no API subdomain is prefixed), the scheme
// follows Core.Secure, and the active port is Core.ExternalPort when set else
// Core.Port. An empty root domain yields "" (no host to derive).
func derivePortalAPIURL(core config.CoreConfig) string {
	root := strings.Trim(strings.ToLower(core.Domain), ".")
	if root == "" {
		return ""
	}
	scheme := "http"
	if core.Secure {
		scheme = "https"
	}
	port := core.Port
	if core.ExternalPort != 0 {
		port = core.ExternalPort
	}
	return scheme + "://" + net.JoinHostPort(root, strconv.FormatUint(uint64(port), 10))
}

// validatePortalAPIURL rejects an empty derived portal API URL. An empty URL
// means the core domain is unconfigured, so PORTAL_API_URL would be injected
// empty into every workspace runtime; fail fast at startup instead.
func validatePortalAPIURL(url string) error {
	if url == "" {
		return errors.New("workspace: portal api url is empty: core domain is not configured")
	}
	return nil
}

// newLabel returns a fresh DNS-safe opaque label via the injected generator.
func (s *WorkspaceService) newLabel() (string, error) {
	gen := s.slugGen
	if gen == nil {
		gen = generateOpaqueLabel
	}
	return gen()
}

// getByWebsite returns the live workspace for a website, or (nil, nil) when the
// website does not have one yet. Used by Create to enforce one-workspace-per-
// website with a clean error before label generation.
func (s *WorkspaceService) getByWebsite(ctx context.Context, websiteID uint) (*pluginDb.Workspace, error) {
	var ws pluginDb.Workspace
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.Where("website_id = ?", websiteID).First(&ws).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("workspace: failed to check existing workspace: %w", err)
	}
	return &ws, nil
}

// purgeWorkspaceTombstone hard-deletes any soft-deleted workspace tombstone
// that still holds the strict UNIQUE(website_id) key for the given website, so
// a destroy-then-recreate cycle can re-attach the same website. Only rows whose
// deleted_at IS NOT NULL are removed (a soft-deleted row is created only after
// all provider cleanup succeeded in Delete, so purging it is lossless); a live
// same-website row is left untouched for the unique key to reject. This mirrors
// the repository's tombstone-purge-before-insert contract (CreatePlatformDomain,
// CreateDomain).
func (s *WorkspaceService) purgeWorkspaceTombstone(ctx context.Context, websiteID uint) error {
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.
			Where("website_id = ? AND deleted_at IS NOT NULL", websiteID).
			Unscoped().Delete(&pluginDb.Workspace{}).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		return fmt.Errorf("workspace: failed to purge stale workspace tombstone: %w", err)
	}
	return nil
}

// isDuplicateWorkspaceError reports whether a duplicate-key error is a
// violation of the strict UNIQUE(website_id) key (one workspace per website)
// rather than a label collision on (platform_domain_id, label). SQLite surfaces
// the affected columns in the message; MySQL names the unique key index.
func isDuplicateWorkspaceError(err error) bool {
	if !isDuplicateKeyError(err) {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "website_id") || strings.Contains(msg, "uk_workspaces_website_id")
}

// insert persists a provisioning workspace. A duplicate-key error is returned
// unwrapped so the Create loop can retry with a fresh label.
func (s *WorkspaceService) insert(ctx context.Context, ws *pluginDb.Workspace) (*pluginDb.Workspace, error) {
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.Create(ws).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		return nil, err
	}
	return ws, nil
}

// isDuplicateKeyError reports whether err is a database unique-key (duplicate)
// violation. GORM only maps these to gorm.ErrDuplicatedKey when TranslateError
// is enabled (it is not here), so fall back to driver-agnostic detection.
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "Duplicate entry") ||
		strings.Contains(msg, "duplicate key value")
}

// labelAlphabet is a DNS-safe lowercase alphanumeric alphabet (RFC 1035 label
// chars, no separators so the generated label never needs trimming).
const labelAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

// labelRandomChars is the number of random chars in the generated label.
const labelRandomChars = 8

// generateOpaqueLabel returns a DNS-safe, opaque label that does not encode a
// username or website title, e.g. "k7x4p9zq". It is a valid RFC 1035 label:
// 1-63 chars, alphanumerics only (no separators, so no leading/trailing
// hyphen), always lower-case. crypto/rand makes labels non-sequential and hard
// to enumerate.
func generateOpaqueLabel() (string, error) {
	buf := make([]byte, labelRandomChars)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("workspace: failed to generate label: %w", err)
	}
	var sb strings.Builder
	sb.Grow(labelRandomChars)
	for _, v := range buf {
		sb.WriteByte(labelAlphabet[int(v)%len(labelAlphabet)])
	}
	return sb.String(), nil
}
