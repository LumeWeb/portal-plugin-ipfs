package website

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	dnslink "github.com/dnslink-std/go"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pluginEvent "go.lumeweb.com/portal-plugin-ipfs/internal/event"
	domsvc "go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
	"golang.org/x/sync/errgroup"

	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	"go.uber.org/zap"
	"golang.org/x/net/idna"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (s *WebsiteServiceDefault) verificationTokenKey() string {
	if s.dnsConfig != nil && s.dnsConfig.VerificationTokenKey != "" {
		return s.dnsConfig.VerificationTokenKey
	}
	return "lumeweb-verify"
}

const (
	msgTokenExpired      = "Validation token expired for %s — a new token has been generated. Please add the updated TXT record at %s.%s to your DNS configuration"
	msgDNSMissing        = "No DNS records found for %s. Please add the required TXT records to your DNS configuration"
	msgDNSMismatch       = "DNS validation failed: missing or incorrect dnslink record (expected: %s, found: %s)"
	msgTokenMissing      = "DNS validation failed: missing validation token at %s.%s for %s"
	msgValidated         = "DNS validation successful for %s"
	msgDelegationPending = "Domain delegation not yet published"
)

func extractParentDomain(domain string) string {
	parts := strings.Split(domain, ".")
	if len(parts) <= 2 {
		return ""
	}
	return strings.Join(parts[1:], ".")
}

// Validation error types
var (
	ErrInvalidCID      = errors.New("invalid CID")
	ErrInvalidIPNS     = errors.New("invalid IPNS name")
	ErrInvalidTarget   = errors.New("invalid target")
	ErrInvalidDomain   = errors.New("invalid domain")
	ErrCIDNotPinned    = errors.New("CID is not pinned")
	ErrIPNSKeyNotFound = errors.New("IPNS key not found")
)

// WebsiteServiceDefault implements the WebsiteService interface
type WebsiteServiceDefault struct {
	*core.BaseComponent
	pinSvc             pluginCore.IPFSPinService
	ipnsKeySvc         pluginCore.IPNSKeyService
	mailerSvc          core.MailerService
	userSvc            core.UserService
	dnsSvc             pluginCore.DNSService
	config             *pluginConfig.WebsiteConfig
	dnsConfig          *pluginConfig.DnsConfig
	delegatedDomainSvc delegatedDomainService
	resolver           DNSResolver
	publishWg          sync.WaitGroup
}

// Ensure WebsiteServiceDefault implements the interface
var _ pluginCore.WebsiteService = (*WebsiteServiceDefault)(nil)

// delegatedDomainService is the subset of *domsvc.DelegatedDomainService
// used by WebsiteServiceDefault for delegation-aware validation.
type delegatedDomainService interface {
	UsesDelegationForOwnership(domain string) bool
	VerifyDomain(ctx context.Context, wd *pluginDb.WebsiteDomain) (bool, error)
	GetNamespaceForDomain(domain string) (string, bool)
	GetWebsiteDomainByName(ctx context.Context, domain string) (*pluginDb.WebsiteDomain, error)
	GetPendingWebsiteDomainsPaginated(ctx context.Context, status pluginDb.DomainStatus, limit, offset int) ([]pluginDb.WebsiteDomain, error)
}

// resolverForDomain returns the appropriate DNSResolver for the given domain.
// Different roots (ICANN vs HNS etc.) may require different DNS resolvers
// because alt-root records are not visible to the system default resolver.
func (s *WebsiteServiceDefault) resolverForDomain(domain string) DNSResolver {
	// Explicitly set resolver (used by tests via setMockResolver) takes top priority.
	if s.resolver != nil {
		return s.resolver
	}

	// Namespace-specific resolver for alt-roots.
	if s.delegatedDomainSvc != nil {
		if ns, ok := s.delegatedDomainSvc.GetNamespaceForDomain(domain); ok {
			if ns == string(pluginDb.DomainNamespaceHNS) {
				if s.dnsConfig != nil && s.dnsConfig.HNSResolver != "" {
					return NewLiveResolver(s.dnsConfig.HNSResolver)
				}
			}
		}
	}

	return NewLiveResolver("")
}

// primaryWebsiteDomain resolves the website's primary (apex) WebsiteDomain
// binding, which owns the DNS hosting state (dns_hosting_enabled, zone_id)
// for the site. It resolves via Website.PrimaryDomainID when set, otherwise
// falls back to the oldest active binding for safety. Returns
// gorm.ErrRecordNotFound when the website has no primary binding.
func (s *WebsiteServiceDefault) primaryWebsiteDomain(ctx context.Context, website *pluginDb.Website) (*pluginDb.WebsiteDomain, error) {
	var wd pluginDb.WebsiteDomain
	q := s.DB().WithContext(ctx).Where("website_id = ?", website.ID)
	if website.PrimaryDomainID != nil {
		q = q.Where("id = ?", *website.PrimaryDomainID)
	} else {
		q = q.Where("status = ?", pluginDb.DomainStatusActive)
	}
	err := q.Order("id ASC").First(&wd).Error
	if err != nil {
		return nil, err
	}
	return &wd, nil
}

// primaryDomainName returns the primary domain's name for the given website, or
// an empty string when no primary binding resolves. DNS/validation code that
// needs the apex domain name should use this instead of the removed
// Website.Domain field.
func (s *WebsiteServiceDefault) primaryDomainName(ctx context.Context, website *pluginDb.Website) string {
	wd, err := s.primaryWebsiteDomain(ctx, website)
	if err != nil {
		return ""
	}
	return wd.Domain
}

// NewWebsiteService creates a new website service
func NewWebsiteService() (core.Service, []core.ContextBuilderOption, error) {
	svc := &WebsiteServiceDefault{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			svc.pinSvc = core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
			svc.ipnsKeySvc = core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
			svc.mailerSvc = core.GetService[core.MailerService](ctx, core.MAILER_SERVICE)
			svc.userSvc = core.GetService[core.UserService](ctx, core.USER_SERVICE)
			svc.dnsSvc = core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
			var dds *domsvc.DelegatedDomainService = core.GetServiceOptional[*domsvc.DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			if dds != nil {
				svc.delegatedDomainSvc = dds
			}

			// Load configuration from service config
			svc.config = core.GetServiceConfig[*pluginConfig.WebsiteConfig](ctx, pluginCore.WEBSITE_SERVICE)
			svc.dnsConfig = core.GetServiceConfig[*pluginConfig.DnsConfig](ctx, pluginCore.DNS_SERVICE)

			if svc.resolver == nil {
				svc.resolver = LiveResolver{}
			}

			return nil
		}),
	)

	return svc, opts, nil
}

func (s *WebsiteServiceDefault) ID() string {
	return pluginCore.WEBSITE_SERVICE
}

func (s *WebsiteServiceDefault) GetConfig() (any, error) {
	return &pluginConfig.WebsiteConfig{}, nil
}

// CreateWebsite creates a new website configuration
func (s *WebsiteServiceDefault) CreateWebsite(ctx context.Context, website *pluginDb.Website) (*pluginDb.Website, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.CreateWebsite")
	defer span.End()

	return core.MetricTrackResult(
		CreateWebsiteDuration.WithLabelValues(),
		CreateWebsiteTotal.WithLabelValues(LabelStatusError),
		func() (*pluginDb.Website, error) {
			// Validate target type and hash up front (domain normalization and
			// validation moved to the primary-domain binding creation in the
			// API layer; the Website model no longer carries a domain string).
			// When target_type=ipns with a plain CID hash (CIDVersion set) the
			// hash is the *input* to the IPNS auto-convert path below, so it
			// must validate as a CID, not as a peer ID.
			if website.TargetType == string(pluginDb.WebsiteTargetTypeIPNS) && website.CIDVersion != nil {
				if _, err := cid.Decode(website.TargetHash()); err != nil {
					return nil, fmt.Errorf("invalid target: %w: %v", ErrInvalidCID, err)
				}
			} else if err := s.validateTarget(website.TargetType, website.TargetHash()); err != nil {
				return nil, fmt.Errorf("invalid target: %w", err)
			}

			// Generate validation token
			token, err := s.generateValidationToken()
			if err != nil {
				return nil, fmt.Errorf("failed to generate validation token: %w", err)
			}
			website.ValidationToken = token

			// Set validation expiry
			expiresAt := time.Now().Add(s.config.ValidationTokenTTL)
			website.ValidationExpiresAt = &expiresAt

			// Set initial status
			website.Status = string(pluginDb.WebsiteStatusPendingValidation)

			// Resolve the primary (apex) WebsiteDomain, which owns the DNS hosting
			// state for this site. The primary domain may have been bound before
			// the website was created (Phase 4 API layer); if no binding exists yet
			// we create the website without domain-keyed side-effects and let the
			// domain-add flow drive DNS/IPNS setup later.
			primaryWD, perr := s.primaryWebsiteDomain(ctx, website)
			var primaryDomain string
			if perr == nil && primaryWD != nil {
				primaryDomain = primaryWD.Domain
			} else {
				s.Logger().Debug("No primary domain binding available at website creation, deferring DNS/IPNS side-effects",
					zap.String("target_type", website.TargetType))
			}

			// Auto-convert: target_type=ipns with a plain IPFS CID means
			// "create/use IPNS key and publish this CID to it". Requires a
			// primary domain to name the key; without one the raw target stands.
			// This must run before the website row is written so the persisted
			// target is already a valid IPNS peer ID (Website.BeforeSave rejects
			// a non-nil CIDVersion for IPNS targets).
			if website.TargetType == string(pluginDb.WebsiteTargetTypeIPNS) && website.CIDVersion != nil && primaryDomain != "" {
				publishCID := website.TargetHash()

				ipnsKey, err := s.ensureIPNSKey(ctx, website.UserID, primaryDomain, publishCID)
				if err != nil {
					return nil, err
				}

				website.TargetType = string(pluginDb.WebsiteTargetTypeIPNS)
				website.TargetMultihash = ipnsKey.PeerIDMultihash
				website.CIDVersion = nil
				website.CIDType = nil
				website.IPNSKeyID = &ipnsKey.ID
			}

			// Auto-create IPNS key for managed DNS when using IPFS target and
			// the primary domain has DNS hosting enabled.
			if primaryWD != nil && primaryWD.DNSHostingEnabled && website.TargetType == string(pluginDb.WebsiteTargetTypeIPFS) {
				ipnsKey, err := s.ensureIPNSKey(ctx, website.UserID, primaryDomain, website.TargetHash())
				if err != nil {
					return nil, err
				}

				website.TargetType = string(pluginDb.WebsiteTargetTypeIPNS)
				website.TargetMultihash = ipnsKey.PeerIDMultihash
				website.CIDVersion = nil
				website.CIDType = nil
				website.IPNSKeyID = &ipnsKey.ID
			}

			// Create website in database (with the final, converted target).
			err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				return tx.Create(website)
			})
			if err != nil {
				s.Logger().Error("Failed to create website",
					zap.Error(err),
					zap.Uint("user_id", website.UserID))
				return nil, fmt.Errorf("failed to create website: %w", err)
			}

			// Create website DNS records if hosting is enabled on the primary
			// domain. Reuse an existing canonical ZoneID; only resolve/create a
			// zone when the binding does not have one yet.
			if primaryWD != nil && primaryWD.DNSHostingEnabled && s.dnsSvc != nil {
				var dnsZone *pluginDb.DNSZone
				zoneCreated := false

				if primaryWD.ZoneID != 0 {
					dnsZone, err = s.dnsSvc.GetZone(ctx, primaryWD.ZoneID)
					if err != nil {
						s.Logger().Warn("Failed to load existing DNS zone for website",
							zap.Error(err),
							zap.Uint("website_id", website.ID),
							zap.Uint("zone_id", primaryWD.ZoneID))
						dnsZone = nil
					}
				} else {
					// Check if a parent zone already exists for this user (e.g.
					// pinner.xyz for docs.pinner.xyz).
					if parentDomain := extractParentDomain(primaryDomain); parentDomain != "" {
						existingZone, lookupErr := s.dnsSvc.GetZoneByDomain(ctx, parentDomain)
						if lookupErr == nil && existingZone != nil && existingZone.UserID == website.UserID {
							dnsZone = existingZone
							s.Logger().Info("Reusing existing parent zone for subdomain website",
								zap.String("domain", primaryDomain),
								zap.String("parent_zone_domain", parentDomain),
								zap.Uint("zone_id", existingZone.ID))
						}
					}

					// No parent zone found — create one for this domain.
					if dnsZone == nil {
						var createErr error
						dnsZone, createErr = s.dnsSvc.CreateZone(ctx, primaryDomain, website.UserID)
						if createErr != nil {
							s.Logger().Warn("Failed to create DNS zone for website",
								zap.Error(createErr),
								zap.String("domain", primaryDomain))
							// Continue without DNS zone - website is still created.
							dnsZone = nil
						} else {
							zoneCreated = true
						}
					}
				}

				if dnsZone != nil {
					// Persist association only for a binding that had no canonical
					// zone. Existing ZoneID must never be overwritten.
					if primaryWD.ZoneID == 0 {
						err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
							return tx.Model(primaryWD).Update("zone_id", dnsZone.ID)
						})
						if err != nil {
							s.Logger().Error("Failed to update website with DNS zone ID, attempting to clean up DNS zone",
								zap.Error(err),
								zap.Uint("website_id", website.ID),
								zap.Uint("zone_id", dnsZone.ID))

							if zoneCreated {
								if cleanupErr := s.dnsSvc.DeleteZone(ctx, dnsZone.ID); cleanupErr != nil {
									s.Logger().Error("Failed to clean up orphaned DNS zone",
										zap.Error(cleanupErr),
										zap.Uint("zone_id", dnsZone.ID))
									return nil, fmt.Errorf("failed to associate DNS zone with website (and cleanup failed: %w): %w", cleanupErr, err)
								}
							}

							return nil, fmt.Errorf("failed to associate DNS zone with website: %w", err)
						}
						primaryWD.ZoneID = dnsZone.ID
					}

					s.Logger().Info("DNS zone available for website",
						zap.Uint("website_id", website.ID),
						zap.Uint("zone_id", dnsZone.ID),
						zap.String("domain", primaryDomain))

					if err := s.createWebsiteDNSRecords(ctx, primaryWD, website, website.ValidationToken); err != nil {
						s.Logger().Error("Failed to create DNS records for website",
							zap.Error(err),
							zap.Uint("website_id", website.ID),
							zap.Uint("zone_id", dnsZone.ID))
						// Continue without DNS records - website is still created.
					}
				}
			}

			s.Logger().Info("Website created",
				zap.Uint("id", website.ID),
				zap.String("domain", primaryDomain),
				zap.Uint("user_id", website.UserID),
				zap.String("target_type", website.TargetType),
				zap.String("target_hash", website.TargetHash()),
				zap.Bool("dns_hosting_enabled", primaryWD != nil && primaryWD.DNSHostingEnabled))

			// Admin "website created" notification. When a delegated-domain
			// service is wired it fires inside DelegatedDomainService.CreateDomain
			// (so the domain resolves); otherwise fire here so every created
			// website notifies. The two paths are mutually exclusive per
			// deployment, so the email is never sent twice.
			if s.delegatedDomainSvc == nil {
				s.NotifyAdminWebsiteCreated(ctx, website.ID)
			}

			// Emit website published event for SSE gateway notification
			core.Fire(s.Context(), pluginEvent.EVENT_WEBSITE_PUBLISHED, pluginEvent.NewWebsitePublishedEvent(
				ctx, primaryDomain, website.TargetHash(), website.UserID, website.ID,
			))

			return website, nil
		},
	)
}

// GetWebsite retrieves a single website by ID
func (s *WebsiteServiceDefault) GetWebsite(ctx context.Context, userID uint, websiteID uint) (*pluginDb.Website, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.GetWebsite")
	defer span.End()

	return core.MetricTrackResult(
		GetWebsiteDuration.WithLabelValues(),
		GetWebsiteTotal.WithLabelValues(LabelStatusError),
		func() (*pluginDb.Website, error) {
			var website pluginDb.Website

			err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				return tx.Where("user_id = ? AND id = ?", userID, websiteID).First(&website)
			})

			if err != nil {
				if err == gorm.ErrRecordNotFound {
					s.Logger().Debug("Website not found",
						zap.Uint("user_id", userID),
						zap.Uint("website_id", websiteID))
					return nil, nil
				}
				s.Logger().Error("Failed to get website",
					zap.Error(err),
					zap.Uint("user_id", userID),
					zap.Uint("website_id", websiteID))
				return nil, fmt.Errorf("failed to get website: %w", err)
			}

			return &website, nil
		},
	)
}

// GetWebsiteByDomain retrieves a website by domain name, along with its
// namespace. For alt-root domains the namespace comes from website_domains;
// for legacy ipfs_websites.domain lookups it defaults to ICANN.
func (s *WebsiteServiceDefault) GetWebsiteByDomain(ctx context.Context, domain string) (*pluginDb.Website, pluginDb.DomainNamespace, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.GetWebsiteByDomain")
	defer span.End()

	domain = domsvc.NormalizeDomain(domain)

	var namespace pluginDb.DomainNamespace

	website, err := core.MetricTrackResult(
		GetWebsiteByDomainDuration.WithLabelValues(),
		GetWebsiteByDomainTotal.WithLabelValues(LabelStatusError),
		func() (*pluginDb.Website, error) {
			// Resolve purely via website_domains (legacy ipfs_websites.domain
			// lookup no longer exists — the column was removed). If the domain
			// service is unavailable, no domain mapping can be resolved.
			if s.delegatedDomainSvc != nil {
				wd, err := s.delegatedDomainSvc.GetWebsiteDomainByName(ctx, domain)
				if err == nil && wd != nil && !wd.DeletedAt.Valid {
					var website pluginDb.Website
					if err := s.DB().WithContext(ctx).
						Where("id = ?", wd.WebsiteID).
						First(&website).Error; err != nil {
						if errors.Is(err, gorm.ErrRecordNotFound) {
							return nil, nil
						}
						return nil, fmt.Errorf("failed to get website by domain (join): %w", err)
					}
					namespace = wd.Namespace
					return &website, nil
				}
			}

			return nil, nil
		},
	)
	if err != nil {
		return nil, pluginDb.DomainNamespaceICANN, err
	}
	if namespace == "" {
		namespace = pluginDb.DomainNamespaceICANN
	}
	return website, namespace, err
}

// ListWebsites retrieves a paginated and filtered list of websites
func (s *WebsiteServiceDefault) ListWebsites(ctx context.Context, userID uint, filter []queryutil.CrudFilter, sort []filter.Sort, pagination queryutil.Pagination) ([]*pluginDb.Website, int64, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.ListWebsites")
	defer span.End()

	var result struct {
		websites []*pluginDb.Website
		total    int64
	}

	err := core.MetricTrack(
		ListWebsitesDuration.WithLabelValues(),
		ListWebsitesTotal.WithLabelValues(LabelStatusError),
		func() error {
			var websites []*pluginDb.Website
			var total int64

			err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				// Construct the query with user ID filter
				query := tx.Model(&pluginDb.Website{}).Where("user_id = ?", userID)

				// The `domain` filter no longer maps to a website column: a
				// website's domain lives on its WebsiteDomain bindings, so match
				// websites that have a (non-deleted) binding with that domain.
				if domainVal, ok := filterValue(filter, "domain"); ok {
					query = query.Where("EXISTS (SELECT 1 FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.domain = ? AND wd.deleted_at IS NULL)", domainVal)
					filter = removeFilter(filter, "domain")
				}

				query = queryutil.ApplyFilters(query, filter, nil)
				query = queryutil.ApplySort(query, sort)
				query = queryutil.ApplyPagination(query, pagination)

				// Get total count
				if err := query.Count(&total).Error; err != nil {
					_ = tx.AddError(fmt.Errorf("failed to count websites: %w", err))
					return tx
				}

				// Get the records
				if err := query.Find(&websites).Error; err != nil {
					_ = tx.AddError(fmt.Errorf("failed to list websites: %w", err))
					return tx
				}

				return tx
			})

			if err != nil {
				s.Logger().Error("Failed to list websites",
					zap.Error(err),
					zap.Uint("user_id", userID),
					zap.Any("filters", filter),
					zap.Any("pagination", pagination))
				return err
			}

			s.Logger().Debug("Listed websites",
				zap.Int("count", len(websites)),
				zap.Int64("total", total),
				zap.Uint("user_id", userID))

			result.websites = websites
			result.total = total
			return nil
		})

	if err != nil {
		return nil, 0, err
	}

	return result.websites, result.total, nil
}

// filterValue returns the value of the first filter matching the given field,
// and a bool indicating presence. Used to special-case the `domain` filter in
// ListWebsites, which no longer maps to a website column.
func filterValue(filters []queryutil.CrudFilter, field string) (any, bool) {
	for _, f := range filters {
		if f.GetField() == field {
			return f.GetValue(), true
		}
	}
	return nil, false
}

// removeFilter returns the filters with all entries for the given field removed.
func removeFilter(filters []queryutil.CrudFilter, field string) []queryutil.CrudFilter {
	out := filters[:0]
	for _, f := range filters {
		if f.GetField() != field {
			out = append(out, f)
		}
	}
	return out
}

// UpdateWebsite updates an existing website
func (s *WebsiteServiceDefault) UpdateWebsite(ctx context.Context, userID uint, websiteID uint, updates map[string]any) (*pluginDb.Website, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.UpdateWebsite")
	defer span.End()

	var updatedWebsite *pluginDb.Website
	// dnsEnabledChanged is true when the caller toggled DNS hosting; enableDNS
	// records the requested direction (true = enabling, false = disabling).
	var dnsEnabledChanged bool
	var enableDNS bool
	var targetHashChanged bool

	err := core.MetricTrack(
		UpdateWebsiteDuration.WithLabelValues(),
		UpdateWebsiteTotal.WithLabelValues(LabelStatusError),
		func() error {
			err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				// Get the website first
				var website pluginDb.Website
				if err := tx.Where("user_id = ? AND id = ?", userID, websiteID).First(&website).Error; err != nil {
					if err == gorm.ErrRecordNotFound {
						_ = tx.AddError(fmt.Errorf("website not found"))
						return tx
					}
					_ = tx.AddError(fmt.Errorf("failed to get website: %w", err))
					return tx
				}

				// Store old values for DNS updates and state transitions
				oldTargetHash := website.TargetHash()
				oldTargetType := pluginDb.WebsiteTargetType(website.TargetType)
				targetHashChanged = false
				ipnsAutoCreated := false
				var newTargetHashStr string

				// Resolve the primary (apex) WebsiteDomain, which owns the DNS
				// hosting state. DNS-hosted side-effects operate on this binding;
				// the IPNS key stays on the Website itself.
				primaryWD, perr := s.primaryWebsiteDomain(ctx, &website)
				if perr != nil && !errors.Is(perr, gorm.ErrRecordNotFound) {
					_ = tx.AddError(fmt.Errorf("failed to resolve primary domain: %w", perr))
					return tx
				}
				var primaryDomain string
				oldDNSEnabled := false
				if primaryWD != nil {
					primaryDomain = primaryWD.Domain
					oldDNSEnabled = primaryWD.DNSHostingEnabled
				}
				dnsEnabledChanged = false

				// (Primary-domain additions/renames are handled by the domain
				// binding flow in the API layer; a website update no longer
				// carries a domain string.)

				// Validate and convert target if being updated
				if targetHashStr, ok := updates["target_hash"].(string); ok {
					targetType := website.TargetType
					if tt, ok := updates["target_type"].(string); ok {
						targetType = tt
					}

					// Auto-detect: target_type=ipns with a plain IPFS CID means
					// "create/use IPNS key and publish this CID to it"
					if targetType == string(pluginDb.WebsiteTargetTypeIPNS) && isIPFSCid(targetHashStr) && !isValidIPNSTarget(targetHashStr) {
						publishCID := targetHashStr
						var publishHash string

						// Verify the CID is pinned before publishing to IPNS
						if err := s.validateIPFSTarget(ctx, userID, publishCID); err != nil {
							_ = tx.AddError(fmt.Errorf("CID validation failed: %w", err))
							return tx
						}

						ipnsKey, err := s.ensureIPNSKey(ctx, website.UserID, s.primaryDomainName(ctx, &website), publishCID)
						if err != nil {
							_ = tx.AddError(err)
							return tx
						}
						publishHash = ipnsKey.PeerID().String()
						ipnsAutoCreated = true

						setIPNSTargetUpdates(updates, ipnsKey)
						updates["target_type"] = string(pluginDb.WebsiteTargetTypeIPNS)

						if publishHash != oldTargetHash {
							targetHashChanged = true
							newTargetHashStr = publishHash
						}
						delete(updates, "target_hash")
					} else {
						// Auto-detect targetType if it's IPFS but target_hash is a peer ID
						if targetType == string(pluginDb.WebsiteTargetTypeIPFS) && isValidIPNSTarget(targetHashStr) {
							targetType = string(pluginDb.WebsiteTargetTypeIPNS)
							updates["target_type"] = targetType
						}

						// Validate target hash
						if err := s.validateTarget(targetType, targetHashStr); err != nil {
							_ = tx.AddError(fmt.Errorf("invalid target: %w", err))
							return tx
						}

						// For IPNS targets, verify the key exists and belongs to the user
						if targetType == string(pluginDb.WebsiteTargetTypeIPNS) {
							if err := s.validateIPNSKeyResolution(ctx, userID, targetHashStr); err != nil {
								_ = tx.AddError(fmt.Errorf("IPNS key validation failed: %w", err))
								return tx
							}
						}

						// Check if target hash changed
						if targetHashStr != oldTargetHash {
							targetHashChanged = true
							newTargetHashStr = targetHashStr
						}

						// Convert string to multihash and CID version
						if targetType == string(pluginDb.WebsiteTargetTypeIPFS) {
							c, err := cid.Decode(targetHashStr)
							if err != nil {
								_ = tx.AddError(fmt.Errorf("failed to decode CID: %w", err))
								return tx
							}

							// Verify the CID is pinned before accepting the update
							if err := s.validateIPFSTarget(ctx, userID, targetHashStr); err != nil {
								_ = tx.AddError(fmt.Errorf("CID validation failed: %w", err))
								return tx
							}

							normalizedCid := encoding.NormalizeCid(c)
							updates["target_multihash"] = normalizedCid.Hash()
							version := uint8(normalizedCid.Version())
							updates["cid_version"] = &version
							codec := uint8(normalizedCid.Type())
							updates["cid_type"] = &codec
						} else {
							target, err := pluginDb.NewIPNSTargetFromString(targetHashStr)
							if err != nil {
								_ = tx.AddError(fmt.Errorf("failed to parse IPNS target: %w", err))
								return tx
							}
							updates["target_multihash"] = target.ToMultihash()
							updates["cid_version"] = nil
							updates["cid_type"] = nil
						}

						// Remove old target_hash from updates
						delete(updates, "target_hash")
					}
				} else if newTargetType, ok := updates["target_type"].(string); ok {
					// target_type provided without target_hash — conversion request
					requestedType := pluginDb.WebsiteTargetType(newTargetType)

					if requestedType == pluginDb.WebsiteTargetTypeIPNS && oldTargetType == pluginDb.WebsiteTargetTypeIPFS {
						// IPFS → IPNS: auto-create key, publish current CID
						publishCID := oldTargetHash

						// Verify the CID is pinned before publishing to IPNS
						if err := s.validateIPFSTarget(ctx, userID, publishCID); err != nil {
							_ = tx.AddError(fmt.Errorf("CID validation failed: %w", err))
							return tx
						}

						ipnsKey, err := s.ensureIPNSKey(ctx, website.UserID, s.primaryDomainName(ctx, &website), publishCID)
						if err != nil {
							_ = tx.AddError(err)
							return tx
						}
						ipnsAutoCreated = true

						setIPNSTargetUpdates(updates, ipnsKey)

						newPeerID := ipnsKey.PeerID().String()
						if newPeerID != oldTargetHash {
							targetHashChanged = true
							newTargetHashStr = newPeerID
						}
					} else if requestedType == pluginDb.WebsiteTargetTypeIPFS && oldTargetType == pluginDb.WebsiteTargetTypeIPNS {
						_ = tx.AddError(fmt.Errorf("cannot convert from IPNS to IPFS without specifying a target CID"))
						return tx
					} else if requestedType == oldTargetType {
						// Same type — no-op, remove from updates
						delete(updates, "target_type")
					}
				}

				// Check if DNS hosting is being toggled (dns_hosting_enabled).
				// The website no longer has a dns_enabled column — the flag now
				// lives on the primary WebsiteDomain, so it is removed from the
				// website update and driven by the transition handlers below.
				if dnsEnabledVal, exists := updates["dns_enabled"]; exists {
					newDNSEnabled, ok := dnsEnabledVal.(bool)
					if !ok {
						_ = tx.AddError(fmt.Errorf("dns_enabled must be a boolean"))
						return tx
					}
					delete(updates, "dns_enabled")
					dnsEnabledChanged = (newDNSEnabled != oldDNSEnabled)
					enableDNS = newDNSEnabled
				}

				// Apply updates
				if err := tx.Model(&website).Updates(updates).Error; err != nil {
					_ = tx.AddError(fmt.Errorf("failed to update website: %w", err))
					return tx
				}

				updatedWebsite = &website

				// If target hash changed and website has auto-created IPNS key, republish to IPNS
				// Skip if ensureIPNSKey already handled the publish
				if targetHashChanged && website.IPNSKeyID != nil && !ipnsAutoCreated {
					ipnsKey, err := s.ipnsKeySvc.GetKeyByID(ctx, website.UserID, *website.IPNSKeyID)
					if err != nil {
						s.Logger().Warn("Failed to get IPNS key for republishing",
							zap.Error(err),
							zap.Uint("website_id", websiteID),
							zap.Uint("ipns_key_id", *website.IPNSKeyID))
					} else {
						publishHash := newTargetHashStr
						if publishHash == "" {
							publishHash = website.TargetHash()
						}
						s.publishCIDAsync(ctx, ipnsKey.PeerID().String(), publishHash, primaryDomain)
					}
				}

				// Update DNS records if target changed and DNS hosting is enabled
				// on the primary domain.
				// Note: Skip DNS only when staying as IPNS (peer ID doesn't change)
				if targetHashChanged && primaryWD != nil && primaryWD.DNSHostingEnabled && primaryWD.ZoneID != 0 && !primaryWD.DelegationRecordsOwned() && s.dnsSvc != nil {
					newTargetType := pluginDb.WebsiteTargetType(website.TargetType)
					if oldTargetType != pluginDb.WebsiteTargetTypeIPNS || newTargetType != pluginDb.WebsiteTargetTypeIPNS {
						newTargetHash := website.TargetHash()
						if err := s.dnsSvc.UpdateWebsiteDNSRecords(ctx, primaryWD.ZoneID, primaryDomain, newTargetHash, newTargetType); err != nil {
							s.Logger().Warn("Failed to update DNS records for website",
								zap.Error(err),
								zap.Uint("website_id", websiteID),
								zap.Uint("zone_id", primaryWD.ZoneID))
						}
					}
				}

				return tx
			})

			return err
		})

	if err != nil {
		return nil, err
	}

	// Handle DNS hosting transitions if DNS hosting was toggled. The toggle is
	// applied to the primary WebsiteDomain (the website no longer carries DNS
	// state); IPNS key logic is untouched here.
	if dnsEnabledChanged && updatedWebsite != nil {
		// Set the new flag on the primary binding first (mirroring
		// SetDomainDNSEnabled) so the persisted flag matches the DNS state that
		// the enable/disable transitions produce, then drive the zone / record
		// lifecycle for it.
		wd, err := s.primaryWebsiteDomain(ctx, updatedWebsite)
		if err != nil {
			s.Logger().Warn("Failed to resolve primary domain for DNS hosting transition",
				zap.Error(err),
				zap.Uint("website_id", websiteID))
		} else if wd != nil {
			wd.DNSHostingEnabled = enableDNS
			if uerr := s.DB().WithContext(ctx).Model(wd).Update("dns_hosting_enabled", enableDNS).Error; uerr != nil {
				s.Logger().Warn("Failed to persist dns_hosting_enabled on primary domain",
					zap.Error(uerr),
					zap.Uint("website_id", websiteID))
			} else if enableDNS {
				// DNS hosting enabled: create zone/records and reset to pending_validation.
				// handleDNSEnabledTransition may auto-convert a plain-CID IPNS target
				// to an IPNS key and persist it BEFORE the fallible zone-creation steps,
				// so the conversion can be committed even when a later zone step fails.
				// Reload the website whenever the enable transition started (not only on
				// success) so updatedWebsite (and EVENT_WEBSITE_PUBLISHED below) reflect
				// the converted target rather than stale pre-conversion state.
				if err := s.handleDNSEnabledTransition(ctx, wd); err != nil {
					s.Logger().Warn("Failed to handle DNS hosting enable transition",
						zap.Error(err),
						zap.Uint("website_id", websiteID))
					// Continue despite failure - website is updated but DNS setup incomplete
				}
				if wd.WebsiteID != 0 {
					var reloaded pluginDb.Website
					if rerr := s.DB().WithContext(ctx).First(&reloaded, wd.WebsiteID).Error; rerr == nil {
						updatedWebsite = &reloaded
					}
				}
			} else {
				// DNS hosting disabled: delete records and reset to pending_validation
				if err := s.handleDNSDisabledTransition(ctx, wd); err != nil {
					s.Logger().Warn("Failed to handle DNS hosting disable transition",
						zap.Error(err),
						zap.Uint("website_id", websiteID))
					// Continue despite failure - website is updated but DNS cleanup incomplete
				}
			}
		}
	}

	s.Logger().Info("Website updated",
		zap.Uint("id", websiteID),
		zap.Uint("user_id", userID),
		zap.Any("updates", updates))

	// Emit website published event if target hash changed (content was republished)
	if targetHashChanged && updatedWebsite != nil {
		core.Fire(s.Context(), pluginEvent.EVENT_WEBSITE_PUBLISHED, pluginEvent.NewWebsitePublishedEvent(
			ctx, s.primaryDomainName(ctx, updatedWebsite), updatedWebsite.TargetHash(), updatedWebsite.UserID, updatedWebsite.ID,
		))
	}

	// Send notification to admin
	if err := s.notifyAdminWebsiteUpdated(ctx, updatedWebsite, updates); err != nil {
		s.Logger().Warn("Failed to send website updated notification", zap.Error(err))
	}

	return updatedWebsite, nil
}

// handleDNSEnabledTransition handles the transition when DNS hosting is enabled
// for the website's primary domain binding. DNS state (zone_id) lives on the
// WebsiteDomain; the website still supplies the target and validation token.
func (s *WebsiteServiceDefault) handleDNSEnabledTransition(ctx context.Context, wd *pluginDb.WebsiteDomain) error {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.handleDNSEnabledTransition")
	defer span.End()

	// Load the owning website for target and validation-token state.
	var website pluginDb.Website
	if err := s.DB().WithContext(ctx).Where("id = ?", wd.WebsiteID).First(&website).Error; err != nil {
		return fmt.Errorf("failed to load website for DNS hosting transition: %w", err)
	}

	// Track zone lifecycle for rollback. A failed enable detaches any zone
	// reference this transition attached, but deletes the PowerDNS zone only
	// when this transition created it (reused parent zones may have consumers).
	zoneCreated := false
	zoneAttached := false

	// Auto-convert a plain-CID IPNS target (target_type=ipns with a raw IPFS
	// CID) to an IPNS key now that a primary domain binding exists. On the web
	// create path the binding is only created after CreateWebsite runs, so the
	// conversion is deferred here; without it the DNS records below would be
	// created for the raw CID instead of the IPNS peer id. Guarded by
	// CIDVersion != nil so already-converted targets are skipped (idempotent
	// for the UpdateWebsite path, which converts earlier).
	if website.TargetType == string(pluginDb.WebsiteTargetTypeIPNS) && website.CIDVersion != nil {
		publishCID := website.TargetHash()
		ipnsKey, err := s.ensureIPNSKey(ctx, website.UserID, wd.Domain, publishCID)
		if err != nil {
			return fmt.Errorf("failed to auto-create IPNS key for managed DNS: %w", err)
		}
		website.TargetType = string(pluginDb.WebsiteTargetTypeIPNS)
		website.TargetMultihash = ipnsKey.PeerIDMultihash
		website.CIDVersion = nil
		website.CIDType = nil
		website.IPNSKeyID = &ipnsKey.ID
		if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			return tx.Save(&website)
		}); err != nil {
			return fmt.Errorf("failed to update website with IPNS target: %w", err)
		}
	}

	s.Logger().Info("Handling DNS hosting enable transition",
		zap.Uint("website_id", website.ID),
		zap.String("domain", wd.Domain))

	// Associate the binding with the PowerDNS zone. The zone is canonicalized
	// on ZoneID (set by CreateDomain via resolveManagedZone; a legacy binding
	// may still hold ZoneID == 0 and need one resolved/created here).
	if wd.ZoneID == 0 && s.dnsSvc != nil {
		var dnsZone *pluginDb.DNSZone

		// Check if a parent zone already exists for this user (e.g. pinner.xyz for docs.pinner.xyz)
		if parentDomain := extractParentDomain(wd.Domain); parentDomain != "" {
			existingZone, err := s.dnsSvc.GetZoneByDomain(ctx, parentDomain)
			if err == nil && existingZone != nil && existingZone.UserID == website.UserID {
				dnsZone = existingZone
				s.Logger().Info("Reusing existing parent zone for subdomain website",
					zap.String("domain", wd.Domain),
					zap.String("parent_zone_domain", parentDomain),
					zap.Uint("zone_id", existingZone.ID))
			}
		}

		// No parent zone found — create one for this domain
		var createErr error
		if dnsZone == nil {
			dnsZone, createErr = s.dnsSvc.CreateZone(ctx, wd.Domain, website.UserID)
			if createErr != nil {
				return fmt.Errorf("failed to create DNS zone: %w", createErr)
			}
			zoneCreated = true
		}

		// Persist the canonical zone reference on the binding.
		err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			return tx.Model(wd).Update("zone_id", dnsZone.ID)
		})
		if err != nil {
			s.Logger().Error("Failed to update website with DNS zone ID",
				zap.Error(err),
				zap.Uint("website_id", website.ID))
			// Only attempt zone cleanup if we created this zone (not reusing a parent)
			if zoneCreated {
				_ = s.dnsSvc.DeleteZone(ctx, dnsZone.ID)
			}
			return fmt.Errorf("failed to associate DNS zone with website: %w", err)
		}

		zoneAttached = true
		wd.ZoneID = dnsZone.ID
		s.Logger().Info("DNS zone associated with website",
			zap.Uint("website_id", website.ID),
			zap.Uint("zone_id", dnsZone.ID),
			zap.String("domain", wd.Domain))
	}

	// Create DNS records if zone exists
	if wd.ZoneID != 0 && s.dnsSvc != nil {
		// Regenerate validation token if expired or not set
		var newToken string
		if website.IsExpired() || website.ValidationToken == "" {
			var err error
			newToken, err = s.generateValidationToken()
			if err != nil {
				return fmt.Errorf("failed to generate validation token: %w", err)
			}
		} else {
			newToken = website.ValidationToken
		}

		// Delegation owns shared DNSLink and apex records. Use the
		// ownership-aware writer so website hosting cannot replace them.
		err := s.createWebsiteDNSRecords(ctx, wd, &website, newToken)
		if err != nil {
			// Roll back the partially-created DNS state. Delegation-owned
			// bindings share DNSLink and apex records with delegation, so only
			// the website validation record may be removed. Non-delegated
			// bindings own the complete website record set. A newly-created
			// non-delegated zone is deleted and its binding reference cleared.
			s.Logger().Error("Failed to create DNS records, rolling back DNS setup",
				zap.Error(err),
				zap.Uint("website_id", website.ID),
				zap.Uint("zone_id", wd.ZoneID),
				zap.String("domain", wd.Domain))
			if wd.DelegationRecordsOwned() {
				_ = s.dnsSvc.DeleteWebsiteValidationRecord(ctx, wd.ZoneID, wd.Domain)
			} else {
				_ = s.dnsSvc.DeleteWebsiteDNSRecords(ctx, wd.ZoneID, wd.Domain)
			}
			if !wd.DelegationRecordsOwned() && zoneAttached {
				if zoneCreated {
					_ = s.dnsSvc.DeleteZone(ctx, wd.ZoneID)
				}
				_ = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
					return tx.Model(wd).Update("zone_id", 0)
				})
				wd.ZoneID = 0
			}
			return fmt.Errorf("failed to create DNS records: %w", err)
		}

		// Update website with new token and status
		expiresAt := time.Now().Add(s.config.ValidationTokenTTL)
		err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			return tx.Model(&website).Updates(map[string]interface{}{
				"validation_token":      newToken,
				"validation_expires_at": expiresAt,
				"status":                string(pluginDb.WebsiteStatusPendingValidation),
			})
		})
		if err != nil {
			s.Logger().Error("Failed to update website validation info",
				zap.Error(err),
				zap.Uint("website_id", website.ID))
			return fmt.Errorf("failed to update website validation: %w", err)
		}

		s.Logger().Info("DNS records created, website reset to pending_validation",
			zap.Uint("website_id", website.ID),
			zap.String("domain", wd.Domain))
	}

	return nil
}

// handleDNSDisabledTransition handles the transition when DNS hosting is disabled
// for the website's primary domain binding. DNS state (zone_id) lives on the
// WebsiteDomain; the website status is reset to pending_validation.
func (s *WebsiteServiceDefault) handleDNSDisabledTransition(ctx context.Context, wd *pluginDb.WebsiteDomain) error {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.handleDNSDisabledTransition")
	defer span.End()

	// Load the owning website to reset its validation status.
	var website pluginDb.Website
	if err := s.DB().WithContext(ctx).Where("id = ?", wd.WebsiteID).First(&website).Error; err != nil {
		return fmt.Errorf("failed to load website for DNS hosting disable transition: %w", err)
	}

	s.Logger().Info("Handling DNS hosting disable transition",
		zap.Uint("website_id", website.ID),
		zap.String("domain", wd.Domain))

	// A delegation-owned binding holds its PowerDNS zone for alt-root
	// delegation (DS/DNSSEC/apex), not just website hosting. The website DNS
	// hosting disable path must not tear down that zone — deleting the zone or
	// its records would break the delegation (VerifyDomain, EnableDNSSEC,
	// GetActiveDNSSECDS, and republish all read zone_id / the zone records).
	// For such a binding, disabling website DNS hosting is a no-op for the
	// zone: only the website's validation state is reset and the hosting flag
	// is cleared (lines 2081-2169), leaving the delegation ownership intact.
	if wd.DelegationRecordsOwned() {
		s.Logger().Info("Skipping DNS zone teardown: zone is delegation-owned",
			zap.Uint("website_id", website.ID),
			zap.String("domain", wd.Domain),
			zap.Uint("zone_id", wd.ZoneID))
		return s.resetWebsiteValidationState(ctx, website)
	}

	recordsDeleted := false

	// Delete DNS records for this website (not the zone — other websites may share it)
	if wd.ZoneID != 0 && s.dnsSvc != nil {
		err := s.dnsSvc.DeleteWebsiteDNSRecords(ctx, wd.ZoneID, wd.Domain)
		if err != nil {
			s.Logger().Warn("Failed to delete DNS records for website",
				zap.Error(err),
				zap.Uint("website_id", website.ID),
				zap.Uint("zone_id", wd.ZoneID))
		} else {
			recordsDeleted = true
			s.Logger().Info("DNS records deleted for website",
				zap.Uint("website_id", website.ID),
				zap.Uint("zone_id", wd.ZoneID))
		}
	}

	// Check if any other domain bindings still reference this zone
	zoneIsEmpty := false
	if recordsDeleted && wd.ZoneID != 0 {
		var count int64
		err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			return tx.Model(&pluginDb.WebsiteDomain{}).
				Where("zone_id = ? AND id != ? AND deleted_at IS NULL", wd.ZoneID, wd.ID).
				Count(&count)
		})
		if err != nil {
			s.Logger().Warn("Failed to count domain bindings sharing DNS zone",
				zap.Error(err),
				zap.Uint("zone_id", wd.ZoneID))
		} else {
			zoneIsEmpty = count == 0
		}
	}

	// Only delete the zone if no other domain bindings are using it
	zoneDeleted := false
	if zoneIsEmpty && wd.ZoneID != 0 && s.dnsSvc != nil {
		err := s.dnsSvc.DeleteZone(ctx, wd.ZoneID)
		if err != nil {
			s.Logger().Warn("Failed to delete DNS zone for website",
				zap.Error(err),
				zap.Uint("website_id", website.ID),
				zap.Uint("zone_id", wd.ZoneID))
		} else {
			zoneDeleted = true
			s.Logger().Info("DNS zone deleted (no other domain bindings using it)",
				zap.Uint("website_id", website.ID),
				zap.Uint("zone_id", wd.ZoneID))
		}
	}

	// Reset website status to pending_validation.
	// Only clear zone_id on the primary binding if the zone was actually
	// deleted from PowerDNS.
	if err := s.resetWebsiteValidationState(ctx, website); err != nil {
		return err
	}

	if zoneDeleted {
		if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			return tx.Model(wd).Update("zone_id", 0)
		}); err != nil {
			s.Logger().Error("Failed to clear DNS zone ID on primary domain binding",
				zap.Error(err),
				zap.Uint("website_id", website.ID))
			return fmt.Errorf("failed to clear DNS zone ID: %w", err)
		}
		wd.ZoneID = 0
	}

	s.Logger().Info("DNS hosting disabled, website reset to pending_validation",
		zap.Uint("website_id", website.ID),
		zap.String("domain", wd.Domain),
		zap.Bool("zone_deleted", zoneDeleted))

	return nil
}

// resetWebsiteValidationState resets a website's status to pending_validation
// after DNS-hosting teardown. It is the part of handleDNSDisabledTransition
// that also applies when the zone is delegation-owned and must be preserved.
func (s *WebsiteServiceDefault) resetWebsiteValidationState(ctx context.Context, website pluginDb.Website) error {
	updates := map[string]interface{}{
		"status": string(pluginDb.WebsiteStatusPendingValidation),
	}
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&website).Updates(updates)
	})
	if err != nil {
		s.Logger().Error("Failed to update website",
			zap.Error(err),
			zap.Uint("website_id", website.ID))
		return fmt.Errorf("failed to update website: %w", err)
	}
	return nil
}

// DeleteWebsite soft-deletes a website by ID
func (s *WebsiteServiceDefault) DeleteWebsite(ctx context.Context, userID uint, websiteID uint) error {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.DeleteWebsite")
	defer span.End()

	return core.MetricTrack(
		DeleteWebsiteDuration.WithLabelValues(),
		DeleteWebsiteTotal.WithLabelValues(LabelStatusError),
		func() error {
			var count int64
			var dnsZoneID uint
			var websiteDomain string
			var dnsDelegationOwned bool

			err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				// First, check if the website exists and is not blocked
				var website pluginDb.Website
				if err := tx.Where("user_id = ? AND id = ?", userID, websiteID).First(&website).Error; err != nil {
					if err == gorm.ErrRecordNotFound {
						_ = tx.AddError(fmt.Errorf("website not found"))
						return tx
					}
					_ = tx.AddError(fmt.Errorf("failed to get website: %w", err))
					return tx
				}

				// Prevent deletion of blocked websites for security reasons
				if pluginDb.WebsiteStatus(website.Status) == pluginDb.WebsiteStatusBlocked {
					_ = tx.AddError(fmt.Errorf("cannot delete blocked website"))
					return tx
				}

				// Store DNS zone ID and domain for cleanup, resolved from the
				// website's primary domain binding (the website no longer
				// carries DNS state).
				primaryWD, _ := s.primaryWebsiteDomain(ctx, &website)
				if primaryWD != nil {
					dnsZoneID = primaryWD.ZoneID
					websiteDomain = primaryWD.Domain
					dnsDelegationOwned = primaryWD.DelegationRecordsOwned()
				}

				// Perform the soft delete
				result := tx.Delete(&website)
				if result.Error != nil {
					_ = tx.AddError(fmt.Errorf("failed to delete website: %w", result.Error))
					return tx
				}
				count = result.RowsAffected

				// Cascade soft-delete the website's domain bindings so the
				// domain can be re-bound after this website is removed,
				// matching the rest of the system's soft-delete semantics.
				// The (domain, namespace) unique key would otherwise block a
				// fresh binding; the app resolves this at bind time by purging
				// the prior soft-deleted tombstone (see AddDomain guardrail)
				// rather than relying on a partial index.
				if count > 0 {
					if derr := tx.Where("website_id = ?", websiteID).Delete(&pluginDb.WebsiteDomain{}).Error; derr != nil {
						_ = tx.AddError(fmt.Errorf("failed to delete website domain bindings: %w", derr))
						return tx
					}
				}
				return tx
			})

			if err != nil {
				return err
			}

			if count == 0 {
				return fmt.Errorf("website not found")
			}

			s.Logger().Info("Website deleted",
				zap.Uint("id", websiteID),
				zap.Uint("user_id", userID))

			// Emit website removed event for SSE gateway notification
			core.Fire(s.Context(), pluginEvent.EVENT_WEBSITE_REMOVED, pluginEvent.NewWebsiteRemovedEvent(
				ctx, websiteDomain, userID, websiteID,
			))

			if dnsZoneID != 0 && s.dnsSvc != nil {
				var cleanupErr error
				if dnsDelegationOwned {
					// DNSLink/apex records are shared with delegation. Remove only
					// website-owned validation state.
					cleanupErr = s.dnsSvc.DeleteWebsiteValidationRecord(ctx, dnsZoneID, websiteDomain)
				} else {
					// Non-delegated bindings own their website DNS records.
					cleanupErr = s.dnsSvc.DeleteWebsiteDNSRecords(ctx, dnsZoneID, websiteDomain)
				}
				if cleanupErr != nil {
					s.Logger().Warn("Failed to delete DNS records for website",
						zap.Error(cleanupErr),
						zap.Uint("website_id", websiteID),
						zap.Uint("zone_id", dnsZoneID))
					// Continue despite DNS cleanup failure - website is already deleted
				}
			}

			return nil
		},
	)
}

// BlockWebsite blocks a website by setting its status to blocked (admin operation)
func (s *WebsiteServiceDefault) BlockWebsite(ctx context.Context, websiteID uint) error {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.BlockWebsite")
	defer span.End()

	return core.MetricTrack(
		UpdateWebsiteDuration.WithLabelValues(),
		UpdateWebsiteTotal.WithLabelValues(LabelStatusError),
		func() error {
			var website pluginDb.Website

			err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				// First, fetch the website to ensure it exists
				if err := tx.First(&website, websiteID).Error; err != nil {
					if err == gorm.ErrRecordNotFound {
						_ = tx.AddError(fmt.Errorf("website not found"))
						return tx
					}
					_ = tx.AddError(fmt.Errorf("failed to get website: %w", err))
					return tx
				}

				// Update the status
				website.Status = string(pluginDb.WebsiteStatusBlocked)
				if err := tx.Save(&website).Error; err != nil {
					_ = tx.AddError(fmt.Errorf("failed to block website: %w", err))
					return tx
				}

				return tx
			})

			if err != nil {
				return err
			}

			s.Logger().Info("Website blocked",
				zap.Uint("id", websiteID))

			return nil
		},
	)
}

// UnblockWebsite unblocks a website by setting its status back to active (admin operation)
func (s *WebsiteServiceDefault) UnblockWebsite(ctx context.Context, websiteID uint) error {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.UnblockWebsite")
	defer span.End()

	return core.MetricTrack(
		UpdateWebsiteDuration.WithLabelValues(),
		UpdateWebsiteTotal.WithLabelValues(LabelStatusError),
		func() error {
			var website pluginDb.Website

			err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				// First, fetch the website to ensure it exists and is blocked
				if err := tx.Where("id = ? AND status = ?", websiteID, pluginDb.WebsiteStatusBlocked).First(&website).Error; err != nil {
					if err == gorm.ErrRecordNotFound {
						_ = tx.AddError(fmt.Errorf("website not found or not blocked"))
						return tx
					}
					_ = tx.AddError(fmt.Errorf("failed to get website: %w", err))
					return tx
				}

				// Update the status
				website.Status = string(pluginDb.WebsiteStatusActive)
				if err := tx.Save(&website).Error; err != nil {
					_ = tx.AddError(fmt.Errorf("failed to unblock website: %w", err))
					return tx
				}

				return tx
			})

			if err != nil {
				return err
			}

			s.Logger().Info("Website unblocked",
				zap.Uint("id", websiteID))

			return nil
		},
	)
}

func (s *WebsiteServiceDefault) loadWebsite(ctx context.Context, userID, websiteID uint) (pluginDb.Website, error) {
	var website pluginDb.Website
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("user_id = ? AND id = ?", userID, websiteID).First(&website)
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return pluginDb.Website{}, fmt.Errorf("website not found")
		}
		return pluginDb.Website{}, fmt.Errorf("failed to get website: %w", err)
	}
	return website, nil
}

// ValidateDNS validates the DNS TXT record for a website's primary domain
func (s *WebsiteServiceDefault) shouldPerformTokenCheck(website *pluginDb.Website, primaryDomain string) bool {
	needs := website.Status == string(pluginDb.WebsiteStatusPendingValidation)
	if needs && s.delegatedDomainSvc != nil && s.delegatedDomainSvc.UsesDelegationForOwnership(primaryDomain) {
		s.Logger().Debug("skipping TXT token (ownership proven via delegation verification)", zap.String("domain", primaryDomain))
		return false
	}
	return needs
}

func (s *WebsiteServiceDefault) ValidateDNS(ctx context.Context, userID uint, websiteID uint) (pluginCore.ValidateDNSResult, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.ValidateDNS")
	defer span.End()

	return core.MetricTrackResult(
		ValidateDNSDuration.WithLabelValues(),
		ValidateDNSTotal.WithLabelValues(LabelStatusError),
		func() (pluginCore.ValidateDNSResult, error) {
			website, err := s.loadWebsite(ctx, userID, websiteID)
			if err != nil {
				return pluginCore.ValidateDNSResult{}, err
			}

			// Resolve the primary domain binding, which owns the DNS hosting
			// state. Validation operates on the apex domain's name and zone.
			primaryWD, perr := s.primaryWebsiteDomain(ctx, &website)
			if perr != nil {
				return pluginCore.ValidateDNSResult{}, fmt.Errorf("failed to resolve primary domain for validation: %w", perr)
			}
			primaryDomain := primaryWD.Domain

			needsTokenCheck := s.shouldPerformTokenCheck(&website, primaryDomain)

			if needsTokenCheck && website.IsExpired() {
				if err := s.regenerateExpiredToken(ctx, &website, primaryWD); err != nil {
					return pluginCore.ValidateDNSResult{}, err
				}
				return pluginCore.ValidateDNSResult{
					Valid:   false,
					Message: fmt.Sprintf(msgTokenExpired, primaryDomain, s.verificationTokenKey(), primaryDomain),
					Reason:  pluginCore.ValidationReasonTokenExpired,
				}, nil
			}

			result, err := s.resolverForDomain(primaryDomain).ResolveDNSLink(primaryDomain)
			if err != nil {
				if dnsErr, ok := errors.AsType[dnslink.DNSRCodeError](err); ok && dnsErr.DNSRCode == 3 {
					s.Logger().Debug("DNS validation failed: no DNS records found (NXDOMAIN)",
						zap.Error(err),
						zap.String("domain", primaryDomain),
						zap.Uint("website_id", website.ID))
					return pluginCore.ValidateDNSResult{
						Valid:   false,
						Message: fmt.Sprintf(msgDNSMissing, primaryDomain),
						Reason:  pluginCore.ValidationReasonDNSMissing,
					}, nil
				}

				return pluginCore.ValidateDNSResult{}, fmt.Errorf("DNS lookup failed for %s: %w", primaryDomain, err)
			}

			if ok, msg, reason := s.checkDNSLinkMatch(&website, primaryDomain, result); !ok {
				return pluginCore.ValidateDNSResult{
					Valid:   false,
					Message: msg,
					Reason:  reason,
				}, nil
			}

			_ = s.determineFoundDNSLink(result, &website)

			if needsTokenCheck {
				if ok, msg, reason, err := s.checkValidationToken(ctx, &website, primaryDomain); err != nil {
					return pluginCore.ValidateDNSResult{}, err
				} else if !ok {
					return pluginCore.ValidateDNSResult{
						Valid:   false,
						Message: msg,
						Reason:  reason,
					}, nil
				}
			}

			if ok, msg, reason, err := s.checkAttachedDelegations(ctx, &website); err != nil {
				return pluginCore.ValidateDNSResult{}, err
			} else if !ok {
				return pluginCore.ValidateDNSResult{
					Valid:   false,
					Message: msg,
					Reason:  reason,
				}, nil
			}

			if err := s.activateValidatedWebsite(ctx, &website); err != nil {
				return pluginCore.ValidateDNSResult{}, err
			}

			s.Logger().Info("DNS validation completed",
				zap.Uint("user_id", userID),
				zap.Bool("validated", true))

			return pluginCore.ValidateDNSResult{
				Valid:   true,
				Message: fmt.Sprintf(msgValidated, primaryDomain),
				Reason:  pluginCore.ValidationReasonValidated,
			}, nil
		},
	)
}

func (s *WebsiteServiceDefault) checkDNSLinkMatch(website *pluginDb.Website, primaryDomain string, result dnslink.Result) (bool, string, pluginCore.ValidationReason) {
	expectedDNSlink := pluginDb.WebsiteTargetType(website.TargetType).ToDNSLinkPath(website.TargetHash())

	var foundDNSlink string
	if ipfsLinks, ok := result.Links["ipfs"]; ok && len(ipfsLinks) > 0 {
		foundDNSlink = dto.IPFSPath(ipfsLinks[0].Identifier)
		if foundDNSlink == expectedDNSlink {
			s.Logger().Debug("Found valid DNSlink record",
				zap.String("domain", primaryDomain),
				zap.String("dnslink", foundDNSlink))
			return true, "", ""
		}
	}
	if ipnsLinks, ok := result.Links["ipns"]; ok && len(ipnsLinks) > 0 {
		foundDNSlink = dto.IPNSPath(ipnsLinks[0].Identifier)
		if foundDNSlink == expectedDNSlink {
			s.Logger().Debug("Found valid DNSlink record",
				zap.String("domain", primaryDomain),
				zap.String("dnslink", foundDNSlink))
			return true, "", ""
		}
	}

	s.Logger().Warn("DNS validation failed: missing or incorrect dnslink record",
		zap.String("domain", primaryDomain),
		zap.String("expected", expectedDNSlink),
		zap.String("found", foundDNSlink))
	return false, fmt.Sprintf(msgDNSMismatch, expectedDNSlink, foundDNSlink), pluginCore.ValidationReasonDNSMismatch
}

func (s *WebsiteServiceDefault) determineFoundDNSLink(result dnslink.Result, website *pluginDb.Website) string {
	if ipfsLinks, ok := result.Links["ipfs"]; ok && len(ipfsLinks) > 0 {
		return dto.IPFSPath(ipfsLinks[0].Identifier)
	}
	if ipnsLinks, ok := result.Links["ipns"]; ok && len(ipnsLinks) > 0 {
		return dto.IPNSPath(ipnsLinks[0].Identifier)
	}
	return ""
}

func (s *WebsiteServiceDefault) checkValidationToken(ctx context.Context, website *pluginDb.Website, primaryDomain string) (bool, string, pluginCore.ValidationReason, error) {
	expectedTokenRecord := fmt.Sprintf("%s=%s", s.verificationTokenKey(), website.ValidationToken)
	txtRecords, err := s.resolverForDomain(primaryDomain).LookupTXT(ctx, s.verificationTokenKey()+"."+primaryDomain)
	if err != nil {
		return false, "", "", fmt.Errorf("DNS TXT lookup failed for %s.%s: %w", s.verificationTokenKey(), primaryDomain, err)
	}

	for _, txtRecord := range txtRecords {
		if strings.Contains(txtRecord, expectedTokenRecord) {
			s.Logger().Debug("Found valid validation token",
				zap.String("domain", primaryDomain),
				zap.String("token", website.ValidationToken))
			return true, "", "", nil
		}
	}

	s.Logger().Warn("DNS validation failed: missing validation token",
		zap.String("domain", primaryDomain),
		zap.String("expected_token", website.ValidationToken))
	return false, fmt.Sprintf(msgTokenMissing, s.verificationTokenKey(), primaryDomain, primaryDomain), pluginCore.ValidationReasonTokenMissing, nil
}

func (s *WebsiteServiceDefault) checkAttachedDelegations(ctx context.Context, website *pluginDb.Website) (bool, string, pluginCore.ValidationReason, error) {
	if s.delegatedDomainSvc == nil {
		return true, "", "", nil
	}

	var attached []pluginDb.WebsiteDomain
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("website_id = ?", website.ID).Find(&attached)
	}); err != nil {
		s.Logger().Error("failed to load attached domains for delegation verification",
			zap.Error(err),
			zap.Uint("website_id", website.ID))
		return false, "", "", fmt.Errorf("failed to verify domain delegations: %w", err)
	}

	// Verify delegations concurrently with a bounded worker pool and a
	// shared context deadline to avoid N*10s serial blocking.
	grp, verifyCtx := errgroup.WithContext(ctx)
	grp.SetLimit(5)
	var mu sync.Mutex
	var failedDomain string
	var failed bool
	for i := range attached {
		ad := &attached[i]
		grp.Go(func() error {
			verifyCtx2, cancel := context.WithTimeout(verifyCtx, 10*time.Second)
			defer cancel()
			verified, verr := s.delegatedDomainSvc.VerifyDomain(verifyCtx2, ad)
			if verr != nil || !verified {
				s.Logger().Info("delegation not verified for attached domain",
					zap.String("domain", ad.Domain), zap.Error(verr))
				mu.Lock()
				if !failed {
					failed = true
					failedDomain = ad.Domain
				}
				mu.Unlock()
				return errors.New("delegation not verified")
			}
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		s.Logger().Info("delegation verification failed for attached domain",
			zap.String("domain", failedDomain))
		return false, msgDelegationPending, pluginCore.ValidationReasonDelegationPending, nil
	}
	return true, "", "", nil
}

// createWebsiteDNSRecords writes only the DNS records that website hosting owns.
// Delegation-owned bindings share DNSLink and apex records with the delegation
// service, so website lifecycle operations may update only validation TXT.
func (s *WebsiteServiceDefault) createWebsiteDNSRecords(ctx context.Context, wd *pluginDb.WebsiteDomain, website *pluginDb.Website, validationToken string) error {
	if s.dnsSvc == nil || wd.ZoneID == 0 {
		return nil
	}

	tokenRecord := fmt.Sprintf("%s=%s", s.verificationTokenKey(), validationToken)
	// HNS zones are DNSSEC-signed at the apex. The generic website writer's
	// ALIAS apex path is unsafe there even before delegation reaches a status
	// that makes DelegationOwned true. HNS delegation owns the shared records.
	if wd.DelegationRecordsOwned() {
		return s.dnsSvc.CreateWebsiteValidationRecord(ctx, wd.ZoneID, wd.Domain, tokenRecord)
	}
	return s.dnsSvc.CreateWebsiteDNSRecords(ctx, wd.ZoneID, wd.Domain, website.TargetHash(), pluginDb.WebsiteTargetType(website.TargetType), tokenRecord)
}

func (s *WebsiteServiceDefault) regenerateExpiredToken(ctx context.Context, website *pluginDb.Website, wd *pluginDb.WebsiteDomain) error {
	newToken, err := s.generateValidationToken()
	if err != nil {
		return fmt.Errorf("failed to regenerate expired validation token: %w", err)
	}

	if wd.ZoneID != 0 && s.dnsSvc != nil {
		if recordErr := s.createWebsiteDNSRecords(ctx, wd, website, newToken); recordErr != nil {
			s.Logger().Warn("Failed to update DNS records with new validation token",
				zap.Error(recordErr),
				zap.Uint("website_id", website.ID),
				zap.String("domain", wd.Domain))
		}
	}

	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		expiresAt := time.Now().Add(s.config.ValidationTokenTTL)
		website.ValidationToken = newToken
		website.ValidationExpiresAt = &expiresAt
		return tx.Save(website)
	}); err != nil {
		return fmt.Errorf("failed to save regenerated validation token: %w", err)
	}

	s.Logger().Info("Regenerated expired validation token",
		zap.Uint("website_id", website.ID),
		zap.String("domain", wd.Domain))

	return nil
}

func (s *WebsiteServiceDefault) activateValidatedWebsite(ctx context.Context, website *pluginDb.Website) error {
	return db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		website.Status = string(pluginDb.WebsiteStatusActive)
		newExpiry := time.Now().Add(s.config.ValidationTokenTTL)
		website.ValidationExpiresAt = &newExpiry
		return tx.Save(website)
	})
}

// CheckStatus checks the status of a website by validating its target
func (s *WebsiteServiceDefault) CheckStatus(ctx context.Context, website *pluginDb.Website) (pluginDb.WebsiteStatus, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.CheckStatus")
	defer span.End()

	return core.MetricTrackResult(
		CheckStatusDuration.WithLabelValues(),
		CheckStatusTotal.WithLabelValues(LabelStatusError),
		func() (pluginDb.WebsiteStatus, error) {
			var newStatus pluginDb.WebsiteStatus
			oldStatus := pluginDb.WebsiteStatus(website.Status)

			// Check based on target type
			switch pluginDb.WebsiteTargetType(website.TargetType) {
			case pluginDb.WebsiteTargetTypeIPFS:
				// For IPFS targets, check if the CID is pinned
				if err := s.validateIPFSTarget(ctx, website.UserID, website.TargetHash()); err != nil {
					s.Logger().Error("Failed to validate IPFS target",
						zap.Error(err),
						zap.String("target_hash", website.TargetHash()))
					return pluginDb.WebsiteStatusBroken, fmt.Errorf("failed to validate IPFS target: %w", err)
				}
				newStatus = pluginDb.WebsiteStatusActive

			case pluginDb.WebsiteTargetTypeIPNS:
				// For IPNS targets, check if the key exists and belongs to the owner
				if err := s.validateIPNSKeyResolution(ctx, website.UserID, website.TargetHash()); err != nil {
					s.Logger().Error("Failed to validate IPNS target",
						zap.Error(err),
						zap.String("target_hash", website.TargetHash()))
					return pluginDb.WebsiteStatusBroken, fmt.Errorf("failed to validate IPNS target: %w", err)
				}
				newStatus = pluginDb.WebsiteStatusActive

			default:
				return pluginDb.WebsiteStatusBroken, fmt.Errorf("unknown target type: %s", website.TargetType)
			}

			// Send notification if status changed
			if oldStatus != newStatus {
				if err := s.notifyUserStatusChanged(ctx, website, oldStatus, newStatus); err != nil {
					s.Logger().Warn("Failed to send status changed notification", zap.Error(err))
				}
			}

			return newStatus, nil
		},
	)
}

// validateDomain validates the domain name format
func (s *WebsiteServiceDefault) validateDomain(domain string) error {
	if domain == "" {
		return fmt.Errorf("domain cannot be empty")
	}
	if len(domain) > 255 {
		return fmt.Errorf("domain too long (max 255 characters)")
	}

	// Convert to ASCII using IDNA for internationalized domain names
	idna := idna.New()
	asciiDomain, err := idna.ToASCII(domain)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDomain, err)
	}

	// RFC 1035 compliant domain validation
	// Pattern: subdomains (alphanumeric with hyphens) followed by TLD
	domainRegex := regexp.MustCompile(`^(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z0-9][A-Za-z0-9\-]{0,61}[A-Za-z]$`)
	if !domainRegex.MatchString(asciiDomain) {
		return fmt.Errorf("invalid domain format")
	}

	return nil
}

// isValidIPNSTarget checks if a target hash is a valid IPNS target
// Returns true if the peer ID or CIDv1 with libp2p-key codec is valid
func isValidIPNSTarget(targetHash string) bool {
	// Check CID first — CIDv0 accidentally passes peer.Decode since both
	// use base58btc multihash encoding, but a content hash is not a peer ID.
	c, cidErr := cid.Decode(targetHash)
	if cidErr == nil {
		return c.Version() == 1 && c.Type() == cid.Libp2pKey
	}

	_, err := peer.Decode(targetHash)
	return err == nil
}

// ensureIPNSKey creates or reuses an IPNS key for a domain, and publishes the given CID to it.
// Returns the IPNS key on success.
func (s *WebsiteServiceDefault) ensureIPNSKey(ctx context.Context, userID uint, domain string, publishCID string) (*pluginDb.IPFSIPNSKey, error) {
	keyName := fmt.Sprintf("%s-auto", domain)

	keys, err := s.ipnsKeySvc.ListKeys(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to list existing IPNS keys: %w", err)
	}

	var ipnsKey *pluginDb.IPFSIPNSKey
	for i := range keys {
		if keys[i].Name == keyName {
			ipnsKey = &keys[i]
			break
		}
	}

	if ipnsKey == nil {
		ipnsKey, err = s.ipnsKeySvc.CreateKey(ctx, userID, keyName, 1)
		if err != nil {
			return nil, fmt.Errorf("failed to create IPNS key: %w", err)
		}
		s.Logger().Info("Created new IPNS key for managed DNS",
			zap.String("domain", domain),
			zap.String("key_name", keyName),
			zap.Stringer("peer_id", ipnsKey.PeerID()))
	} else {
		s.Logger().Info("Reusing existing IPNS key for managed DNS",
			zap.String("domain", domain),
			zap.String("key_name", keyName),
			zap.Stringer("peer_id", ipnsKey.PeerID()))
	}

	if s.ipnsKeySvc != nil && publishCID != "" {
		s.publishCIDAsync(ctx, ipnsKey.PeerID().String(), publishCID, domain)
	}

	return ipnsKey, nil
}

func (s *WebsiteServiceDefault) publishCIDAsync(ctx context.Context, peerID, cid, domain string) {
	s.publishWg.Add(1)
	go func() {
		defer s.publishWg.Done()
		defer func() {
			if r := recover(); r != nil {
				s.Logger().Error("Recovered from panic in publishCIDAsync",
					zap.Any("panic", r),
					zap.String("domain", domain),
					zap.String("peer_id", peerID),
					zap.String("cid", cid))
			}
		}()
		if err := s.ipnsKeySvc.PublishCID(core.DetachContext(ctx), peerID, cid, 24*time.Hour); err != nil {
			s.Logger().Error("Failed to publish CID to IPNS key (async)",
				zap.Error(err),
				zap.String("domain", domain),
				zap.String("peer_id", peerID),
				zap.String("cid", cid))
		} else {
			s.Logger().Info("Published CID to IPNS key (async)",
				zap.String("domain", domain),
				zap.String("peer_id", peerID),
				zap.String("cid", cid))
		}
	}()
}

func (s *WebsiteServiceDefault) WaitForPublishes() {
	s.publishWg.Wait()
}

// setIPNSTargetUpdates populates the updates map with IPNS target fields
// from an IPNS key, preparing them for a GORM Updates call.
func setIPNSTargetUpdates(updates map[string]any, ipnsKey *pluginDb.IPFSIPNSKey) {
	updates["target_multihash"] = ipnsKey.PeerIDMultihash
	updates["cid_version"] = nil
	updates["cid_type"] = nil
	updates["ipns_key_id"] = ipnsKey.ID
}

// isIPFSCid checks if a string is a valid IPFS CID (not an IPNS peer ID).
func isIPFSCid(hash string) bool {
	_, err := cid.Decode(hash)
	return err == nil
}

// validateTarget validates the target type and hash
func (s *WebsiteServiceDefault) validateTarget(targetType string, targetHash string) error {
	switch pluginDb.WebsiteTargetType(targetType) {
	case pluginDb.WebsiteTargetTypeIPFS:
		// Validate CID format
		_, err := cid.Decode(targetHash)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidCID, err)
		}
	case pluginDb.WebsiteTargetTypeIPNS:
		// Validate IPNS target (peer ID or CIDv1 libp2p-key codec)
		if !isValidIPNSTarget(targetHash) {
			return fmt.Errorf("%w: invalid IPNS target", ErrInvalidIPNS)
		}
	default:
		return fmt.Errorf("%w: invalid type %s", ErrInvalidTarget, targetType)
	}
	return nil
}

// validateIPFSTarget checks if an IPFS CID is pinned and returns an error if not.
func (s *WebsiteServiceDefault) validateIPFSTarget(ctx context.Context, userID uint, targetHash string) error {
	c, err := cid.Decode(targetHash)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCID, err)
	}

	c = encoding.NormalizeCid(c)

	pin, err := s.pinSvc.GetPinByCIDAndUser(ctx, c, userID)
	if err != nil || pin == nil {
		return ErrCIDNotPinned
	}

	if pin.Status != pluginDb.PinningStatusPinned {
		return ErrCIDNotPinned
	}

	return nil
}

// validateIPNSKeyResolution checks that the IPNS key for the given peer ID
// exists and belongs to the website owner.
func (s *WebsiteServiceDefault) validateIPNSKeyResolution(ctx context.Context, userID uint, peerID string) error {
	_, keyUserID, err := s.ipnsKeySvc.GetPrivateKeyByPeerID(ctx, peerID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrIPNSKeyNotFound, err)
	}

	if keyUserID != userID {
		return fmt.Errorf("%w: key belongs to user %d, not %d", ErrIPNSKeyNotFound, keyUserID, userID)
	}

	return nil
}

// generateValidationToken generates a random validation token
func (s *WebsiteServiceDefault) generateValidationToken() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// resolveUserEmail looks up a user's email address by ID. Returns an empty
// string when the user service is unavailable or the account can't be found,
// so notification callers never fail on an unresolvable email.
func (s *WebsiteServiceDefault) resolveUserEmail(ctx context.Context, userID uint) string {
	if s.userSvc == nil {
		return ""
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, u, err := s.userSvc.AccountExists(queryCtx, userID)
	if err != nil || u == nil {
		return ""
	}
	return u.Email
}

// notifyAdminWebsiteCreated sends an email notification to admin when a new website is created.
// The notification must be fired after the primary domain binding exists (the API layer
// creates/points it before calling this) so the domain resolves.
func (s *WebsiteServiceDefault) notifyAdminWebsiteCreated(ctx context.Context, website *pluginDb.Website) error {
	if !s.config.NotificationsEnabled || s.mailerSvc == nil {
		return nil
	}

	if s.config.AdminEmail == "" {
		s.Logger().Debug("Admin email not configured, skipping notification")
		return nil
	}

	primaryDomain := s.primaryDomainName(ctx, website)

	vars := map[string]interface{}{
		"Domain":     primaryDomain,
		"UserEmail":  s.resolveUserEmail(ctx, website.UserID),
		"TargetType": website.TargetType,
		"TargetHash": website.TargetHash(),
		"Status":     website.Status,
		"CreatedAt":  website.CreatedAt.Format(time.RFC3339),
	}

	if err := s.mailerSvc.TemplateSend("website_created_admin", vars, vars, s.config.AdminEmail); err != nil {
		s.Logger().Error("Failed to send website created notification",
			zap.Error(err),
			zap.String("domain", primaryDomain),
			zap.String("admin_email", s.config.AdminEmail))
		return err
	}

	s.Logger().Debug("Website created notification sent",
		zap.String("domain", primaryDomain),
		zap.String("admin_email", s.config.AdminEmail))
	return nil
}

// NotifyAdminWebsiteCreated implements the WebsiteService interface method. It
// reloads the website fresh so the primary domain binding (created by
// CreateDomain) resolves, then fires the admin notification.
func (s *WebsiteServiceDefault) NotifyAdminWebsiteCreated(ctx context.Context, websiteID uint) error {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var website pluginDb.Website
	if err := s.DB().WithContext(queryCtx).First(&website, websiteID).Error; err != nil {
		s.Logger().Warn("Failed to load website for created notification",
			zap.Error(err), zap.Uint("website_id", websiteID))
		return err
	}
	return s.notifyAdminWebsiteCreated(ctx, &website)
}

// notifyAdminWebsiteUpdated sends an email notification to admin when a website is updated
func (s *WebsiteServiceDefault) notifyAdminWebsiteUpdated(ctx context.Context, website *pluginDb.Website, changes map[string]interface{}) error {
	if !s.config.NotificationsEnabled || s.mailerSvc == nil {
		return nil
	}

	if s.config.AdminEmail == "" {
		s.Logger().Debug("Admin email not configured, skipping notification")
		return nil
	}

	primaryDomain := s.primaryDomainName(ctx, website)

	vars := map[string]interface{}{
		"Domain":     primaryDomain,
		"UserEmail":  s.resolveUserEmail(ctx, website.UserID),
		"TargetType": website.TargetType,
		"TargetHash": website.TargetHash(),
		"Status":     website.Status,
		"UpdatedAt":  website.UpdatedAt.Format(time.RFC3339),
		"Changes":    changes,
	}

	if err := s.mailerSvc.TemplateSend("website_updated_admin", vars, vars, s.config.AdminEmail); err != nil {
		s.Logger().Error("Failed to send website updated notification",
			zap.Error(err),
			zap.String("domain", primaryDomain),
			zap.String("admin_email", s.config.AdminEmail))
		return err
	}

	s.Logger().Debug("Website updated notification sent",
		zap.String("domain", primaryDomain),
		zap.String("admin_email", s.config.AdminEmail))
	return nil
}

// notifyUserStatusChanged sends an email notification to user when website status changes
func (s *WebsiteServiceDefault) notifyUserStatusChanged(ctx context.Context, website *pluginDb.Website, oldStatus pluginDb.WebsiteStatus, newStatus pluginDb.WebsiteStatus) error {
	if !s.config.NotificationsEnabled || s.mailerSvc == nil {
		return nil
	}

	userEmail := s.resolveUserEmail(ctx, website.UserID)
	if userEmail == "" {
		s.Logger().Debug("User email not available, skipping notification")
		return nil
	}

	primaryDomain := s.primaryDomainName(ctx, website)

	vars := map[string]interface{}{
		"Domain":     primaryDomain,
		"UserEmail":  userEmail,
		"OldStatus":  string(oldStatus),
		"NewStatus":  string(newStatus),
		"ChangedAt":  time.Now().Format(time.RFC3339),
		"TargetType": website.TargetType,
		"TargetHash": website.TargetHash(),
	}

	if err := s.mailerSvc.TemplateSend("website_status_changed_user", vars, vars, userEmail); err != nil {
		s.Logger().Error("Failed to send website status changed notification",
			zap.Error(err),
			zap.String("domain", primaryDomain),
			zap.String("user_email", userEmail))
		return err
	}

	s.Logger().Debug("Website status changed notification sent",
		zap.String("domain", primaryDomain),
		zap.String("user_email", userEmail),
		zap.String("old_status", string(oldStatus)),
		zap.String("new_status", string(newStatus)))
	return nil
}

// UpdateSSLStatus updates the SSL certificate status for a domain. SSL state is
// a per-domain property, so the update resolves the domain binding (handling
// both the primary and additional-domain records on a website) and persists the
// state on the WebsiteDomain row. The updated binding is returned as the source
// of truth for certificate status.
func (s *WebsiteServiceDefault) UpdateSSLStatus(ctx context.Context, domain string, status pluginDb.SSLStatus, sslError string, timestamp *time.Time) (*pluginDb.WebsiteDomain, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.UpdateSSLStatus")
	defer span.End()

	// Normalize so that a www.-prefixed hostname (e.g. the CDN/certificate
	// hostname) resolves to the stored apex domain record.
	domain = domsvc.NormalizeDomain(domain)

	var wd pluginDb.WebsiteDomain

	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		// Lock the domain binding row for update. The binding is the SSL context:
		// each bound hostname carries its own certificate lifecycle.
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("domain = ?", domain).
			First(&wd).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				_ = tx.AddError(fmt.Errorf("website not found"))
				return tx
			}
			_ = tx.AddError(fmt.Errorf("failed to get website domain: %w", err))
			return tx
		}

		// Use provided timestamp or current time
		updateTime := time.Now()
		if timestamp != nil {
			updateTime = *timestamp
		}

		// Prepare updates
		updates := map[string]interface{}{
			"SSLStatus":        string(status),
			"SSLLastUpdatedAt": updateTime,
		}

		// Set issued_at only when transitioning to ready
		if status == pluginDb.SSLStatusReady && wd.SSLStatus != string(pluginDb.SSLStatusReady) {
			updates["SSLIssuedAt"] = &updateTime
		}

		// Clear issued_at when transitioning away from ready
		if status != pluginDb.SSLStatusReady && wd.SSLStatus == string(pluginDb.SSLStatusReady) {
			updates["SSLIssuedAt"] = nil
		}

		// Set or clear error based on status
		if status == pluginDb.SSLStatusFailed {
			updates["SSLError"] = sslError
		} else {
			updates["SSLError"] = ""
		}

		// Update the domain binding (source of truth for per-domain SSL).
		if err := tx.Model(&wd).Updates(updates).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to update SSL status: %w", err))
			return tx
		}

		// Reload the binding to get updated values.
		if err := tx.First(&wd, wd.ID).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to reload website domain after update: %w", err))
			return tx
		}

		return tx
	})

	if err != nil {
		s.Logger().Error("Failed to update SSL status",
			zap.Error(err),
			zap.String("domain", domain),
			zap.String("ssl_status", string(status)))
		return nil, fmt.Errorf("failed to update SSL status: %w", err)
	}

	return &wd, nil
}

// GetApexDomainBinding returns the website's primary/apex domain binding. The
// apex binding is the one whose domain matches the website's primary domain; its
// SSL state is synthesized onto the website-level response for backward
// compatibility. If the delegated domain service is unavailable or no binding
// matches, it returns gorm.ErrRecordNotFound.
func (s *WebsiteServiceDefault) GetApexDomainBinding(ctx context.Context, websiteID uint) (*pluginDb.WebsiteDomain, error) {
	var website pluginDb.Website
	if err := s.DB().WithContext(ctx).Where("id = ?", websiteID).First(&website).Error; err != nil {
		return nil, err
	}
	if website.PrimaryDomainID == nil {
		return nil, gorm.ErrRecordNotFound
	}
	var apex pluginDb.WebsiteDomain
	if err := s.DB().WithContext(ctx).Where("id = ?", *website.PrimaryDomainID).First(&apex).Error; err != nil {
		return nil, err
	}
	if apex.DeletedAt.Valid {
		return nil, gorm.ErrRecordNotFound
	}
	return &apex, nil
}

// SetPrimaryDomain repoints the website's primary (apex) domain binding to the
// given WebsiteDomain. It validates that the binding belongs to the website and
// that the website is owned by userID, then updates Website.PrimaryDomainID. If
// the binding is already the primary, it's a no-op. Returns the new primary
// binding so callers can chain per-domain operations (e.g. SetDomainDNSEnabled).
func (s *WebsiteServiceDefault) SetPrimaryDomain(ctx context.Context, userID, websiteID, domainID uint) (*pluginDb.WebsiteDomain, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.SetPrimaryDomain")
	defer span.End()

	var wd pluginDb.WebsiteDomain
	if err := s.DB().WithContext(ctx).
		Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
		First(&wd).Error; err != nil {
		return nil, fmt.Errorf("domain lookup failed: %w", err)
	}
	if wd.DeletedAt.Valid {
		return nil, gorm.ErrRecordNotFound
	}

	var website pluginDb.Website
	if err := s.DB().WithContext(ctx).Where("id = ? AND user_id = ?", websiteID, userID).First(&website).Error; err != nil {
		return nil, fmt.Errorf("website lookup failed: %w", err)
	}
	if website.PrimaryDomainID != nil && *website.PrimaryDomainID == wd.ID {
		return &wd, nil
	}

	if err := s.DB().WithContext(ctx).Model(&website).Update("primary_domain_id", wd.ID).Error; err != nil {
		return nil, fmt.Errorf("failed to set primary domain: %w", err)
	}
	return &wd, nil
}

// SetDomainDNSEnabled enables or disables DNS hosting for a specific domain
// binding, running the corresponding enable/disable DNS transition. This is the
// per-domain primitive: one domain on a website can be DNS-managed while
// another is not.
func (s *WebsiteServiceDefault) SetDomainDNSEnabled(ctx context.Context, userID, websiteID, domainID uint, enabled bool) (*pluginDb.WebsiteDomain, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.SetDomainDNSEnabled")
	defer span.End()

	var wd pluginDb.WebsiteDomain
	if err := s.DB().WithContext(ctx).
		Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
		First(&wd).Error; err != nil {
		return nil, fmt.Errorf("domain lookup failed: %w", err)
	}

	// Already in the desired state with nothing left to reconcile. The binding
	// is reconciled when the flag and the zone presence agree: a managed
	// (true) binding has a zone, an unmanaged (false) binding has none. Any
	// disagreement is an orphan left by a partial transition and must be
	// re-reconciled rather than skipped:
	//   - flag true, zone 0: enable-orphan (UpdateWebsite persists the flag
	//     before the transition; the transition may roll the zone back on
	//     failure without reverting the flag).
	//   - flag false, zone non-zero: disable-orphan (the disable transition
	//     clears zone_id only when PowerDNS actually deleted the zone, and
	//     zone-deletion failures are non-fatal).
	// Requiring agreement lets a retry (enable or disable) recover either
	// orphan instead of silently leaking the DNS state.
	//
	// Exception: a delegation-owned binding legitimately holds its zone (zone_id
	// != 0) for alt-root delegation even when dns_hosting_enabled is false — the
	// website-DNS disable path preserves the delegation zone (see
	// handleDNSDisabledTransition), so flag=false + zone!=0 is a valid steady
	// state, not an orphan. Such bindings are always considered reconciled.
	if wd.DNSHostingEnabled == enabled && (wd.DNSHostingEnabled == (wd.ZoneID != 0) || wd.DelegationRecordsOwned()) {
		return &wd, nil
	}

	wd.DNSHostingEnabled = enabled

	// Run the transition first and persist the flag only after it fully
	// succeeds. handleDNSEnabledTransition performs the external DNS/IPNS side
	// effects (zone setup, website DNS records); writing dns_hosting_enabled
	// before it could leave the binding marked DNS-managed with no (or partial)
	// setup applied if the transition fails, and the line-2098 idempotency
	// guard would then make a retry no-op instead of finishing the setup.
	if enabled {
		if err := s.handleDNSEnabledTransition(ctx, &wd); err != nil {
			return nil, err
		}
	} else {
		if err := s.handleDNSDisabledTransition(ctx, &wd); err != nil {
			return nil, err
		}
	}

	if err := s.DB().WithContext(ctx).Model(&wd).Update("dns_hosting_enabled", enabled).Error; err != nil {
		return nil, fmt.Errorf("failed to set dns_hosting_enabled: %w", err)
	}

	// Reload to pick up any zone/IPNS mutations from the transition.
	if err := s.DB().WithContext(ctx).Where("id = ?", wd.ID).First(&wd).Error; err != nil {
		return nil, err
	}
	return &wd, nil
}
