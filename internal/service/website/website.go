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
	"golang.org/x/sync/errgroup"
	"github.com/libp2p/go-libp2p/core/peer"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pluginEvent "go.lumeweb.com/portal-plugin-ipfs/internal/event"
	domsvc "go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"

	"go.lumeweb.com/ipfs-sdk/dnsname"
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

const sslCertValidity = 90 * 24 * time.Hour

func (s *WebsiteServiceDefault) verificationTokenKey() string {
	if s.dnsConfig != nil && s.dnsConfig.VerificationTokenKey != "" {
		return s.dnsConfig.VerificationTokenKey
	}
	return "lumeweb-verify"
}

const (
	msgTokenExpired = "Validation token expired for %s — a new token has been generated. Please add the updated TXT record at %s.%s to your DNS configuration"
	msgDNSMissing   = "No DNS records found for %s. Please add the required TXT records to your DNS configuration"
	msgDNSMismatch  = "DNS validation failed: missing or incorrect dnslink record (expected: %s, found: %s)"
	msgTokenMissing = "DNS validation failed: missing validation token at %s.%s for %s"
	msgValidated    = "DNS validation successful for %s"
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


// NewWebsiteService creates a new website service
func NewWebsiteService() (core.Service, []core.ContextBuilderOption, error) {
	svc := &WebsiteServiceDefault{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			svc.pinSvc = core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
			svc.ipnsKeySvc = core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
			svc.mailerSvc = core.GetService[core.MailerService](ctx, core.MAILER_SERVICE)
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
			// Normalize and validate domain
			website.Domain = domsvc.NormalizeDomain(website.Domain)
			if err := s.validateDomain(website.Domain); err != nil {
				return nil, fmt.Errorf("invalid domain: %w", err)
			}

			// Auto-convert: target_type=ipns with a plain IPFS CID means
			// "create/use IPNS key and publish this CID to it"
			if website.TargetType == string(pluginDb.WebsiteTargetTypeIPNS) && website.CIDVersion != nil {
				publishCID := website.TargetHash()

				ipnsKey, err := s.ensureIPNSKey(ctx, website.UserID, website.Domain, publishCID)
				if err != nil {
					return nil, err
				}

				website.TargetType = string(pluginDb.WebsiteTargetTypeIPNS)
				website.TargetMultihash = ipnsKey.PeerIDMultihash
				website.CIDVersion = nil
				website.CIDType = nil
				website.IPNSKeyID = &ipnsKey.ID
			} else {
				// Validate target type and hash
				if err := s.validateTarget(website.TargetType, website.TargetHash()); err != nil {
					return nil, fmt.Errorf("invalid target: %w", err)
				}
			}

			// Check if domain already exists
			existing, _, err := s.GetWebsiteByDomain(ctx, website.Domain)
			if err != nil && err != gorm.ErrRecordNotFound {
				return nil, fmt.Errorf("failed to check existing domain: %w", err)
			}
			if existing != nil {
				return nil, fmt.Errorf("domain already exists: %s", website.Domain)
			}

			// Inherit SSL state from soft-deleted website if cert is still valid
			var deletedWebsite pluginDb.Website
			err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				return tx.Unscoped().
					Where("domain = ? AND deleted_at IS NOT NULL", website.Domain).
					Where("ssl_status = ?", string(pluginDb.SSLStatusReady)).
					Order("deleted_at DESC").
					First(&deletedWebsite)
			})
			if err == nil && deletedWebsite.SSLIssuedAt != nil {
				if time.Since(*deletedWebsite.SSLIssuedAt) < sslCertValidity {
					website.SSLStatus = string(pluginDb.SSLStatusReady)
					website.SSLIssuedAt = deletedWebsite.SSLIssuedAt
					website.SSLLastUpdatedAt = deletedWebsite.SSLLastUpdatedAt
				}
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

			// Create website in database
			err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				return tx.Create(website)
			})
			if err != nil {
				s.Logger().Error("Failed to create website",
					zap.Error(err),
					zap.String("domain", website.Domain),
					zap.Uint("user_id", website.UserID))
				return nil, fmt.Errorf("failed to create website: %w", err)
			}

			// Auto-create IPNS key for managed DNS when using IPFS target
			if website.Enabled && website.TargetType == string(pluginDb.WebsiteTargetTypeIPFS) {
				ipnsKey, err := s.ensureIPNSKey(ctx, website.UserID, website.Domain, website.TargetHash())
				if err != nil {
					return nil, err
				}

				website.TargetType = string(pluginDb.WebsiteTargetTypeIPNS)
				website.TargetMultihash = ipnsKey.PeerIDMultihash
				website.CIDVersion = nil
				website.CIDType = nil
				website.IPNSKeyID = &ipnsKey.ID

				err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
					return tx.Save(website)
				})
				if err != nil {
					s.Logger().Error("Failed to update website with IPNS target",
						zap.Error(err),
						zap.Uint("website_id", website.ID))
					return nil, fmt.Errorf("failed to update website with IPNS target: %w", err)
				}
			}

			// Create DNS zone if hosting is enabled
			if website.Enabled && s.dnsSvc != nil {
				var dnsZone *pluginDb.DNSZone

				// Check if a parent zone already exists for this user (e.g. pinner.xyz for docs.pinner.xyz)
				if parentDomain := extractParentDomain(website.Domain); parentDomain != "" {
					existingZone, err := s.dnsSvc.GetZoneByDomain(ctx, parentDomain)
					if err == nil && existingZone != nil && existingZone.UserID == website.UserID {
						dnsZone = existingZone
						s.Logger().Info("Reusing existing parent zone for subdomain website",
							zap.String("domain", website.Domain),
							zap.String("parent_zone_domain", parentDomain),
							zap.Uint("zone_id", existingZone.ID))
					}
				}

				// No parent zone found — create one for this domain
				if dnsZone == nil {
					var err error
					dnsZone, err = s.dnsSvc.CreateZone(ctx, website.Domain, website.UserID)
					if err != nil {
						s.Logger().Warn("Failed to create DNS zone for website",
							zap.Error(err),
							zap.String("domain", website.Domain))
						// Continue without DNS zone - website is still created
						dnsZone = nil
					}
				}

				if dnsZone != nil {
					// Update website with DNS zone ID
					website.DNSZoneID = &dnsZone.ID
					err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
						return tx.Save(website)
					})
					if err != nil {
						s.Logger().Error("Failed to update website with DNS zone ID, attempting to clean up DNS zone",
							zap.Error(err),
							zap.Uint("website_id", website.ID),
							zap.Uint("dns_zone_id", dnsZone.ID))

						// Only attempt zone cleanup if we created this zone (not reusing a parent)
						if dnsname.Equal(dnsZone.Domain, website.Domain) {
							if cleanupErr := s.dnsSvc.DeleteZone(ctx, dnsZone.ID); cleanupErr != nil {
								s.Logger().Error("Failed to clean up orphaned DNS zone",
									zap.Error(cleanupErr),
									zap.Uint("dns_zone_id", dnsZone.ID))
								return nil, fmt.Errorf("failed to associate DNS zone with website (and cleanup failed: %w): %w", cleanupErr, err)
							}
						}

						return nil, fmt.Errorf("failed to associate DNS zone with website: %w", err)
					}

					s.Logger().Info("DNS zone associated with website",
						zap.Uint("website_id", website.ID),
						zap.Uint("dns_zone_id", dnsZone.ID),
						zap.String("domain", website.Domain))

					// Create DNS records for the website
					if err := s.dnsSvc.CreateWebsiteDNSRecords(ctx, dnsZone.ID, website.Domain, website.TargetHash(), pluginDb.WebsiteTargetType(website.TargetType), fmt.Sprintf("%s=%s", s.verificationTokenKey(), website.ValidationToken)); err != nil {
						s.Logger().Error("Failed to create DNS records for website",
							zap.Error(err),
							zap.Uint("website_id", website.ID),
							zap.Uint("dns_zone_id", dnsZone.ID))
						// Continue without DNS records - website is still created
					}
				}
			}

			s.Logger().Info("Website created",
				zap.Uint("id", website.ID),
				zap.String("domain", website.Domain),
				zap.Uint("user_id", website.UserID),
				zap.String("target_type", website.TargetType),
				zap.String("target_hash", website.TargetHash()),
				zap.Bool("enabled", website.Enabled))

			// Emit website published event for SSE gateway notification
			core.Fire(s.Context(), pluginEvent.EVENT_WEBSITE_PUBLISHED, pluginEvent.NewWebsitePublishedEvent(
				ctx, website.Domain, website.TargetHash(), website.UserID, website.ID,
			))

			// Send notification to admin
			if err := s.notifyAdminWebsiteCreated(ctx, website, ""); err != nil {
				s.Logger().Warn("Failed to send website created notification", zap.Error(err))
			}

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
			// First: check website_domains (alt-root / delegated domains)
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

			// Fallback: legacy ipfs_websites.domain lookup
			var website pluginDb.Website
			err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				return tx.Where("domain = ?", domain).First(&website)
			})
			if err != nil {
				if err == gorm.ErrRecordNotFound {
					return nil, nil
				}
				return nil, fmt.Errorf("failed to get website by domain: %w", err)
			}

			return &website, nil
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

// UpdateWebsite updates an existing website
func (s *WebsiteServiceDefault) UpdateWebsite(ctx context.Context, userID uint, websiteID uint, updates map[string]any) (*pluginDb.Website, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.UpdateWebsite")
	defer span.End()

	var updatedWebsite *pluginDb.Website
	var oldEnabled bool
	var dnsEnabledChanged bool
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
				oldEnabled = website.Enabled
				targetHashChanged = false
				dnsEnabledChanged = false
				ipnsAutoCreated := false
				var newTargetHashStr string

				// Validate domain if being updated
				if domain, ok := updates["domain"].(string); ok {
					domain = domsvc.NormalizeDomain(domain)
					updates["domain"] = domain
					if err := s.validateDomain(domain); err != nil {
						_ = tx.AddError(fmt.Errorf("invalid domain: %w", err))
						return tx
					}
					// Check if new domain already exists
					var existing pluginDb.Website
					if err := tx.Where("domain = ? AND id != ?", domain, websiteID).First(&existing).Error; err == nil {
						_ = tx.AddError(fmt.Errorf("domain already exists: %s", domain))
						return tx
					}
				}

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

						ipnsKey, err := s.ensureIPNSKey(ctx, website.UserID, website.Domain, publishCID)
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

						ipnsKey, err := s.ensureIPNSKey(ctx, website.UserID, website.Domain, publishCID)
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

				// Check if dns_enabled is being changed with validation
				if dnsEnabledVal, exists := updates["dns_enabled"]; exists {
					newDNSEnabled, ok := dnsEnabledVal.(bool)
					if !ok {
						_ = tx.AddError(fmt.Errorf("dns_enabled must be a boolean"))
						return tx
					}
					dnsEnabledChanged = (newDNSEnabled != oldEnabled)
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
						s.publishCIDAsync(ctx, ipnsKey.PeerID().String(), publishHash, website.Domain)
					}
				}

				// Update DNS records if target changed and DNS hosting is enabled
				// Note: Skip DNS only when staying as IPNS (peer ID doesn't change)
				if targetHashChanged && website.Enabled && website.DNSZoneID != nil && s.dnsSvc != nil {
					newTargetType := pluginDb.WebsiteTargetType(website.TargetType)
					if oldTargetType != pluginDb.WebsiteTargetTypeIPNS || newTargetType != pluginDb.WebsiteTargetTypeIPNS {
						newTargetHash := website.TargetHash()
						if err := s.dnsSvc.UpdateWebsiteDNSRecords(ctx, *website.DNSZoneID, website.Domain, newTargetHash, newTargetType); err != nil {
							s.Logger().Warn("Failed to update DNS records for website",
								zap.Error(err),
								zap.Uint("website_id", websiteID),
								zap.Uint("dns_zone_id", *website.DNSZoneID))
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

	// Handle DNS hosting transitions if dns_enabled changed
	if dnsEnabledChanged {
		if updatedWebsite.Enabled && !oldEnabled {
			// DNS hosting enabled: create zone/records and reset to pending_validation
			if err := s.handleDNSEnabledTransition(ctx, updatedWebsite); err != nil {
				s.Logger().Warn("Failed to handle DNS hosting enable transition",
					zap.Error(err),
					zap.Uint("website_id", websiteID))
				// Continue despite failure - website is updated but DNS setup incomplete
			}
		} else if !updatedWebsite.Enabled && oldEnabled {
			// DNS hosting disabled: delete records and reset to pending_validation
			if err := s.handleDNSDisabledTransition(ctx, updatedWebsite); err != nil {
				s.Logger().Warn("Failed to handle DNS hosting disable transition",
					zap.Error(err),
					zap.Uint("website_id", websiteID))
				// Continue despite failure - website is updated but DNS cleanup incomplete
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
			ctx, updatedWebsite.Domain, updatedWebsite.TargetHash(), updatedWebsite.UserID, updatedWebsite.ID,
		))
	}

	// Send notification to admin
	if err := s.notifyAdminWebsiteUpdated(ctx, updatedWebsite, "", updates); err != nil {
		s.Logger().Warn("Failed to send website updated notification", zap.Error(err))
	}

	return updatedWebsite, nil
}

// handleDNSEnabledTransition handles the transition when DNS hosting is enabled for a website
func (s *WebsiteServiceDefault) handleDNSEnabledTransition(ctx context.Context, website *pluginDb.Website) error {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.handleDNSEnabledTransition")
	defer span.End()

	s.Logger().Info("Handling DNS hosting enable transition",
		zap.Uint("website_id", website.ID),
		zap.String("domain", website.Domain))

	// Create DNS zone if it doesn't exist
	if website.DNSZoneID == nil && s.dnsSvc != nil {
		var dnsZone *pluginDb.DNSZone

		// Check if a parent zone already exists for this user (e.g. pinner.xyz for docs.pinner.xyz)
		if parentDomain := extractParentDomain(website.Domain); parentDomain != "" {
			existingZone, err := s.dnsSvc.GetZoneByDomain(ctx, parentDomain)
			if err == nil && existingZone != nil && existingZone.UserID == website.UserID {
				dnsZone = existingZone
				s.Logger().Info("Reusing existing parent zone for subdomain website",
					zap.String("domain", website.Domain),
					zap.String("parent_zone_domain", parentDomain),
					zap.Uint("zone_id", existingZone.ID))
			}
		}

		// No parent zone found — create one for this domain
		var createErr error
		if dnsZone == nil {
			dnsZone, createErr = s.dnsSvc.CreateZone(ctx, website.Domain, website.UserID)
			if createErr != nil {
				return fmt.Errorf("failed to create DNS zone: %w", createErr)
			}
		}

		// Update website with DNS zone ID
		err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			return tx.Model(website).Update("dns_zone_id", dnsZone.ID)
		})
		if err != nil {
			s.Logger().Error("Failed to update website with DNS zone ID",
				zap.Error(err),
				zap.Uint("website_id", website.ID))
			// Only attempt zone cleanup if we created this zone (not reusing a parent)
			if dnsname.Equal(dnsZone.Domain, website.Domain) {
				_ = s.dnsSvc.DeleteZone(ctx, dnsZone.ID)
			}
			return fmt.Errorf("failed to associate DNS zone with website: %w", err)
		}

		website.DNSZoneID = &dnsZone.ID
		s.Logger().Info("DNS zone associated with website",
			zap.Uint("website_id", website.ID),
			zap.Uint("dns_zone_id", dnsZone.ID),
			zap.String("domain", website.Domain))
	}

	// Create DNS records if zone exists
	if website.DNSZoneID != nil && s.dnsSvc != nil {
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

		// Create DNS records
		err := s.dnsSvc.CreateWebsiteDNSRecords(ctx, *website.DNSZoneID, website.Domain, website.TargetHash(), pluginDb.WebsiteTargetType(website.TargetType), fmt.Sprintf("%s=%s", s.verificationTokenKey(), newToken))
		if err != nil {
			return fmt.Errorf("failed to create DNS records: %w", err)
		}

		// Update website with new token and status
		expiresAt := time.Now().Add(s.config.ValidationTokenTTL)
		err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			return tx.Model(website).Updates(map[string]interface{}{
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
			zap.String("domain", website.Domain))
	}

	return nil
}

// handleDNSDisabledTransition handles the transition when DNS hosting is disabled for a website
func (s *WebsiteServiceDefault) handleDNSDisabledTransition(ctx context.Context, website *pluginDb.Website) error {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.handleDNSDisabledTransition")
	defer span.End()

	s.Logger().Info("Handling DNS hosting disable transition",
		zap.Uint("website_id", website.ID),
		zap.String("domain", website.Domain))

	recordsDeleted := false

	// Delete DNS records for this website (not the zone — other websites may share it)
	if website.DNSZoneID != nil && s.dnsSvc != nil {
		err := s.dnsSvc.DeleteWebsiteDNSRecords(ctx, *website.DNSZoneID, website.Domain)
		if err != nil {
			s.Logger().Warn("Failed to delete DNS records for website",
				zap.Error(err),
				zap.Uint("website_id", website.ID),
				zap.Uint("dns_zone_id", *website.DNSZoneID))
		} else {
			recordsDeleted = true
			s.Logger().Info("DNS records deleted for website",
				zap.Uint("website_id", website.ID),
				zap.Uint("dns_zone_id", *website.DNSZoneID))
		}
	}

	// Check if any other websites still reference this zone
	zoneIsEmpty := false
	if recordsDeleted && website.DNSZoneID != nil {
		var count int64
		err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			return tx.Model(&pluginDb.Website{}).
				Where("dns_zone_id = ? AND id != ? AND deleted_at IS NULL", *website.DNSZoneID, website.ID).
				Count(&count)
		})
		if err != nil {
			s.Logger().Warn("Failed to count websites sharing DNS zone",
				zap.Error(err),
				zap.Uint("dns_zone_id", *website.DNSZoneID))
		} else {
			zoneIsEmpty = count == 0
		}
	}

	// Only delete the zone if no other websites are using it
	zoneDeleted := false
	if zoneIsEmpty && website.DNSZoneID != nil && s.dnsSvc != nil {
		err := s.dnsSvc.DeleteZone(ctx, *website.DNSZoneID)
		if err != nil {
			s.Logger().Warn("Failed to delete DNS zone for website",
				zap.Error(err),
				zap.Uint("website_id", website.ID),
				zap.Uint("dns_zone_id", *website.DNSZoneID))
		} else {
			zoneDeleted = true
			s.Logger().Info("DNS zone deleted (no other websites using it)",
				zap.Uint("website_id", website.ID),
				zap.Uint("dns_zone_id", *website.DNSZoneID))
		}
	}

	// Reset status to pending_validation
	// Only clear dns_zone_id if the zone was actually deleted from PowerDNS
	updates := map[string]interface{}{
		"status": string(pluginDb.WebsiteStatusPendingValidation),
	}
	if zoneDeleted {
		updates["dns_zone_id"] = nil
	}

	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Model(website).Updates(updates)
	})
	if err != nil {
		s.Logger().Error("Failed to update website",
			zap.Error(err),
			zap.Uint("website_id", website.ID))
		return fmt.Errorf("failed to update website: %w", err)
	}

	s.Logger().Info("DNS hosting disabled, website reset to pending_validation",
		zap.Uint("website_id", website.ID),
		zap.String("domain", website.Domain),
		zap.Bool("zone_deleted", zoneDeleted))

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
			var dnsZoneID *uint
			var websiteDomain string

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

				// Store DNS zone ID and domain for cleanup
				dnsZoneID = website.DNSZoneID
				websiteDomain = website.Domain

				// Perform the soft delete
				result := tx.Delete(&website)
				if result.Error != nil {
					_ = tx.AddError(fmt.Errorf("failed to delete website: %w", result.Error))
					return tx
				}
				count = result.RowsAffected
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

			// Clean up DNS records if DNS hosting was enabled
			// Note: We do NOT delete the zone itself as zones are independent from websites
			if dnsZoneID != nil && s.dnsSvc != nil {
				if err := s.dnsSvc.DeleteWebsiteDNSRecords(ctx, *dnsZoneID, websiteDomain); err != nil {
					s.Logger().Warn("Failed to delete DNS records for website",
						zap.Error(err),
						zap.Uint("website_id", websiteID),
						zap.Uint("dns_zone_id", *dnsZoneID))
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

// ValidateDNS validates the DNS TXT record for a website domain
func (s *WebsiteServiceDefault) shouldPerformTokenCheck(website *pluginDb.Website) bool {
	needs := website.Status == string(pluginDb.WebsiteStatusPendingValidation)
	if needs && s.delegatedDomainSvc != nil && s.delegatedDomainSvc.UsesDelegationForOwnership(website.Domain) {
		s.Logger().Debug("skipping TXT token (ownership proven via delegation verification)", zap.String("domain", website.Domain))
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

			needsTokenCheck := s.shouldPerformTokenCheck(&website)

			if needsTokenCheck && website.IsExpired() {
				if err := s.regenerateExpiredToken(ctx, &website); err != nil {
					return pluginCore.ValidateDNSResult{}, err
				}
				return pluginCore.ValidateDNSResult{
					Valid:   false,
					Message: fmt.Sprintf(msgTokenExpired, website.Domain, s.verificationTokenKey(), website.Domain),
					Reason:  pluginCore.ValidationReasonTokenExpired,
				}, nil
			}

			result, err := s.resolverForDomain(website.Domain).ResolveDNSLink(website.Domain)
			if err != nil {
				if dnsErr, ok := errors.AsType[dnslink.DNSRCodeError](err); ok && dnsErr.DNSRCode == 3 {
					s.Logger().Debug("DNS validation failed: no DNS records found (NXDOMAIN)",
						zap.Error(err),
						zap.String("domain", website.Domain),
						zap.Uint("website_id", website.ID))
					return pluginCore.ValidateDNSResult{
						Valid:   false,
						Message: fmt.Sprintf(msgDNSMissing, website.Domain),
						Reason:  pluginCore.ValidationReasonDNSMissing,
					}, nil
				}

				return pluginCore.ValidateDNSResult{}, fmt.Errorf("DNS lookup failed for %s: %w", website.Domain, err)
			}

			if ok, msg, reason := s.checkDNSLinkMatch(&website, result); !ok {
				return pluginCore.ValidateDNSResult{
					Valid:   false,
					Message: msg,
					Reason:  reason,
				}, nil
			}

			_ = s.determineFoundDNSLink(result, &website)

			if needsTokenCheck {
				if ok, msg, reason, err := s.checkValidationToken(ctx, &website); err != nil {
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
				Message: fmt.Sprintf(msgValidated, website.Domain),
				Reason:  pluginCore.ValidationReasonValidated,
			}, nil
		},
	)
}

func (s *WebsiteServiceDefault) checkDNSLinkMatch(website *pluginDb.Website, result dnslink.Result) (bool, string, pluginCore.ValidationReason) {
	expectedDNSlink := pluginDb.WebsiteTargetType(website.TargetType).ToDNSLinkPath(website.TargetHash())

	var foundDNSlink string
	if ipfsLinks, ok := result.Links["ipfs"]; ok && len(ipfsLinks) > 0 {
		foundDNSlink = dto.IPFSPath(ipfsLinks[0].Identifier)
		if foundDNSlink == expectedDNSlink {
			s.Logger().Debug("Found valid DNSlink record",
				zap.String("domain", website.Domain),
				zap.String("dnslink", foundDNSlink))
			return true, "", ""
		}
	}
	if ipnsLinks, ok := result.Links["ipns"]; ok && len(ipnsLinks) > 0 {
		foundDNSlink = dto.IPNSPath(ipnsLinks[0].Identifier)
		if foundDNSlink == expectedDNSlink {
			s.Logger().Debug("Found valid DNSlink record",
				zap.String("domain", website.Domain),
				zap.String("dnslink", foundDNSlink))
			return true, "", ""
		}
	}

	s.Logger().Warn("DNS validation failed: missing or incorrect dnslink record",
		zap.String("domain", website.Domain),
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

func (s *WebsiteServiceDefault) checkValidationToken(ctx context.Context, website *pluginDb.Website) (bool, string, pluginCore.ValidationReason, error) {
	expectedTokenRecord := fmt.Sprintf("%s=%s", s.verificationTokenKey(), website.ValidationToken)
	txtRecords, err := s.resolverForDomain(website.Domain).LookupTXT(ctx, s.verificationTokenKey()+"."+website.Domain)
	if err != nil {
		return false, "", "", fmt.Errorf("DNS TXT lookup failed for %s.%s: %w", s.verificationTokenKey(), website.Domain, err)
	}

	for _, txtRecord := range txtRecords {
		if strings.Contains(txtRecord, expectedTokenRecord) {
			s.Logger().Debug("Found valid validation token",
				zap.String("domain", website.Domain),
				zap.String("token", website.ValidationToken))
			return true, "", "", nil
		}
	}

	s.Logger().Warn("DNS validation failed: missing validation token",
		zap.String("domain", website.Domain),
		zap.String("expected_token", website.ValidationToken))
	return false, fmt.Sprintf(msgTokenMissing, s.verificationTokenKey(), website.Domain, website.Domain), pluginCore.ValidationReasonTokenMissing, nil
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

func (s *WebsiteServiceDefault) regenerateExpiredToken(ctx context.Context, website *pluginDb.Website) error {
	newToken, err := s.generateValidationToken()
	if err != nil {
		return fmt.Errorf("failed to regenerate expired validation token: %w", err)
	}

	if website.DNSZoneID != nil && s.dnsSvc != nil {
		tokenRecord := fmt.Sprintf("%s=%s", s.verificationTokenKey(), newToken)
		if err := s.dnsSvc.CreateWebsiteDNSRecords(ctx, *website.DNSZoneID, website.Domain, website.TargetHash(), pluginDb.WebsiteTargetType(website.TargetType), tokenRecord); err != nil {
			s.Logger().Warn("Failed to update DNS records with new validation token",
				zap.Error(err),
				zap.Uint("website_id", website.ID),
				zap.String("domain", website.Domain))
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
		zap.String("domain", website.Domain))

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
				if err := s.notifyUserStatusChanged(ctx, website, "", oldStatus, newStatus); err != nil {
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

// notifyAdminWebsiteCreated sends an email notification to admin when a new website is created
func (s *WebsiteServiceDefault) notifyAdminWebsiteCreated(ctx context.Context, website *pluginDb.Website, userEmail string) error {
	if !s.config.NotificationsEnabled || s.mailerSvc == nil {
		return nil
	}

	if s.config.AdminEmail == "" {
		s.Logger().Debug("Admin email not configured, skipping notification")
		return nil
	}

	vars := map[string]interface{}{
		"Domain":     website.Domain,
		"UserEmail":  userEmail,
		"TargetType": website.TargetType,
		"TargetHash": website.TargetHash(),
		"Status":     website.Status,
		"CreatedAt":  website.CreatedAt.Format(time.RFC3339),
	}

	if err := s.mailerSvc.TemplateSend("website_created_admin", vars, vars, s.config.AdminEmail); err != nil {
		s.Logger().Error("Failed to send website created notification",
			zap.Error(err),
			zap.String("domain", website.Domain),
			zap.String("admin_email", s.config.AdminEmail))
		return err
	}

	s.Logger().Debug("Website created notification sent",
		zap.String("domain", website.Domain),
		zap.String("admin_email", s.config.AdminEmail))
	return nil
}

// notifyAdminWebsiteUpdated sends an email notification to admin when a website is updated
func (s *WebsiteServiceDefault) notifyAdminWebsiteUpdated(ctx context.Context, website *pluginDb.Website, userEmail string, changes map[string]interface{}) error {
	if !s.config.NotificationsEnabled || s.mailerSvc == nil {
		return nil
	}

	if s.config.AdminEmail == "" {
		s.Logger().Debug("Admin email not configured, skipping notification")
		return nil
	}

	vars := map[string]interface{}{
		"Domain":     website.Domain,
		"UserEmail":  userEmail,
		"TargetType": website.TargetType,
		"TargetHash": website.TargetHash(),
		"Status":     website.Status,
		"UpdatedAt":  website.UpdatedAt.Format(time.RFC3339),
		"Changes":    changes,
	}

	if err := s.mailerSvc.TemplateSend("website_updated_admin", vars, vars, s.config.AdminEmail); err != nil {
		s.Logger().Error("Failed to send website updated notification",
			zap.Error(err),
			zap.String("domain", website.Domain),
			zap.String("admin_email", s.config.AdminEmail))
		return err
	}

	s.Logger().Debug("Website updated notification sent",
		zap.String("domain", website.Domain),
		zap.String("admin_email", s.config.AdminEmail))
	return nil
}

// notifyUserStatusChanged sends an email notification to user when website status changes
func (s *WebsiteServiceDefault) notifyUserStatusChanged(ctx context.Context, website *pluginDb.Website, userEmail string, oldStatus pluginDb.WebsiteStatus, newStatus pluginDb.WebsiteStatus) error {
	if !s.config.NotificationsEnabled || s.mailerSvc == nil {
		return nil
	}

	if userEmail == "" {
		s.Logger().Debug("User email not available, skipping notification")
		return nil
	}

	vars := map[string]interface{}{
		"Domain":     website.Domain,
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
			zap.String("domain", website.Domain),
			zap.String("user_email", userEmail))
		return err
	}

	s.Logger().Debug("Website status changed notification sent",
		zap.String("domain", website.Domain),
		zap.String("user_email", userEmail),
		zap.String("old_status", string(oldStatus)),
		zap.String("new_status", string(newStatus)))
	return nil
}

// UpdateSSLStatus updates the SSL certificate status for a website domain
func (s *WebsiteServiceDefault) UpdateSSLStatus(ctx context.Context, domain string, status pluginDb.SSLStatus, sslError string, timestamp *time.Time) (*pluginDb.Website, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.UpdateSSLStatus")
	defer span.End()

	// Normalize so that a www.-prefixed hostname (e.g. the CDN/certificate
	// hostname) resolves to the stored apex domain record.
	domain = domsvc.NormalizeDomain(domain)

	var website pluginDb.Website
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		// Lock row for update
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("domain = ?", domain).
			First(&website).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				_ = tx.AddError(fmt.Errorf("website not found"))
				return tx
			}
			_ = tx.AddError(fmt.Errorf("failed to get website: %w", err))
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
		if status == pluginDb.SSLStatusReady && website.SSLStatus != string(pluginDb.SSLStatusReady) {
			updates["SSLIssuedAt"] = &updateTime
		}

		// Clear issued_at when transitioning away from ready
		if status != pluginDb.SSLStatusReady && website.SSLStatus == string(pluginDb.SSLStatusReady) {
			updates["SSLIssuedAt"] = nil
		}

		// Set or clear error based on status
		if status == pluginDb.SSLStatusFailed {
			updates["SSLError"] = sslError
		} else {
			updates["SSLError"] = ""
		}

		// Update website
		if err := tx.Model(&website).Updates(updates).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to update SSL status: %w", err))
			return tx
		}

		// Reload the website to get updated values
		if err := tx.First(&website, website.ID).Error; err != nil {
			_ = tx.AddError(fmt.Errorf("failed to reload website after update: %w", err))
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

	return &website, nil
}
