package website

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	dnslink "github.com/dnslink-std/go"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
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

// Validation error types
var (
	ErrInvalidCID    = errors.New("invalid CID")
	ErrInvalidIPNS   = errors.New("invalid IPNS name")
	ErrInvalidTarget = errors.New("invalid target")
	ErrInvalidDomain = errors.New("invalid domain")
)

// WebsiteServiceDefault implements the WebsiteService interface
type WebsiteServiceDefault struct {
	*core.BaseComponent
	pinSvc           pluginCore.IPFSPinService
	ipnsKeySvc       pluginCore.IPNSKeyService
	mailerSvc        core.MailerService
	dnsSvc           pluginCore.DNSService
	config           *pluginConfig.WebsiteConfig
}

// Ensure WebsiteServiceDefault implements the interface
var _ pluginCore.WebsiteService = (*WebsiteServiceDefault)(nil)

// NewWebsiteService creates a new website service
func NewWebsiteService() (core.Service, []core.ContextBuilderOption, error) {
	svc := &WebsiteServiceDefault{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			svc.pinSvc = core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
			svc.ipnsKeySvc = core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
			svc.mailerSvc = core.GetService[core.MailerService](ctx, core.MAILER_SERVICE)
			svc.dnsSvc = core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)

			// Load configuration from service config
			svc.config = core.GetServiceConfig[*pluginConfig.WebsiteConfig](ctx, pluginCore.WEBSITE_SERVICE)

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
			// Validate domain name format
			if err := s.validateDomain(website.Domain); err != nil {
				return nil, fmt.Errorf("invalid domain: %w", err)
			}

			// Validate target type and hash
			if err := s.validateTarget(website.TargetType, website.TargetHash()); err != nil {
				return nil, fmt.Errorf("invalid target: %w", err)
			}

			// Check if domain already exists
			existing, err := s.GetWebsiteByDomain(ctx, website.Domain)
			if err != nil && err != gorm.ErrRecordNotFound {
				return nil, fmt.Errorf("failed to check existing domain: %w", err)
			}
			if existing != nil {
				return nil, fmt.Errorf("domain already exists: %s", website.Domain)
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
			var ipnsKey *pluginDb.IPFSIPNSKey
			if website.Enabled && website.TargetType == string(pluginDb.WebsiteTargetTypeIPFS) {
				// Generate IPNS key name based on domain
				keyName := fmt.Sprintf("%s-auto", website.Domain)

				// Check if IPNS key with this name already exists
				keys, err := s.ipnsKeySvc.ListKeys(ctx, website.UserID)
				if err != nil {
					s.Logger().Error("Failed to list existing IPNS keys",
						zap.Error(err),
						zap.String("domain", website.Domain))
					return nil, fmt.Errorf("failed to list existing IPNS keys: %w", err)
				}
				for _, k := range keys {
					if k.Name == keyName {
						ipnsKey = &k
						break
					}
				}

				// Create IPNS key if it doesn't exist
				if ipnsKey == nil {
					ipnsKey, err = s.ipnsKeySvc.CreateKey(ctx, website.UserID, keyName, 1)
					if err != nil {
						s.Logger().Error("Failed to create IPNS key for managed DNS",
							zap.Error(err),
							zap.String("domain", website.Domain))
						return nil, fmt.Errorf("failed to create IPNS key: %w", err)
					}
					s.Logger().Info("Created new IPNS key for managed DNS",
						zap.String("domain", website.Domain),
						zap.String("key_name", keyName),
						zap.Stringer("peer_id", ipnsKey.PeerID()))
				} else {
					s.Logger().Info("Reusing existing IPNS key for managed DNS",
						zap.String("domain", website.Domain),
						zap.String("key_name", keyName),
						zap.Stringer("peer_id", ipnsKey.PeerID()))
				}
				if err != nil {
					s.Logger().Error("Failed to create IPNS key for managed DNS",
						zap.Error(err),
						zap.String("domain", website.Domain))
					return nil, fmt.Errorf("failed to create IPNS key: %w", err)
				}

				// Publish the IPFS CID to the IPNS key
				if s.ipnsKeySvc != nil {
					// Use default TTL (24 hours)
					ttl := 24 * time.Hour
					err = s.ipnsKeySvc.PublishCID(ctx, ipnsKey.PeerID().String(), website.TargetHash(), ttl)
					if err != nil {
						s.Logger().Warn("Failed to publish CID to IPNS key for managed DNS",
							zap.Error(err),
							zap.String("domain", website.Domain),
							zap.String("peer_id", ipnsKey.PeerID().String()))
						// Continue - DNS records will still be created
					} else {
						s.Logger().Info("Published CID to IPNS key for managed DNS",
							zap.String("domain", website.Domain),
							zap.String("peer_id", ipnsKey.PeerID().String()),
							zap.String("cid", website.TargetHash()))
					}
				}

				// Update website to use IPNS target instead of IPFS
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
				dnsZone, err := s.dnsSvc.CreateZone(ctx, website.Domain, website.UserID)
				if err != nil {
					s.Logger().Warn("Failed to create DNS zone for website",
						zap.Error(err),
						zap.String("domain", website.Domain))
					// Continue without DNS zone - website is still created
				} else {
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

						// Attempt to clean up the created zone to prevent orphans.
						if cleanupErr := s.dnsSvc.DeleteZone(ctx, dnsZone.ID); cleanupErr != nil {
							s.Logger().Error("Failed to clean up orphaned DNS zone",
								zap.Error(cleanupErr),
								zap.Uint("dns_zone_id", dnsZone.ID))
							// Return a wrapped error to inform the caller that cleanup failed and a resource may be orphaned.
							return nil, fmt.Errorf("failed to associate DNS zone with website (and cleanup failed: %w): %w", cleanupErr, err)
						}

						return nil, fmt.Errorf("failed to associate DNS zone with website: %w", err)
					}

					s.Logger().Info("DNS zone created for website",
						zap.Uint("website_id", website.ID),
						zap.Uint("dns_zone_id", dnsZone.ID),
						zap.String("domain", website.Domain))

					// Create DNS records for the website
					if err := s.dnsSvc.CreateWebsiteDNSRecords(ctx, dnsZone.ID, website.TargetHash(), pluginDb.WebsiteTargetType(website.TargetType), website.ValidationToken); err != nil {
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

// GetWebsiteByDomain retrieves a website by domain name
func (s *WebsiteServiceDefault) GetWebsiteByDomain(ctx context.Context, domain string) (*pluginDb.Website, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.GetWebsiteByDomain")
	defer span.End()

	return core.MetricTrackResult(
		GetWebsiteByDomainDuration.WithLabelValues(),
		GetWebsiteByDomainTotal.WithLabelValues(LabelStatusError),
		func() (*pluginDb.Website, error) {
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
func (s *WebsiteServiceDefault) UpdateWebsite(ctx context.Context, userID uint, websiteID uint, updates map[string]interface{}) (*pluginDb.Website, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.UpdateWebsite")
	defer span.End()

	var updatedWebsite *pluginDb.Website

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

				// Store old values for DNS update
				oldTargetHash := website.TargetHash()
				targetHashChanged := false

				// Validate domain if being updated
				if domain, ok := updates["domain"].(string); ok {
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

					// Validate target hash
					if err := s.validateTarget(targetType, targetHashStr); err != nil {
						_ = tx.AddError(fmt.Errorf("invalid target: %w", err))
						return tx
					}

					// Check if target hash changed
					if targetHashStr != oldTargetHash {
						targetHashChanged = true
					}

					// Convert string to multihash and CID version
					if targetType == string(pluginDb.WebsiteTargetTypeIPFS) {
						c, err := cid.Decode(targetHashStr)
						if err != nil {
							_ = tx.AddError(fmt.Errorf("failed to decode CID: %w", err))
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

				// Apply updates
				if err := tx.Model(&website).Updates(updates).Error; err != nil {
					_ = tx.AddError(fmt.Errorf("failed to update website: %w", err))
					return tx
				}

				updatedWebsite = &website

				// If target hash changed and website has auto-created IPNS key, republish to IPNS
				if targetHashChanged && website.IPNSKeyID != nil {
					ipnsKey, err := s.ipnsKeySvc.GetKeyByID(ctx, website.UserID, *website.IPNSKeyID)
					if err != nil {
						s.Logger().Warn("Failed to get IPNS key for republishing",
							zap.Error(err),
							zap.Uint("website_id", websiteID),
							zap.Uint("ipns_key_id", *website.IPNSKeyID))
					} else {
						// Get the new target hash (from updates, since website was just updated)
						newTargetHash := oldTargetHash
						if targetHashStr, ok := updates["target_hash"].(string); ok {
							newTargetHash = targetHashStr
						}

						// Publish the new CID to the IPNS key
						ttl := 24 * time.Hour
						err = s.ipnsKeySvc.PublishCID(ctx, ipnsKey.PeerID().String(), newTargetHash, ttl)
						if err != nil {
							s.Logger().Warn("Failed to republish new CID to IPNS key",
								zap.Error(err),
								zap.String("domain", website.Domain),
								zap.String("peer_id", ipnsKey.PeerID().String()),
								zap.String("cid", newTargetHash))
						} else {
							s.Logger().Info("Republished new CID to IPNS key",
								zap.String("domain", website.Domain),
								zap.String("peer_id", ipnsKey.PeerID().String()),
								zap.String("cid", newTargetHash))
						}
					}
				}

				// Update DNS records if target changed and DNS hosting is enabled
				// Note: For IPNS targets, DNS records don't need updating since the peer ID stays the same
				if targetHashChanged && website.Enabled && website.DNSZoneID != nil && s.dnsSvc != nil {
					// Only update DNS if not using IPNS (IPNS peer ID doesn't change)
					if website.IPNSKeyID == nil {
						newTargetHash := website.TargetHash()
						newTargetType := pluginDb.WebsiteTargetType(website.TargetType)
						if err := s.dnsSvc.UpdateWebsiteDNSRecords(ctx, *website.DNSZoneID, newTargetHash, newTargetType); err != nil {
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

	s.Logger().Info("Website updated",
		zap.Uint("id", websiteID),
		zap.Uint("user_id", userID),
		zap.Any("updates", updates))

	// Send notification to admin
	if err := s.notifyAdminWebsiteUpdated(ctx, updatedWebsite, "", updates); err != nil {
		s.Logger().Warn("Failed to send website updated notification", zap.Error(err))
	}

	return updatedWebsite, nil
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

				// Store DNS zone ID for cleanup
				dnsZoneID = website.DNSZoneID

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

			// Clean up DNS records if DNS hosting was enabled
			// Note: We do NOT delete the zone itself as zones are independent from websites
			if dnsZoneID != nil && s.dnsSvc != nil {
				if err := s.dnsSvc.DeleteWebsiteDNSRecords(ctx, *dnsZoneID); err != nil {
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

// ValidateDNS validates the DNS TXT record for a website domain
func (s *WebsiteServiceDefault) ValidateDNS(ctx context.Context, userID uint, websiteID uint) (bool, error) {
	ctx, span := core.TraceMethod(ctx, "WebsiteServiceDefault.ValidateDNS")
	defer span.End()

	return core.MetricTrackResult(
		ValidateDNSDuration.WithLabelValues(),
		ValidateDNSTotal.WithLabelValues(LabelStatusError),
		func() (bool, error) {
			var validated bool

			err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				// Get the website
				var website pluginDb.Website
				if err := tx.Where("user_id = ? AND id = ?", userID, websiteID).First(&website).Error; err != nil {
					if err == gorm.ErrRecordNotFound {
						_ = tx.AddError(fmt.Errorf("website not found"))
						return tx
					}
					_ = tx.AddError(fmt.Errorf("failed to get website: %w", err))
					return tx
				}

				// Check if validation token is expired
				if website.IsExpired() {
					_ = tx.AddError(fmt.Errorf("validation token expired"))
					return tx
				}

				// Query DNS for TXT records using dnslink library
				// The library handles the _dnslink. prefix automatically
				result, err := dnslink.Resolve(website.Domain)
				if err != nil {
					// Check if this is an NXDOMAIN error (no DNS records exist)
					// This typically means the user hasn't configured their DNS yet
					if dnsErr, ok := errors.AsType[dnslink.DNSRCodeError](err); ok && dnsErr.DNSRCode == 3 {
						s.Logger().Debug("DNS validation failed: no DNS records found (NXDOMAIN)",
							zap.Error(err),
							zap.String("domain", website.Domain),
							zap.Uint("website_id", website.ID))
						_ = tx.AddError(fmt.Errorf("DNS validation failed: no DNS records found for %s. Please add the required TXT records to your DNS configuration", website.Domain))
						return tx
					}

					// Other DNS errors (timeout, server error, etc.)
					s.Logger().Debug("DNS lookup failed",
						zap.Error(err),
						zap.String("domain", website.Domain),
						zap.Uint("website_id", website.ID))
					_ = tx.AddError(fmt.Errorf("DNS lookup failed for %s: %w", website.Domain, err))
					return tx
				}

				// Build expected DNSlink value based on target type
				expectedDNSlink := pluginDb.WebsiteTargetType(website.TargetType).ToDNSLinkPath(website.TargetHash())

				// Check for BOTH required records
				var hasDNSlink, hasToken bool
				var foundDNSlink string

				// Check dnslink records from result.Links
				if ipfsLinks, ok := result.Links["ipfs"]; ok && len(ipfsLinks) > 0 {
					foundDNSlink = dto.IPFSPath(ipfsLinks[0].Identifier)
					if foundDNSlink == expectedDNSlink {
						hasDNSlink = true
						s.Logger().Debug("Found valid DNSlink record",
							zap.String("domain", website.Domain),
							zap.String("dnslink", foundDNSlink))
					}
				}
				if ipnsLinks, ok := result.Links["ipns"]; ok && len(ipnsLinks) > 0 {
					foundDNSlink = dto.IPNSPath(ipnsLinks[0].Identifier)
					if foundDNSlink == expectedDNSlink {
						hasDNSlink = true
						s.Logger().Debug("Found valid DNSlink record",
							zap.String("domain", website.Domain),
							zap.String("dnslink", foundDNSlink))
					}
				}

				// Check for validation token in raw TXT entries
				expectedTokenRecord := fmt.Sprintf("%s=%s", s.config.VerificationTokenKey, website.ValidationToken)
				for _, txtEntry := range result.TxtEntries {
					if strings.Contains(txtEntry.Value, expectedTokenRecord) {
						hasToken = true
						s.Logger().Debug("Found valid validation token",
							zap.String("domain", website.Domain),
							zap.String("token", website.ValidationToken))
						break
					}
				}

				// Both records must be present and correct
				if !hasDNSlink {
					s.Logger().Warn("DNS validation failed: missing or incorrect dnslink record",
						zap.String("domain", website.Domain),
						zap.String("expected", expectedDNSlink),
						zap.String("found", foundDNSlink))
					_ = tx.AddError(fmt.Errorf("DNS validation failed: missing or incorrect dnslink record (expected: %s, found: %s)", expectedDNSlink, foundDNSlink))
					return tx
				}

				if !hasToken {
					s.Logger().Warn("DNS validation failed: missing validation token",
						zap.String("domain", website.Domain),
						zap.String("expected_token", website.ValidationToken))
					_ = tx.AddError(fmt.Errorf("DNS validation failed: missing validation token"))
					return tx
				}

				// Validation passes - both records are present and correct
				validated = true
				s.Logger().Info("DNS validation successful",
					zap.String("domain", website.Domain),
					zap.Uint("website_id", website.ID),
					zap.String("dnslink", foundDNSlink))

				// Update status to active
				website.Status = string(pluginDb.WebsiteStatusActive)
				if err := tx.Save(&website).Error; err != nil {
					_ = tx.AddError(fmt.Errorf("failed to update website status: %w", err))
					return tx
				}

				return tx
			})

			if err != nil {
				return false, err
			}

			s.Logger().Info("DNS validation completed",
				zap.Uint("website_id", websiteID),
				zap.Uint("user_id", userID),
				zap.Bool("validated", validated))

			return validated, nil
		},
	)
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
				valid, err := s.validateIPFSTarget(ctx, website.TargetHash())
				if err != nil {
					s.Logger().Error("Failed to validate IPFS target",
						zap.Error(err),
						zap.String("target_hash", website.TargetHash()))
					return pluginDb.WebsiteStatusBroken, fmt.Errorf("failed to validate IPFS target: %w", err)
				}
				if valid {
					newStatus = pluginDb.WebsiteStatusActive
				} else {
					newStatus = pluginDb.WebsiteStatusBroken
				}

			case pluginDb.WebsiteTargetTypeIPNS:
				// For IPNS targets, check if the key exists
				valid, err := s.validateIPNSTarget(ctx, website.TargetHash())
				if err != nil {
					s.Logger().Error("Failed to validate IPNS target",
						zap.Error(err),
						zap.String("target_hash", website.TargetHash()))
					return pluginDb.WebsiteStatusBroken, fmt.Errorf("failed to validate IPNS target: %w", err)
				}
				if valid {
					newStatus = pluginDb.WebsiteStatusActive
				} else {
					newStatus = pluginDb.WebsiteStatusBroken
				}

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
		// Try peer ID decode first (IPNS uses libp2p peer IDs in base36)
		_, err := peer.Decode(targetHash)
		if err != nil {
			// FALLBACK: Try CID decode (supports CIDv1 with libp2p-key codec)
			_, err := cid.Decode(targetHash)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrInvalidIPNS, err)
			}
		}
	default:
		return fmt.Errorf("%w: invalid type %s", ErrInvalidTarget, targetType)
	}
	return nil
}

// validateIPFSTarget checks if an IPFS CID is pinned
func (s *WebsiteServiceDefault) validateIPFSTarget(ctx context.Context, targetHash string) (bool, error) {
	c, err := cid.Decode(targetHash)
	if err != nil {
		return false, err
	}

	// Check if the CID is pinned by any user
	// For Phase 1, we'll just check if it's a valid CID
	// In production, we would check the pin status
	_, err = s.pinSvc.GetPinByCIDAndUser(ctx, c, 0)
	if err != nil {
		return false, nil // Not pinned
	}

	return true, nil
}

// validateIPNSTarget checks if an IPNS key exists
func (s *WebsiteServiceDefault) validateIPNSTarget(ctx context.Context, targetHash string) (bool, error) {
	// For Phase 1, we'll just check if the IPNS name is valid
	// In production, we would check if the key exists in the database
	_, err := cid.Decode(targetHash)
	if err != nil {
		return false, err
	}

	return true, nil
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
