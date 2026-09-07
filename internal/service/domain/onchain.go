package domain

import (
	"context"
	"errors"
	"fmt"

	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/db"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Sentinel errors returned by ConvertToOnChain for user-correctable state
// conflicts, so the API can map them to 4xx while genuine infrastructure
// failures (DB, DNS service) surface as 5xx. Match with errors.Is.
var (
	ErrDomainAlreadyOnChain = errors.New("domain is already on-chain managed")
	ErrDomainNotOnChain     = errors.New("domain is not yet on-chain managed")
	ErrDomainZoneShared     = errors.New("domain's DNS zone is shared by other bindings")
)

// ConvertToOnChain converts a bound domain into an on-chain managed (HIP-5)
// binding after Inspect confirms that handover serves it authoritatively. It
// is the ONLY route-conversion path: verification only reports route drift,
// so the caller (the HTTP convert
// endpoint) is the explicit, auditable transition command. On success the
// transition's persisted operation state is the binding row itself: status
// flips to onchain_managed and the legacy zone/delegation fields are cleared
// atomically with the authority handover, so operators observe the final
// state; debug/info logs on this function plus the zone-delete warnings mark
// each stage. A repeat call returns the typed ErrDomainAlreadyOnChain
// sentinel without performing any DNS or persistence work.
func (s *DelegatedDomainService) ConvertToOnChain(ctx context.Context, websiteID, userID, domainID uint) (*pluginDb.WebsiteDomain, error) {
	if s.DB() == nil {
		return nil, fmt.Errorf("database not available")
	}

	var wd pluginDb.WebsiteDomain
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.
			Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
			First(&wd).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	}); err != nil {
		return nil, err
	}

	if wd.Status == pluginDb.DomainStatusOnchainManaged {
		return nil, fmt.Errorf("%w: %q", ErrDomainAlreadyOnChain, wd.Domain)
	}

	s.Logger().Info("explicit on-chain conversion started (the only route-transition command)",
		zap.Uint("id", wd.ID),
		zap.String("domain", wd.Domain),
		zap.Uint("website_id", wd.WebsiteID),
		zap.String("from_status", string(wd.Status)),
		zap.Uint("zone_id", wd.ZoneID))

	provider := s.registry.Get(string(wd.Namespace))
	if provider == nil {
		return nil, fmt.Errorf("unsupported namespace: %s", wd.Namespace)
	}
	onchain, err := provider.Inspect(ctx, wd.Domain)
	if err != nil {
		return nil, fmt.Errorf("domain inspection failed: %w", err)
	}
	if !onchain {
		return nil, fmt.Errorf("%w: %q; the resolver reported the name is not served on-chain (HIP-5)", ErrDomainNotOnChain, wd.Domain)
	}

	// DANE still applies once the name is chain-managed (the TLSA is served
	// from the on-chain zone data), so the stable DANE identity must exist
	// before any destructive work. Running this after inspection but before
	// convertInspectedBindingToOnChain makes a bootstrap failure fail-fast with
	// nothing converted or torn down; it is a no-op when the binding already
	// holds a persisted key (the common case for a site, via prior cert pushes).
	if err := s.ensureDANEIdentity(ctx, provider, string(wd.Namespace), wd.Domain); err != nil {
		return nil, err
	}

	if err := s.convertInspectedBindingToOnChain(ctx, &wd); err != nil {
		return nil, err
	}
	s.Logger().Info("explicit on-chain conversion completed",
		zap.Uint("id", wd.ID),
		zap.String("domain", wd.Domain),
		zap.Uint("website_id", wd.WebsiteID),
		zap.String("to_status", string(wd.Status)))
	return &wd, nil
}

// convertInspectedBindingToOnChain applies an already-confirmed handover
// decision. Callers must hold the decision that the domain is on-chain (since
// the ONLY caller is the explicit ConvertToOnChain command — VerifyDomain
// reports route drift and never converts, so deleting zones from
// verification is no longer possible).
func (s *DelegatedDomainService) convertInspectedBindingToOnChain(ctx context.Context, wd *pluginDb.WebsiteDomain) error {
	if wd.Status == pluginDb.DomainStatusOnchainManaged {
		return nil
	}

	zoneID := wd.ZoneID
	if err := s.withZoneLifecycleLock(zoneLifecycleKey(wd.Domain), func() error {
		if zoneID != 0 {
			var sharers int64
			if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				if err := tx.
					Model(&pluginDb.WebsiteDomain{}).
					Where("zone_id = ? AND id != ? AND deleted_at IS NULL", zoneID, wd.ID).
					Count(&sharers).Error; err != nil {
					_ = tx.AddError(err)
				}
				return tx
			}); err != nil {
				return fmt.Errorf("failed to count bindings sharing zone %d: %w", zoneID, err)
			}
			if sharers > 0 {
				return fmt.Errorf("%w: %q (%d other binding(s)); remove or convert them first", ErrDomainZoneShared, wd.Domain, sharers)
			}
		}

		// Re-arm the website to pending_validation BEFORE committing the
		// conversion. A failure here is clean (nothing has been converted yet
		// and the caller can retry), whereas failing after the on-chain commit
		// would leave an already-converted domain reported as a 500 and make
		// retry hit "already on-chain managed". A blocked website stays blocked
		// (only an admin can lift an admin block); a pending one needs no
		// change. Crucially, only re-arm when the converted binding is the
		// website's PRIMARY (apex) domain — website validation keys on the
		// primary, so converting a secondary must not knock the primary website
		// back to pending_validation.
		website, isPrimary, err := s.bindingIsWebsitePrimary(ctx, wd.WebsiteID, wd.ID)
		if err != nil {
			return fmt.Errorf("failed to resolve website primary for on-chain conversion: %w", err)
		}
		if isPrimary && website.Status != string(pluginDb.WebsiteStatusBlocked) && website.Status != string(pluginDb.WebsiteStatusPendingValidation) {
			if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				if err := tx.Model(website).Update("status", pluginDb.WebsiteStatusPendingValidation).Error; err != nil {
					_ = tx.AddError(err)
				}
				return tx
			}); err != nil {
				return fmt.Errorf("failed to reset website to pending_validation: %w", err)
			}
		}

		updates := map[string]any{
			"zone_id":             0,
			"zone_name":           "",
			"gateway_host":        "",
			"delegation_data":     nil,
			"dns_hosting_enabled": false,
			"status":              pluginDb.DomainStatusOnchainManaged,
			// The row survives conversion (status flips, not deletion), so
			// the janitor's route-drift backoff marker must be cleared with
			// the same atomic update — a converted binding must not carry a
			// stale drift_detected_at into any later lifecycle state.
			"drift_detected_at": nil,
		}
		if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			if err := tx.Model(wd).Updates(updates).Error; err != nil {
				_ = tx.AddError(err)
			}
			return tx
		}); err != nil {
			return fmt.Errorf("failed to persist on-chain managed state: %w", err)
		}
		wd.ZoneID = 0
		wd.ZoneName = ""
		wd.GatewayHost = ""
		wd.DelegationData = nil
		wd.DNSHostingEnabled = false
		wd.Status = pluginDb.DomainStatusOnchainManaged
		wd.DriftDetectedAt = nil

		if zoneID != 0 && s.dnsSvc != nil {
			var sharers int64
			rcErr := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				if err := tx.
					Model(&pluginDb.WebsiteDomain{}).
					Where("zone_id = ? AND deleted_at IS NULL", zoneID).
					Count(&sharers).Error; err != nil {
					_ = tx.AddError(err)
				}
				return tx
			})
			if rcErr != nil {
				s.Logger().Warn("failed to re-count zone sharers before on-chain conversion zone delete",
					zap.Uint("zone_id", zoneID), zap.String("domain", wd.Domain), zap.Error(rcErr))
			} else if sharers > 0 {
				s.Logger().Info("on-chain conversion: zone picked up by another binding; leaving it intact",
					zap.Uint("zone_id", zoneID), zap.String("domain", wd.Domain))
			} else if err := s.dnsSvc.DeleteZone(ctx, zoneID); err != nil {
				s.Logger().Warn("on-chain conversion: failed to delete orphaned managed zone (non-fatal)",
					zap.Uint("zone_id", zoneID), zap.String("domain", wd.Domain), zap.Error(err))
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// bindingIsWebsitePrimary reports whether the given domain is its website's
// primary (apex) binding, returning the owning website for the caller to
// branch on. Website validation keys on the primary, so domain-origin side
// effects that reset validation state must fire only for the primary — never
// for a secondary, which would knock the primary website back to
// pending_validation. An explicit Website.PrimaryDomainID wins; when none is
// designated, the binding is primary only if it is the website's sole binding
// (a freshly bound apex).
func (s *DelegatedDomainService) bindingIsWebsitePrimary(ctx context.Context, websiteID, domainID uint) (*pluginDb.Website, bool, error) {
	var website pluginDb.Website
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.First(&website, websiteID).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		return nil, false, err
	}
	if website.PrimaryDomainID != nil {
		return &website, domainID == *website.PrimaryDomainID, nil
	}
	// No explicit primary is designated: resolve the apex the same way
	// WebsiteServiceDefault.primaryWebsiteDomain does — the oldest active
	// (status=active) non-deleted binding. This keeps the conversion's
	// primary determination consistent with website validation, so converting
	// a legacy multi-binding website's apex still re-arms validation.
	var apex pluginDb.WebsiteDomain
	err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.
			Where("website_id = ? AND status = ? AND deleted_at IS NULL", websiteID, pluginDb.DomainStatusActive).
			Order("id ASC").First(&apex).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, err
		}
		// No active binding exists (e.g. the apex is still in
		// records_generated / waiting_delegation / error). Fall back to
		// treating a sole non-deleted binding as the primary so converting it
		// to on-chain still proceeds (and re-arms) instead of aborting with
		// ErrRecordNotFound (which the API maps to a 404).
		var count int64
		if cerr := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			if err := tx.Model(&pluginDb.WebsiteDomain{}).
				Where("website_id = ? AND deleted_at IS NULL", websiteID).
				Count(&count).Error; err != nil {
				_ = tx.AddError(err)
			}
			return tx
		}); cerr != nil {
			return nil, false, cerr
		}
		return &website, count <= 1, nil
	}
	return &website, apex.ID == domainID, nil
}
