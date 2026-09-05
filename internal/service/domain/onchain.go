package domain

import (
	"context"
	"errors"
	"fmt"

	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.uber.org/zap"
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
// binding after Inspect confirms that handover serves it authoritatively.
func (s *DelegatedDomainService) ConvertToOnChain(ctx context.Context, websiteID, userID, domainID uint) (*pluginDb.WebsiteDomain, error) {
	if s.DB() == nil {
		return nil, fmt.Errorf("database not available")
	}

	var wd pluginDb.WebsiteDomain
	if err := s.DB().WithContext(ctx).
		Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
		First(&wd).Error; err != nil {
		return nil, err
	}

	if wd.Status == pluginDb.DomainStatusOnchainManaged {
		return nil, fmt.Errorf("%w: %q", ErrDomainAlreadyOnChain, wd.Domain)
	}

	provider := s.registry.Get(string(wd.Namespace))
	if provider == nil {
		return nil, fmt.Errorf("unsupported namespace: %s", wd.Namespace)
	}
	onchain, err := provider.Inspect(ctx, wd.Domain)
	if err != nil {
		return nil, fmt.Errorf("domain inspection failed: %w", err)
	}
	if !onchain {
		return nil, fmt.Errorf("%w: %q; handover did not select an authoritative response", ErrDomainNotOnChain, wd.Domain)
	}

	if err := s.convertInspectedBindingToOnChain(ctx, &wd); err != nil {
		return nil, err
	}
	return &wd, nil
}

// convertInspectedBindingToOnChain applies an already-confirmed handover
// decision. Callers must hold the decision that the domain is on-chain; this
// keeps VerifyDomain from issuing a second source-detection query.
func (s *DelegatedDomainService) convertInspectedBindingToOnChain(ctx context.Context, wd *pluginDb.WebsiteDomain) error {
	if wd.Status == pluginDb.DomainStatusOnchainManaged {
		return nil
	}

	zoneID := wd.ZoneID
	if err := s.withZoneLifecycleLock(zoneLifecycleKey(wd.Domain), func() error {
		if zoneID != 0 {
			var sharers int64
			if err := s.DB().WithContext(ctx).
				Model(&pluginDb.WebsiteDomain{}).
				Where("zone_id = ? AND id != ? AND deleted_at IS NULL", zoneID, wd.ID).
				Count(&sharers).Error; err != nil {
				return fmt.Errorf("failed to count bindings sharing zone %d: %w", zoneID, err)
			}
			if sharers > 0 {
				return fmt.Errorf("%w: %q (%d other binding(s)); remove or convert them first", ErrDomainZoneShared, wd.Domain, sharers)
			}
		}

		var website pluginDb.Website
		if err := s.DB().WithContext(ctx).First(&website, wd.WebsiteID).Error; err != nil {
			return fmt.Errorf("failed to load website %d for on-chain conversion: %w", wd.WebsiteID, err)
		}
		if website.Status != string(pluginDb.WebsiteStatusBlocked) &&
			website.Status != string(pluginDb.WebsiteStatusPendingValidation) {
			if err := s.DB().WithContext(ctx).Model(&website).Update("status", pluginDb.WebsiteStatusPendingValidation).Error; err != nil {
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
		}
		if err := s.DB().WithContext(ctx).Model(wd).Updates(updates).Error; err != nil {
			return fmt.Errorf("failed to persist on-chain managed state: %w", err)
		}
		wd.ZoneID = 0
		wd.ZoneName = ""
		wd.GatewayHost = ""
		wd.DelegationData = nil
		wd.DNSHostingEnabled = false
		wd.Status = pluginDb.DomainStatusOnchainManaged

		if zoneID != 0 && s.dnsSvc != nil {
			var sharers int64
			if err := s.DB().WithContext(ctx).
				Model(&pluginDb.WebsiteDomain{}).
				Where("zone_id = ? AND deleted_at IS NULL", zoneID).
				Count(&sharers).Error; err != nil {
				s.Logger().Warn("failed to re-count zone sharers before on-chain conversion zone delete",
					zap.Uint("zone_id", zoneID), zap.String("domain", wd.Domain), zap.Error(err))
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
