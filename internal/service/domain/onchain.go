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
// binding — the explicit, one-way path for the HNS-DNS → on-chain-DNS
// transition, used when the name's NS record now points at an external
// contract (a HIP-5 TX record the owner set on-chain).
//
// The portal stops owning the PowerDNS-served DNS for the name: the managed
// zone (and the DNSSEC/DS carried by it) is deleted and the binding is
// re-marked onchain_managed with dns_hosting_enabled=false, so ownership is
// proven with the TXT token through the resolver instead of NS/DS delegation.
// DANE/SSL state (ProtocolData: key, cert, TLSA, owner) is deliberately KEPT —
// DANE still applies on-chain; only the PowerDNS-published records and DNSSEC
// (not a concept on-chain) are dropped. The owning website is reset to
// pending_validation so the TXT-token verification flow re-runs.
//
// Safety (never tear down on the caller's word alone):
//   - Refused until Inspect confirms the name genuinely serves a HIP-5 record.
//   - The PowerDNS zone is deleted only when no other live binding shares it:
//     a shared zone is the parent of other native bindings, and deleting it
//     would destroy their records/DNSSEC. Conversion is refused for shared
//     zones with a clear instruction to detach those bindings first — a HIP-5
//     apex resolves every subdomain via the contract anyway.
//   - Refused for a binding already onchain_managed.
func (s *DelegatedDomainService) ConvertToOnChain(ctx context.Context, websiteID, userID, domainID uint) (*pluginDb.WebsiteDomain, error) {
	if s.DB() == nil {
		return nil, fmt.Errorf("database not available")
	}

	var wd pluginDb.WebsiteDomain
	if err := s.DB().WithContext(ctx).
		Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
		First(&wd).Error; err != nil {
		return nil, err // includes gorm.ErrRecordNotFound
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
		return nil, fmt.Errorf("%w: %q; its NS record does not point at an external contract", ErrDomainNotOnChain, wd.Domain)
	}

	// Only the binding's sole-owner PowerDNS zone may be deleted. A zone shared
	// with other live bindings (their parent/apex zone) must be kept — deleting
	// it would destroy their records and DNSSEC.
	zoneID := wd.ZoneID
	if zoneID != 0 {
		var sharers int64
		if err := s.DB().WithContext(ctx).
			Model(&pluginDb.WebsiteDomain{}).
			Where("zone_id = ? AND id != ? AND deleted_at IS NULL", zoneID, wd.ID).
			Count(&sharers).Error; err != nil {
			return nil, fmt.Errorf("failed to count bindings sharing zone %d: %w", zoneID, err)
		}
		if sharers > 0 {
			return nil, fmt.Errorf("%w: %q (%d other binding(s)); remove or convert them first", ErrDomainZoneShared, wd.Domain, sharers)
		}
	}

	// Drop the PowerDNS-backed delegation state. ProtocolData (DANE key, cert,
	// TLSA, owner) and per-domain SSL state are intentionally preserved: DANE/
	// SSL still apply to the name — the records are simply served by the
	// external contract now — and DNSSEC is not a concept on-chain.
	updates := map[string]any{
		"zone_id":             0,
		"zone_name":           "",
		"gateway_host":        "",
		"delegation_data":     nil,
		"dns_hosting_enabled": false,
		"status":              pluginDb.DomainStatusOnchainManaged,
	}
	if err := s.DB().WithContext(ctx).Model(&wd).Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("failed to persist on-chain managed state: %w", err)
	}
	wd.ZoneID = 0
	wd.ZoneName = ""
	wd.GatewayHost = ""
	wd.DelegationData = nil
	wd.DNSHostingEnabled = false
	wd.Status = pluginDb.DomainStatusOnchainManaged

	// Delete the now-unreferenced PowerDNS zone LAST, best-effort. Order is
	// deliberate: the DB runs first so a delete failure can never strand the
	// binding pointing at a destroyed zone (the unrecoverable state). If a
	// concurrent bind landed in this zone between the check above and here, the
	// zone is re-counted and left alone so another binding's records/DNSSEC are
	// not destroyed. A leftover/unreferenced zone is harmless (nothing serves
	// it — the name resolves via the contract) and is logged for ops cleanup.
	if zoneID != 0 && s.dnsSvc != nil {
		var sharers int64
		if err := s.DB().WithContext(ctx).
			Model(&pluginDb.WebsiteDomain{}).
			Where("zone_id = ? AND id != ? AND deleted_at IS NULL", zoneID, wd.ID).
			Count(&sharers).Error; err != nil {
			s.Logger().Warn("failed to re-count zone sharers before on-chain conversion zone delete",
				zap.Uint("zone_id", zoneID), zap.String("domain", wd.Domain), zap.Error(err))
		} else if sharers > 0 {
			s.Logger().Info("on-chain conversion: zone now shared by other bindings; leaving it intact",
				zap.Uint("zone_id", zoneID), zap.String("domain", wd.Domain))
		} else if err := s.dnsSvc.DeleteZone(ctx, zoneID); err != nil {
			// Non-fatal: the binding is already cleanly on-chain; the orphaned
			// zone is unreachable garbage an operator can clear.
			s.Logger().Warn("on-chain conversion: failed to delete orphaned managed zone (non-fatal)",
				zap.Uint("zone_id", zoneID), zap.String("domain", wd.Domain), zap.Error(err))
		}
	}

	// DNS moved on-chain: the site must prove ownership against the contract
	// with the TXT token, so re-arm validation. A blocked website stays blocked
	// (only an admin can lift an admin block); a pending one needs no change.
	// A website-load failure is surfaced rather than silently skipped so the
	// caller knows validation was not re-armed.
	var website pluginDb.Website
	if err := s.DB().WithContext(ctx).First(&website, wd.WebsiteID).Error; err != nil {
		return nil, fmt.Errorf("failed to load website %d for on-chain conversion: %w", wd.WebsiteID, err)
	}
	if website.Status != string(pluginDb.WebsiteStatusBlocked) &&
		website.Status != string(pluginDb.WebsiteStatusPendingValidation) {
		if err := s.DB().WithContext(ctx).Model(&website).Update("status", pluginDb.WebsiteStatusPendingValidation).Error; err != nil {
			return nil, fmt.Errorf("failed to reset website to pending_validation: %w", err)
		}
	}

	return &wd, nil
}
