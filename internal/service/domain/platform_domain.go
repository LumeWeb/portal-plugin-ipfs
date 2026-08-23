package domain

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"gorm.io/gorm"
)

// isDuplicateKeyError reports whether err is a database unique-key (duplicate)
// violation, mirroring the portal core / API detection. GORM only returns
// gorm.ErrDuplicatedKey when TranslateError is enabled (it is not here), so on
// MySQL a duplicate surfaces as a *mysql.MySQLError number 1062; fall back to
// driver-agnostic string matching for SQLite et al. Used to route collision
// races in platform subdomain claims so they retry with a fresh label rather
// than surfacing a 500.
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr != nil && mysqlErr.Number == 1062 {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "Duplicate entry") ||
		strings.Contains(msg, "duplicate key value")
}

// platformRootForDomain returns the enabled platform root whose domain is a
// suffix of `domain` (and domain is not the root itself), or nil when none
// matches. Suffix matching covers both multi-label roots ("myblog" →
// "myblog.pinner.site" matches root "pinner.site") and single-label alt-root
// roots ("mylabel.pinner" matches root "pinner"). The longest matching root
// wins to avoid ambiguity if nested platform roots ever exist.
func (s *DelegatedDomainService) platformRootForDomain(ctx context.Context, domain string) *pluginDb.PlatformDomain {
	pds, err := s.ListPlatformDomains(ctx)
	if err != nil || len(pds) == 0 {
		return nil
	}
	var best *pluginDb.PlatformDomain
	for _, pd := range pds {
		if !pd.Enabled {
			continue
		}
		if domain == pd.Domain {
			continue // the apex of the root is not a subdomain of itself
		}
		if strings.HasSuffix(domain, "."+pd.Domain) {
			if best == nil || len(pd.Domain) > len(best.Domain) {
				best = pd
			}
		}
	}
	return best
}

// PlatformDomainAvailability reports whether a label is claimable on a single
// platform root. It is the per-root element of an availability response.
type PlatformDomainAvailability struct {
	PlatformDomain string                     `json:"platform_domain"`
	Namespace      pluginDb.DomainNamespace   `json:"namespace"`
	Available      bool                       `json:"available"`
}

// GetEnabledPlatformDomain returns the single enabled PlatformDomain matching
// the given root domain. When namespace is non-empty it disambiguates across
// alt-root namespaces (the same root may be registered under both ICANN and
// HNS); when it is empty and multiple enabled roots share the domain across
// different namespaces, an ambiguity error is returned instead of silently
// picking one. Returns (nil, nil) when no enabled root matches.
func (s *DelegatedDomainService) GetEnabledPlatformDomain(ctx context.Context, domain string, namespace pluginDb.DomainNamespace) (*pluginDb.PlatformDomain, error) {
	if s.DB() == nil {
		return nil, nil
	}
	domain = NormalizeDomain(domain)
	var matches []*pluginDb.PlatformDomain
	err := s.DB().WithContext(ctx).
		Where("domain = ? AND enabled = ?", domain, true).
		Order("id ASC").
		Find(&matches).Error
	if err != nil {
		return nil, err
	}
	// Filter by namespace when the caller constrained it; otherwise keep all.
	filtered := matches[:0]
	for _, pd := range matches {
		if namespace != "" && pd.Namespace != namespace {
			continue
		}
		filtered = append(filtered, pd)
	}
	if len(filtered) == 0 {
		return nil, nil
	}
	if len(filtered) > 1 {
		return nil, fmt.Errorf("platform domain %q is registered under multiple namespaces; specify one", domain)
	}
	return filtered[0], nil
}

// GetEnabledPlatformDomainByDomain returns the single enabled PlatformDomain
// matching the given root domain, or nil when none is registered/enabled.
// When the same root domain is registered under multiple namespaces, an
// ambiguity error is returned (callers should use GetEnabledPlatformDomain
// with a namespace to disambiguate).
func (s *DelegatedDomainService) GetEnabledPlatformDomainByDomain(ctx context.Context, domain string) (*pluginDb.PlatformDomain, error) {
	return s.GetEnabledPlatformDomain(ctx, domain, "")
}

// labelFor returns the fully-qualified subdomain for a label on a platform root.
func labelFor(label, root string) string {
	return NormalizeDomain(label + "." + root)
}

// GetPlatformDomainByName returns a PlatformDomain by (domain, namespace), or
// nil when none match.
func (s *DelegatedDomainService) GetPlatformDomainByName(ctx context.Context, domain string, namespace pluginDb.DomainNamespace) (*pluginDb.PlatformDomain, error) {
	if s.DB() == nil {
		return nil, nil
	}
	var pd pluginDb.PlatformDomain
	err := s.DB().WithContext(ctx).
		Where("domain = ? AND namespace = ?", domain, string(namespace)).
		First(&pd).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &pd, nil
}

// CreatePlatformDomain registers a new platform-owned root. It is an
// operator-only operation (admin API). It does not provision the zone itself:
// the caller must supply a zone already owned by the operator (a PlatformDomain
// without a provisioned zone is invalid, so callers should create/designate the
// zone before registering).
//
// Operator ownership of the zone is enforced by the admin-only route (only an
// operator can register a root); here we at least verify the zone actually
// exists so we never register a root that would fail at claim time with a
// dangling zone reference.
func (s *DelegatedDomainService) CreatePlatformDomain(ctx context.Context, domain string, namespace pluginDb.DomainNamespace, zoneID uint, enabled bool) (*pluginDb.PlatformDomain, error) {
	if s.DB() == nil {
		return nil, fmt.Errorf("database not available")
	}
	// The DNSZoneService is optional at construction (the factory only wires it
	// when a DNS service is present); the admin registration endpoint can reach
	// here with a nil dnsSvc, so guard before dereferencing.
	if s.dnsSvc == nil {
		return nil, fmt.Errorf("DNS service not configured")
	}
	if zoneID == 0 {
		return nil, fmt.Errorf("platform domain requires a provisioned zone")
	}
	domain = NormalizeDomain(domain)
	// The DNSZoneService has no by-ID lookup; GetZoneByDomain serves the same
	// purpose for a root (the root's domain == its zone apex domain).
	z, err := s.dnsSvc.GetZoneByDomain(ctx, domain)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("lookup platform zone: %w", err)
	}
	if z == nil {
		return nil, fmt.Errorf("platform root %q has no provisioned zone", domain)
	}
	// GetZoneByDomain intentionally returns soft-deleted zones; never register a
	// root against a logically-removed zone — claims would silently write
	// DNSLink/apex records into a dead zone.
	if z.DeletedAt.Valid {
		return nil, fmt.Errorf("platform root %q references a deleted zone", domain)
	}
	if z.ID != zoneID {
		return nil, fmt.Errorf("zone %d does not match provisioned zone for %q", zoneID, domain)
	}
	pd := &pluginDb.PlatformDomain{
		Domain:    domain,
		Namespace: namespace,
		ZoneID:    zoneID,
		Enabled:   enabled,
	}
	if err := s.DB().WithContext(ctx).Create(pd).Error; err != nil {
		return nil, fmt.Errorf("persist platform domain: %w", err)
	}
	return pd, nil
}

// ListPlatformDomains returns all registered platform roots (including
// disabled ones), for operator/admin listing.
func (s *DelegatedDomainService) ListPlatformDomains(ctx context.Context) ([]*pluginDb.PlatformDomain, error) {
	if s.DB() == nil {
		return nil, nil
	}
	var domains []*pluginDb.PlatformDomain
	if err := s.DB().WithContext(ctx).Order("id ASC").Find(&domains).Error; err != nil {
		return nil, err
	}
	return domains, nil
}

// UpdatePlatformDomain toggles registration state (currently only Enabled).
func (s *DelegatedDomainService) UpdatePlatformDomain(ctx context.Context, id uint, enabled bool) (*pluginDb.PlatformDomain, error) {
	if s.DB() == nil {
		return nil, fmt.Errorf("database not available")
	}
	var pd pluginDb.PlatformDomain
	if err := s.DB().WithContext(ctx).First(&pd, id).Error; err != nil {
		return nil, err
	}
	pd.Enabled = enabled
	if err := s.DB().WithContext(ctx).Model(&pd).Update("enabled", enabled).Error; err != nil {
		return nil, fmt.Errorf("update platform domain: %w", err)
	}
	return &pd, nil
}

// DeletePlatformDomain removes a platform root registration via soft delete
// (DeletedAt tombstone), keeping existing website_domains.platform_domain_id
// references valid. Existing subdomain bindings remain served; only new claims
// on the root are blocked (Enabled=false) or the root is no longer resolvable
// (soft-deleted rows are filtered from lookups).
func (s *DelegatedDomainService) DeletePlatformDomain(ctx context.Context, id uint) error {
	if s.DB() == nil {
		return fmt.Errorf("database not available")
	}
	res := s.DB().WithContext(ctx).Delete(&pluginDb.PlatformDomain{}, id)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// CreatePlatformSubdomain claims a subdomain under a platform root for a
// website. When generate is true, a unique label is generated automatically
// (GitHub-style adjective-noun-number); otherwise label is used verbatim.
// The resulting binding is created through the normal CreateDomain flow
// (which resolves the platform-owned zone via resolveManagedZone) and then
// marked with the PlatformDomain reference.
func (s *DelegatedDomainService) CreatePlatformSubdomain(ctx context.Context, websiteID, userID, platformDomainID uint, label string, generate bool) (*pluginDb.WebsiteDomain, error) {
	if s.DB() == nil {
		return nil, fmt.Errorf("database not available")
	}

	var pd pluginDb.PlatformDomain
	if err := s.DB().WithContext(ctx).First(&pd, platformDomainID).Error; err != nil {
		return nil, fmt.Errorf("platform domain lookup failed: %w", err)
	}
	if !pd.Enabled {
		return nil, fmt.Errorf("platform domain is disabled")
	}

	provider := s.registry.Get(string(pd.Namespace))
	if provider == nil {
		return nil, fmt.Errorf("unsupported namespace: %s", pd.Namespace)
	}

	// Generate or validate the label, then claim it under the root.
	//
	// Collision handling: the (domain, namespace) unique key is the ground
	// truth, not the pre-check labelAvailable (which is a fast, non-atomic look
	// ahead). In the generate path the insert itself is inside the retry loop,
	// so a concurrent claim that wins the unique key surfaces as a duplicate-key
	// error and is retried with a fresh slug instead of a 500. The explicit
	// (non-generate) label path is a single attempt and returns a clean
	// "already taken" error on contention.
	if generate {
		var fqdn string
		for attempt := 0; attempt < 100; attempt++ {
			candidate := pluginConfig.GenerateDNSSlug()
			fqdn = labelFor(candidate, pd.Domain)
			if provider.Validate(fqdn) != nil {
				continue
			}
			// Fast pre-check to avoid most insert attempts, but never relied on
			// for correctness — the insert's unique key races are handled below.
			if available, err := s.labelAvailable(ctx, fqdn, pd.Namespace); err != nil {
				return nil, err
			} else if !available {
				continue
			}
			wd, err := s.createPlatformBinding(ctx, websiteID, userID, &pd, provider, fqdn)
			if err == nil {
				return wd, nil
			}
			if !isDuplicateKeyError(err) {
				return nil, err
			}
			// Lost the race for this slug; loop to regenerate a new one.
		}
		return nil, fmt.Errorf("failed to generate a unique platform subdomain after retries")
	}

	if label == "" {
		return nil, fmt.Errorf("label is required when generate is false")
	}
	fqdn := labelFor(label, pd.Domain)
	if err := provider.Validate(fqdn); err != nil {
		return nil, fmt.Errorf("invalid platform subdomain: %w", err)
	}
	wd, err := s.createPlatformBinding(ctx, websiteID, userID, &pd, provider, fqdn)
	if err != nil {
		if isDuplicateKeyError(err) {
			return nil, fmt.Errorf("platform subdomain %q is already taken", fqdn)
		}
		return nil, err
	}
	return wd, nil
}

// createPlatformBinding runs the shared managed-DNS creation for a composed
// platform subdomain and records the PlatformDomain reference on the binding.
func (s *DelegatedDomainService) createPlatformBinding(ctx context.Context, websiteID, userID uint, pd *pluginDb.PlatformDomain, provider DomainProvider, fqdn string) (*pluginDb.WebsiteDomain, error) {
	wd, err := s.CreateDomain(ctx, string(pd.Namespace), fqdn, websiteID, userID, true, false, nil, true)
	if err != nil {
		return nil, err
	}
	// The platform controls both sides of the DNS check (see VerifyDomain's
	// platform guard), so the binding is active as soon as it is created.
	updates := map[string]any{"platform_domain_id": pd.ID, "status": pluginDb.DomainStatusActive}
	if err := s.DB().WithContext(ctx).Model(wd).Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("failed to mark platform subdomain: %w", err)
	}
	wd.PlatformDomainID = &pd.ID
	wd.Status = pluginDb.DomainStatusActive
	return wd, nil
}

// labelAvailable reports whether the fully-qualified name is unclaimed among
// live website_domains bindings in the given namespace.
func (s *DelegatedDomainService) labelAvailable(ctx context.Context, fqdn string, namespace pluginDb.DomainNamespace) (bool, error) {
	var count int64
	err := s.DB().WithContext(ctx).
		Model(&pluginDb.WebsiteDomain{}).
		Where("domain = ? AND namespace = ? AND deleted_at IS NULL", fqdn, string(namespace)).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

// CheckAvailability computes per-root availability for a candidate label across
// all enabled platform roots. It is scoped strictly to platform roots — it
// never probes user-managed zones — so it reveals nothing about other users'
// bindings on their own domains. Auth (and per-user rate limiting) is enforced
// by the API layer, not here.
func (s *DelegatedDomainService) CheckAvailability(ctx context.Context, label string) ([]PlatformDomainAvailability, error) {
	domains, err := s.ListPlatformDomains(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]PlatformDomainAvailability, 0, len(domains))
	for _, pd := range domains {
		if !pd.Enabled {
			continue
		}
		available, err := s.labelAvailable(ctx, labelFor(label, pd.Domain), pd.Namespace)
		if err != nil {
			return nil, err
		}
		out = append(out, PlatformDomainAvailability{
			PlatformDomain: pd.Domain,
			Namespace:      pd.Namespace,
			Available:      available,
		})
	}
	return out, nil
}
