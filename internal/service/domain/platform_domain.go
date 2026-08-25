package domain

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
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

// PlatformDomainAvailability reports whether a label is claimable on a single
// platform root. It is the per-root element of an availability response.
type PlatformDomainAvailability struct {
	PlatformDomain string                   `json:"platform_domain"`
	Namespace      pluginDb.DomainNamespace `json:"namespace"`
	Available      bool                     `json:"available"`
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

// IsPlatformRootDomain reports whether the given domain (case/format
// normalized) is an enabled platform root apex. The apex of a platform root is
// operator-owned: it may only be bound to the operator's own site via the
// admin apex-binding flow, never claimed by an end user through the normal
// create/bind endpoints. Returns (false, nil) when the domain is not an
// enabled platform root, or when the delegated-domain service has no DB wired.
func (s *DelegatedDomainService) IsPlatformRootDomain(ctx context.Context, domain string) (bool, error) {
	if s.DB() == nil {
		return false, nil
	}
	var count int64
	err := s.DB().WithContext(ctx).
		Model(&pluginDb.PlatformDomain{}).
		Where("domain = ? AND enabled = ?", NormalizeDomain(domain), true).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// enabledPlatformRootForDomain walks up the ancestor domains of the given name
// and returns the deepest enabled PlatformDomain root it is nested under, or
// nil if none. Used by resolveManagedZone's normal (non-claim) bind path to
// reject subdomains that sit under an operator-owned platform root — such a
// binding must go through the platform claim flow instead.
func (s *DelegatedDomainService) enabledPlatformRootForDomain(ctx context.Context, domain string) (*pluginDb.PlatformDomain, error) {
	ancestor := domain
	for len(ancestor) > 0 {
		pd, err := s.GetEnabledPlatformDomainByDomain(ctx, ancestor)
		if err != nil {
			return nil, fmt.Errorf("resolve platform root for %q: %w", ancestor, err)
		}
		if pd != nil {
			return pd, nil
		}
		ancestor = parentDomain(ancestor)
	}
	return nil, nil
}

// labelFor returns the fully-qualified subdomain for a label on a platform root.
// It deliberately does NOT call NormalizeDomain, which strips a leading "www."
// prefix — that would map the label "www" to the bare root apex, bypassing
// platform zone-reuse and minting a user-owned zone at the operator's root.
func labelFor(label, root string) string {
	return strings.ToLower(label) + "." + root
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
// operator-only operation (admin API). Auto-creates (idempotently) the DNS
// zone the operator owns for the root domain: if a zone already exists for the
// domain it is reused (owned by the same operator), otherwise a new zone is
// provisioned. The resulting zone ID is stored on the PlatformDomain row, so a
// root never references a dangling zone. Operator ownership of the zone is
// enforced by the admin-only route (only an operator can register a root).
func (s *DelegatedDomainService) CreatePlatformDomain(ctx context.Context, domain string, namespace pluginDb.DomainNamespace, operatorUserID uint, enabled bool) (*pluginDb.PlatformDomain, error) {
	if s.DB() == nil {
		return nil, fmt.Errorf("database not available")
	}
	// The DNSZoneService is optional at construction (the factory only wires it
	// when a DNS service is present); the admin registration endpoint can reach
	// here with a nil dnsSvc, so guard before dereferencing.
	if s.dnsSvc == nil {
		return nil, fmt.Errorf("DNS service not configured")
	}
	if operatorUserID == 0 {
		return nil, fmt.Errorf("platform domain requires an operator user")
	}
	domain = NormalizeDomain(domain)
	// Create (or reuse) the operator's zone for the root apex. CreateZone is
	// idempotent: when a zone already exists for this domain it returns the
	// existing one (owned by the same operator), otherwise it provisions a new
	// zone. This guarantees the PlatformDomain always references a live zone.
	z, err := s.dnsSvc.CreateZone(ctx, domain, operatorUserID)
	if err != nil {
		return nil, fmt.Errorf("provision platform zone: %w", err)
	}
	if z == nil {
		return nil, fmt.Errorf("platform root %q has no provisioned zone", domain)
	}
	// Soft deletes leave a tombstone that still occupies the strict
	// (domain, namespace) unique key, so re-registering a root after
	// DeletePlatformDomain would violate the constraint. Matching the
	// website_domains soft-delete semantics (see CreateDomain), purge any prior
	// tombstone for this key so re-registration via a strict unique key works.
	// Only tombstones (deleted_at IS NOT NULL) are removed; a live same-key row
	// is a genuine conflict and left to the unique key to reject.
	if err := s.DB().WithContext(ctx).
		Where("domain = ? AND namespace = ? AND deleted_at IS NOT NULL", domain, string(namespace)).
		Unscoped().Delete(&pluginDb.PlatformDomain{}).Error; err != nil {
		return nil, fmt.Errorf("failed to purge stale platform domain: %w", err)
	}

	pd := &pluginDb.PlatformDomain{
		Domain:    domain,
		Namespace: namespace,
		ZoneID:    z.ID,
		Enabled:   enabled,
	}
	if err := s.DB().WithContext(ctx).Create(pd).Error; err != nil {
		if isDuplicateKeyError(err) {
			return nil, fmt.Errorf("platform domain %q is already registered for namespace %q", domain, namespace)
		}
		return nil, fmt.Errorf("persist platform domain: %w", err)
	}
	return pd, nil
}

// ListPlatformDomains returns registered platform roots with filtering,
// sorting, and pagination, plus the pre-pagination total.
func (s *DelegatedDomainService) ListPlatformDomains(ctx context.Context, filters []queryutil.CrudFilter, sort []filter.Sort, pagination queryutil.Pagination) ([]*pluginDb.PlatformDomain, int64, error) {
	if s.DB() == nil {
		return nil, 0, nil
	}
	query := s.DB().WithContext(ctx).Model(&pluginDb.PlatformDomain{})
	query = queryutil.ApplyFilters(query, filters, nil)
	query = queryutil.ApplySort(query, sort)

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	query = queryutil.ApplyPagination(query, pagination)
	var domains []*pluginDb.PlatformDomain
	if err := query.Find(&domains).Error; err != nil {
		return nil, 0, err
	}
	return domains, total, nil
}

// ListEnabledPlatformDomains returns only enabled platform roots, ordered by
// domain, with pagination. This is the user-facing view of supported platform
// domains: only enabled roots are claimable, so disabled ones are filtered out.
func (s *DelegatedDomainService) ListEnabledPlatformDomains(ctx context.Context, pagination queryutil.Pagination) ([]*pluginDb.PlatformDomain, int64, error) {
	if s.DB() == nil {
		return nil, 0, nil
	}
	query := s.DB().WithContext(ctx).Model(&pluginDb.PlatformDomain{}).
		Where("enabled = ?", true).
		Order("domain ASC")

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	query = queryutil.ApplyPagination(query, pagination)
	var domains []*pluginDb.PlatformDomain
	if err := query.Find(&domains).Error; err != nil {
		return nil, 0, err
	}
	return domains, total, nil
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

// BindPlatformRootApex binds an operator-owned website directly to the root
// apex of a platform root (e.g. "pinner.site"), rather than to a subdomain
// underneath it. The website must be owned by the same operator that owns the
// PlatformDomain. Reuses the shared managed-DNS pipeline (DNSLink, apex
// records, delegation, SSL) via createPlatformBinding with the FQDN being the
// root itself, and marks the binding with the PlatformDomain reference.
func (s *DelegatedDomainService) BindPlatformRootApex(ctx context.Context, websiteID, userID, platformDomainID uint) (*pluginDb.WebsiteDomain, error) {
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

	return s.createPlatformBinding(ctx, websiteID, userID, &pd, provider, pd.Domain)
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
		gen := s.slugGen
		if gen == nil {
			gen = pluginConfig.GenerateDNSSlug
		}
		var fqdn string
		for attempt := 0; attempt < 100; attempt++ {
			candidate := gen()
			fqdn = labelFor(candidate, pd.Domain)
			if fqdn == pd.Domain {
				continue
			}
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
	label = strings.ToLower(strings.TrimSpace(label))
	fqdn := labelFor(label, pd.Domain)
	if fqdn == pd.Domain {
		return nil, fmt.Errorf("platform subdomain label %q resolves to the root apex %q; it must be a proper subdomain", label, fqdn)
	}
	// The "www" label is deliberately reserved: NormalizeDomain strips a leading
	// "www." on write (WebsiteDomain.BeforeSave), so composing any label whose
	// fully-qualified name starts with "www." would be mangled (or collapse to
	// the root apex). Reject it explicitly up front instead of letting it fail
	// obscurely downstream, keeping "www" special and unusable.
	if NormalizeDomain(fqdn) != fqdn {
		return nil, fmt.Errorf("label %q is reserved: the resulting subdomain %q starts with a leading \"www.\" and is not allowed", label, fqdn)
	}
	if err := provider.Validate(fqdn); err != nil {
		return nil, fmt.Errorf("invalid platform subdomain: %w", err)
	}
	// Pre-check availability: the (domain, namespace, deleted_at) unique key
	// does not enforce live-row uniqueness when deleted_at is NULL (NULLs are
	// distinct in both MySQL and SQLite), so the app layer must check.
	available, err := s.labelAvailable(ctx, fqdn, pd.Namespace)
	if err != nil {
		return nil, err
	}
	if !available {
		return nil, fmt.Errorf("platform subdomain %q is already taken", fqdn)
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
// It threads the granted root's ID into CreateDomain so resolveManagedZone
// resolves the operator zone from this exact root — never by re-deriving via
// longest suffix-match across registered roots (which could mis-allocate a
// claim to a differently-registered nested root).
func (s *DelegatedDomainService) createPlatformBinding(ctx context.Context, websiteID, userID uint, pd *pluginDb.PlatformDomain, provider DomainProvider, fqdn string) (*pluginDb.WebsiteDomain, error) {
	wd, err := s.CreateDomain(ctx, string(pd.Namespace), fqdn, websiteID, userID, true, false, nil, &pd.ID)
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
	// Availability scans every root regardless of page size, so pass a zero-value
	// pagination (no limit) and ignore the total.
	domains, _, err := s.ListPlatformDomains(ctx, nil, nil, queryutil.Pagination{})
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
