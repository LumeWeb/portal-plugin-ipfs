package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	dane "go.lumeweb.com/dane"
	"go.lumeweb.com/ipfs-sdk/dnsname"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainapp"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.uber.org/zap"
)

type DelegatedDomainService struct {
	*core.BaseComponent
	registry *Registry
	dnsSvc   DNSZoneService
	// zoneLifecycleLocks serializes zone lifecycle transitions (a bind that
	// provisions/reuses a zone in CreateDomain vs a conversion that deletes a
	// zone in ConvertToOnChain), keyed by the zone's canonical apex name, so a
	// concurrent bind can never have its zone destroyed by a conversion's
	// delete. sync.Map so the service can be copied/value-constructed safely.
	zoneLifecycleLocks sync.Map // string(zone apex) -> *sync.Mutex
	// websiteSvc is resolved once at startup for cross-service calls (e.g.
	// activating a website after a platform subdomain is claimed). It is always
	// present: the portal registers all service instances before running
	// startup funcs, and WEBSITE_SERVICE is part of this same plugin.
	websiteSvc pluginCore.WebsiteService
	// slugGen produces a DNS-safe label for auto-generated platform
	// subdomains. It defaults to pluginConfig.GenerateDNSSlug and is
	// injectable so tests can control the slug sequence.
	slugGen func() string
}

type DNSZoneService interface {
	CreateZone(ctx context.Context, domain string, userID uint) (*pluginDb.DNSZone, error)
	// GetZoneByDomain retrieves a zone by its domain name (including
	// soft-deleted zones). Returns gorm.ErrRecordNotFound when none match.
	GetZoneByDomain(ctx context.Context, domain string) (*pluginDb.DNSZone, error)
	DeleteZone(ctx context.Context, zoneID uint) error
	// CreateDNSLinkRecord writes the DNSLink TXT record for a domain's owner
	// name(`_dnslink.<domain>`) into zone zoneID. domain is the FQDN of the
	// record's owner: the zone apex for an apex binding, or a subdomain that
	// lives inside a reused parent zone. Naming the record after domain (not
	// the zone apex) keeps subdomain records from colliding with the parent's
	// own DNSLink record.
	CreateDNSLinkRecord(ctx context.Context, zoneID uint, domain string, target string) error
	// CreateApexRecord creates the authoritative record for domain (not the
	// zone apex) of the given record type (e.g. RecordTypeA or RecordTypeALIAS).
	// content is the raw value: an IP address for A, a gateway hostname for
	// ALIAS. When domain equals the zone apex this is the zone root record;
	// for a subdomain reusing a parent zone it is the subdomain's record.
	CreateApexRecord(ctx context.Context, zoneID uint, domain string, recordType pluginCore.RecordType, content string) error
	// SetTLSARecord writes (or replaces) the DANE TLSA record for domain's
	// HTTPS/TCP owner `_443._tcp` pointing at the portal-managed authoritative
	// zone. content is the TLSA rdata: "usage selector matching hash" (e.g.
	// "3 1 1 <hex>")). For HNS managed zones this makes DANE validators resolve
	// the TLSA against the portal's PowerDNS zone; without it, authoritative
	// queries return NXDOMAIN. The owner is named after domain so a subdomain
	// reusing a parent zone gets its own TLSA, not the parent's.
	SetTLSARecord(ctx context.Context, zoneID uint, domain string, content string) error
	// EnableDNSSEC enables DNSSEC on a zone and returns the DNSKEY.
	EnableDNSSEC(ctx context.Context, zoneID uint) (dnskey string, err error)
	// GetActiveDNSSECDS returns the SHA-256 DS RDATA (type 2) for a zone's
	// currently-active signing key, computed live from PowerDNS. Returns ""
	// when the zone has no active signing key; errors when multiple active keys
	// exist (in-progress rollover).
	GetActiveDNSSECDS(ctx context.Context, zoneID uint) (ds string, err error)
	// EnsureSOAMNAME idempotently corrects a zone's SOA MNAME to the primary
	// authorized nameserver, no-op'ing when it is already correct. PowerDNS
	// seeds freshly created zones with a placeholder MNAME that is only fixed
	// on the fresh-create path; this lets verification re-ensure a portal
	// managed zone's SOA points at the right authority, mirroring the DNSSEC
	// self-heal. It is best-effort (the SOA MNAME is a secondary authoritative
	// pointer; delegation is carried by the NS record), so callers must not
	// treat an error here as a hard verification failure.
	EnsureSOAMNAME(ctx context.Context, zoneID uint, domain string, nameservers []string) error

	// GetZoneSOAMNAME returns the zone's current SOA MNAME (the first field
	// of the apex SOA RRSet). It is the observation read for the plan-driven
	// zone heal: the self-heal represents SOA drift as a domainpolicy
	// observation instead of writing blind. Callers treat an error as an
	// observation transport failure.
	GetZoneSOAMNAME(ctx context.Context, zoneID uint) (string, error)
}

// DSRecord represents a Delegation Signer record for DNSSEC.
type DSRecord struct {
	KeyTag     uint16 `json:"key_tag"`
	Algorithm  uint8  `json:"algorithm"`
	DigestType uint8  `json:"digest_type"`
	Digest     string `json:"digest"`
}

// DelegationVerificationState distinguishes the delegation outcomes a caller
// must be able to act on without re-deriving hosting rules.
type DelegationVerificationState uint8

const (
	// DelegationNotApplicable means the binding's hosting class does not use
	// portal delegation verification (self-hosted, on-chain managed, or
	// unresolved). This is neither success nor failure of delegation — it
	// simply does not apply, and it is not ownership proof by itself.
	DelegationNotApplicable DelegationVerificationState = iota
	// DelegationPending means the portal-managed delegation is not yet live
	// (NS/DS not yet visible at the parent).
	DelegationPending
	// DelegationVerified means the portal-managed delegation is live, or the
	// binding is an operator-trusted platform binding.
	DelegationVerified
)

// DelegationVerificationResult is the typed outcome of VerifyDomain. Callers
// must switch on State rather than interpreting a bare boolean, which cannot
// distinguish "not applicable" from "pending" and would deadlock valid
// on-chain/self-hosted bindings.
type DelegationVerificationResult struct {
	State DelegationVerificationState
	// ApprovedNS / LiveNS carry the expected vs discovered nameservers for
	// pending delegations (mirroring the janitor's zone NS validation) so a
	// stuck waiting_delegation is diagnosable from logs alone. Both are empty
	// unless the state is pending.
	ApprovedNS []string
	LiveNS     []string
	// Checks enumerates each delegation gate and its outcome, reusing the
	// shared core.ValidationCheck type so clients render the same per-gate
	// fix-up guidance as website DNS validation.
	Checks []pluginCore.ValidationCheck
}

// NewDelegatedDomainService creates a DelegatedDomainService with the given
// registry and DNS service. BaseComponent is injected by the framework.
func NewDelegatedDomainService(reg *Registry, dns DNSZoneService) *DelegatedDomainService {
	return &DelegatedDomainService{
		registry: reg,
		dnsSvc:   dns,
		slugGen:  pluginConfig.GenerateDNSSlug,
	}
}

// RegisterProvider registers a namespace provider with the service's registry,
// delegating to Registry.Register (including its policy validation and
// duplicate-protocol panic). The service factory registers the built-in ICANN
// and HNS providers at startup; this is the entry point for operators wiring
// additional namespaces (and for tests injecting synthetic providers).
func (s *DelegatedDomainService) RegisterProvider(p DomainProvider) {
	if s.registry == nil {
		s.registry = NewRegistry()
	}
	s.registry.Register(p)
}

// gatewayHost returns the configured gateway domain for ALIAS records,
// read from the DNS service config at call time.
func (s *DelegatedDomainService) gatewayHost() string {
	if s.BaseComponent == nil {
		return ""
	}
	dnsCfg := core.GetServiceConfig[*pluginConfig.DnsConfig](s.Context(), pluginCore.DNS_SERVICE)
	if dnsCfg == nil {
		return ""
	}
	return dnsCfg.GatewayDomain
}

// gatewayIP returns the configured gateway IP published as the apex A
// record for DNSSEC-signed alt-root zones, read from the DNS service config.
func (s *DelegatedDomainService) gatewayIP() string {
	if s.BaseComponent == nil {
		return ""
	}
	dnsCfg := core.GetServiceConfig[*pluginConfig.DnsConfig](s.Context(), pluginCore.DNS_SERVICE)
	if dnsCfg == nil {
		return ""
	}
	return dnsCfg.GatewayIP
}

// dnsLinkReconcilerEnabled reports whether the plan-driven DNSLink reconciler
// owns DNSLink desired-state application.
// The flag defaults to FALSE: with it off, the legacy bind-time DNSLink
// writer remains the exact current behavior. The flag is read from the DNS
// service config at call time (same pattern as gatewayHost/gatewayIP).
func (s *DelegatedDomainService) dnsLinkReconcilerEnabled() bool {
	if s.BaseComponent == nil {
		return false
	}
	dnsCfg := core.GetServiceConfig[*pluginConfig.DnsConfig](s.Context(), pluginCore.DNS_SERVICE)
	if dnsCfg == nil {
		return false
	}
	return dnsCfg.DomainPolicyDNSLinkReconcilerEnabled
}

// repairReconcilerEnabled reports whether the plan-driven repair reconciler
// owns the remaining repair effect families: the DNSSEC ensure + SOA MNAME zone heal in this service, and the
// expired-challenge rotation in the website service. The flag defaults to
// FALSE: with it off, the legacy self-heal paths run verbatim. Both writers
// are never active for the same operation — the flagged path either
// reconciles or defers to the legacy one with a loud log.
func (s *DelegatedDomainService) repairReconcilerEnabled() bool {
	if s.BaseComponent == nil {
		return false
	}
	dnsCfg := core.GetServiceConfig[*pluginConfig.DnsConfig](s.Context(), pluginCore.DNS_SERVICE)
	if dnsCfg == nil {
		return false
	}
	return dnsCfg.DomainPolicyRepairReconcilerEnabled
}

// reconcileBindDNSLink runs the plan-driven DNSLink reconciliation for the
// bind-time first-publication write. The zone-id commit happens later in the
// bind flow, so the resolved zone reference is set on the in-memory row
// before mapping facts (mirroring the final assignment below) — otherwise the
// persisted-class mapper would see an unresolved binding and reject the plan.
//
// The legacy bind path wrote the record unconditionally; no live DNSLink
// observation is performed here (there is no observation collector on the
// bind path), so the reconciler derives its effects from the unobserved
// record and deterministically yields the same idempotent write the legacy
// path performed. It reports whether the reconciler HANDLED the write;
// false leaves the operation untouched for the legacy fallback. Any
// reconcile failure — including a DNS write error from the effect
// executor — reports false so the caller runs the legacy fallback write
// (and its documented rollback) rather than silently treating the
// operation as handled-on-success.
func (s *DelegatedDomainService) reconcileBindDNSLink(ctx context.Context, wd *pluginDb.WebsiteDomain, website *pluginDb.Website, zoneID uint, domain string, target string) bool {
	logger := s.Logger()
	if s.dnsSvc == nil || wd == nil || website == nil {
		return false
	}
	wd.ZoneID = zoneID
	plan, err := s.CurrentBindingPlan(wd, website)
	if err != nil {
		logger.Warn("plan-driven DNSLink reconciler unavailable at bind: no current-behavior plan for binding; deferring to the legacy DNSLink writer",
			zap.String("domain", domain),
			zap.Uint("domain_id", wd.ID),
			zap.Error(err))
		return false
	}
	result, err := domainapp.ReconcileDNSLink(ctx, domainapp.DNSLinkReconcileInput{
		Plan:          &plan,
		Domain:        domain,
		ZoneID:        zoneID,
		DesiredTarget: target,
	},
		nil, // first-publication write: the legacy bind wrote unconditionally
		bindDNSLinkEffectExecutor{svc: s.dnsSvc},
		logger.Logger)
	if err != nil {
		if errors.Is(err, domainapp.ErrDNSLinkNotReconciled) {
			logger.Warn("plan-driven DNSLink reconciler could not represent the bind-time write; deferring to the legacy DNSLink writer",
				zap.String("domain", domain),
				zap.Uint("domain_id", wd.ID),
				zap.String("profile", plan.ProfileID.String()),
				zap.Error(err))
			return false
		}
		logger.Warn("plan-driven DNSLink bind write failed",
			zap.String("domain", domain),
			zap.Uint("domain_id", wd.ID),
			zap.Uint("zone_id", zoneID),
			zap.Error(err))
		// A failed write was not handled: report false so the caller runs the
		// legacy fallback write and its rollback instead of proceeding as if
		// the DNSLink record is in place.
		return false
	}
	if len(result.Deferred) > 0 {
		logger.Debug("plan-driven DNSLink bind reconcile deferred non-DNSLink effects",
			zap.String("domain", domain),
			zap.String("profile", plan.ProfileID.String()),
			zap.Strings("deferred", result.Deferred))
	}
	return true
}

// bindDNSLinkEffectExecutor adapts the domain-side DNSZoneService into the
// domainapp EffectExecutor port for the bind path. The bind path only ever
// creates (first publication), so no delete executor is wired: a delete
// effect (unreachable at bind — no observations) fails closed.
type bindDNSLinkEffectExecutor struct {
	svc DNSZoneService
}

func (e bindDNSLinkEffectExecutor) WriteDNSLinkRecord(ctx context.Context, zoneID uint, domain string, target string) error {
	if e.svc == nil {
		return fmt.Errorf("no DNS zone service wired for the DNSLink write on %s", domain)
	}
	return e.svc.CreateDNSLinkRecord(ctx, zoneID, domain, target)
}

func (e bindDNSLinkEffectExecutor) DeleteDNSLinkRecord(_ context.Context, zoneID uint, domain string) error {
	return fmt.Errorf("DNSLink record deletion is not supported on the bind path (zone %d, domain %s)", zoneID, domain)
}

// resolveManagedZone returns the PowerDNS zone a managed binding's authoritative
// records live in, applying the one-zone topology rule:
//   - apex domains own their zone (create/reuse the zone for the domain).
//   - subdomains reuse their parent's zone (no new zone for the subdomain).
//
// It returns the zone and whether this call created it (so callers can roll
// back a freshly-created zone on a later step failure).
//
// One-zone invariant: a subdomain never owns its own authoritative zone. If a
// parent zone already exists it MUST be reused, and it must belong to the same
// user — otherwise the subdomain would create a competing authoritative zone
// and let another user squat authority over a name their neighbor already
// hosts. A new zone is only created when no parent zone exists at all.
//
// platformRootID, when non-nil, marks this as a genuine platform claim and
// carries the exact PlatformDomain the claim was granted under. A platform
// root (e.g. "platform.test") is operator-owned; a subdomain under it reuses
// the operator's zone even though the binding's UserID differs. This is the
// intended, narrow relaxation of the one-zone invariant, and it must be gated
// on an actual platform claim — otherwise any user could mint arbitrary
// hostnames under a platform root via the normal bind flow (bypassing label
// validation, availability, and claim semantics) by setting
// domain="anything.<root>" directly. The root is threaded in by the caller
// (the claim was already authorized against it) rather than re-derived by
// suffix-matching the domain across every registered root: a re-derivation
// could mis-allocate a claim to a longer, differently-registered nested root
// whose zone should not be touched.
//
// Sharing the operator's zone (rather than delegating a child zone per
// claim) is a deliberate topology decision: every claim inherits the root's
// one-time DNSSEC setup (a single DS/keyset for the root against its parent)
// with zero per-claim DNSSEC work, and for DANE-capable namespaces (HNS)
// each claim still gets its own _443._tcp.<label>.<root> TLSA inside that
// shared signed zone. Per-claim child zones are intentionally avoided (see
// docs/platform-subdomains-dane-zones.md).
func (s *DelegatedDomainService) resolveManagedZone(ctx context.Context, domain string, userID uint, platformRootID *uint) (*pluginDb.DNSZone, bool, error) {
	// Platform claim: resolve the operator zone for the exact granted root.
	// This never creates a new zone (zoneCreated is always false), so a
	// failing claim cannot take down the shared zone via a later stray
	// DeleteZone — callers must still guard cleanup with zoneCreated.
	if platformRootID != nil {
		var pd pluginDb.PlatformDomain
		if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			if err := tx.First(&pd, *platformRootID).Error; err != nil {
				_ = tx.AddError(err)
			}
			return tx
		}); err != nil {
			return nil, false, fmt.Errorf("load platform root %d: %w", *platformRootID, err)
		}
		if !pd.Enabled {
			return nil, false, fmt.Errorf("platform root %q is disabled", pd.Domain)
		}
		// The claim must reference the granted root itself: an apex match
		// (domain == pd.Domain) for a root-apex binding (BindPlatformRootApex),
		// or a name that descends from the granted root for a subdomain claim.
		// Either way the authoritative zone is the operator's platform-root
		// zone. The subdomain path is also guarded upstream (CreatePlatformSubdomain
		// rejects compositions that collapse to the apex), so this is the
		// enforcement point of record for the one-zone platform relaxation.
		// Apex match (domain == pd.Domain) for a root-apex binding
		// (BindPlatformRootApex), or a name that label-boundary-descends from
		// the granted root for a subdomain claim.
		if !isPlatformRootApexOrDescendant(domain, pd.Domain) {
			return nil, false, fmt.Errorf("domain %q is not the apex or a subdomain of platform root %q", domain, pd.Domain)
		}
		z, err := s.dnsSvc.GetZoneByDomain(ctx, pd.Domain)
		if err != nil {
			return nil, false, fmt.Errorf("lookup platform zone for %q: %w", pd.Domain, err)
		}
		if z == nil {
			return nil, false, fmt.Errorf("platform root %q has no provisioned zone", pd.Domain)
		}
		return z, false, nil
	}

	// A subdomain (e.g. docs.example.xyz) lives inside its parent's zone
	// (example.xyz); only the apex owns a zone.
	if parent := parentDomain(domain); parent != "" {
		// A subdomain nested under an operator-owned platform root must only be
		// minted through the platform claim flow (CreatePlatformSubdomain /
		// BindPlatformRootApex), which runs label validation, availability checks
		// and sets PlatformDomainID. The normal bind path must refuse it even
		// when the requesting user happens to own the parent zone — otherwise the
		// operator admin (or anyone matching the zone owner) could mint arbitrary
		// hostnames under the root, bypassing claim semantics entirely.
		if rootPD, rerr := s.enabledPlatformRootForDomain(ctx, parent); rerr != nil {
			return nil, false, rerr
		} else if rootPD != nil {
			return nil, false, fmt.Errorf("domain %q is under platform root %q; it must be claimed via the platform subdomain flow", domain, rootPD.Domain)
		}

		z, err := s.dnsSvc.GetZoneByDomain(ctx, parent)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, fmt.Errorf("lookup parent zone %q: %w", parent, err)
		}
		if err == nil && z != nil {
			if z.UserID != userID {
				return nil, false, fmt.Errorf("parent zone %q is owned by another user", parent)
			}
			return z, false, nil
		}
		// No parent zone exists — fall through and create a zone for the domain.
	}

	z, err := s.dnsSvc.CreateZone(ctx, domain, userID)
	if err != nil {
		return nil, false, fmt.Errorf("zone creation failed: %w", err)
	}
	return z, true, nil
}

// zoneLifecycleKey returns the canonical apex name of the PowerDNS zone a
// bind/convert of `domain` lives in, mirroring the one-zone rule in
// resolveManagedZone (apex owns its zone; a subdomain reuses the parent's
// zone). Both CreateDomain and ConvertToOnChain derive the same key so their
// per-zone lifecycle locks collide.
func zoneLifecycleKey(domain string) string {
	if p := parentDomain(domain); p != "" {
		return canonicalZoneName(p)
	}
	return canonicalZoneName(domain)
}

// zoneLifecycleLock returns the per-zone mutex serializing zone lifecycle
// transitions for the given zone apex.
func (s *DelegatedDomainService) zoneLifecycleLock(zoneApex string) *sync.Mutex {
	v, _ := s.zoneLifecycleLocks.LoadOrStore(zoneApex, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// withZoneLifecycleLock runs fn while holding the per-zone lifecycle lock for
// the given zone apex, serializing zone provisioning (CreateDomain) against
// zone deletion (ConvertToOnChain) for the same zone.
func (s *DelegatedDomainService) withZoneLifecycleLock(zoneApex string, fn func() error) error {
	lock := s.zoneLifecycleLock(zoneApex)
	lock.Lock()
	defer lock.Unlock()
	return fn()
}

// parentDomain returns the domain's parent (everything after the first label),
// or "" when the domain is an apex (single-label HNS, or a bare TLD-less ICANN
// name). Mirrors the website service's extractParentDomain.
func parentDomain(domain string) string {
	parts := strings.Split(strings.TrimSuffix(domain, "."), ".")
	if len(parts) <= 2 {
		return ""
	}
	return strings.Join(parts[1:], ".")
}

func (s *DelegatedDomainService) CreateDomain(ctx context.Context,
	namespace, domain string, websiteID, userID uint, dnsHostingEnabled bool,
	notifyCreated bool, config json.RawMessage, platformRootID *uint) (*pluginDb.WebsiteDomain, error) {

	// Require a database connection up front: many call sites feed through a
	// service that may not be wired to a DB (e.g. the website-create API when
	// only the website service is exercised), and using s.DB() below without a
	// guard would panic with a nil pointer dereference.
	if s.DB() == nil {
		return nil, fmt.Errorf("database not available")
	}

	provider := s.registry.Get(namespace)
	if provider == nil {
		return nil, fmt.Errorf("unsupported namespace: %s", namespace)
	}

	domain = NormalizeDomain(domain)

	if err := provider.Validate(domain); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Detect whether the name is managed on-chain (e.g. a Handshake HIP-5 name
	// whose NS record points at an external contract). Best-effort: providers
	// without an on-chain concept (ICANN) return false immediately; HNS defaults
	// to native when the resolver cannot answer.
	//
	// A HIP-5 name serves its own DNS from the contract, so portal DNS hosting
	// cannot apply to it. Binding is NOT rejected for dnsHostingEnabled=true
	// (users default to managed DNS in the UX flow): the request is coerced to
	// onchain_managed with dns_hosting_enabled=false — the only coherent state —
	// and the response (status onchain_managed) lets the UI explain that DNS is
	// served on-chain rather than failing the bind.
	//
	// Platform subdomain claims (platformRootID != nil) never inspect: the
	// platform root is operator-owned and its subdomains resolve via the
	// operator's own portal-managed zone, so they can never be HIP-5 — and
	// skipping the resolver query keeps the operator hot path (every platform
	// claim) free of a per-bind DNS round-trip.
	var onchainManaged bool
	if platformRootID == nil {
		var inspectErr error
		onchainManaged, inspectErr = provider.Inspect(ctx, domain)
		if inspectErr != nil {
			return nil, fmt.Errorf("domain inspection failed: %w", inspectErr)
		}
	}

	var website pluginDb.Website
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.
			Where("user_id = ? AND id = ?", userID, websiteID).
			First(&website).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	}); err != nil {
		return nil, fmt.Errorf("website lookup failed: %w", err)
	}

	wd := &pluginDb.WebsiteDomain{
		WebsiteID: websiteID,
		UserID:    userID,
		Domain:    domain,
		Namespace: pluginDb.DomainNamespace(namespace),
		ZoneName:  canonicalZoneName(domain),
		Status:    pluginDb.DomainStatusDraft,
		// The per-domain DNS hosting flag is threaded from the bind request
		// (default true). It gates whether this flow provisions a PowerDNS
		// zone; when false, no zone is created and the binding is self-hosted
		// DNS (see the zone-creation decision below).
		DNSHostingEnabled: dnsHostingEnabled,
	}

	// Soft deletes leave a tombstone row that still occupies the
	// (domain, namespace) unique key, so re-binding the same domain after a
	// delete would violate the constraint. This app-level guardrail (matching
	// the system's soft-delete semantics without relying on a partial index)
	// purges any prior soft-deleted tombstone for this key before inserting,
	// freeing it for a fresh binding. Only tombstones (deleted_at IS NOT NULL)
	// are removed; a live same-key binding is a genuine conflict and left to the
	// unique key to reject.
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.
			Where("domain = ? AND namespace = ? AND deleted_at IS NOT NULL", domain, namespace).
			Unscoped().Delete(&pluginDb.WebsiteDomain{}).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	}); err != nil {
		return nil, fmt.Errorf("failed to purge stale domain binding: %w", err)
	}

	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.Create(wd).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	}); err != nil {
		return nil, fmt.Errorf("persist failed: %w", err)
	}

	// An on-chain managed name (HIP-5) serves its DNS from an external
	// contract: the portal owns only ownership verification (a TXT token
	// resolved through the HNS-aware resolver, which bridges to the contract)
	// — no zone, no DNSLink/apex, no DNSSEC, and no portal-published DANE. The
	// binding is still recorded with a distinct status so downstream code
	// routes it to TXT-token verification instead of the delegation/DS flow
	// used by native HNS. DANE is not dropped, though: the stable TLSA the
	// on-chain zone data must serve is bootstrapped and exposed at bind time
	// (see ensureDANEIdentity), because DANE still applies on-chain.
	// dnsHostingEnabled is always coerced to false here, even when the caller
	// requested managed DNS (the default in the UX flow): portal hosting is
	// impossible for a contract-served name, and the persisted flag must agree
	// with the absence of a zone.
	if onchainManaged {
		wd.Status = pluginDb.DomainStatusOnchainManaged
		wd.DNSHostingEnabled = false
		var finErr error
		if finErr = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			if err := tx.Model(wd).Updates(map[string]any{
				"status":              pluginDb.DomainStatusOnchainManaged,
				"dns_hosting_enabled": false,
			}).Error; err != nil {
				_ = tx.AddError(err)
			}
			return tx
		}); finErr != nil {
			// The row was already inserted with dns_hosting_enabled from the
			// request (true by default). A half-finalized onchain binding is the
			// worst failure shape: it would look like an "enable-orphan" to
			// SetDomainDNSEnabled and a retry could provision a PowerDNS zone
			// for a genuinely HIP-5 name. Remove the row so the bind is cleanly
			// rolled back and the name can be retried.
			s.deleteBindingBestEffort(ctx, wd)
			return nil, fmt.Errorf("failed to finalize domain record: %w", finErr)
		}
		// DANE still applies to a chain-managed name (the TLSA is served from
		// the name's on-chain zone data), so the stable DANE key must exist at
		// bind time — the same bootstrap invariant the self-hosted path
		// enforces. A failure rolls the just-inserted row back so the bind is
		// cleanly retryable.
		if err := s.ensureDANEIdentity(ctx, provider, namespace, domain); err != nil {
			s.deleteBindingBestEffort(ctx, wd)
			return nil, err
		}
		// This binding is the new website's first (primary) domain. Record it
		// so the website service resolves the apex domain via PrimaryDomainID
		// rather than the status=active fallback. A failure rolls the just-
		// inserted row back so the bind is cleanly retryable.
		if err := s.assignPrimaryAndNotify(ctx, &website, wd, notifyCreated); err != nil {
			s.deleteBindingBestEffort(ctx, wd)
			return nil, err
		}
		return wd, nil
	}

	// A self-hosted DNS binding owns no PowerDNS zone: the user runs the
	// authoritative server, so the portal must not create a zone, DNSLink,
	// apex, or generated delegation. The binding is marked self_hosted (bound,
	// DNS not provisioned by Pinner); the user enables portal DNS hosting
	// later via domain update (SetDomainDNSEnabled) if they want Pinner to
	// host.
	if !dnsHostingEnabled {
		wd.Status = pluginDb.DomainStatusSelfHosted
		if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			if err := tx.Model(wd).Update("status", pluginDb.DomainStatusSelfHosted).Error; err != nil {
				_ = tx.AddError(err)
			}
			return tx
		}); err != nil {
			return nil, fmt.Errorf("failed to finalize domain record: %w", err)
		}
		if err := s.ensureDANEIdentity(ctx, provider, namespace, domain); err != nil {
			s.deleteBindingBestEffort(ctx, wd)
			return nil, err
		}
		// This binding is the new website's first (primary) domain. Record it
		// so the website service resolves the apex domain via PrimaryDomainID
		// rather than the status=active fallback — a self-hosted binding is
		// not active, so it would otherwise resolve to an empty domain. The
		// created notification fires here because self-hosted bindings skip
		// the managed-DNS assignment path below.
		if err := s.assignPrimaryAndNotify(ctx, &website, wd, notifyCreated); err != nil {
			return nil, err
		}
		return wd, nil
	}

	// Managed DNS: create DNS resources only after the DB row is committed.
	// The authoritative zone follows the one-zone rule — apex owns, subdomain
	// reuses the parent's zone. The per-zone lifecycle lock is held across zone
	// resolution through the row's zone-id commit so a concurrent
	// ConvertToOnChain cannot delete the zone (and its records/DNSSEC) between
	// the bind deciding to use it and the binding committing its reference.
	if err := s.withZoneLifecycleLock(zoneLifecycleKey(domain), func() error {
		zone, zoneCreated, err := s.resolveManagedZone(ctx, domain, userID, platformRootID)
		if err != nil {
			s.deleteBindingBestEffort(ctx, wd)
			return fmt.Errorf("zone resolution failed: %w", err)
		}

		target := pluginDb.WebsiteTargetType(website.TargetType).ToDNSLinkPath(website.TargetHash())
		// Name the record after the binding's domain (not the zone apex) so a
		// subdomain reusing a parent zone writes its own _dnslink.<subdomain>,
		// not the parent's.
		//
		// With the DNSLink reconciler feature
		// flag enabled, this bind-time creation goes through the plan-driven
		// reconciler — the binding plan (already from this service's own
		// CurrentBindingPlan) derives the write effect. The legacy
		// unconditional CreateDNSLinkRecord below is the fallback for anything
		// the flagged path cannot represent (mapper rejection, fail-closed
		// diff): same write, same rollback, same error semantics.
		if !(s.dnsLinkReconcilerEnabled() && s.reconcileBindDNSLink(ctx, wd, &website, zone.ID, domain, target)) {
			if err := s.dnsSvc.CreateDNSLinkRecord(ctx, zone.ID, domain, target); err != nil {
				s.deleteBindingBestEffort(ctx, wd)
				if zoneCreated {
					_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
				}
				return fmt.Errorf("dnslink creation failed: %w", err)
			}
		}

		// Create apex record pointing to the gateway. DNSSEC-signed alt-root
		// providers (e.g. HNS) need a real A record (gateway IP) so the apex
		// carries an RRSIG; otherwise use an ALIAS to the gateway hostname.
		apexType := provider.ApexRecordType()
		var apexContent string
		if apexType == pluginCore.RecordTypeA {
			apexContent = s.gatewayIP()
			if apexContent == "" {
				s.deleteBindingBestEffort(ctx, wd)
				if zoneCreated {
					_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
				}
				return fmt.Errorf("gateway_ip not configured: alt-root apex requires a real A record and cannot fall back to ALIAS (set dns.gateway_ip, e.g. to the gateway IP)")
			}
		} else if gatewayHost := s.gatewayHost(); gatewayHost != "" {
			apexContent = gatewayHost
		}

		if apexContent != "" {
			if err := s.dnsSvc.CreateApexRecord(ctx, zone.ID, domain, apexType, apexContent); err != nil {
				s.deleteBindingBestEffort(ctx, wd)
				if zoneCreated {
					_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
				}
				return fmt.Errorf("apex record creation failed: %w", err)
			}
			wd.GatewayHost = apexContent
		}

		// Build delegation after zone is created (needs zone ID). The provider
		// returns its typed delegation already serialized as json.RawMessage, so
		// no untyped any crosses the provider boundary here.
		delegationBytes, err := provider.BuildDelegation(ctx, zone.ID, domain, &website, config)
		if err != nil {
			s.deleteBindingBestEffort(ctx, wd)
			// Only tear down a zone this call created. For a platform claim the
			// zone is the operator's shared platform-root zone (zoneCreated is
			// false), which must never be deleted on a per-claim failure —
			// doing so would take down every subdomain on that root.
			if zoneCreated {
				_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
			}
			return fmt.Errorf("delegation build failed: %w", err)
		}

		// Update the row with zone info, delegation data, and final status.
		wd.ZoneID = zone.ID
		wd.Status = pluginDb.DomainStatusRecordsGenerated
		wd.DelegationData = jsonToMap(delegationBytes)
		if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			if err := tx.Model(wd).Updates(map[string]any{
				"zone_id":             zone.ID,
				"status":              pluginDb.DomainStatusRecordsGenerated,
				"delegation_data":     wd.DelegationData,
				"dns_hosting_enabled": wd.DNSHostingEnabled,
			}).Error; err != nil {
				_ = tx.AddError(err)
			}
			return tx
		}); err != nil {
			return fmt.Errorf("failed to finalize domain record: %w", err)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	// Primary/notify handled by the shared helper: a fresh website gets its
	// first binding recorded as primary, and only a genuine website creation
	// (flag set by the create API caller) fires the admin created email —
	// domain-add operations on an existing website must not emit one.
	if err := s.assignPrimaryAndNotify(ctx, &website, wd, notifyCreated); err != nil {
		return nil, err
	}

	return wd, nil
}

// deleteBindingBestEffort hard-deletes a WebsiteDomain row as a cleanup /
// compensation step. Matches the original fire-and-forget compensation
// semantics: it now retries on lock contention via the retryable-transaction
// wrapper, but errors are still swallowed (never propagated to the caller).
func (s *DelegatedDomainService) deleteBindingBestEffort(ctx context.Context, wd *pluginDb.WebsiteDomain) {
	_ = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		_ = tx.Unscoped().Delete(wd).Error
		return tx
	})
}

// assignPrimaryAndNotify records wd as the website's primary when the website
// has none yet — so the website service resolves the apex domain via
// PrimaryDomainID rather than the status=active fallback — and, when
// notifyCreated, fires the admin "website created" notification. Shared by the
// managed, self-hosted, and on-chain managed bind paths. Only the primary
// write is fatal; the notification (via notifyAdminWebsiteCreated) is
// best-effort.
func (s *DelegatedDomainService) assignPrimaryAndNotify(ctx context.Context, website *pluginDb.Website, wd *pluginDb.WebsiteDomain, notifyCreated bool) error {
	if website.PrimaryDomainID == nil {
		if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			if err := tx.Model(website).Update("primary_domain_id", wd.ID).Error; err != nil {
				_ = tx.AddError(err)
			}
			return tx
		}); err != nil {
			return fmt.Errorf("failed to set primary domain: %w", err)
		}
		website.PrimaryDomainID = &wd.ID
	}
	if notifyCreated {
		s.notifyAdminWebsiteCreated(ctx, website.ID)
	}
	return nil
}

// notifyAdminWebsiteCreated fires the admin "website created" notification for
// the given website, delegating to the WebsiteService. It never fails the
// caller: a resolution or template/mail error is logged and swallowed.
func (s *DelegatedDomainService) notifyAdminWebsiteCreated(ctx context.Context, websiteID uint) {
	if ws := core.GetServiceOptional[pluginCore.WebsiteService](s.Context(), pluginCore.WEBSITE_SERVICE); ws != nil {
		if nerr := ws.NotifyAdminWebsiteCreated(ctx, websiteID); nerr != nil {
			s.Logger().Warn("Failed to send website created notification",
				zap.Uint("website_id", websiteID), zap.Error(nerr))
		}
	}
}

// Human-readable messages reported on DelegationVerificationResult.Checks.
// Package-level vars so verify messages are single-sourced and tests assert
// them without restating inline literals.
const (
	msgPlatformBinding     = "operator-trusted platform binding"
	msgOnChainNoDelegation = "name held on-chain (HIP-5); no portal delegation to verify"
	msgNoPortalDelegation  = "no portal delegation to verify (self-hosted / on-chain managed / unresolved)"
	msgDNSSECNotRequired   = "namespace does not require DNSSEC"
	msgDNSSECNoKey         = "no active DNSSEC signing key"
	msgDNSSECKeyPresent    = "DNSSEC signing key present"
	msgDelegationLive      = "approved nameservers are live"
	msgDelegationPending   = "nameservers not yet visible at the parent zone"
)

// verifyDomainOptions carries the internal switches set through
// VerifyDomainOption; the zero value preserves the historical behavior.
type verifyDomainOptions struct {
	// skipPlatformTrustValidation suppresses the ValidatePlatformBinding DB
	// revalidation for bindings whose platform trust was already validated
	// earlier in the same validation pass (see WithPrevalidatedPlatformTrust).
	skipPlatformTrustValidation bool
}

// VerifyDomainOption adjusts VerifyDomain's internal work for callers that
// already performed part of the verification flow in the same validation
// pass. No option changes VerifyDomain's output contract: the returned
// states, typed errors, and persisted effects stay identical.
type VerifyDomainOption func(*verifyDomainOptions)

// WithPrevalidatedPlatformTrust tells VerifyDomain that the binding's
// platform-trust relation (ValidatePlatformBinding) already succeeded in the
// same validation pass, so the duplicate DB revalidation is skipped. Callers
// must only pass this when the shared validator ran on this exact binding row
// and succeeded — in the ValidateDNS flow, plan derivation
// (CurrentBindingPlan) runs ValidatePlatformBinding fail-closed for every
// portal-managed platform binding before the delegation gate is reached, so a
// trust failure aborts validation before this point. VerifyDomain's remaining
// platform work (auto-activation status write and result shape) is unchanged.
func WithPrevalidatedPlatformTrust() VerifyDomainOption {
	return func(o *verifyDomainOptions) { o.skipPlatformTrustValidation = true }
}

// VerifyDomain checks delegation and persists the result. It returns a typed
// DelegationVerificationResult so callers can distinguish "not applicable"
// (self-hosted / on-chain / unresolved bindings) from "pending" (portal
// delegation not yet live) from "verified" without re-deriving hosting rules.
func (s *DelegatedDomainService) VerifyDomain(ctx context.Context,
	wd *pluginDb.WebsiteDomain, opts ...VerifyDomainOption) (DelegationVerificationResult, error) {

	var vopts verifyDomainOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&vopts)
		}
	}

	provider := s.registry.Get(string(wd.Namespace))
	if provider == nil {
		return DelegationVerificationResult{}, fmt.Errorf("unsupported namespace: %s", wd.Namespace)
	}

	// A platform subdomain is minted under an operator-owned root: the platform
	// controls both sides of the DNS check, so there is no user-side TXT
	// verification to perform and no external delegation to wait on. It is
	// considered active as soon as it exists. The auto-activation is only sound
	// when the full operator-trust relationship holds (shared validator): the
	// root resolves live, the namespaces match, the domain is the apex or a
	// label-boundary descendant, and the binding's zone equals the root's zone.
	// A mismatch is data-integrity corruption and must NOT auto-activate the
	// binding or mutate any status.
	if wd.PlatformDomainID != nil {
		// The trust revalidation can be skipped only when the caller
		// guarantees it already ran on this row in the same pass; every other
		// caller still gets the full shared-validator check.
		if !vopts.skipPlatformTrustValidation {
			if err := s.ValidatePlatformBinding(ctx, wd); err != nil {
				return DelegationVerificationResult{}, err
			}
		}
		wd.Status = pluginDb.DomainStatusActive
		if s.DB() != nil {
			if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				if err := tx.Model(wd).Update("status", wd.Status).Error; err != nil {
					_ = tx.AddError(err)
				}
				return tx
			}); err != nil {
				return DelegationVerificationResult{}, fmt.Errorf("failed to persist domain status: %w", err)
			}
		}
		return DelegationVerificationResult{
			State: DelegationVerified,
			Checks: []pluginCore.ValidationCheck{
				{Name: pluginCore.ValidationCheckPlatform, OK: true, Message: msgPlatformBinding},
			},
		}, nil
	}

	// A binding created before handover source detection may still carry a
	// portal-managed zone even though the name has since become HIP-5. Inspect
	// that source before touching DNSSEC or portal delegation; the same response
	// is then passed to the conversion helper so verification performs one query.
	// The inspection is consumed as the typed route observation; the
	// legacy Inspect bool is exactly "the observed route is cross-chain"
	// (OnChainManagedFromRoute), so this decision is unchanged. VerifyDomain
	// is the only sanctioned inspection point: the website-validation hot
	// path (ValidateDNS/shouldPerformTokenCheck/checkDelegation) must never
	// probe the namespace — gate selection is done from the
	// persisted-facts plan instead.
	if wd.Namespace == pluginDb.DomainNamespaceHNS && wd.ZoneID != 0 &&
		wd.Status != pluginDb.DomainStatusOnchainManaged {
		route, inspectErr := provider.InspectRoute(ctx, wd.Domain)
		if inspectErr != nil {
			return DelegationVerificationResult{}, fmt.Errorf("domain inspection failed: %w", inspectErr)
		}
		if route.Route == domainpolicy.ResolutionRouteCrossChain {
			if err := s.convertInspectedBindingToOnChain(ctx, wd); err != nil {
				if errors.Is(err, ErrDomainZoneShared) {
					s.Logger().Info("HIP-5 binding shares its zone; skipping reclassification",
						zap.Uint("id", wd.ID),
						zap.String("domain", wd.Domain),
						zap.Error(err))
				} else {
					return DelegationVerificationResult{}, fmt.Errorf("convert on-chain binding: %w", err)
				}
			} else {
				return DelegationVerificationResult{
					State: DelegationNotApplicable,
					Checks: []pluginCore.ValidationCheck{
						{Name: pluginCore.ValidationCheckOnChain, OK: true, Message: msgOnChainNoDelegation},
					},
				}, nil
			}
		}
	}

	// Only portal-managed bindings have portal delegation to verify
	// (NeedsDelegationVerification, the authoritative derived hosting locus):
	//   - self-hosted: the user runs the authoritative server; DNSSEC/DANE
	//     delegation is theirs.
	//   - on-chain managed (HIP-5): ownership is proven via the namespace-aware
	//     TXT token flow, never via delegation.
	//   - unresolved (draft/error/empty): not classifiable until provisioning
	//     resolves it.
	// All return NotApplicable without touching PowerDNS. In particular an
	// on-chain binding carrying a stray zone ID is data incoherence and must
	// not authorize any portal DNS work: log the inconsistency and still return
	// NotApplicable.
	if !wd.NeedsDelegationVerification() {
		if wd.Status == pluginDb.DomainStatusOnchainManaged && wd.ZoneID != 0 {
			s.Logger().Warn("on-chain managed binding carries a stray zone; refusing portal DNS operations",
				zap.Uint("id", wd.ID),
				zap.String("domain", wd.Domain),
				zap.String("status", string(wd.Status)),
				zap.Uint("zone_id", wd.ZoneID))
		}
		s.Logger().Debug("delegation verification not applicable for binding",
			zap.Uint("id", wd.ID),
			zap.String("domain", wd.Domain),
			zap.String("namespace", string(wd.Namespace)),
			zap.String("status", string(wd.Status)),
			zap.Uint("zone_id", wd.ZoneID))
		return DelegationVerificationResult{
			State: DelegationNotApplicable,
			Checks: []pluginCore.ValidationCheck{
				{Name: pluginCore.ValidationCheckDelegation, OK: true, Message: msgNoPortalDelegation},
			},
		}, nil
	}

	// Expected DS is computed live from PowerDNS's current active signing key
	// (never persisted, so it cannot go stale on key rotation). Only
	// managed-DNSSEC namespaces (provider.RequiresDNSSEC, e.g. HNS)
	// require it: HNS uses the DS to require the parent zone to serve it before
	// marking the domain Active. Other providers (e.g. ICANN) verify on NS
	// visibility alone and ignore DS, so a DS-resolution failure must never fail
	// their verification — otherwise transient PowerDNS/DS slowness on an ICANN
	// root blocks delegation validation entirely.
	//
	// For a managed-DNSSEC zone with no active signing key (("", nil)) the zone
	// is genuinely self-managed — the portal generated no DS, so NS-only
	// verification is correct. But if resolution ERRORS (key rollover with
	// multiple active keys, PowerDNS unreachable), the zone is portal-managed and
	// the live DS is indeterminate. We must NOT silently weaken a managed zone to
	// NS-only on a transient failure: that would mark Active a zone whose DS
	// chain of trust was not actually confirmed.
	var expectedDS string
	if provider.RequiresDNSSEC() {
		var dsErr error
		expectedDS, dsErr = s.dnsSvc.GetActiveDNSSECDS(ctx, wd.ZoneID)
		if dsErr != nil {
			return DelegationVerificationResult{}, fmt.Errorf("resolve live DS for zone %d: %w", wd.ZoneID, dsErr)
		}
	} else {
		// Best-effort for non-DNSSEC providers: surface DB/PowerDNS errors so
		// they are observable, but the provider verifies on NS and ignores DS,
		// so a failure here must not block delegation.
		if ds, dsErr := s.dnsSvc.GetActiveDNSSECDS(ctx, wd.ZoneID); dsErr != nil {
			s.Logger().Warn("failed to resolve live DS for non-DNSSEC namespace, continuing",
				zap.Uint("zone_id", wd.ZoneID),
				zap.String("domain", wd.Domain),
				zap.Error(dsErr))
		} else {
			expectedDS = ds
		}
	}

	// Self-heal re-ensures the portal-managed-zone invariants that are
	// otherwise only established at bind/create time (see selfHealZone).
	// Gate 1 (DNSSEC) covers managed-DNSSEC namespaces; gate 2 (SOA MNAME)
	// covers any portal-managed PowerDNS zone, ICANN included.
	//
	// With the repair-reconciler flag enabled,
	// the plan-driven heal derives its effects from the binding plan and
	// Diff (observation + reconcile); when it cannot represent the heal it
	// defers to the legacy selfHealZone with a loud log. Flag OFF (default):
	// the legacy body runs verbatim.
	healHandled := false
	if s.repairReconcilerEnabled() {
		handled, healedDS, healErr := s.healZonePlanDriven(ctx, provider, wd, expectedDS)
		if handled {
			healHandled = true
			if healErr != nil {
				return DelegationVerificationResult{}, healErr
			}
			expectedDS = healedDS
		}
	}
	if !healHandled {
		legacyDS, legacyErr := s.selfHealZone(ctx, provider, wd, expectedDS)
		if legacyErr != nil {
			return DelegationVerificationResult{}, legacyErr
		}
		expectedDS = legacyDS
	}

	verified, err := provider.VerifyDelegation(ctx, wd.Domain, expectedDS)
	if err != nil {
		wd.Status = pluginDb.DomainStatusError
		if s.DB() != nil {
			_ = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				_ = tx.Model(wd).Update("status", wd.Status).Error
				return tx
			})
		}
		return DelegationVerificationResult{}, err
	}

	state := DelegationPending
	var approvedNS, liveNS []string
	if verified {
		wd.Status = pluginDb.DomainStatusActive
		state = DelegationVerified
	} else {
		// Grab the expected vs discovered NS (same comparison the janitor's
		// zone validation performs) so a stuck waiting_delegation can be
		// diagnosed from logs alone. Best-effort: an NS lookup failure must
		// not mask the pending outcome.
		approvedNS = provider.Nameservers()
		// With the repair-reconciler flag enabled, the live delegation
		// evidence flows through the domainapp.DelegationNSCollector port
		// (wired by the repair reconciler; checks remain checks — the delegation verdict
		// itself stays on VerifyDelegation above, this port carries
		// diagnostics only). Flag OFF (default): the same lookup is done
		// directly, unchanged.
		if s.repairReconcilerEnabled() {
			collected, colErr := delegationNSCollector{svc: s.dnsSvc, provider: provider, zoneID: wd.ZoneID}.CollectDelegation(ctx, wd.Domain)
			if colErr != nil {
				s.Logger().Debug("failed to resolve live delegation evidence for pending delegation",
					zap.String("domain", wd.Domain),
					zap.Error(colErr))
			} else {
				liveNS = collected.NS.Nameservers
			}
		} else if live, nsErr := provider.LiveNameservers(ctx, wd.Domain); nsErr != nil {
			s.Logger().Debug("failed to resolve live nameservers for pending delegation",
				zap.String("domain", wd.Domain),
				zap.Error(nsErr))
		} else {
			liveNS = live
		}
		s.Logger().Debug("delegation not visible at parent zone yet",
			zap.Uint("id", wd.ID),
			zap.String("domain", wd.Domain),
			zap.String("namespace", string(wd.Namespace)),
			zap.Uint("zone_id", wd.ZoneID),
			zap.Bool("dnssec_required", provider.RequiresDNSSEC()),
			zap.Bool("expected_ds_present", expectedDS != ""),
			zap.Strings("approved_ns", approvedNS),
			zap.Strings("live_ns", liveNS))
		wd.Status = pluginDb.DomainStatusWaitingDelegation
	}

	if s.DB() != nil {
		if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			if err := tx.Model(wd).Update("status", wd.Status).Error; err != nil {
				_ = tx.AddError(err)
			}
			return tx
		}); err != nil {
			return DelegationVerificationResult{}, fmt.Errorf("failed to persist domain status: %w", err)
		}
	}

	return DelegationVerificationResult{
		State:      state,
		ApprovedNS: approvedNS,
		LiveNS:     liveNS,
		Checks: []pluginCore.ValidationCheck{
			dnssecCheck(provider.RequiresDNSSEC(), expectedDS),
			delegationCheck(state == DelegationVerified, approvedNS, liveNS),
		},
	}, nil
}

// dnssecCheck builds the DNSSEC gate check, deriving the single-sourced
// message (package-level const) from whether the namespace requires DNSSEC and
// whether an active signing key (DS) is present.
func dnssecCheck(required bool, ds string) pluginCore.ValidationCheck {
	msg := msgDNSSECKeyPresent
	switch {
	case !required:
		msg = msgDNSSECNotRequired
	case ds == "":
		msg = msgDNSSECNoKey
	}
	return pluginCore.ValidationCheck{
		Name: pluginCore.ValidationCheckDNSSEC,
		// A namespace that does not require DNSSEC satisfies the gate by
		// definition, so a non-DNSSEC (e.g. ICANN) domain must report OK even
		// with no DS — otherwise every validated ICANN delegation would show a
		// red dnssec gate with the contradictory "not required" message.
		OK:       ds != "" || !required,
		Message:  msg,
		Expected: ds,
	}
}

// delegationCheck builds the delegation gate check, carrying the expected vs
// discovered nameservers and a single-sourced message derived from whether the
// approved NS set is live.
func delegationCheck(verified bool, approvedNS, liveNS []string) pluginCore.ValidationCheck {
	msg := msgDelegationLive
	if !verified {
		msg = msgDelegationPending
	}
	return pluginCore.ValidationCheck{
		Name:     pluginCore.ValidationCheckDelegation,
		OK:       verified,
		Message:  msg,
		Expected: strings.Join(approvedNS, ", "),
		Found:    strings.Join(liveNS, ", "),
	}
}

// selfHealZone re-ensures the portal-managed-zone invariants that are
// otherwise only established at bind/create time, so verification recovers a
// zone that slipped past (or drifted from) those one-time setup steps without
// requiring the user to re-bind. The two invariants are gated independently:
//
//  1. DNSSEC active signing key (fatal). For managed-DNSSEC namespaces
//     (RequiresDNSSEC, e.g. HNS). Uses the same capability as the expectedDS
//     computation in VerifyDomain: a namespace that requires a live DS still
//     heals its signing key even when it does not publish DANE. A
//     "no active key" result (("", nil))
//     means DNSSEC was never enabled or the key was rotated away: EnableDNSSEC
//     is idempotent (reuses an active key, mints one only when none exists),
//     then the live DS is re-read. Failure is fatal — a managed zone without a
//     key cannot be safely verified. The error path (GetActiveDNSSECDS errored,
//     not empty) is left to fail loudly: that state is indeterminate (PowerDNS
//     down / key rollover), so we do not mint keys on it.
//
//  2. SOA MNAME (best-effort). For any portal-managed PowerDNS zone
//     (wd.ZoneID != 0 — every hosted binding, HNS and ICANN). PowerDNS seeds
//     new zones with a placeholder MNAME that is only corrected once at create;
//     this re-ensures it idempotently for all portal-managed zones. Non-fatal:
//     the SOA MNAME is a secondary authoritative pointer (delegation is carried
//     by the NS record), so a failed correction is logged, not raised.
//
// It returns the (possibly healed) expected DS and a fatal error, or nil.
//
// Fail-closed contract for the DNSSEC gate: if EnableDNSSEC successfully
// returns but the post-heal live DS read is non-empty we proceed; if the read
// succeeds but is empty we return a wrapped error (the invariant could not be
// established) so VerifyDomain does not fall through to VerifyDelegation and
// must not mark the binding Active. An erroring read is indeterminate and also
// returns an error. Only a non-DNSSEC policy treats a missing/erroring DS as
// non-fatal (handled by VerifyDomain before selfHealZone is consulted).
func (s *DelegatedDomainService) selfHealZone(ctx context.Context, provider DomainProvider, wd *pluginDb.WebsiteDomain, expectedDS string) (string, error) {
	// DNSSEC self-heal: only managed-DNSSEC namespaces (policy DNSSEC required,
	// the same gate VerifyDomain uses for expectedDS; a DNSSEC-required
	// namespace that does not publish DANE must still heal its signing key).
	if provider.RequiresDNSSEC() && expectedDS == "" {
		if _, err := s.dnsSvc.EnableDNSSEC(ctx, wd.ZoneID); err != nil {
			return "", fmt.Errorf("enable dnssec for zone %d: %w", wd.ZoneID, err)
		}
		// Re-read the DS now that the zone should have an active key.
		healedDS, dsErr := s.dnsSvc.GetActiveDNSSECDS(ctx, wd.ZoneID)
		if dsErr != nil {
			return "", fmt.Errorf("resolve live DS for zone %d after enable: %w", wd.ZoneID, dsErr)
		}
		if healedDS == "" {
			return "", fmt.Errorf("dnssec self-heal failed for zone %d (domain %s): no active signing key after EnableDNSSEC; cannot verify delegation", wd.ZoneID, wd.Domain)
		}
		expectedDS = healedDS
	}

	// SOA MNAME self-heal: any portal-managed PowerDNS zone (wd.ZoneID != 0),
	// independent of DANE/DNSSEC — applies to ICANN-hosted zones too.
	if wd.ZoneID != 0 {
		if err := s.dnsSvc.EnsureSOAMNAME(ctx, wd.ZoneID, wd.Domain, provider.Nameservers()); err != nil {
			s.Logger().Warn("SOA MNAME self-heal failed (best-effort)",
				zap.String("domain", wd.Domain),
				zap.Uint("zone_id", wd.ZoneID),
				zap.Error(err))
		}
	}

	return expectedDS, nil
}

// healZonePlanDriven is the plan-driven replacement for selfHealZone:
// it represents DNSSEC key absence and SOA drift as observations, derives
// the effects from the binding's plan (domainpolicy.Diff), and applies them
// through the domainapp repair reconciler. Legacy semantics are preserved
// exactly:
//
//   - DNSSEC: only a RequiresDNSSEC namespace whose live DS read came back
//     empty (ZoneDNSSECStateDisabled) yields an ensure-dnssec effect. The
//     executor's failure is fatal, and after a successful ensure this caller
//     re-reads the live DS fail-closed: an empty or erroring post-heal read
//     fails the verification exactly like the legacy self-heal did.
//   - SOA MNAME: any portal-managed zone. The drift observation comes from
//     GetZoneSOAMNAME (the read half of the heal); an unreadable SOA is an
//     observation transport failure and behaves like the legacy
//     unconditional ensure (the executor's EnsureSOAMNAME is idempotent and
//     no-ops when the MNAME is already correct). Failures of the write are
//     best-effort (logged inside the reconciler, never raised). No
//     nameservers means nothing to correct — the observation is skipped and
//     the heal is a no-op, like the legacy EnsureSOAMNAME nil-swap.
//
// It reports whether the plan-driven heal HANDLED the zone (false = the
// operation was left untouched and the caller must run the legacy
// selfHealZone verbatim); handled=true means no legacy heal write may run
// afterwards (no double-write fallback).
func (s *DelegatedDomainService) healZonePlanDriven(ctx context.Context, provider DomainProvider, wd *pluginDb.WebsiteDomain, expectedDS string) (bool, string, error) {
	logger := s.Logger()
	if s.dnsSvc == nil || wd == nil || wd.ZoneID == 0 {
		return false, "", nil
	}

	// The plan is mapped from persisted facts only. It needs the owning
	// website's target (facts.Target feeds the DNSLink intent); load it from
	// the binding's owning website like the verification flow's callers do.
	// A missing/invalid website is an unrepresentable input: legacy heals.
	var website pluginDb.Website
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.Where("id = ?", wd.WebsiteID).First(&website).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	}); err != nil {
		logger.Warn("plan-driven zone heal unavailable: cannot load the owning website; deferring to the legacy self-heal",
			zap.String("domain", wd.Domain),
			zap.Uint("domain_id", wd.ID),
			zap.Error(err))
		return false, "", nil
	}
	plan, err := s.CurrentBindingPlan(wd, &website)
	if err != nil {
		logger.Warn("plan-driven zone heal unavailable: no current-behavior plan for binding; deferring to the legacy self-heal — report plan mapping gaps for follow-up",
			zap.String("domain", wd.Domain),
			zap.Uint("domain_id", wd.ID),
			zap.Error(err))
		return false, "", nil
	}

	// DNSSEC observation: the live DS read VerifyDomain already performed is
	// the signing-state evidence (the same gate the legacy self-heal used).
	// An empty DS means no active key (Disabled); a present DS means the key
	// exists (Enabled). Indeterminate states never reach this point for
	// DNSSEC-required namespaces (VerifyDomain aborted on the read error);
	// namespaces without a DNSSEC requirement carry no ensure-dnssec repair
	// in their plans, so passing no observation is safe.
	var dnssecObs *domainpolicy.ZoneDNSSECObservation
	if provider.RequiresDNSSEC() {
		if expectedDS == "" {
			dnssecObs = &domainpolicy.ZoneDNSSECObservation{State: domainpolicy.ZoneDNSSECStateDisabled}
		} else {
			dnssecObs = &domainpolicy.ZoneDNSSECObservation{State: domainpolicy.ZoneDNSSECStateEnabled}
		}
	}

	// SOA MNAME observation (read half of the heal). An unreadable SOA is an
	// observation transport failure and behaves like the legacy path wrote
	// unconditionally: report drift and let the idempotent ensure converge.
	nameservers := provider.Nameservers()
	var soaObs *domainpolicy.SOAMNAMEObservation
	if len(nameservers) > 0 && nameservers[0] != "" {
		current, err := s.dnsSvc.GetZoneSOAMNAME(ctx, wd.ZoneID)
		switch {
		case err != nil:
			logger.Debug("plan-driven zone heal: live SOA observation failed; deriving the ensure unconditionally (legacy parity)",
				zap.String("domain", wd.Domain), zap.Uint("zone_id", wd.ZoneID), zap.Error(err))
			soaObs = &domainpolicy.SOAMNAMEObservation{Found: true, MatchesPortalMNAME: false}
		default:
			soaObs = &domainpolicy.SOAMNAMEObservation{
				Found:              true,
				Current:            current,
				MatchesPortalMNAME: dnsname.Equal(current, dnsname.EnsureFQDN(nameservers[0])),
			}
		}
	}

	result, err := domainapp.ReconcileZoneHeal(ctx, domainapp.ZoneHealInput{
		Plan:        &plan,
		Domain:      wd.Domain,
		ZoneID:      wd.ZoneID,
		ZoneDNSSEC:  dnssecObs,
		SOAMNAME:    soaObs,
		Nameservers: nameservers,
	}, delegatedRepairEffectExecutor{svc: s.dnsSvc}, logger.Logger)
	if err != nil {
		if errors.Is(err, domainapp.ErrRepairNotReconciled) {
			logger.Warn("plan-driven zone heal could not represent this zone; deferring to the legacy self-heal — report this for follow-up",
				zap.String("domain", wd.Domain),
				zap.Uint("domain_id", wd.ID),
				zap.Uint("zone_id", wd.ZoneID),
				zap.String("profile", plan.ProfileID.String()),
				zap.Error(err))
			return false, "", nil
		}
		// Effect execution failure: the heal write itself could not be
		// applied. Same handling as the legacy self-heal (DNSSEC errors are
		// fatal; SOA errors were already demoted best-effort inside the
		// reconciler), and no legacy re-write afterwards (no double-write).
		return true, "", err
	}
	if len(result.Deferred) > 0 {
		logger.Debug("plan-driven zone heal deferred non-heal effects",
			zap.String("domain", wd.Domain),
			zap.Uint("zone_id", wd.ZoneID),
			zap.String("profile", plan.ProfileID.String()),
			zap.Strings("deferred", result.Deferred))
	}

	if result.DNSSECEnsured {
		// Fail-closed DS doctrine (legacy parity): after a successful ensure,
		// the live DS must actually exist before verification may proceed.
		healedDS, dsErr := s.dnsSvc.GetActiveDNSSECDS(ctx, wd.ZoneID)
		if dsErr != nil {
			return true, "", fmt.Errorf("resolve live DS for zone %d after enable: %w", wd.ZoneID, dsErr)
		}
		if healedDS == "" {
			return true, "", fmt.Errorf("dnssec self-heal failed for zone %d (domain %s): no active signing key after EnableDNSSEC; cannot verify delegation", wd.ZoneID, wd.Domain)
		}
		expectedDS = healedDS
	}

	return true, expectedDS, nil
}

// delegationNSCollector adapts the delegated-domain providers into the
// domainapp.DelegationNSCollector port: the live NS lookup the
// pending-delegation diagnostics used to do directly, plus the zone's live DS
// (the same GetActiveDNSSECDS read the delegation gate uses). It carries
// observation evidence only — no checks, no effects.
type delegationNSCollector struct {
	svc      DNSZoneService
	provider DomainProvider
	// zoneID is the binding's portal zone; zero (or a nil service) leaves the
	// DS leg unobserved.
	zoneID uint
}

func (c delegationNSCollector) CollectDelegation(ctx context.Context, domain string) (domainapp.DelegationObserved, error) {
	if c.provider == nil {
		return domainapp.DelegationObserved{}, fmt.Errorf("delegation observation for %s: no namespace provider wired", domain)
	}
	live, err := c.provider.LiveNameservers(ctx, domain)
	if err != nil {
		return domainapp.DelegationObserved{}, fmt.Errorf("live NS lookup for %s: %w", domain, err)
	}
	obs := domainapp.DelegationObserved{
		NS: domainpolicy.NSObservation{Found: len(live) > 0, Nameservers: live},
	}
	if c.svc != nil && c.zoneID != 0 {
		ds, dsErr := c.svc.GetActiveDNSSECDS(ctx, c.zoneID)
		if dsErr != nil {
			return domainapp.DelegationObserved{}, fmt.Errorf("live DS read for zone %d: %w", c.zoneID, dsErr)
		}
		obs.DS = &domainpolicy.DSObservation{Found: ds != "", Value: ds}
	}
	return obs, nil
}

// delegatedRepairEffectExecutor adapts the domain-side DNSZoneService into
// the domainapp RepairExecutor port for the plan-driven zone heal.
// Every method is the existing legacy write adapter, reproduced verbatim.
type delegatedRepairEffectExecutor struct {
	svc DNSZoneService
}

func (e delegatedRepairEffectExecutor) WriteDNSLinkRecord(ctx context.Context, zoneID uint, domain string, target string) error {
	if e.svc == nil {
		return fmt.Errorf("no DNS zone service wired for the DNSLink write on %s", domain)
	}
	return e.svc.CreateDNSLinkRecord(ctx, zoneID, domain, target)
}

func (e delegatedRepairEffectExecutor) WriteChallengeRecord(_ context.Context, zoneID uint, domain string, _ string) error {
	// The zone heal never rotates challenges (the repair is flow-scoped by
	// the observations); a rotate effect here is unreachable and fails closed.
	return fmt.Errorf("challenge write is not a zone-heal effect (zone %d, domain %s)", zoneID, domain)
}

func (e delegatedRepairEffectExecutor) EnableZoneDNSSEC(ctx context.Context, zoneID uint) error {
	if e.svc == nil {
		return fmt.Errorf("no DNS zone service wired for the DNSSEC ensure on zone %d", zoneID)
	}
	_, err := e.svc.EnableDNSSEC(ctx, zoneID)
	return err
}

func (e delegatedRepairEffectExecutor) EnsureZoneSOAMNAME(ctx context.Context, zoneID uint, domain string, nameservers []string) error {
	if e.svc == nil {
		return fmt.Errorf("no DNS zone service wired for the SOA MNAME heal on %s", domain)
	}
	return e.svc.EnsureSOAMNAME(ctx, zoneID, domain, nameservers)
}

// DeleteDomain deletes a WebsiteDomain row scoped by id, website_id, and user_id.
// Returns gorm.ErrRecordNotFound if no row was deleted.
func (s *DelegatedDomainService) DeleteDomain(ctx context.Context, domainID, websiteID, userID uint) error {
	// If the domain being deleted is the website's primary, repoint
	// Website.PrimaryDomainID to the next remaining active binding (or clear it)
	// so the FK never dangles. Do this before the delete so we can read the
	// remaining bindings accurately.
	var wd pluginDb.WebsiteDomain
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.
			Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
			First(&wd).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return gorm.ErrRecordNotFound
		}
		return err
	}

	var website pluginDb.Website
	if werr := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.Where("id = ?", websiteID).First(&website).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	}); werr == nil &&
		website.PrimaryDomainID != nil && *website.PrimaryDomainID == wd.ID {

		// Pick the next active (non-deleted) binding on this website.
		var next pluginDb.WebsiteDomain
		nextErr := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			if err := tx.
				Where("website_id = ? AND id != ? AND deleted_at IS NULL", websiteID, wd.ID).
				Order("id ASC").
				First(&next).Error; err != nil {
				_ = tx.AddError(err)
			}
			return tx
		})
		if errors.Is(nextErr, gorm.ErrRecordNotFound) {
			// No other binding remains: clear the primary FK.
			if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				if err := tx.Model(&website).Update("primary_domain_id", nil).Error; err != nil {
					_ = tx.AddError(err)
				}
				return tx
			}); err != nil {
				return fmt.Errorf("failed to clear primary domain: %w", err)
			}
		} else if nextErr != nil {
			return nextErr
		} else {
			if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
				if err := tx.Model(&website).Update("primary_domain_id", next.ID).Error; err != nil {
					_ = tx.AddError(err)
				}
				return tx
			}); err != nil {
				return fmt.Errorf("failed to repoint primary domain: %w", err)
			}
		}
	}

	var rowsAffected int64
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		res := tx.
			Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
			Delete(&pluginDb.WebsiteDomain{})
		if res.Error != nil {
			_ = tx.AddError(res.Error)
			return tx
		}
		rowsAffected = res.RowsAffected
		return tx
	}); err != nil {
		return err
	}
	if rowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func canonicalZoneName(domain string) string {
	name := dnsname.Normalize(domain)
	if name == "" {
		return "."
	}
	return dnsname.EnsureFQDN(name)
}

// delegationRecord is used for typed access to delegation records.
type delegationRecord struct {
	Type    string `json:"type"`
	Value   string `json:"value"`
	NS      string `json:"ns,omitempty"`
	Address string `json:"address,omitempty"`
}

func jsonToMap(raw json.RawMessage) datatypes.JSONMap {
	var m datatypes.JSONMap
	_ = json.Unmarshal(raw, &m)
	return m
}

// UpdateTLSAFromCert computes TLSA from a pushed cert and stores it. When
// privateKeyPEM is non-empty and the domain has no persisted key yet, the
// private key is stored so Caddy can later fetch the same key (stable SPKI)
// and re-issue certs around it without touching DNS. The key is only ever
// persisted when absent — it is never overwritten by a later push.
func (s *DelegatedDomainService) UpdateTLSAFromCert(ctx context.Context, namespace, domain, certPEM, privateKeyPEM string) (tlsa, ownerName string, err error) {
	provider := s.registry.Get(namespace)
	if provider == nil {
		return "", "", fmt.Errorf("unsupported namespace: %s", namespace)
	}

	hash, err := dane.ComputeTLSAFromCert(certPEM)
	if err != nil {
		return "", "", fmt.Errorf("compute tlsa: %w", err)
	}
	tlsa = TLSAHashPrefix() + hash
	ownerName = dane.TLSAOwnerName(domain, DaneTLSAPort, DaneTLSATransport)
	// SPKI-drift is policed inside the row-locked transaction below (an
	// out-of-transaction preflight cannot win the race between concurrent
	// pushes); see the stored-key classification there.

	if s.DB() == nil {
		// test context (no persistence): compute the response and notify the
		// provider; there is no identity to guard.
		if certProvider, ok := provider.(CertificateProvider); ok {
			if err := certProvider.OnCertAvailable(ctx, domain, certPEM); err != nil {
				return "", "", fmt.Errorf("provider OnCertAvailable: %w", err)
			}
		}
		return tlsa, ownerName, nil
	}

	ns := pluginDb.DomainNamespace(namespace)

	// Persist the per-domain DANE state under a row lock so concurrent cert
	// pushes serialize. The private key is the source of truth for the SPKI that
	// DANE TLSA (selector 1) pins — it is written at most once and never
	// overwritten, since a later push with a different key would rotate the
	// published pin and break every DANE-validating client. The cert, TLSA, and
	// owner name, by contrast, are refreshed on every push (a cert may be freely
	// re-issued from the same key with an identical SPKI).
	var zoneID uint
	portalManaged := false
	txErr := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		// Lock the target row so concurrent cert pushes serialize per-domain.
		locked := tx.Clauses(clause.Locking{Strength: "UPDATE"})
		var wd pluginDb.WebsiteDomain
		if err := locked.Where("domain = ? AND namespace = ?", domain, ns).First(&wd).Error; err != nil {
			_ = tx.AddError(err) // includes gorm.ErrRecordNotFound
			return tx
		}
		zoneID = wd.ZoneID
		portalManaged = wd.Class() == pluginDb.ClassPortalManaged

		// Identity classification (first-writer-wins, atomic under the row
		// lock): the first usable persisted key becomes the immutable SPKI
		// identity; pushes that disagree with it are rejected. A key push
		// whose private key is genuinely garbage is rejected too — PKCS#8
		// parse + SPKI derivation, not a shape guess, decides usability.
		// DANE-identity classification applies only to DANE-capable providers
		// (e.g. HNS). Non-DANE namespaces (ICANN) receive cert pushes for
		// ordinary HTTPS with no stable-key concept, so they skip it entirely.
		if provider.UsesManagedZoneTLSA() {
			stored := wd.GetDANEPrivKeyPEM()
			switch {
			case stored == "":
				if privateKeyPEM == "" {
					// A bound domain with no stable key cannot adopt a
					// certificate-only identity: the cert-derived TLSA would be
					// overwritten (rotated) the next time a stable key is
					// bootstrapped. Reject until the identity exists. Callers
					// that only need a best-effort TLSA (cert webhook) match on
					// ErrDANENotBootstrapped.
					_ = tx.AddError(fmt.Errorf("%s: %w",
						fmt.Sprintf("DANE key for %s is not bootstrapped and the cert push carried no private key; bootstrap a stable key (run dane republish) before cert issuance", domain),
						ErrDANENotBootstrapped))
					return tx
				}
				keyHash, keyErr := dane.ComputeTLSAFromPrivateKey(privateKeyPEM)
				if keyErr != nil {
					_ = tx.AddError(fmt.Errorf("pushed private key for %s is not a parseable DANE key: %w", domain, keyErr))
					return tx
				}
				// First bootstrap: the pushed cert's SPKI must belong to the
				// key being persisted, or the identity is incoherent from day
				// one.
				if TLSAHashPrefix()+keyHash != tlsa {
					_ = tx.AddError(fmt.Errorf(
						"DANE key mismatch for %s: the pushed certificate's SPKI does not match the pushed private key",
						domain))
					return tx
				}
				// Stored as plaintext by design: the DANE key's value is SPKI
				// stability, not secrecy — database access already implies
				// control of portal-managed zones, and chain-managed name
				// routing lives in the owner's HNS key, not this DB (audit
				// finding 3, accepted threat model).
				wd.SetDANEPrivKeyPEM(privateKeyPEM)
			case !daneKeyUsable(stored):
				// Legacy data integrity: pre-plaintext rows may carry AES-GCM
				// ciphertext that no longer decrypts (the at-rest encryption and
				// its config were removed). With an installed identity the stored
				// (dead) key is authoritative — a fresh bootstrap would rotate the
				// live pin; without an identity a usable pushed key replaces it.
				if wd.GetDANETLSA() != "" {
					_ = tx.AddError(fmt.Errorf(
						"DANE key for %s is unreadable while a TLSA identity is installed (%s); this push would rotate the live SPKI pin — resolve manually",
						domain, wd.GetDANETLSA()))
					return tx
				}
				if privateKeyPEM == "" {
					// No identity to preserve (no installed TLSA) and no
					// replacement key: behave like a keyless row so cert-only
					// renewals resolve best-effort instead of failing forever.
					_ = tx.AddError(fmt.Errorf("stored DANE key for %s is unusable and this push carries no replacement private key: %w", domain, ErrDANENotBootstrapped))
					return tx
				}
				replHash, keyErr := dane.ComputeTLSAFromPrivateKey(privateKeyPEM)
				if keyErr != nil {
					_ = tx.AddError(fmt.Errorf("stored DANE key for %s is unusable and this push supplies no usable replacement key: %w", domain, keyErr))
					return tx
				}
				if TLSAHashPrefix()+replHash != tlsa {
					_ = tx.AddError(fmt.Errorf(
						"DANE key mismatch for %s: the pushed certificate's SPKI does not match the pushed private key",
						domain))
					return tx
				}
				wd.SetDANEPrivKeyPEM(privateKeyPEM)
			default:
				// A usable stable key exists: the identity is the stored key's
				// SPKI. The persisted key is never overwritten; every push must
				// match that SPKI — compared by imprint, never by raw PEM bytes
				// (a re-serialized / line-normalized encoding of the same key
				// is semantically identical and must not be rejected).
				storedHash, hErr := dane.ComputeTLSAFromPrivateKey(stored)
				if hErr != nil {
					_ = tx.AddError(fmt.Errorf("derive SPKI from persisted DANE key for %s: %w", domain, hErr))
					return tx
				}
				if privateKeyPEM != "" {
					pushedHash, pErr := dane.ComputeTLSAFromPrivateKey(privateKeyPEM)
					if pErr != nil {
						_ = tx.AddError(fmt.Errorf("pushed private key for %s is not a parseable DANE key: %w", domain, pErr))
						return tx
					}
					if pushedHash != storedHash {
						_ = tx.AddError(fmt.Errorf(
							"DANE key mismatch for %s: the pushed private key's SPKI does not match the persisted stable key",
							domain))
						return tx
					}
				}
				if TLSAHashPrefix()+storedHash != tlsa {
					_ = tx.AddError(fmt.Errorf(
						"DANE key mismatch for %s: the pushed certificate's SPKI does not match the persisted stable key",
						domain))
					return tx
				}
			}
		}
		// Always refresh the cert + TLSA + owner name on every push — only
		// reached when the identity is consistent with the pushed key.
		wd.SetDANECertPEM(certPEM)
		wd.SetDANETLSA(tlsa)
		wd.SetDANETLSAOwner(ownerName)

		// sync TLSA for HNS
		if namespace == string(pluginDb.DomainNamespaceHNS) && wd.DelegationData != nil {
			if rawAuth, ok := wd.DelegationData["authoritative_records"]; ok {
				data, _ := json.Marshal(rawAuth)
				var auth []delegationRecord
				if json.Unmarshal(data, &auth) == nil {
					for i := range auth {
						if auth[i].Type == "TLSA" {
							zone := wd.ZoneName
							if zone == "" {
								zone = canonicalZoneName(domain)
							}
							auth[i].Value = formatFullTLSARecord(tlsa, zone)
							// Store the updated records as a real JSON structure, not the raw
							// marshaled []byte. JSONMap encodes []byte as base64 on save, which
							// silently breaks DTO projection (json.Unmarshal into []delegationRecord).
							raw, _ := json.Marshal(auth)
							var out any
							if json.Unmarshal(raw, &out) == nil {
								wd.DelegationData["authoritative_records"] = out
							}
							break
						}
					}
				}
			}
		}

		// Persist the updated JSON maps scoped by primary key (avoids the locked
		// read's WHERE clause making the UPDATE column ambiguous). GORM's struct
		// auto-update of UpdatedAt is bypassed by map updates, so set it explicitly.
		if err := tx.Model(&pluginDb.WebsiteDomain{}).
			Where("id = ?", wd.ID).
			Updates(map[string]any{
				"protocol_data":   wd.ProtocolData,
				"delegation_data": wd.DelegationData,
				"updated_at":      time.Now(),
			}).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if txErr != nil {
		return "", "", fmt.Errorf("save domain tlsa: %w", txErr)
	}

	// Only after the identity was validated and persisted under the row lock
	// do we notify the provider: a rejected push must not mutate the provider's
	// in-memory cert cache, which BuildDelegation reads to derive TLSA.
	if certProvider, ok := provider.(CertificateProvider); ok {
		if err := certProvider.OnCertAvailable(ctx, domain, certPEM); err != nil {
			return "", "", fmt.Errorf("provider OnCertAvailable: %w", err)
		}
	}

	// Publish the TLSA to the portal-managed authoritative zone in PowerDNS.
	// Without this, the TLSA was only stored in the DB (DelegationData) and
	// never served, so DANE validators get NXDOMAIN for `_443._tcp.<domain>`.
	// Only providers whose namespace uses DANE and whose zone the portal
	// manages (e.g. HNS) do this; the decision is a provider capability, not
	// a namespace string comparison, so any future DANE-capable namespace
	// opts in here rather than via a hardcoded "hns" check. The TLSA owner is
	// named after the binding's domain (not the zone apex) so a subdomain
	// reusing a parent zone publishes _443._tcp.<subdomain> rather than
	// overwriting the parent's TLSA.
	// Only portal-managed bindings authorize a managed-zone TLSA write. An
	// on-chain (HIP-5) or self-hosted/unresolved binding carrying a stray
	// zone reference must never publish TLSA into it — the zone may be shared
	// and is not this binding's portal authority.
	if s.dnsSvc != nil && portalManaged && zoneID != 0 && provider.UsesManagedZoneTLSA() {
		if err := s.dnsSvc.SetTLSARecord(ctx, zoneID, domain, tlsa); err != nil {
			// DNS publish failure is surfaced but the persisted cert/TLSA state
			// is retained so a later cert push retries the publish.
			return tlsa, ownerName, fmt.Errorf("publish tlsa to zone: %w", err)
		}
	}

	return tlsa, ownerName, nil
}

// UsesDelegationForOwnership returns true for namespaces that use delegation (e.g. HNS).
func (s *DelegatedDomainService) UsesDelegationForOwnership(domain string) bool {
	ns, ok := s.getNamespaceForDomain(domain)
	return ok && ns != string(pluginDb.DomainNamespaceICANN)
}

// CurrentBindingPlan returns the current-behavior pure domainpolicy.Plan for
// the given binding, built exclusively from facts already persisted on the
// loaded rows: the namespace, the status-derived hosting class (db.Class),
// the zone reference (plus its shared/dedicated topology), the platform
// trust relation, and the owning website's content target. It performs no
// network probes — in particular it never calls Inspect/InspectRoute — so
// website validation may consult it to select its gates without adding DNS
// traffic to the ValidateDNS hot path.
// Route inspection stays where verification already performs it (VerifyDomain).
//
// The mapping is the legacy compatibility mapper (legacyFacts +
// legacyProfileFor), promoted to this single runtime entry point: profile
// selection assumes the route derived from persisted state (a zero route
// observation means "not probed"). Incoherent persisted state is rejected
// with a typed CompatError rather than guessed. Callers must treat any error
// as "no plan available" and fall back to the legacy predicates — which
// remain authoritative at runtime for current fixtures — while reporting the
// unavailability/d divergence loudly (the website service does both).
func (s *DelegatedDomainService) CurrentBindingPlan(wd *pluginDb.WebsiteDomain, website *pluginDb.Website) (domainpolicy.Plan, error) {
	facts, err := s.legacyFacts(wd, website)
	if err != nil {
		return domainpolicy.Plan{}, err
	}
	profileID, err := s.legacyProfileFor(wd, domainpolicy.RouteObservation{})
	if err != nil {
		return domainpolicy.Plan{}, err
	}
	profile, ok := domainpolicy.DefaultRegistry().Lookup(profileID)
	if !ok {
		return domainpolicy.Plan{}, newCompatError(CompatErrorProfileUnregistered, wd,
			"current-behavior profile %q is not registered", profileID.String())
	}
	return domainpolicy.PlanBinding(profile, facts)
}

// NamespaceUsesManagedZoneTLSA reports whether the given namespace's provider
// translates certs into a DANE TLSA. Only such namespaces (e.g. HNS) carry a
// DANE publication duty anywhere — into a portal-managed zone for native
// bindings or into the on-chain zone data for chain-managed bindings. Non-DANE
// namespaces (e.g. ICANN) have no TLSA anywhere.
func (s *DelegatedDomainService) NamespaceUsesManagedZoneTLSA(namespace string) bool {
	if s.registry == nil {
		return false
	}
	prov := s.registry.Get(namespace)
	return prov != nil && prov.UsesManagedZoneTLSA()
}

// DANEPublicationTarget describes where a DANE-capable binding's TLSA is served.
type DANEPublicationTarget string

const (
	// DANEPublishManagedZone marks a portal-managed binding whose TLSA is
	// published into the portal's authoritative PowerDNS zone.
	DANEPublishManagedZone DANEPublicationTarget = "managed_zone"
	// DANEPublishChain marks a chain-managed (HIP-5) binding whose TLSA is
	// served from the name's on-chain zone data: the portal only computes and
	// stores the record, and the owner installs it in the chain zone.
	DANEPublishChain DANEPublicationTarget = "chain"
)

// ErrDANENotBootstrapped reports that a DANE-capable bound domain has no
// stable key yet and the operation did not (and will not) establish one —
// e.g. a certificate-only push from the cert webhook. Such callers that only
// need a best-effort TLSA value may match on this sentinel and fall back.
var ErrDANENotBootstrapped = errors.New("dane identity not bootstrapped")

// DANEPublicationTargetFor resolves where a bound domain's DANE TLSA is served,
// and whether the DANE republish flow applies to it at all. It is the single
// source of truth for DANE publication eligibility: a DANE-capable namespace
// republishes either into the portal-managed zone (portal-managed bindings) or
// into the on-chain name data (chain-managed bindings — DANE still applies
// on-chain). Self-hosted and unresolved bindings carry no portal DANE
// publication duty and are rejected.
func (s *DelegatedDomainService) DANEPublicationTargetFor(wd *pluginDb.WebsiteDomain) (DANEPublicationTarget, bool) {
	if !s.NamespaceUsesManagedZoneTLSA(string(wd.Namespace)) {
		return "", false
	}
	switch wd.Class() {
	case pluginDb.ClassPortalManaged:
		return DANEPublishManagedZone, true
	case pluginDb.ClassOnChainManaged:
		return DANEPublishChain, true
	default:
		return "", false
	}
}

// ValidateOnChainTLSA compares the TLSA a chain-managed (HIP-5) binding's
// on-chain zone data actually serves against the portal-stored DANE record.
// The gate applies ONLY to chain-managed bindings: they are the locus with
// owner-side DANE publication duty. Portal-managed bindings publish TLSA into
// their own PowerDNS zone (UpdateTLSAFromCert), and self-hosted/ICANN bindings
// carry no portal DANE obligation — for both, the gate is not applicable and
// reports OK. Missing/mismatched records and un-bootstrapped identities fail
// the check (ok=false + expected/found); query transport failures return an
// error so a broken resolver never reads as compliance.
func (s *DelegatedDomainService) ValidateOnChainTLSA(ctx context.Context, wd *pluginDb.WebsiteDomain) (ok bool, detail, expected, found string, err error) {
	locus, eligible := s.DANEPublicationTargetFor(wd)
	if !eligible || locus != DANEPublishChain {
		return true, "", "", "", nil
	}

	provider := s.registry.Get(string(wd.Namespace))
	if provider == nil {
		return false, "", "", "", fmt.Errorf("unsupported namespace: %s", wd.Namespace)
	}
	verifier, hasLiveQuery := provider.(DANEVerifier)
	if !hasLiveQuery {
		// No live surface to check for this namespace: not applicable.
		return true, "", "", "", nil
	}

	stored, _, err := s.GetDANERecord(ctx, string(wd.Namespace), wd.Domain)
	if err != nil {
		return false, "", "", "", fmt.Errorf("load stored DANE record for %s: %w", wd.Domain, err)
	}
	if stored == "" {
		return false,
			fmt.Sprintf("no DANE identity stored for %s — bootstrap it with `dane republish`, then publish the returned record in the name's on-chain zone data", wd.Domain),
			"", "", nil
	}
	expected = normalizeTLSARdata(stored)

	live, err := verifier.QueryTLSARdata(ctx, wd.Domain)
	if err != nil {
		return false, "", "", "", err
	}
	if live == "" {
		return false,
			fmt.Sprintf("TLSA record _%d._%s.%s is not published in the name's on-chain zone data", DaneTLSAPort, DaneTLSATransport, wd.Domain),
			expected, "", nil
	}
	found = live
	if live != expected {
		return false,
			fmt.Sprintf("TLSA record served by %s does not match the portal-stored DANE identity", wd.Domain),
			expected, live, nil
	}
	return true, fmt.Sprintf("TLSA served from %s's on-chain zone data matches the stored DANE identity", wd.Domain), expected, live, nil
}

// normalizeTLSARdata canonicalizes a stored TLSA rdata ("<usage> <selector>
// <matching> <hash>") for comparison with a live query result: lowercased
// hash, whitespace-collapsed.
func normalizeTLSARdata(tlsa string) string {
	fields := strings.Fields(tlsa)
	if len(fields) != 4 {
		return strings.ToLower(strings.Join(fields, " "))
	}
	return fmt.Sprintf("%s %s %s %s", fields[0], fields[1], fields[2], strings.ToLower(fields[3]))
}

// ensureDANEIdentity bootstraps the stable DANE key/identity for namespaces
// whose provider translates certs into DANE TLSA. Binding paths (self-hosted
// and on-chain managed) call it so the TLSA the owner must publish exists at
// bind time, before any certificate push; the key is stable, so the published
// SPKI pin never rotates across cert re-issuance. It is a no-op for providers
// without a DANE concept.
func (s *DelegatedDomainService) ensureDANEIdentity(ctx context.Context, provider DomainProvider, namespace, domain string) error {
	if !provider.UsesManagedZoneTLSA() {
		return nil
	}
	if _, err := s.EnsureCertificateKey(ctx, namespace, domain); err != nil {
		return fmt.Errorf("failed to bootstrap DANE identity: %w", err)
	}
	return nil
}

// NamespaceRequiresDNSSEC reports whether the given namespace's provider
// confirms delegation against a live DS served by the parent zone
// (managed-DNSSEC policy). DNS-requirements exposes live DS state based on
// this: a provider that requires DNSSEC but does not publish DANE still needs
// the live DS in the response, so the DNSSEC policy — never the TLSA policy —
// gates DS exposure.
func (s *DelegatedDomainService) NamespaceRequiresDNSSEC(namespace string) bool {
	if s.registry == nil {
		return false
	}
	prov := s.registry.Get(namespace)
	return prov != nil && prov.Policy().DNSSEC == pluginCore.DNSSECRequired
}

// GetNamespaceForDomain returns the namespace for the given domain if it
// matches a registered provider. This is used to select the correct DNS
// resolver for alt-root domains (different roots require different resolvers).
func (s *DelegatedDomainService) GetNamespaceForDomain(domain string) (string, bool) {
	return s.getNamespaceForDomain(domain)
}

func (s *DelegatedDomainService) getNamespaceForDomain(domain string) (string, bool) {
	if s.registry == nil {
		return "", false
	}
	// Route through providerForDomain so namespace classification (and its
	// unloaded-list semantics) has a single implementation. This call is a
	// best-effort hint (resolver selection, ownership verification mode), so
	// an unloaded IANA list degrades to "no special namespace" instead of
	// failing: downstream resolution/verification fails loudly on its own
	// rather than silently publishing misrouted DNS (providerForDomain's
	// ErrTLDListUnavailable path is for the NS publication surfaces).
	prov, err := s.registry.providerForDomain(domain)
	if err != nil || prov == nil {
		return "", false
	}
	return prov.Protocol(), true
}

// GetWebsiteDomainByName looks up a domain across all namespaces.
func (s *DelegatedDomainService) GetWebsiteDomainByName(ctx context.Context, domain string) (*pluginDb.WebsiteDomain, error) {
	var wd pluginDb.WebsiteDomain
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.Where("domain = ?", domain).First(&wd).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		return nil, err
	}
	return &wd, nil
}

// GetWebsiteDomainByDomainAndNamespace looks up a domain by namespace.
// GetWebsiteDomainByDomainAndNamespace returns the binding for a domain in a
// namespace, or gorm.ErrRecordNotFound when absent (a normal business outcome
// several API and DANE callers match on with errors.Is). The retry wrapper
// preserves the sentinel: gorm's First() sets tx.Error to the sentinel itself
// and AddError re-wraps with %w, so errors.Is keeps matching; the retry loop
// only replays lock-class errors (deadlock, lock wait timeout, db locked),
// never not-found.
func (s *DelegatedDomainService) GetWebsiteDomainByDomainAndNamespace(ctx context.Context, domain string, ns pluginDb.DomainNamespace) (*pluginDb.WebsiteDomain, error) {
	if s.DB() == nil {
		return nil, gorm.ErrRecordNotFound
	}
	var wd pluginDb.WebsiteDomain
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.Where("domain = ? AND namespace = ?", domain, ns).First(&wd).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		return nil, err
	}
	return &wd, nil
}

// StoredCert holds the DANE key material returned to a Caddy cert
// getter so it can re-issue a certificate around the persisted key (stable SPKI).
type StoredCert struct {
	PrivateKeyPEM string
	CertPEM       string
	TLSA          string
	OwnerName     string
}

// daneKeyUsable decides whether a persisted DANE private-key value is a real,
// parseable key whose SPKI can be derived — PKCS#8 parse + SPKI computation,
// never a shape guess. Legacy rows holding pre-plaintext AES ciphertext fail
// this check.
func daneKeyUsable(keyPEM string) bool {
	if keyPEM == "" {
		return false
	}
	hash, err := dane.ComputeTLSAFromPrivateKey(keyPEM)
	return err == nil && hash != ""
}

// EnsureCertificateKey creates the stable DANE key for a domain if it does not
// already exist, then computes and stores TLSA from that key's SPKI. The row
// lock prevents concurrent bootstraps from publishing TLSA for different keys.
func (s *DelegatedDomainService) EnsureCertificateKey(ctx context.Context, namespace, domain string) (*StoredCert, error) {
	if s.DB() == nil {
		return nil, fmt.Errorf("database not available")
	}

	ns := pluginDb.DomainNamespace(namespace)
	var keyPEM string
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		var wd pluginDb.WebsiteDomain
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("domain = ? AND namespace = ?", domain, ns).First(&wd).Error; err != nil {
			_ = tx.AddError(err)
			return tx
		}
		key := wd.GetDANEPrivKeyPEM()
		if key != "" {
			// Data-integrity guard: pre-plaintext rows may carry AES-GCM
			// ciphertext that no longer decrypts (the at-rest encryption and
			// its config were removed). Usability is decided by real parse +
			// SPKI derivation, not shape sniffing.
			if !daneKeyUsable(key) {
				if wd.GetDANETLSA() != "" {
					_ = tx.AddError(fmt.Errorf(
						"DANE key for %s is unreadable while a TLSA identity is installed (%s); bootstrapping a fresh key here would rotate the live SPKI pin — resolve manually",
						domain, wd.GetDANETLSA()))
					return tx
				}
				// Unusable key with no installed identity: discard it and
				// bootstrap a fresh one below.
				wd.DeleteDANEPrivKey()
			} else {
				keyPEM = key
				return tx
			}
		}

		generated, err := dane.GenerateKey()
		if err != nil {
			_ = tx.AddError(fmt.Errorf("generate DANE key: %w", err))
			return tx
		}
		wd.SetDANEPrivKeyPEM(generated)
		if err := tx.Model(&pluginDb.WebsiteDomain{}).Where("id = ?", wd.ID).
			Updates(map[string]any{"protocol_data": wd.ProtocolData, "updated_at": time.Now()}).Error; err != nil {
			_ = tx.AddError(err)
			return tx
		}
		keyPEM = generated
		return tx
	}); err != nil {
		return nil, err
	}

	// TLSA 3 1 1 pins the key's SPKI, not a certificate fingerprint. Caddy
	// issues and pushes the certificate later; bootstrap must not fabricate or
	// persist one here.
	hash, err := dane.ComputeTLSAFromPrivateKey(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("compute TLSA from DANE key: %w", err)
	}
	tlsa := TLSAHashPrefix() + hash
	ownerName := dane.TLSAOwnerName(domain, DaneTLSAPort, DaneTLSATransport)
	if err := s.persistTLSAKeyMetadata(ctx, namespace, domain, tlsa, ownerName); err != nil {
		return nil, err
	}
	return &StoredCert{PrivateKeyPEM: keyPEM, TLSA: tlsa, OwnerName: ownerName}, nil
}

func (s *DelegatedDomainService) persistTLSAKeyMetadata(ctx context.Context, namespace, domain, tlsa, ownerName string) error {
	ns := pluginDb.DomainNamespace(namespace)
	return db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		var wd pluginDb.WebsiteDomain
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("domain = ? AND namespace = ?", domain, ns).First(&wd).Error; err != nil {
			_ = tx.AddError(err)
			return tx
		}
		wd.SetDANETLSA(tlsa)
		wd.SetDANETLSAOwner(ownerName)
		if err := tx.Model(&pluginDb.WebsiteDomain{}).Where("id = ?", wd.ID).
			Updates(map[string]any{
				"protocol_data": wd.ProtocolData,
				"updated_at":    time.Now(),
			}).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
}

// GetCertificateKey returns the stored DANE key material for a domain.
// Returns gorm.ErrRecordNotFound when the domain has no persisted key yet
// (i.e. first bootstrap) or when the stored value is not a usable key.
func (s *DelegatedDomainService) GetCertificateKey(ctx context.Context, namespace, domain string) (*StoredCert, error) {
	ns := pluginDb.DomainNamespace(namespace)
	wd, err := s.GetWebsiteDomainByDomainAndNamespace(ctx, domain, ns)
	if err != nil {
		return nil, err // includes gorm.ErrRecordNotFound
	}
	keyPEM := wd.GetDANEPrivKeyPEM()
	// Same usability bar as EnsureCertificateKey: legacy ciphertext rows
	// report NotFound rather than handing out garbage PEM.
	if keyPEM == "" || !daneKeyUsable(keyPEM) {
		return nil, gorm.ErrRecordNotFound
	}
	return &StoredCert{
		PrivateKeyPEM: keyPEM,
		CertPEM:       wd.GetDANECertPEM(),
		TLSA:          wd.GetDANETLSA(),
		OwnerName:     wd.GetDANETLSAOwner(),
	}, nil
}

// GetDANERecord returns the stored DANE TLSA rdata and owner name for a domain
// without returning the private key — the lightweight read surface for
// consumers that only need the record to publish into the name's zone (e.g.
// dns-requirements). Returns gorm.ErrRecordNotFound when the binding does not
// exist and empty strings when no DANE identity has been computed yet.
func (s *DelegatedDomainService) GetDANERecord(ctx context.Context, namespace, domain string) (tlsa, ownerName string, err error) {
	wd, err := s.GetWebsiteDomainByDomainAndNamespace(ctx, domain, pluginDb.DomainNamespace(namespace))
	if err != nil {
		return "", "", err
	}
	return wd.GetDANETLSA(), wd.GetDANETLSAOwner(), nil
}

// RepublishChainDANERecord returns the DANE TLSA the owner must install in a
// chain-managed (HIP-5) binding's on-chain zone data. Republish must preserve
// an already-installed identity — deriving from the key would rotate the SPKI
// pin and invalidate the live on-chain record — so an existing stored TLSA is
// returned unchanged. Only a binding with no on-chain identity yet is
// bootstrapped from the stable DANE key (the source of truth for TLSA 3 1 1,
// never a certificate), and no PowerDNS zone write occurs. A bootstrap
// failure always returns error — it can no longer be silently skipped.
func (s *DelegatedDomainService) RepublishChainDANERecord(ctx context.Context, namespace, domain string) (tlsa, ownerName string, err error) {
	// Preserve the already-installed on-chain identity instead of rotating the
	// SPKI pin: a stored TLSA is authoritative for what is live on-chain. The
	// owner name is deterministic from the domain, so if the stored record has
	// no owner_name (missing/corrupt metadata) recompute it rather than return a
	// bare TLSA the client cannot install.
	if tlsa, ownerName, err := s.GetDANERecord(ctx, namespace, domain); err != nil {
		return "", "", err
	} else if tlsa != "" {
		if ownerName == "" {
			ownerName = dane.TLSAOwnerName(domain, DaneTLSAPort, DaneTLSATransport)
		}
		return tlsa, ownerName, nil
	}

	sc, err := s.EnsureCertificateKey(ctx, namespace, domain)
	if err != nil {
		return "", "", fmt.Errorf("refresh on-chain DANE identity: %w", err)
	}
	return sc.TLSA, sc.OwnerName, nil
}

// GetActiveWebsiteDomainByDomain finds an active domain across all namespaces.
func (s *DelegatedDomainService) GetActiveWebsiteDomainByDomain(ctx context.Context, domain string) (*pluginDb.WebsiteDomain, error) {
	var wd pluginDb.WebsiteDomain
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		if err := tx.Where("domain = ? AND status = ?", domain, pluginDb.DomainStatusActive).First(&wd).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	})
	if err != nil {
		return nil, err
	}
	return &wd, nil
}

// GetPendingWebsiteDomainsPaginated returns a batch of domains in a given status,
// using keyset pagination (id > lastID) to avoid offset drift when rows are
// modified between pages.
func (s *DelegatedDomainService) GetPendingWebsiteDomainsPaginated(ctx context.Context, status pluginDb.DomainStatus, limit, lastID int) ([]pluginDb.WebsiteDomain, error) {
	var wds []pluginDb.WebsiteDomain
	if err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		q := tx.Where("status = ?", status)
		if lastID > 0 {
			q = q.Where("id > ?", lastID)
		}
		if err := q.Order("id ASC").Limit(limit).Find(&wds).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	}); err != nil {
		return nil, err
	}
	return wds, nil
}

// NewDelegatedDomainServiceFactory is the standard service factory for registration.
func NewDelegatedDomainServiceFactory() (core.Service, []core.ContextBuilderOption, error) {
	svc := &DelegatedDomainService{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			reg := NewRegistry()

			dnsCfg := core.GetServiceConfig[*pluginConfig.DnsConfig](ctx, pluginCore.DNS_SERVICE)
			var nsList []string
			var hnsNSList []string
			hnsResolver := ""
			if dnsCfg != nil {
				nsList = dnsCfg.Nameservers
				hnsNSList = dnsCfg.HNSNameservers
				// Fall back to the ICANN list if no HNS-specific nameservers
				// are configured, so existing deployments keep working.
				if len(hnsNSList) == 0 {
					hnsNSList = nsList
				}
				hnsResolver = dnsCfg.HNSResolver
			}
			// Prefetch the IANA root zone list so provider Validate calls
			// hit the in-memory snapshot instead of racing a cold network
			// fetch on the bind path.
			go warmTLDList()
			reg.Register(NewICANNProvider(nsList))
			hnsProv := NewHNSProvider(hnsResolver, hnsNSList, TLSASource{})
			dns := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
			if dns != nil {
				hnsProv.SetDNSService(dns)
				// Give the DNS service the per-namespace nameserver resolver
				// (this registry) so it provisions/validates HNS zones via the
				// HNS provider (nameservers + HNS resolver) rather than
				// hardcoding ICANN nameservers and the system resolver.
				if setter, ok := dns.(interface {
					SetNameserverResolver(pluginCore.NameserverResolver)
				}); ok {
					setter.SetNameserverResolver(reg)
				}
			}
			reg.Register(hnsProv)

			svc.registry = reg

			svc.dnsSvc = dns

			// Resolve the website service once for cross-service calls (e.g.
			// activating a site after a platform subdomain claim). All service
			// instances are registered before startup funcs run, so this is
			// always present.
			svc.websiteSvc = core.GetServiceOptional[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			return nil
		}),
	)

	return svc, opts, nil
}

func (s *DelegatedDomainService) ID() string {
	return pluginCore.DELEGATED_DOMAIN_SERVICE
}

func (s *DelegatedDomainService) GetConfig() (any, error) {
	return &pluginConfig.DelegatedDomainConfig{}, nil
}
