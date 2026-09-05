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
	"go.lumeweb.com/portal/core"
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
		if err := s.DB().WithContext(ctx).First(&pd, *platformRootID).Error; err != nil {
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
	if err := s.DB().WithContext(ctx).
		Where("user_id = ? AND id = ?", userID, websiteID).
		First(&website).Error; err != nil {
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
	if err := s.DB().WithContext(ctx).
		Where("domain = ? AND namespace = ? AND deleted_at IS NOT NULL", domain, namespace).
		Unscoped().Delete(&pluginDb.WebsiteDomain{}).Error; err != nil {
		return nil, fmt.Errorf("failed to purge stale domain binding: %w", err)
	}

	if err := s.DB().WithContext(ctx).Create(wd).Error; err != nil {
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
		if err := s.DB().WithContext(ctx).Model(wd).Updates(map[string]any{
			"status":              pluginDb.DomainStatusOnchainManaged,
			"dns_hosting_enabled": false,
		}).Error; err != nil {
			// The row was already inserted with dns_hosting_enabled from the
			// request (true by default). A half-finalized onchain binding is the
			// worst failure shape: it would look like an "enable-orphan" to
			// SetDomainDNSEnabled and a retry could provision a PowerDNS zone
			// for a genuinely HIP-5 name. Remove the row so the bind is cleanly
			// rolled back and the name can be retried.
			s.DB().WithContext(ctx).Unscoped().Delete(wd)
			return nil, fmt.Errorf("failed to finalize domain record: %w", err)
		}
		// DANE still applies to a chain-managed name (the TLSA is served from
		// the name's on-chain zone data), so the stable DANE key must exist at
		// bind time — the same bootstrap invariant the self-hosted path
		// enforces. A failure rolls the just-inserted row back so the bind is
		// cleanly retryable.
		if err := s.ensureDANEIdentity(ctx, provider, namespace, domain); err != nil {
			s.DB().WithContext(ctx).Unscoped().Delete(wd)
			return nil, err
		}
		// This binding is the new website's first (primary) domain. Record it
		// so the website service resolves the apex domain via PrimaryDomainID
		// rather than the status=active fallback. A failure rolls the just-
		// inserted row back so the bind is cleanly retryable.
		if err := s.assignPrimaryAndNotify(ctx, &website, wd, notifyCreated); err != nil {
			s.DB().WithContext(ctx).Unscoped().Delete(wd)
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
		if err := s.DB().WithContext(ctx).Model(wd).Update("status", pluginDb.DomainStatusSelfHosted).Error; err != nil {
			return nil, fmt.Errorf("failed to finalize domain record: %w", err)
		}
		if err := s.ensureDANEIdentity(ctx, provider, namespace, domain); err != nil {
			s.DB().WithContext(ctx).Unscoped().Delete(wd)
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
			s.DB().WithContext(ctx).Unscoped().Delete(wd)
			return fmt.Errorf("zone resolution failed: %w", err)
		}

		target := pluginDb.WebsiteTargetType(website.TargetType).ToDNSLinkPath(website.TargetHash())
		// Name the record after the binding's domain (not the zone apex) so a
		// subdomain reusing a parent zone writes its own _dnslink.<subdomain>,
		// not the parent's.
		if err := s.dnsSvc.CreateDNSLinkRecord(ctx, zone.ID, domain, target); err != nil {
			s.DB().WithContext(ctx).Unscoped().Delete(wd)
			if zoneCreated {
				_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
			}
			return fmt.Errorf("dnslink creation failed: %w", err)
		}

		// Create apex record pointing to the gateway. DNSSEC-signed alt-root
		// providers (e.g. HNS) need a real A record (gateway IP) so the apex
		// carries an RRSIG; otherwise use an ALIAS to the gateway hostname.
		apexType := provider.ApexRecordType()
		var apexContent string
		if apexType == pluginCore.RecordTypeA {
			apexContent = s.gatewayIP()
			if apexContent == "" {
				s.DB().WithContext(ctx).Unscoped().Delete(wd)
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
				s.DB().WithContext(ctx).Unscoped().Delete(wd)
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
			s.DB().WithContext(ctx).Unscoped().Delete(wd)
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
		if err := s.DB().WithContext(ctx).Model(wd).Updates(map[string]any{
			"zone_id":             zone.ID,
			"status":              pluginDb.DomainStatusRecordsGenerated,
			"delegation_data":     wd.DelegationData,
			"dns_hosting_enabled": wd.DNSHostingEnabled,
		}).Error; err != nil {
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

// assignPrimaryAndNotify records wd as the website's primary when the website
// has none yet — so the website service resolves the apex domain via
// PrimaryDomainID rather than the status=active fallback — and, when
// notifyCreated, fires the admin "website created" notification. Shared by the
// managed, self-hosted, and on-chain managed bind paths. Only the primary
// write is fatal; the notification (via notifyAdminWebsiteCreated) is
// best-effort.
func (s *DelegatedDomainService) assignPrimaryAndNotify(ctx context.Context, website *pluginDb.Website, wd *pluginDb.WebsiteDomain, notifyCreated bool) error {
	if website.PrimaryDomainID == nil {
		if err := s.DB().WithContext(ctx).Model(website).Update("primary_domain_id", wd.ID).Error; err != nil {
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

// VerifyDomain checks delegation and persists the result. It returns a typed
// DelegationVerificationResult so callers can distinguish "not applicable"
// (self-hosted / on-chain / unresolved bindings) from "pending" (portal
// delegation not yet live) from "verified" without re-deriving hosting rules.
func (s *DelegatedDomainService) VerifyDomain(ctx context.Context,
	wd *pluginDb.WebsiteDomain) (DelegationVerificationResult, error) {

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
		if err := s.ValidatePlatformBinding(ctx, wd); err != nil {
			return DelegationVerificationResult{}, err
		}
		wd.Status = pluginDb.DomainStatusActive
		if s.DB() != nil {
			if err := s.DB().WithContext(ctx).Model(wd).Update("status", wd.Status).Error; err != nil {
				return DelegationVerificationResult{}, fmt.Errorf("failed to persist domain status: %w", err)
			}
		}
		return DelegationVerificationResult{State: DelegationVerified}, nil
	}

	// A binding created before handover source detection may still carry a
	// portal-managed zone even though the name has since become HIP-5. Inspect
	// that source before touching DNSSEC or portal delegation; the same response
	// is then passed to the conversion helper so verification performs one query.
	if wd.Namespace == pluginDb.DomainNamespaceHNS && wd.ZoneID != 0 &&
		wd.Status != pluginDb.DomainStatusOnchainManaged {
		onchain, inspectErr := provider.Inspect(ctx, wd.Domain)
		if inspectErr != nil {
			return DelegationVerificationResult{}, fmt.Errorf("domain inspection failed: %w", inspectErr)
		}
		if onchain {
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
				return DelegationVerificationResult{State: DelegationNotApplicable}, nil
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
		return DelegationVerificationResult{State: DelegationNotApplicable}, nil
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
	expectedDS, err := s.selfHealZone(ctx, provider, wd, expectedDS)
	if err != nil {
		return DelegationVerificationResult{}, err
	}

	verified, err := provider.VerifyDelegation(ctx, wd.Domain, expectedDS)
	if err != nil {
		wd.Status = pluginDb.DomainStatusError
		if s.DB() != nil {
			_ = s.DB().WithContext(ctx).Model(wd).Update("status", wd.Status)
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
		if live, nsErr := provider.LiveNameservers(ctx, wd.Domain); nsErr != nil {
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
		if err := s.DB().WithContext(ctx).Model(wd).Update("status", wd.Status).Error; err != nil {
			return DelegationVerificationResult{}, fmt.Errorf("failed to persist domain status: %w", err)
		}
	}

	return DelegationVerificationResult{State: state, ApprovedNS: approvedNS, LiveNS: liveNS}, nil
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

// DeleteDomain deletes a WebsiteDomain row scoped by id, website_id, and user_id.
// Returns gorm.ErrRecordNotFound if no row was deleted.
func (s *DelegatedDomainService) DeleteDomain(ctx context.Context, domainID, websiteID, userID uint) error {
	// If the domain being deleted is the website's primary, repoint
	// Website.PrimaryDomainID to the next remaining active binding (or clear it)
	// so the FK never dangles. Do this before the delete so we can read the
	// remaining bindings accurately.
	var wd pluginDb.WebsiteDomain
	if err := s.DB().WithContext(ctx).
		Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
		First(&wd).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return gorm.ErrRecordNotFound
		}
		return err
	}

	var website pluginDb.Website
	if err := s.DB().WithContext(ctx).Where("id = ?", websiteID).First(&website).Error; err == nil &&
		website.PrimaryDomainID != nil && *website.PrimaryDomainID == wd.ID {

		// Pick the next active (non-deleted) binding on this website.
		var next pluginDb.WebsiteDomain
		nextErr := s.DB().WithContext(ctx).
			Where("website_id = ? AND id != ? AND deleted_at IS NULL", websiteID, wd.ID).
			Order("id ASC").
			First(&next).Error
		if errors.Is(nextErr, gorm.ErrRecordNotFound) {
			// No other binding remains: clear the primary FK.
			if err := s.DB().WithContext(ctx).Model(&website).Update("primary_domain_id", nil).Error; err != nil {
				return fmt.Errorf("failed to clear primary domain: %w", err)
			}
		} else if nextErr != nil {
			return nextErr
		} else {
			if err := s.DB().WithContext(ctx).Model(&website).Update("primary_domain_id", next.ID).Error; err != nil {
				return fmt.Errorf("failed to repoint primary domain: %w", err)
			}
		}
	}

	res := s.DB().WithContext(ctx).
		Where("id = ? AND website_id = ? AND user_id = ?", domainID, websiteID, userID).
		Delete(&pluginDb.WebsiteDomain{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
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
// private key is encrypted at rest and stored so Caddy can later fetch the same
// key (stable SPKI) and re-issue certs around it without touching DNS. The key
// is only ever persisted when absent — it is never overwritten by a later push.
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

	// Notify DANE-capable providers that a cert is available. Certificate/DANE
	// handling is an optional sub-interface: a provider without DANE (e.g.
	// ICANN) simply does not implement CertificateProvider, so there is no
	// mandatory no-op to call.
	if certProvider, ok := provider.(CertificateProvider); ok {
		if err := certProvider.OnCertAvailable(ctx, domain, certPEM); err != nil {
			return "", "", fmt.Errorf("provider OnCertAvailable: %w", err)
		}
	}

	if s.DB() == nil {
		// test context
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
	txErr := s.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Lock the target row so concurrent cert pushes serialize per-domain.
		locked := tx.Clauses(clause.Locking{Strength: "UPDATE"})
		var wd pluginDb.WebsiteDomain
		if err := locked.Where("domain = ? AND namespace = ?", domain, ns).First(&wd).Error; err != nil {
			return err // includes gorm.ErrRecordNotFound
		}
		zoneID = wd.ZoneID
		portalManaged = wd.Class() == pluginDb.ClassPortalManaged
		if wd.ProtocolData == nil {
			wd.ProtocolData = make(datatypes.JSONMap)
		}

		// Always refresh the cert + TLSA + owner name on every push.
		wd.ProtocolData[daneKeyField] = certPEM
		wd.ProtocolData[protocolDataTLSAKey] = tlsa
		wd.ProtocolData[protocolDataOwnerKey] = ownerName

		// Persist the private key only when we don't already have one.
		if privateKeyPEM != "" {
			if _, exists := wd.ProtocolData[protocolDataPrivateKeyKey]; !exists {
				enc, encErr := s.encryptPrivateKey(ctx, privateKeyPEM)
				if encErr != nil {
					s.Logger().Warn("dane key not persisted (encryption key not configured?)",
						zap.String("domain", domain), zap.Error(encErr))
				} else {
					wd.ProtocolData[protocolDataPrivateKeyKey] = enc
				}
			} else {
				s.Logger().Warn("dane key push ignored: a key already exists for domain (not overwriting)",
					zap.String("domain", domain))
			}
		}

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
		return tx.Model(&pluginDb.WebsiteDomain{}).
			Where("id = ?", wd.ID).
			Updates(map[string]any{
				"protocol_data":   wd.ProtocolData,
				"delegation_data": wd.DelegationData,
				"updated_at":      time.Now(),
			}).Error
	})
	if txErr != nil {
		return "", "", fmt.Errorf("save domain tlsa: %w", txErr)
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
		// DANE persistence is best-effort at bind time: when the key-encryption
		// key is empty, the contract (config/dns.go) skips persistence rather
		// than failing, mirroring UpdateTLSAFromCert. Match the missing-key
		// sentinel specifically (errors.Is) so a genuine failure that merely
		// co-occurs with an absent key — DB error, AEAD decrypt failure, row
		// not found — is still surfaced instead of silently skipped.
		if errors.Is(err, errDANEKeyNotConfigured) {
			s.Logger().Warn("DANE identity not persisted (encryption key not configured); skipping",
				zap.String("domain", domain), zap.Error(err))
			return nil
		}
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
	err := s.DB().WithContext(ctx).Where("domain = ?", domain).First(&wd).Error
	if err != nil {
		return nil, err
	}
	return &wd, nil
}

// GetWebsiteDomainByDomainAndNamespace looks up a domain by namespace.
func (s *DelegatedDomainService) GetWebsiteDomainByDomainAndNamespace(ctx context.Context, domain string, ns pluginDb.DomainNamespace) (*pluginDb.WebsiteDomain, error) {
	if s.DB() == nil {
		return nil, gorm.ErrRecordNotFound
	}
	var wd pluginDb.WebsiteDomain
	err := s.DB().WithContext(ctx).Where("domain = ? AND namespace = ?", domain, ns).First(&wd).Error
	if err != nil {
		return nil, err
	}
	return &wd, nil
}

// ProtocolData keys for DANE TLS identity. All DANE state for a domain lives in
// the WebsiteDomain.ProtocolData JSON map (the per-protocol data store), keeping
// the key, cert, TLSA, and owner name together as one per-protocol unit.
const (
	daneKeyField              = "dane_cert_pem"    // last pushed cert (not a source of truth)
	protocolDataPrivateKeyKey = "dane_private_key" // encrypted at rest, written once
	protocolDataTLSAKey       = "tlsa"
	protocolDataOwnerKey      = "owner_name"
)

// StoredCert holds the decrypted DANE key material returned to a Caddy cert
// getter so it can re-issue a certificate around the persisted key (stable SPKI).
type StoredCert struct {
	PrivateKeyPEM string
	CertPEM       string
	TLSA          string
	OwnerName     string
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
	if err := s.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var wd pluginDb.WebsiteDomain
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("domain = ? AND namespace = ?", domain, ns).First(&wd).Error; err != nil {
			return err
		}
		if wd.ProtocolData != nil {
			if encrypted, ok := wd.ProtocolData[protocolDataPrivateKeyKey].(string); ok && encrypted != "" {
				decrypted, err := s.decryptPrivateKey(ctx, encrypted)
				if err != nil {
					return err
				}
				keyPEM = decrypted
				return nil
			}
		}

		generated, err := dane.GenerateKey()
		if err != nil {
			return fmt.Errorf("generate DANE key: %w", err)
		}
		encrypted, err := s.encryptPrivateKey(ctx, generated)
		if err != nil {
			return fmt.Errorf("encrypt DANE key: %w", err)
		}
		if wd.ProtocolData == nil {
			wd.ProtocolData = make(datatypes.JSONMap)
		}
		wd.ProtocolData[protocolDataPrivateKeyKey] = encrypted
		if err := tx.Model(&pluginDb.WebsiteDomain{}).Where("id = ?", wd.ID).
			Updates(map[string]any{"protocol_data": wd.ProtocolData, "updated_at": time.Now()}).Error; err != nil {
			return err
		}
		keyPEM = generated
		return nil
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
	return s.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var wd pluginDb.WebsiteDomain
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("domain = ? AND namespace = ?", domain, ns).First(&wd).Error; err != nil {
			return err
		}
		if wd.ProtocolData == nil {
			wd.ProtocolData = make(datatypes.JSONMap)
		}
		wd.ProtocolData[protocolDataTLSAKey] = tlsa
		wd.ProtocolData[protocolDataOwnerKey] = ownerName
		return tx.Model(&pluginDb.WebsiteDomain{}).Where("id = ?", wd.ID).
			Updates(map[string]any{
				"protocol_data": wd.ProtocolData,
				"updated_at":    time.Now(),
			}).Error
	})
}

// GetCertificateKey returns the stored DANE key material for a domain,
// decrypting the private key at rest. Returns gorm.ErrRecordNotFound when the
// domain has no persisted key yet (i.e. first bootstrap).
func (s *DelegatedDomainService) GetCertificateKey(ctx context.Context, namespace, domain string) (*StoredCert, error) {
	ns := pluginDb.DomainNamespace(namespace)
	wd, err := s.GetWebsiteDomainByDomainAndNamespace(ctx, domain, ns)
	if err != nil {
		return nil, err // includes gorm.ErrRecordNotFound
	}
	if wd.ProtocolData == nil {
		return nil, gorm.ErrRecordNotFound
	}
	encKey, ok := wd.ProtocolData[protocolDataPrivateKeyKey].(string)
	if !ok || encKey == "" {
		return nil, gorm.ErrRecordNotFound
	}
	keyPEM, err := s.decryptPrivateKey(ctx, encKey)
	if err != nil {
		return nil, err
	}
	result := &StoredCert{
		PrivateKeyPEM: keyPEM,
	}
	if v, ok := wd.ProtocolData[daneKeyField].(string); ok {
		result.CertPEM = v
	}
	if v, ok := wd.ProtocolData[protocolDataTLSAKey].(string); ok {
		result.TLSA = v
	}
	if v, ok := wd.ProtocolData[protocolDataOwnerKey].(string); ok {
		result.OwnerName = v
	}
	return result, nil
}

// GetDANERecord returns the stored DANE TLSA rdata and owner name for a domain
// without decrypting the private key — the lightweight read surface for
// consumers that only need the record to publish into the name's zone (e.g.
// dns-requirements). Returns gorm.ErrRecordNotFound when the binding does not
// exist and empty strings when no DANE identity has been computed yet.
func (s *DelegatedDomainService) GetDANERecord(ctx context.Context, namespace, domain string) (tlsa, ownerName string, err error) {
	wd, err := s.GetWebsiteDomainByDomainAndNamespace(ctx, domain, pluginDb.DomainNamespace(namespace))
	if err != nil {
		return "", "", err
	}
	if v, ok := wd.ProtocolData[protocolDataTLSAKey].(string); ok {
		tlsa = v
	}
	if v, ok := wd.ProtocolData[protocolDataOwnerKey].(string); ok {
		ownerName = v
	}
	return tlsa, ownerName, nil
}

// RepublishChainDANERecord returns the DANE TLSA the owner must install in a
// chain-managed (HIP-5) binding's on-chain zone data. Republish must preserve
// an already-installed identity — deriving from the key would rotate the SPKI
// pin and invalidate the live on-chain record — so an existing stored TLSA is
// returned unchanged. Only a binding with no on-chain identity yet is
// bootstrapped from the stable DANE key (the source of truth for TLSA 3 1 1,
// never a certificate), and no PowerDNS zone write occurs. If no identity
// exists and a fresh key cannot be persisted (key-encryption key unset), it
// returns gorm.ErrRecordNotFound.
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
	if err == nil {
		return sc.TLSA, sc.OwnerName, nil
	}
	if !errors.Is(err, errDANEKeyNotConfigured) {
		return "", "", fmt.Errorf("refresh on-chain DANE identity: %w", err)
	}
	return "", "", gorm.ErrRecordNotFound
}

// GetActiveWebsiteDomainByDomain finds an active domain across all namespaces.
func (s *DelegatedDomainService) GetActiveWebsiteDomainByDomain(ctx context.Context, domain string) (*pluginDb.WebsiteDomain, error) {
	var wd pluginDb.WebsiteDomain
	err := s.DB().WithContext(ctx).Where("domain = ? AND status = ?", domain, pluginDb.DomainStatusActive).First(&wd).Error
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
	q := s.DB().WithContext(ctx).Where("status = ?", status)
	if lastID > 0 {
		q = q.Where("id > ?", lastID)
	}
	err := q.Order("id ASC").Limit(limit).Find(&wds).Error
	if err != nil {
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
