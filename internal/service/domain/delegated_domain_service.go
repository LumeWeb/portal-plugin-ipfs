package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
}

type DNSZoneService interface {
	CreateZone(ctx context.Context, domain string, userID uint) (*pluginDb.DNSZone, error)
	// GetZoneByDomain retrieves a zone by its domain name (including
	// soft-deleted zones). Returns gorm.ErrRecordNotFound when none match.
	GetZoneByDomain(ctx context.Context, domain string) (*pluginDb.DNSZone, error)
	DeleteZone(ctx context.Context, zoneID uint) error
	CreateDNSLinkRecord(ctx context.Context, zoneID uint, target string) error
	// CreateApexRecord creates the apex (root) record for a zone of the given
	// record type (e.g. RecordTypeA or RecordTypeALIAS). content is the raw
	// value: an IP address for A, a gateway hostname for ALIAS.
	CreateApexRecord(ctx context.Context, zoneID uint, recordType pluginCore.RecordType, content string) error
	// SetTLSARecord writes (or replaces) the DANE TLSA record for a zone's
	// HTTPS/TCP owner `_443._tcp` pointing at the portal-managed authoritative
	// zone. content is the TLSA rdata: "usage selector matching hash" (e.g.
	// "3 1 1 <hex>")). For HNS managed zones this makes DANE validators resolve
	// the TLSA against the portal's PowerDNS zone; without it, authoritative
	// queries return NXDOMAIN.
	SetTLSARecord(ctx context.Context, zoneID uint, content string) error
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

// NewDelegatedDomainService creates a DelegatedDomainService with the given
// registry and DNS service. BaseComponent is injected by the framework.
func NewDelegatedDomainService(reg *Registry, dns DNSZoneService) *DelegatedDomainService {
	return &DelegatedDomainService{
		registry: reg,
		dnsSvc:   dns,
	}
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
func (s *DelegatedDomainService) resolveManagedZone(ctx context.Context, domain string, userID uint) (*pluginDb.DNSZone, bool, error) {
	// A subdomain (e.g. docs.pinner.xyz) lives inside its parent's zone
	// (pinner.xyz); only the apex owns a zone.
	if parent := parentDomain(domain); parent != "" {
		z, err := s.dnsSvc.GetZoneByDomain(ctx, parent)
		if err == nil && z != nil && z.UserID == userID {
			return z, false, nil
		}
		if err != nil && err != gorm.ErrRecordNotFound {
			return nil, false, fmt.Errorf("lookup parent zone %q: %w", parent, err)
		}
	}

	z, err := s.dnsSvc.CreateZone(ctx, domain, userID)
	if err != nil {
		return nil, false, fmt.Errorf("zone creation failed: %w", err)
	}
	return z, true, nil
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
	config json.RawMessage) (*pluginDb.WebsiteDomain, error) {

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
		return wd, nil
	}

	// Managed DNS: create DNS resources only after the DB row is committed.
	// The authoritative zone follows the one-zone rule — apex owns, subdomain
	// reuses the parent's zone.
	zone, zoneCreated, err := s.resolveManagedZone(ctx, domain, userID)
	if err != nil {
		s.DB().WithContext(ctx).Unscoped().Delete(wd)
		return nil, fmt.Errorf("zone resolution failed: %w", err)
	}

	target := pluginDb.WebsiteTargetType(website.TargetType).ToDNSLinkPath(website.TargetHash())
	if err := s.dnsSvc.CreateDNSLinkRecord(ctx, zone.ID, target); err != nil {
		s.DB().WithContext(ctx).Unscoped().Delete(wd)
		if zoneCreated {
			_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
		}
		return nil, fmt.Errorf("dnslink creation failed: %w", err)
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
			return nil, fmt.Errorf("gateway_ip not configured: alt-root apex requires a real A record and cannot fall back to ALIAS (set dns.gateway_ip, e.g. to the gateway IP)")
		}
	} else if gatewayHost := s.gatewayHost(); gatewayHost != "" {
		apexContent = gatewayHost
	}

	if apexContent != "" {
		if err := s.dnsSvc.CreateApexRecord(ctx, zone.ID, apexType, apexContent); err != nil {
			s.DB().WithContext(ctx).Unscoped().Delete(wd)
			if zoneCreated {
				_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
			}
			return nil, fmt.Errorf("apex record creation failed: %w", err)
		}
		wd.GatewayHost = apexContent
	}

	// Build delegation after zone is created (needs zone ID).
	delegationAny, err := provider.BuildDelegation(ctx, zone.ID, domain, &website, config)
	if err != nil {
		s.DB().WithContext(ctx).Unscoped().Delete(wd)
		_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
		return nil, fmt.Errorf("delegation build failed: %w", err)
	}
	delegationBytes, err := json.Marshal(delegationAny)
	if err != nil {
		s.DB().WithContext(ctx).Unscoped().Delete(wd)
		_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
		return nil, fmt.Errorf("marshal delegation data: %w", err)
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
		return nil, fmt.Errorf("failed to finalize domain record: %w", err)
	}

	// If the owning website has no primary domain yet, make this binding the
	// primary so Website.PrimaryDomainID never dangles after the first domain
	// is added. This is how a website created with a transparent primary domain
	// (and additional domains added later) keeps its FK consistent.
	if website.PrimaryDomainID == nil {
		if err := s.DB().WithContext(ctx).Model(&website).Update("primary_domain_id", wd.ID).Error; err != nil {
			return nil, fmt.Errorf("failed to set primary domain: %w", err)
		}
		website.PrimaryDomainID = &wd.ID
	}

	return wd, nil
}

// VerifyDomain checks delegation and persists the result.
func (s *DelegatedDomainService) VerifyDomain(ctx context.Context,
	wd *pluginDb.WebsiteDomain) (bool, error) {

	provider := s.registry.Get(string(wd.Namespace))
	if provider == nil {
		return false, fmt.Errorf("unsupported namespace: %s", wd.Namespace)
	}

	// A self-hosted DNS binding owns no portal-managed zone to verify: the user
	// runs the authoritative server and DNSSEC/DANE delegation is theirs. The
	// binding is not part of the portal's lifecycle (no DS to reconcile, no
	// SOA/DNSSEC to self-heal), so verification is a no-op that leaves the
	// binding's existing status rather than querying PowerDNS for a zone that
	// does not exist. The presence of a real PowerDNS zone (ZoneID != 0) is
	// the authoritative marker of a portal-hosted binding, regardless of the
	// dns_hosting_enabled flag (which may be false on legacy bindings that
	// still hold a zone).
	if wd.ZoneID == 0 {
		return false, nil
	}

	// Expected DS is computed live from PowerDNS's current active signing key
	// (never persisted, so it cannot go stale on key rotation). ICANN's
	// VerifyDelegation ignores it; HNS uses it to require the parent zone to
	// serve the DS before marking the domain Active.
	//
	// A zone with no active signing key (("", nil)) is genuinely self-managed —
	// the portal generated no DS, so NS-only verification is correct. But if
	// resolution ERRORS (key rollover with multiple active keys, PowerDNS
	// unreachable), the zone is portal-managed and the live DS is
	// indeterminate. We must NOT silently weaken a managed zone to NS-only on
	// a transient failure: that would mark Active a zone whose DS chain of
	// trust was not actually confirmed.
	expectedDS, dsErr := s.dnsSvc.GetActiveDNSSECDS(ctx, wd.ZoneID)
	if dsErr != nil {
		return false, fmt.Errorf("resolve live DS for zone %d: %w", wd.ZoneID, dsErr)
	}

	// Self-heal re-ensures the portal-managed-zone invariants that are
	// otherwise only established at bind/create time (see selfHealZone).
	// Gate 1 (DNSSEC) covers managed-DNSSEC namespaces; gate 2 (SOA MNAME)
	// covers any portal-managed PowerDNS zone, ICANN included.
	expectedDS, err := s.selfHealZone(ctx, provider, wd, expectedDS)
	if err != nil {
		return false, err
	}

	verified, err := provider.VerifyDelegation(ctx, wd.Domain, expectedDS)
	if err != nil {
		wd.Status = pluginDb.DomainStatusError
		if s.DB() != nil {
			_ = s.DB().WithContext(ctx).Model(wd).Update("status", wd.Status)
		}
		return false, err
	}

	if verified {
		wd.Status = pluginDb.DomainStatusActive
	} else {
		wd.Status = pluginDb.DomainStatusWaitingDelegation
	}

	if s.DB() != nil {
		if err := s.DB().WithContext(ctx).Model(wd).Update("status", wd.Status).Error; err != nil {
			return false, fmt.Errorf("failed to persist domain status: %w", err)
		}
	}

	return verified, nil
}

// selfHealZone re-ensures the portal-managed-zone invariants that are
// otherwise only established at bind/create time, so verification recovers a
// zone that slipped past (or drifted from) those one-time setup steps without
// requiring the user to re-bind. The two invariants are gated independently:
//
//  1. DNSSEC active signing key (fatal). For managed-DNSSEC namespaces
//     (UsesManagedZoneTLSA, e.g. HNS). A "no active key" result (("", nil))
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
func (s *DelegatedDomainService) selfHealZone(ctx context.Context, provider DomainProvider, wd *pluginDb.WebsiteDomain, expectedDS string) (string, error) {
	// DNSSEC self-heal: only managed-DNSSEC namespaces (UsesManagedZoneTLSA).
	if provider.UsesManagedZoneTLSA() && expectedDS == "" {
		if _, err := s.dnsSvc.EnableDNSSEC(ctx, wd.ZoneID); err != nil {
			return "", fmt.Errorf("enable dnssec for zone %d: %w", wd.ZoneID, err)
		}
		// Re-read the DS now that the zone should have an active key.
		healedDS, dsErr := s.dnsSvc.GetActiveDNSSECDS(ctx, wd.ZoneID)
		if dsErr != nil {
			return "", fmt.Errorf("resolve live DS for zone %d after enable: %w", wd.ZoneID, dsErr)
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

	// Notify the provider that a cert is available
	if err := provider.OnCertAvailable(ctx, domain, certPEM); err != nil {
		return "", "", fmt.Errorf("provider OnCertAvailable: %w", err)
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
	txErr := s.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Lock the target row so concurrent cert pushes serialize per-domain.
		locked := tx.Clauses(clause.Locking{Strength: "UPDATE"})
		var wd pluginDb.WebsiteDomain
		if err := locked.Where("domain = ? AND namespace = ?", domain, ns).First(&wd).Error; err != nil {
			return err // includes gorm.ErrRecordNotFound
		}
		zoneID = wd.ZoneID
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
	// opts in here rather than via a hardcoded "hns" check.
	if s.dnsSvc != nil && zoneID != 0 && provider.UsesManagedZoneTLSA() {
		if err := s.dnsSvc.SetTLSARecord(ctx, zoneID, tlsa); err != nil {
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
// translates certs into a DANE TLSA that the portal publishes into its managed
// authoritative zone. Only such namespaces (e.g. HNS) can be force-republished.
func (s *DelegatedDomainService) NamespaceUsesManagedZoneTLSA(namespace string) bool {
	if s.registry == nil {
		return false
	}
	prov := s.registry.Get(namespace)
	return prov != nil && prov.UsesManagedZoneTLSA()
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
	for _, ns := range s.registry.Names() {
		prov := s.registry.Get(ns)
		if prov != nil {
			if err := prov.Validate(domain); err == nil {
				return ns, true
			}
		}
	}
	return "", false
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
