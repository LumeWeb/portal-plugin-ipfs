package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func (s *DelegatedDomainService) CreateDomain(ctx context.Context,
	namespace, domain string, websiteID, userID uint, config json.RawMessage) (*pluginDb.WebsiteDomain, error) {

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

	// Persist WebsiteDomain first so external DNS side effects are only
	// created for committed rows. Require DB to be available.
	if s.DB() == nil {
		return nil, fmt.Errorf("database not available")
	}

	wd := &pluginDb.WebsiteDomain{
		WebsiteID: websiteID,
		UserID:    userID,
		Domain:    domain,
		Namespace: pluginDb.DomainNamespace(namespace),
		ZoneName:  canonicalZoneName(domain),
		Status:    pluginDb.DomainStatusDraft,
	}

	if err := s.DB().WithContext(ctx).Create(wd).Error; err != nil {
		return nil, fmt.Errorf("persist failed: %w", err)
	}

	// Create DNS resources only after the DB row is committed.
	zone, err := s.dnsSvc.CreateZone(ctx, domain, userID)
	if err != nil {
		s.DB().WithContext(ctx).Unscoped().Delete(wd)
		return nil, fmt.Errorf("zone creation failed: %w", err)
	}

	target := pluginDb.WebsiteTargetType(website.TargetType).ToDNSLinkPath(website.TargetHash())
	if err := s.dnsSvc.CreateDNSLinkRecord(ctx, zone.ID, target); err != nil {
		s.DB().WithContext(ctx).Unscoped().Delete(wd)
		_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
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
			_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
			return nil, fmt.Errorf("gateway_ip not configured: alt-root apex requires a real A record and cannot fall back to ALIAS (set dns.gateway_ip, e.g. to the gateway IP)")
		}
	} else if gatewayHost := s.gatewayHost(); gatewayHost != "" {
		apexContent = gatewayHost
	}

	if apexContent != "" {
		if err := s.dnsSvc.CreateApexRecord(ctx, zone.ID, apexType, apexContent); err != nil {
			s.DB().WithContext(ctx).Unscoped().Delete(wd)
			_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
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
		"zone_id":         zone.ID,
		"status":          pluginDb.DomainStatusRecordsGenerated,
		"delegation_data": wd.DelegationData,
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
