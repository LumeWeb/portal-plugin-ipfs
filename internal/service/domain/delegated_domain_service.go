package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	dane "go.lumeweb.com/dane"
	"go.lumeweb.com/ipfs-sdk/dnsname"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
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
	CreateALIASRecord(ctx context.Context, zoneID uint, gatewayHost string) error
	// EnableDNSSEC enables DNSSEC on a zone and returns the DNSKEY.
	EnableDNSSEC(ctx context.Context, zoneID uint) (dnskey string, err error)
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

	// Create apex ALIAS record pointing to the gateway host.
	if gatewayHost := s.gatewayHost(); gatewayHost != "" {
		if err := s.dnsSvc.CreateALIASRecord(ctx, zone.ID, gatewayHost); err != nil {
			s.DB().WithContext(ctx).Unscoped().Delete(wd)
			_ = s.dnsSvc.DeleteZone(ctx, zone.ID)
			return nil, fmt.Errorf("alias creation failed: %w", err)
		}
		wd.GatewayHost = gatewayHost
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

	return wd, nil
}

// VerifyDomain checks delegation and persists the result.
func (s *DelegatedDomainService) VerifyDomain(ctx context.Context,
	wd *pluginDb.WebsiteDomain) (bool, error) {

	provider := s.registry.Get(string(wd.Namespace))
	if provider == nil {
		return false, fmt.Errorf("unsupported namespace: %s", wd.Namespace)
	}

	data, err := json.Marshal(wd.DelegationData)
	if err != nil {
		return false, err
	}

	verified, err := provider.VerifyDelegation(ctx, wd.Domain, data)
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

// UpdateTLSAFromCert computes TLSA from a pushed cert and stores it.
func (s *DelegatedDomainService) UpdateTLSAFromCert(ctx context.Context, namespace, domain, certPEM string) (tlsa, ownerName string, err error) {
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
	wd, err := s.GetWebsiteDomainByDomainAndNamespace(ctx, domain, ns)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return tlsa, ownerName, gorm.ErrRecordNotFound
		}
		return "", "", fmt.Errorf("lookup domain: %w", err)
	}

	if wd.ProtocolData == nil {
		wd.ProtocolData = make(datatypes.JSONMap)
	}
	wd.ProtocolData["tlsa"] = tlsa
	wd.ProtocolData["owner_name"] = ownerName

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

	if err := s.DB().WithContext(ctx).Save(wd).Error; err != nil {
		return "", "", fmt.Errorf("save domain tlsa: %w", err)
	}

	return tlsa, ownerName, nil
}

// UsesDelegationForOwnership returns true for namespaces that use delegation (e.g. HNS).
func (s *DelegatedDomainService) UsesDelegationForOwnership(domain string) bool {
	ns, ok := s.getNamespaceForDomain(domain)
	return ok && ns != string(pluginDb.DomainNamespaceICANN)
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
