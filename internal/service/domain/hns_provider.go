package domain

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"

	dane "go.lumeweb.com/dane"
	danehns "go.lumeweb.com/dane/hns"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

const (
	DaneTLSAUsage = 3

	DaneTLSASelector = 1

	DaneTLSAMatching = 1

	DaneTLSAPort = 443

	DaneTLSATransport = "tcp"
)

func TLSAHashPrefix() string {
	return fmt.Sprintf("%d %d %d ", DaneTLSAUsage, DaneTLSASelector, DaneTLSAMatching)
}

type HNSDelegationMode string

const (
	HNSModeDelegated HNSDelegationMode = "delegated"
	HNSModeInline    HNSDelegationMode = "inline"
)

func stripTLSAHashPrefix(s string) string {
	p := TLSAHashPrefix()
	if strings.HasPrefix(s, p) {
		return strings.TrimPrefix(s, p)
	}
	return s
}

func formatFullTLSARecord(tlsaData, zoneName string) string {
	tlsaData = stripTLSAHashPrefix(tlsaData)
	rr := dane.FormatTLSARecord(DaneTLSAUsage, DaneTLSASelector, DaneTLSAMatching, tlsaData, zoneName, DaneTLSAPort, DaneTLSATransport)
	owner := strings.SplitN(rr, " ", 2)[0]
	return fmt.Sprintf("%s 3600 IN TLSA %d %d %d %s", owner, DaneTLSAUsage, DaneTLSASelector, DaneTLSAMatching, tlsaData)
}

// HNSDelegationConfig holds per-domain HNS delegation settings.
type HNSDelegationConfig struct {
	Mode HNSDelegationMode `json:"mode"`

	NameserverHost string `json:"nameserver_host"`

	NameserverIPv4 string `json:"nameserver_ipv4"`
	NameserverIPv6 string `json:"nameserver_ipv6"`
}

// Record is a DNS record used in delegation data.
type Record struct {
	Type    string `json:"type"`
	Value   string `json:"value,omitempty"`
	NS      string `json:"ns,omitempty"`
	Address string `json:"address,omitempty"`
}

func nsRecord(value string) Record {
	return Record{Type: "NS", Value: value}
}

func tlsaRecord(value string) Record {
	return Record{Type: "TLSA", Value: value}
}

func glue4Record(ns, addr string) Record {
	return Record{Type: "GLUE4", NS: ns, Address: addr}
}

func glue6Record(ns, addr string) Record {
	return Record{Type: "GLUE6", NS: ns, Address: addr}
}

func synth4Record(addr string) Record {
	return Record{Type: "SYNTH4", Address: addr}
}

func synth6Record(addr string) Record {
	return Record{Type: "SYNTH6", Address: addr}
}

// DelegationBundle is the typed result of BuildDelegation.
type DelegationBundle struct {
	Mode                 string   `json:"mode"`
	ParentRecords        []Record `json:"parent_records"`
	AuthoritativeRecords []Record `json:"authoritative_records"`
	Instructions         string   `json:"instructions"`
}

// HNSProvider implements DomainProvider for the Handshake (HNS) namespace.
//
// Resolution assumptions:
//   - HNS names live on the Handshake blockchain.
//   - To verify delegation (NS records) or resolve names, you MUST supply
//     an HNS-aware DNS resolver address (e.g. an hsd instance or HNS DoH
//     endpoint). The system default resolver is not HNS-friendly.
//   - This is distinct from ICANN, where standard resolvers work and
//     VerifyDelegation is a no-op.
type HNSProvider struct {
	resolverAddr string
	nsRecords    []string
	tlsaSource   TLSASource
	tlsaMu       sync.RWMutex
	dnsSvc       DNSZoneService
}

// TLSASource holds certificates keyed by domain for TLSA computation.
// The cert is set via OnCertAvailable when the Caddy webhook pushes it.
type TLSASource struct {
	Certs map[string]string
}

func NewHNSProvider(resolver string, nsRecords []string, tlsa TLSASource) *HNSProvider {
	return &HNSProvider{
		resolverAddr: resolver,
		nsRecords:    nsRecords,
		tlsaSource:   tlsa,
	}
}

// SetDNSService injects the DNS zone service for DNSSEC enablement.
func (p *HNSProvider) SetDNSService(dns DNSZoneService) {
	p.dnsSvc = dns
}

func (p *HNSProvider) Protocol() string {
	return "hns"
}

var hnsDomainRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

func (p *HNSProvider) Validate(domain string) error {
	domain = strings.TrimSuffix(domain, "/")
	domain = strings.TrimSuffix(domain, ".")
	domain = strings.ToLower(domain)
	if domain == "" {
		return fmt.Errorf("domain is required")
	}
	if strings.Contains(domain, ".") {
		return fmt.Errorf("HNS domain must be single-label (no dots)")
	}
	if len(domain) > 63 {
		return fmt.Errorf("HNS domain exceeds 63 characters")
	}
	if !hnsDomainRe.MatchString(domain) {
		return fmt.Errorf("HNS domain contains invalid characters")
	}
	return nil
}

func (p *HNSProvider) BuildDelegation(ctx context.Context, zoneID uint,
	domain string, website *pluginDb.Website, config json.RawMessage) (any, error) {

	zoneName := domain + "."

	var cfg HNSDelegationConfig
	if len(config) > 0 {
		if err := json.Unmarshal(config, &cfg); err != nil {
			return nil, fmt.Errorf("invalid hns delegation config: %w", err)
		}
	}

	if cfg.Mode == "" {
		cfg.Mode = HNSModeDelegated
	}

	tlsa, err := p.buildTLSA(ctx, zoneName)
	if err != nil {
		// Allow delegation creation before the gateway cert is available;
		// TLSA will be backfilled by OnCertAvailable / UpdateTLSAFromCert.
		tlsa = ""
	}

	// Enable DNSSEC on the zone and compute DS record
	var dsRecords []Record
	if p.dnsSvc != nil {
		ds, dsErr := p.enableDNSSECAndDS(ctx, zoneID, zoneName)
		if dsErr != nil {
			return nil, fmt.Errorf("dnssec enablement failed: %w", dsErr)
		}
		dsRecords = ds
	}

	nsRecords := p.Nameservers()

	var bundle DelegationBundle

	switch cfg.Mode {
	case HNSModeInline:
		var err error
		bundle, err = p.buildHNSInline(zoneName, nsRecords, tlsa, &cfg)
		if err != nil {
			return nil, err
		}
	default:
		bundle = p.buildDelegated(zoneName, nsRecords, tlsa, &cfg)
	}

	// Append DS records to parent (published at the parent zone for chain of trust)
	if len(dsRecords) > 0 {
		bundle.ParentRecords = append(bundle.ParentRecords, dsRecords...)
	}

	return bundle, nil
}

// enableDNSSECAndDS enables DNSSEC on the zone via PowerDNS and computes
// the DS record from the returned DNSKEY using the dane library.
func (p *HNSProvider) enableDNSSECAndDS(ctx context.Context, zoneID uint, zoneName string) ([]Record, error) {
	dnskeyStr, err := p.dnsSvc.EnableDNSSEC(ctx, zoneID)
	if err != nil {
		return nil, fmt.Errorf("enable dnssec: %w", err)
	}

	// PowerDNS returns DNSKEY as "257 3 13 <base64key>" (flags protocol algorithm key)
	parts := strings.Fields(dnskeyStr)
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid dnskey format from powerdns: %q", dnskeyStr)
	}

	flags, err := strconv.ParseUint(parts[0], 10, 16)
	if err != nil {
		return nil, fmt.Errorf("parse dnskey flags: %w", err)
	}
	protocol, err := strconv.ParseUint(parts[1], 10, 8)
	if err != nil {
		return nil, fmt.Errorf("parse dnskey protocol: %w", err)
	}
	algorithm, err := strconv.ParseUint(parts[2], 10, 8)
	if err != nil {
		return nil, fmt.Errorf("parse dnskey algorithm: %w", err)
	}

	pubKey, err := base64.StdEncoding.DecodeString(strings.Join(parts[3:], ""))
	if err != nil {
		return nil, fmt.Errorf("decode dnskey public key: %w", err)
	}

	// Build DNSKEY RDATA: flags(2) + protocol(1) + algorithm(1) + public_key
	rdata := make([]byte, 4+len(pubKey))
	binary.BigEndian.PutUint16(rdata[0:2], uint16(flags))
	rdata[2] = uint8(protocol)
	rdata[3] = uint8(algorithm)
	copy(rdata[4:], pubKey)

	// Compute DS record using SHA-256 digest (type 2)
	canonicalName := []byte(strings.ToLower(zoneName))
	ds, err := dane.ComputeDS(canonicalName, rdata, 2)
	if err != nil {
		return nil, fmt.Errorf("compute ds: %w", err)
	}

	dsStr := dane.FormatDSRecord(strings.TrimSuffix(zoneName, "."), ds)
	return []Record{{
		Type:  "DS",
		Value: dsStr,
	}}, nil
}

func (p *HNSProvider) buildDelegated(zoneName string, nsRecords []string, tlsa string, cfg *HNSDelegationConfig) DelegationBundle {
	effectiveNS := nsRecords
	if len(effectiveNS) == 0 && cfg.NameserverHost != "" {
		effectiveNS = []string{cfg.NameserverHost}
	}

	parent := []Record{
		nsRecord(strings.Join(effectiveNS, ",")),
	}

	if cfg.NameserverHost != "" && (cfg.NameserverIPv4 != "" || cfg.NameserverIPv6 != "") {
		if strings.HasSuffix(strings.TrimSuffix(cfg.NameserverHost, "."), strings.TrimSuffix(zoneName, ".")) {
			if cfg.NameserverIPv4 != "" {
				parent = append(parent, glue4Record(cfg.NameserverHost, cfg.NameserverIPv4))
			}
			if cfg.NameserverIPv6 != "" {
				parent = append(parent, glue6Record(cfg.NameserverHost, cfg.NameserverIPv6))
			}
		}
	}

	auth := []Record{
		nsRecord(strings.Join(effectiveNS, "\n")),
	}
	if tlsa != "" {
		auth = append(auth, tlsaRecord(formatFullTLSARecord(tlsa, zoneName)))
	}

	return DelegationBundle{
		Mode:                 string(HNSModeDelegated),
		ParentRecords:        parent,
		AuthoritativeRecords: auth,
		Instructions:         "Publish the parent_records (NS + optional GLUE) in your HNS wallet. Configure the authoritative_records on your DNS server.",
	}
}

func (p *HNSProvider) buildHNSInline(zoneName string, nsRecords []string, tlsa string, cfg *HNSDelegationConfig) (DelegationBundle, error) {
	parent := []Record{}

	if cfg.NameserverIPv4 != "" {
		parent = append(parent, synth4Record(cfg.NameserverIPv4))
	}
	if cfg.NameserverIPv6 != "" {
		parent = append(parent, synth6Record(cfg.NameserverIPv6))
	}
	if len(parent) == 0 {
		return DelegationBundle{}, fmt.Errorf("inline mode requires nameserver_ipv4 or nameserver_ipv6")
	}

	effectiveNS := nsRecords
	if len(effectiveNS) == 0 && cfg.NameserverHost != "" {
		effectiveNS = []string{cfg.NameserverHost}
	}
	authNS := strings.Join(effectiveNS, "\n")
	if cfg.NameserverIPv4 != "" {
		ip := net.ParseIP(cfg.NameserverIPv4)
		if ip == nil {
			return DelegationBundle{}, fmt.Errorf("invalid nameserver_ipv4: %s", cfg.NameserverIPv4)
		}
		authNS = danehns.SynthName(ip)
	} else if cfg.NameserverIPv6 != "" {
		ip := net.ParseIP(cfg.NameserverIPv6)
		if ip == nil {
			return DelegationBundle{}, fmt.Errorf("invalid nameserver_ipv6: %s", cfg.NameserverIPv6)
		}
		authNS = danehns.SynthName(ip)
	}

	auth := []Record{
		nsRecord(authNS),
	}
	if tlsa != "" {
		auth = append(auth, tlsaRecord(formatFullTLSARecord(tlsa, zoneName)))
	}

	return DelegationBundle{
		Mode:                 string(HNSModeInline),
		ParentRecords:        parent,
		AuthoritativeRecords: auth,
		Instructions:         "Publish the SYNTH records in your HNS wallet. The authoritative side uses synthetic nameserver names derived from the IPs.",
	}, nil
}

func (p *HNSProvider) buildTLSA(ctx context.Context, zoneName string) (string, error) {
	domain := strings.TrimSuffix(zoneName, ".")

	p.tlsaMu.RLock()
	certPEM, ok := p.tlsaSource.Certs[domain]
	p.tlsaMu.RUnlock()

	if !ok || certPEM == "" {
		return "", fmt.Errorf("no cert available for TLSA; waiting for cert push from gateway")
	}

	spkiHash, err := dane.ComputeTLSAFromCert(certPEM)
	if err != nil {
		return "", err
	}
	return spkiHash, nil
}

// Nameservers returns the HNS nameservers configured for the namespace.
// Alt-root namespaces delegate to nameservers that are themselves members
// of the namespace (e.g. HNS domain names), distinct from ICANN's.
func (p *HNSProvider) Nameservers() []string {
	if len(p.nsRecords) == 0 {
		return nil
	}
	return p.nsRecords
}

// VerifyDelegation checks if the expected NS records for a HNS name are visible
// via the configured HNS resolver. HNS names are resolved against the Handshake
// blockchain; this requires an HNS-aware resolver (not the system default).
// For ICANN, verification is intentionally a no-op.
func (p *HNSProvider) VerifyDelegation(ctx context.Context, domain string,
	delegationData json.RawMessage) (bool, error) {

	if p.resolverAddr == "" {
		return false, fmt.Errorf("HNS resolver not configured (DnsConfig.HNSResolver); standard resolvers cannot resolve HNS names")
	}

	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{}
			return d.DialContext(ctx, network, p.resolverAddr)
		},
	}

	nss, err := resolver.LookupNS(ctx, domain+".")
	if err != nil {
		return false, fmt.Errorf("HNS resolver query failed: %w", err)
	}

	expectedNS := p.Nameservers()
	for _, ns := range nss {
		for _, expected := range expectedNS {
			if strings.TrimSuffix(ns.Host, ".") == strings.TrimSuffix(expected, ".") {
				return true, nil
			}
		}
	}

	return false, nil
}

// OnCertAvailable updates the TLSA source when a cert is pushed from the
// Caddy cert webhook. This replaces the legacy config-based TLSASource.
func (p *HNSProvider) OnCertAvailable(ctx context.Context, domain string, certPEM string) error {
	p.tlsaMu.Lock()
	defer p.tlsaMu.Unlock()
	if p.tlsaSource.Certs == nil {
		p.tlsaSource.Certs = make(map[string]string)
	}
	p.tlsaSource.Certs[domain] = certPEM
	return nil
}
