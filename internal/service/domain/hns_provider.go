package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	dane "go.lumeweb.com/dane"
	danehns "go.lumeweb.com/dane/hns"
	"go.lumeweb.com/ipfs-sdk/dnsname"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/tlds"

	"github.com/miekg/dns"
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

// ApexRecordType returns RecordTypeA: HNS zones are DNSSEC-signed at the apex,
// so the apex must be a real A record that carries an RRSIG. PowerDNS cannot
// sign a synthetic ALIAS at the apex.
func (p *HNSProvider) ApexRecordType() pluginCore.RecordType {
	return pluginCore.RecordTypeA
}

var hnsDomainRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

func (p *HNSProvider) Validate(domain string) error {
	domain = strings.TrimSuffix(domain, "/")
	domain = dnsname.TrimDot(domain)
	domain = strings.ToLower(domain)
	if domain == "" {
		return fmt.Errorf("domain is required")
	}
	// An HNS domain may be a TLD (single label, e.g. "altroot") or a
	// subdomain under a TLD (e.g. "blog.altroot"). Each label must be
	// DNS-compliant and within the 63-character limit.
	labels := strings.Split(domain, ".")
	for _, label := range labels {
		if label == "" {
			return fmt.Errorf("HNS domain contains empty label")
		}
		if len(label) > 63 {
			return fmt.Errorf("HNS domain label exceeds 63 characters")
		}
		if !hnsDomainRe.MatchString(label) {
			return fmt.Errorf("HNS domain contains invalid characters")
		}
	}
	// A name whose final label is an IANA ICANN TLD belongs to the ICANN
	// namespace, never HNS. The IANA list is the authoritative decision
	// procedure, so HNS excludes ICANN-suffixed names (e.g. "example.com")
	// even though its labels are otherwise DNS-compliant.
	if tlds.IsICANN(domain) {
		return fmt.Errorf("%q ends in an ICANN TLD; use the ICANN namespace", domain)
	}
	return nil
}

func (p *HNSProvider) BuildDelegation(ctx context.Context, zoneID uint,
	domain string, website *pluginDb.Website, config json.RawMessage) (any, error) {

	zoneName := dnsname.EnsureFQDN(domain)

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

	// Ensure DNSSEC is enabled on the zone so it is signed and has a key to
	// derive the DS from. This zone is either the claim's own apex zone or, for
	// a platform subdomain, the shared operator platform-root zone that all of
	// that root's claims live in (see resolveManagedZone). In both cases the
	// zone must be DNSSEC-active for DANE/TLSA and DS verification to work.
	//
	// This call is idempotent self-heal, not per-claim key minting: EnableDNSSEC
	// reuses the zone's existing active signing key, so a platform claim never
	// rolls the root's key or introduces a new keypair of its own. It exists
	// mainly to recover a zone that an ops error left unsigned. The DS itself is
	// NOT persisted in the delegation bundle — it is derived live from PowerDNS
	// on demand (see GetActiveDNSSECDS), so it can never go stale on key
	// rotation.
	if p.dnsSvc != nil {
		if err := p.enableDNSSEC(ctx, zoneID); err != nil {
			return nil, fmt.Errorf("dnssec enablement failed: %w", err)
		}
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

	return bundle, nil
}

// enableDNSSEC ensures DNSSEC is enabled on the zone via PowerDNS. It is
// idempotent: PowerDNS reuses the zone's existing active signing key, so a
// claim against an already-signed zone (including a platform subdomain
// reusing the operator's platform-root zone) costs no new key material and
// does not roll the root's key. It exists so fresh delegation guarantees a
// signed zone and to self-heal a zone that an ops error left unsigned. The DS
// is deliberately NOT computed or stored here: the portal keeps the
// delegation bundle free of the derived DS, which is instead computed live
// from PowerDNS on demand.
func (p *HNSProvider) enableDNSSEC(ctx context.Context, zoneID uint) error {
	if _, err := p.dnsSvc.EnableDNSSEC(ctx, zoneID); err != nil {
		return fmt.Errorf("enable dnssec: %w", err)
	}
	return nil
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
		if strings.HasSuffix(dnsname.TrimDot(cfg.NameserverHost), dnsname.TrimDot(zoneName)) {
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
	}, nil
}

func (p *HNSProvider) buildTLSA(ctx context.Context, zoneName string) (string, error) {
	domain := dnsname.TrimDot(zoneName)

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

// LiveNameservers returns the NS records currently served for the HNS
// domain, resolved against the configured HNS-aware resolver. HNS names are
// visible only to an HNS-aware resolver (e.g. an hsd instance), not the
// system default resolver. Uses the same raw miekg/dns query path as
// VerifyDelegation so HSD referrals in the authority section are honored.
func (p *HNSProvider) LiveNameservers(ctx context.Context, domain string) ([]string, error) {
	if p.resolverAddr == "" {
		return nil, fmt.Errorf("HNS resolver not configured (DnsConfig.HNSResolver); standard resolvers cannot resolve HNS names")
	}
	nss, err := queryNS(ctx, p.resolverAddr, dnsname.EnsureFQDN(domain))
	if err != nil {
		return nil, err
	}
	return nss, nil
}

// VerifyDelegation checks whether a HNS name's delegation is live via the
// configured HNS resolver. HNS names are resolved against the Handshake
// blockchain; this requires an HNS-aware resolver (not the system default).
//
// For platform-managed zones the portal generated (and stored) the DS record
// from the zone's PowerDNS DNSKEY. Such a zone is only considered live when
// BOTH the expected NS delegation is visible AND the DS the portal generated
// is actually served by the parent zone (i.e. the root has taken effect).
// Requiring the DS prevents a domain from being marked Active before the
// DNSSEC chain of trust is in place. Self-managed zones (no DS the portal
// generated) are validated on NS visibility alone, since only the name owner
// knows their own DNSKEY/DS.
func (p *HNSProvider) VerifyDelegation(ctx context.Context, domain string,
	expectedDS string) (bool, error) {

	if p.resolverAddr == "" {
		return false, fmt.Errorf("HNS resolver not configured (DnsConfig.HNSResolver); standard resolvers cannot resolve HNS names")
	}

	expectedNS := p.Nameservers()

	nss, err := queryNS(ctx, p.resolverAddr, dnsname.EnsureFQDN(domain))
	if err != nil {
		// We query the configured HNS resolver with raw miekg/dns rather than
		// Go's net.Resolver.LookupNS: HSD answers NS queries as a REFERRAL, with
		// the NS records in the authority section (not the answer). Go's LookupNS
		// only parses the answer section, so it sees "no NS records" and reports
		// a spurious NXDOMAIN even though the resolver has the delegation. Raw
		// miekg/dns reads both sections and returns the records correctly.
		return false, fmt.Errorf("HNS resolver query failed (resolver %q): %w", p.resolverAddr, err)
	}

	nsVisible := false
	for _, ns := range nss {
		for _, expected := range expectedNS {
			if dnsname.Equal(ns, expected) {
				nsVisible = true
				break
			}
		}
		if nsVisible {
			break
		}
	}
	if !nsVisible {
		return false, nil
	}

	// Platform-generated DS: when the caller supplies the live DS the portal
	// computed for the PowerDNS-signed zone (see GetActiveDNSSECDS), require
	// that exact DS to be served by the parent zone before considering the
	// delegation live. An empty expectedDS means a self-managed zone (the name
	// owner publishes their own DNSKEY/DS, which the portal cannot know), so NS
	// visibility is sufficient.
	if expectedDS == "" {
		// Self-managed zone: the name owner publishes a DS from their own
		// DNSKEY, which the portal cannot know. NS visibility is sufficient.
		return true, nil
	}

	servedDS, err := queryDS(ctx, p.resolverAddr, dnsname.EnsureFQDN(domain))
	if err != nil {
		// A failed DS query means the DS is not yet resolvable from the parent
		// zone; treat the delegation as not-yet-live (DNSSEC chain not in
		// place) rather than an error the caller surfaces as a soft failure.
		return false, nil
	}
	for _, served := range servedDS {
		if dsEqual(served, expectedDS) {
			return true, nil
		}
	}
	return false, nil
}

// dsEqual compares two DS RDATA presentation strings ignoring the leading
// owner-name/token prefix and collapsing whitespace.
func dsEqual(a, b string) bool {
	return canonicalDS(a) == canonicalDS(b)
}

func canonicalDS(s string) string {
	if idx := strings.Index(s, " DS "); idx >= 0 {
		s = s[idx+len(" DS "):]
	}
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}

// queryNS performs an NS query for the absolute FQDN `name` against the DNS
// server at `addr` using raw miekg/dns, returning the nameserver hostnames from
// both the answer and authority sections. This is the fix for Go's
// net.Resolver.LookupNS, which only ever parses the ANSWER section. HSD (and
// alt-root resolvers in general) answer NS queries as a referral, placing the
// NS records in the AUTHORITY section; net.Resolver.LookupNS therefore reads
// the answer as empty and returns a spurious NXDOMAIN even when the delegation
// exists. miekg/dns reads both sections, so this returns the delegation
// correctly. It tries UDP first, then TCP on failure or truncation (RFC 5966).
func queryNS(ctx context.Context, addr, name string) ([]string, error) {
	r, err := queryResolver(ctx, addr, name, dns.TypeNS)
	if err != nil {
		return nil, err
	}
	var nss []string
	for _, rr := range r.Answer {
		if ns, ok := rr.(*dns.NS); ok {
			nss = append(nss, ns.Ns)
		}
	}
	// NS records may also appear in the authority section for non-authoritative
	// responses; include them rather than only the answer section.
	for _, rr := range r.Ns {
		if ns, ok := rr.(*dns.NS); ok {
			nss = append(nss, ns.Ns)
		}
	}
	return nss, nil
}

// queryDS performs a DS query for the absolute FQDN `name` against the DNS
// server at `addr` using raw miekg/dns, returning the DS RDATA strings served
// by the parent zone (answer and authority sections). This is how
// VerifyDelegation confirms the platform-generated DS has propagated: a DS
// only appears once the parent zone (HSD root for HNS) actually serves it.
func queryDS(ctx context.Context, addr, name string) ([]string, error) {
	r, err := queryResolver(ctx, addr, name, dns.TypeDS)
	if err != nil {
		return nil, err
	}
	var dss []string
	for _, rr := range append(append([]dns.RR{}, r.Answer...), r.Ns...) {
		if ds, ok := rr.(*dns.DS); ok {
			// RDATA presentation (RFC 4034 §5.3): key tag, algorithm, digest
			// type, digest. Built from the structured fields so it matches the
			// persisted dane-computed DS RDATA, with a lowercased digest so the
			// comparison is not tripped by miekg's uppercase String() hex.
			dss = append(dss, strings.Join([]string{
				strconv.Itoa(int(ds.KeyTag)),
				strconv.Itoa(int(ds.Algorithm)),
				strconv.Itoa(int(ds.DigestType)),
				strings.ToLower(ds.Digest),
			}, " "))
		}
	}
	return dss, nil
}

// queryResolver issues a single DNS query of the given record type against the
// server at `addr` and returns the full reply, handling the UDP-then-TCP
// fallback on failure or truncation (RFC 5966) shared by all resolver queries.
// If the TCP retry fails we must not fall through to a truncated UDP response,
// which would silently drop records that didn't fit in the UDP packet.
func queryResolver(ctx context.Context, addr, name string, qtype uint16) (*dns.Msg, error) {
	c := &dns.Client{
		Net:     "udp",
		Timeout: 5 * time.Second,
	}
	m := new(dns.Msg)
	m.SetQuestion(name, qtype)

	r, _, err := c.ExchangeContext(ctx, m, addr)
	if err == nil && r.Truncated {
		tc := &dns.Client{Net: "tcp", Timeout: 5 * time.Second}
		tr, _, terr := tc.ExchangeContext(ctx, m, addr)
		if terr != nil {
			return nil, fmt.Errorf("TCP retry after UDP truncation failed: %w", terr)
		}
		r, err = tr, nil
	}
	if err != nil {
		return nil, err
	}
	if r.Rcode != dns.RcodeSuccess {
		return nil, &net.DNSError{
			Err:        dns.RcodeToString[r.Rcode],
			Name:       name,
			Server:     addr,
			IsNotFound: r.Rcode == dns.RcodeNameError,
		}
	}
	return r, nil
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

// UsesManagedZoneTLSA reports that HNS uses DANE and that its TLSA must be
// published into the portal-managed authoritative PowerDNS zone.
func (p *HNSProvider) UsesManagedZoneTLSA() bool {
	return true
}

// RequiresDNSSEC reports that HNS confirms delegation against a live DS served
// by the alt-root parent before marking the domain Active (managed-DNSSEC).
func (p *HNSProvider) RequiresDNSSEC() bool {
	return true
}
