package config

import (
	"time"

	"go.lumeweb.com/portal/config"
)

var _ config.Defaults = (*DnsConfig)(nil)

// DnsConfig contains the configuration for the DNS hosting feature
type DnsConfig struct {
	// DNS hosting enabled/disabled
	Enabled bool `config:"enabled"`

	// PowerDNS configuration
	// NOTE: SOA serial propagation for API-managed zones works out of the box —
	// PowerDNS sets SOA-EDIT-API metadata to DEFAULT for API-created zones, which
	// auto-increments the serial on RRset edits so secondaries re-transfer. No
	// SOA-EDIT config is required. (Per-zone SOA-EDIT/SOA-EDIT-API are domain
	// metadata, not pdns.conf settings.)
	PowerDNSAPIURL string `config:"powerdns_api_url"`
	PowerDNSAPIKey string `config:"powerdns_api_key"`

	// Approved nameservers for validation
	Nameservers []string `config:"nameservers"`

	// HNS nameservers for the HNS namespace delegation/validation.
	// Alt-root namespaces (e.g. HNS) must delegate to nameservers that are
	// themselves members of that namespace, which differ from the ICANN
	// nameservers in Nameservers. Provided by the operator, not the user.
	HNSNameservers []string `config:"hns_nameservers"`

	// HNSResolver is the address (host:port) of an HNS-aware DNS resolver
	// used for HNS namespace:
	// - Delegation verification (NS lookup via HNSProvider)
	// - Full DNS validation: DNSLink + TXT token (via LiveResolver selection)
	// Different roots require different resolvers because alt-roots are not
	// visible to the system default resolver.
	HNSResolver string `config:"hns_resolver"`

	// Gateway domain for ALIAS records (auto-wiring)
	GatewayDomain string `config:"gateway_domain"`

	// GatewayIP is the IP address to publish as the apex A record for
	// DNSSEC-signed alt-root (e.g. HNS) zones. Alt-root apexes must be real
	// A records (not ALIAS) so they carry an RRSIG; PowerDNS cannot sign a
	// synthetic ALIAS at the apex. This should match where GatewayDomain
	// currently resolves. Keep it in sync when the gateway IP changes.
	GatewayIP string `config:"gateway_ip"`

	// Verification token key used as the subdomain label for validation TXT records
	VerificationTokenKey string `config:"verification_token_key"`

	// HIP5BlockedTLDs lists TLDs that are blocked on the HNS root and therefore
	// mark an NS record as a HIP-5 TX record (e.g. "eth", "bit"), on top of
	// underscore-prefixed labels which are always treated as HIP-5. Empty means
	// only underscore-prefixed protocol tags count.
	HIP5BlockedTLDs []string `config:"hip5_blocked_tlds"`

	// DANEKeyEncryptionKey is the base64-encoded 32-byte AES-256 key used to
	// encrypt per-domain DANE private keys at rest in the portal DB. Must be 32
	// bytes when base64-decoded. If empty, DANE key persistence is skipped.
	DANEKeyEncryptionKey string `config:"dane_key_encryption_key"`

	// Nameserver validation job configuration
	NameserverValidationInterval time.Duration `config:"nameserver_validation_interval"`
}

func (c DnsConfig) Defaults() map[string]any {
	return map[string]any{
		"Enabled":                      false,
		"PowerDNSAPIURL":               "",
		"PowerDNSAPIKey":               "",
		"Nameservers":                  []string{},
		"HNSNameservers":               []string{},
		"HNSResolver":                  "",
		"HIP5BlockedTLDs":              []string{"eth", "bit"},
		"GatewayDomain":                "",
		"GatewayIP":                    "",
		"VerificationTokenKey":         "lumeweb-verify",
		"DANEKeyEncryptionKey":         "",
		"NameserverValidationInterval": 5 * time.Minute,
	}
}
