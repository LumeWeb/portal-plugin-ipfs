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

	// Verification token key used as the subdomain label for validation TXT records
	VerificationTokenKey string `config:"verification_token_key"`

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
		"GatewayDomain":                "",
		"VerificationTokenKey":         "lumeweb-verify",
		"NameserverValidationInterval": 5 * time.Minute,
	}
}
