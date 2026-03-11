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

	// Gateway domain for ALIAS records (auto-wiring)
	GatewayDomain string `config:"gateway_domain"`

	// Nameserver validation job configuration
	NameserverValidationInterval time.Duration `config:"nameserver_validation_interval"`
}

func (c DnsConfig) Defaults() map[string]any {
	return map[string]any{
		"Enabled":                       false,
		"PowerDNSAPIURL":                "",
		"PowerDNSAPIKey":                "",
		"Nameservers":                   []string{},
		"GatewayDomain":                 "",
		"NameserverValidationInterval":  5 * time.Minute,
	}
}
