package config

import (
	"time"

	"go.lumeweb.com/portal/config"
)

var _ config.Defaults = (*Config)(nil)

// Config contains the configuration for the DNS hosting feature
type Config struct {
	// DNS hosting enabled/disabled
	Enabled bool `config:"dns_hosting_enabled"`

	// PowerDNS configuration
	PowerDNSAPIURL string `config:"powerdns_api_url"`
	PowerDNSAPIKey string `config:"powerdns_api_key"`

	// Approved nameservers for validation
	Nameservers []string `config:"nameservers"`

	// Nameserver validation job configuration
	NameserverValidationInterval time.Duration `config:"nameserver_validation_interval"`
}

func (c Config) Defaults() map[string]any {
	return map[string]any{
		"Enabled":             false,
		"PowerDNSAPIURL":                "",
		"PowerDNSAPIKey":                "",
		"Nameservers":                   []string{},
		"NameserverValidationInterval":  5 * time.Minute,
	}
}
