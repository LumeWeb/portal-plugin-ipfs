package dns

import (
	"fmt"
	"net"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// DNSLookup defines the interface for DNS lookup operations
type DNSLookup interface {
	// LookupNS returns the nameservers for the given domain
	LookupNS(domain string) ([]*net.NS, error)
}

// DefaultDNSLookup is the default implementation using net.LookupNS
type DefaultDNSLookup struct{}

// LookupNS performs actual DNS lookup using net.LookupNS
func (d *DefaultDNSLookup) LookupNS(domain string) ([]*net.NS, error) {
	return net.LookupNS(domain)
}

// DNSServiceOptions holds configuration options for DNSService
type DNSServiceOptions struct {
	// PowerDNSClient is the PowerDNS client (for testing)
	PowerDNSClient *PowerDNSClient
	// DNSLookup is the DNS lookup implementation (for testing)
	DNSLookup DNSLookup
}

// DNSServiceOption is a function that configures DNSServiceOptions
type DNSServiceOption func(*DNSServiceOptions)

// WithPowerDNSClient sets the PowerDNS client for the DNS service (for testing)
func WithPowerDNSClient(client *PowerDNSClient) DNSServiceOption {
	return func(opts *DNSServiceOptions) {
		opts.PowerDNSClient = client
	}
}

// WithDNSLookup sets the DNS lookup implementation for the DNS service (for testing)
func WithDNSLookup(lookup DNSLookup) DNSServiceOption {
	return func(opts *DNSServiceOptions) {
		opts.DNSLookup = lookup
	}
}

var _ pluginCore.DNSService = (*DNSServiceDefault)(nil)

// DNSServiceDefault manages DNS zones and PowerDNS integration
type DNSServiceDefault struct {
	*core.BaseComponent
	config     *pluginConfig.DnsConfig
	pdnsClient *PowerDNSClient
	dnsLookup  DNSLookup
}

// GetDNSLookup returns the DNS lookup implementation (for testing)
func (s *DNSServiceDefault) GetDNSLookup() DNSLookup {
	return s.dnsLookup
}

// SetDNSLookup sets the DNS lookup implementation (for testing)
func (s *DNSServiceDefault) SetDNSLookup(lookup DNSLookup) {
	s.dnsLookup = lookup
}

// NewDNSService creates a new DNS service
func NewDNSService() (core.Service, []core.ContextBuilderOption, error) {
	return NewDNSServiceWithOptions()
}

// NewDNSServiceWithOptions creates a new DNS service with configurable options
func NewDNSServiceWithOptions(options ...DNSServiceOption) (core.Service, []core.ContextBuilderOption, error) {
	svc := &DNSServiceDefault{BaseComponent: &core.BaseComponent{}}

	// Apply options to default struct
	serviceOpts := &DNSServiceOptions{
		DNSLookup: &DefaultDNSLookup{}, // Default to real DNS lookups
	}
	for _, option := range options {
		option(serviceOpts)
	}
	svc.dnsLookup = serviceOpts.DNSLookup

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			// Load configuration from service config
			svc.config = core.GetServiceConfig[*pluginConfig.DnsConfig](ctx, pluginCore.DNS_SERVICE)
			if svc.config == nil {
				svc.Logger().Warn("DNS service config not found")
				return nil
			}

			// Initialize PowerDNS client from config if not provided via options
			if serviceOpts.PowerDNSClient == nil && svc.config.Enabled {
				if svc.config.PowerDNSAPIURL == "" || svc.config.PowerDNSAPIKey == "" {
					svc.Logger().Warn("DNS hosting enabled but PowerDNS API URL or key not configured")
					return nil
				}

				pdnsClient, err := NewPowerDNSClient(
					svc.config.PowerDNSAPIURL,
					svc.config.PowerDNSAPIKey,
					svc.Logger(),
				)
				if err != nil {
					return fmt.Errorf("failed to create PowerDNS client: %w", err)
				}
				svc.pdnsClient = pdnsClient

				svc.Logger().Info("DNS service initialized with PowerDNS",
					zap.String("api_url", svc.config.PowerDNSAPIURL))
			} else if serviceOpts.PowerDNSClient != nil {
				svc.pdnsClient = serviceOpts.PowerDNSClient
				svc.Logger().Info("DNS service initialized with provided PowerDNS client")
			} else {
				svc.Logger().Info("DNS hosting disabled, PowerDNS client not initialized")
			}

			return nil
		}),
	)

	return svc, opts, nil
}

func (s *DNSServiceDefault) ID() string {
	return pluginCore.DNS_SERVICE
}

func (s *DNSServiceDefault) GetConfig() (any, error) {
	return &pluginConfig.DnsConfig{}, nil
}
