package dns

import (
	"fmt"
	"net"

	"go.lumeweb.com/portal/core"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
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

// DNSService manages DNS zones and PowerDNS integration
type DNSService struct {
	*core.BaseComponent
	config     pluginConfig.Config
	pdnsClient *PowerDNSClient
	dnsLookup  DNSLookup
}

// GetDNSLookup returns the DNS lookup implementation (for testing)
func (s *DNSService) GetDNSLookup() DNSLookup {
	return s.dnsLookup
}

// SetDNSLookup sets the DNS lookup implementation (for testing)
func (s *DNSService) SetDNSLookup(lookup DNSLookup) {
	s.dnsLookup = lookup
}

// NewDNSService creates a new DNS service
func NewDNSService() (core.Service, []core.ContextBuilderOption, error) {
	return NewDNSServiceWithOptions()
}

// NewDNSServiceWithOptions creates a new DNS service with configurable options
func NewDNSServiceWithOptions(options ...DNSServiceOption) (core.Service, []core.ContextBuilderOption, error) {
	svc := &DNSService{BaseComponent: &core.BaseComponent{}}

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
			// Load configuration
			protocolConfig := core.GetProtocolConfig[*pluginConfig.ProtocolConfig](ctx, internal.ProtocolName)
			if protocolConfig == nil {
				svc.Logger().Warn("Protocol config not found")
				return nil
			}
			svc.config = protocolConfig.DnsHosting

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

func (s *DNSService) ID() string {
	return pluginCore.DNS_SERVICE
}


