package dns

import (
	"fmt"

	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.uber.org/zap"
)

const (
	DNS_SERVICE = "dns_service"
)

// DNSServiceOptions holds configuration options for DNSService
type DNSServiceOptions struct {
	// PowerDNSClient is the PowerDNS client (for testing)
	PowerDNSClient *PowerDNSClient
}

// DNSServiceOption is a function that configures DNSServiceOptions
type DNSServiceOption func(*DNSServiceOptions)

// WithPowerDNSClient sets the PowerDNS client for the DNS service (for testing)
func WithPowerDNSClient(client *PowerDNSClient) DNSServiceOption {
	return func(opts *DNSServiceOptions) {
		opts.PowerDNSClient = client
	}
}

// DNSService manages DNS zones and PowerDNS integration
type DNSService struct {
	*core.BaseComponent
	config     pluginConfig.Config
	pdnsClient *PowerDNSClient
}

// NewDNSService creates a new DNS service
func NewDNSService() (core.Service, []core.ContextBuilderOption, error) {
	return NewDNSServiceWithOptions()
}

// NewDNSServiceWithOptions creates a new DNS service with configurable options
func NewDNSServiceWithOptions(options ...DNSServiceOption) (core.Service, []core.ContextBuilderOption, error) {
	svc := &DNSService{BaseComponent: &core.BaseComponent{}}

	// Apply options to default struct
	serviceOpts := &DNSServiceOptions{}
	for _, option := range options {
		option(serviceOpts)
	}

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
	return DNS_SERVICE
}

// SetPowerDNSClient sets the PowerDNS client (for testing)
func (s *DNSService) SetPowerDNSClient(client *PowerDNSClient) {
	s.pdnsClient = client
}
