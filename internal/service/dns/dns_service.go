package dns

import (
	"context"
	"fmt"
	"net"

	"gorm.io/gorm"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
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

			sanitized := pluginConfig.SanitizeDNSLabel(svc.config.VerificationTokenKey)
			if sanitized != svc.config.VerificationTokenKey {
				svc.Logger().Warn("verification_token_key sanitized for DNS compatibility",
					zap.String("original", svc.config.VerificationTokenKey),
					zap.String("sanitized", sanitized))
				svc.config.VerificationTokenKey = sanitized
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

// CreateDNSLinkRecord creates a DNSLink TXT record in a zone (adapter for domain.DNSZoneService)
func (s *DNSServiceDefault) CreateDNSLinkRecord(ctx context.Context, zoneID uint, target string) error {
	_, err := s.CreateRecord(ctx, zoneID, "_dnslink", "TXT", "dnslink="+target, 300)
	return err
}

// CreateApexRecord creates the apex (root) record in a zone (adapter for
// domain.DNSZoneService). content is raw: an IP for A, a gateway hostname for
// ALIAS/CNAME.
func (s *DNSServiceDefault) CreateApexRecord(ctx context.Context, zoneID uint, recordType pluginCore.RecordType, content string) error {
	_, err := s.CreateRecord(ctx, zoneID, "", string(recordType), content, 300)
	return err
}

// SetTLSARecord writes (or replaces) the DANE TLSA record for a zone's
// HTTPS/TCP owner `_443._tcp` in the portal-managed authoritative zone
// (adapter for domain.DNSZoneService). content is the TLSA rdata, e.g.
// "3 1 1 <sha256hex>". Publishing this is what lets DANE validators resolve the
// TLSA against PowerDNS; without it authoritative queries return NXDOMAIN.
func (s *DNSServiceDefault) SetTLSARecord(ctx context.Context, zoneID uint, content string) error {
	_, err := s.CreateRecord(ctx, zoneID, "_443._tcp", "TLSA", content, 300)
	return err
}

// EnableDNSSEC enables DNSSEC on a zone and returns the active signing key's
// DNSKEY record content.
//
// It validates the DNSZone row in a short transaction (releasing any lock
// immediately), then delegates the actual cryptokey list+create to the PowerDNS
// client, which is idempotent (reuses an existing active signing key) and
// serializes the list+create window per zone with an in-process mutex.
//
// The DB transaction is deliberately NOT held across the PowerDNS calls: those
// can take up to 2x the client timeout (30s each) of external network I/O, and
// holding a write lock for that long would block same-zone DB writes and risk
// exhausting the connection pool during bulk delegation. Correctness of the
// "don't mint a second key" guarantee comes from the client's idempotent
// list-then-reuse logic, not from a long-lived DB lock.
func (s *DNSServiceDefault) EnableDNSSEC(ctx context.Context, zoneID uint) (string, error) {
	if s.pdnsClient == nil {
		return "", fmt.Errorf("PowerDNS client not configured")
	}

	// Short transaction: fetch + validate the zone row and its PowerDNS zone ID,
	// then release before any external network I/O.
	var pdnsZoneID string
	txErr := s.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var zone pluginDb.DNSZone
		if err := tx.First(&zone, zoneID).Error; err != nil {
			return err // includes gorm.ErrRecordNotFound
		}
		if zone.PowerDNSZoneID == "" {
			return fmt.Errorf("zone %d has no PowerDNS zone ID", zoneID)
		}
		pdnsZoneID = zone.PowerDNSZoneID
		return nil
	})
	if txErr != nil {
		return "", txErr
	}

	dnskey, err := s.pdnsClient.EnableDNSSEC(ctx, pdnsZoneID)
	if err != nil {
		s.Logger().Error("Failed to enable DNSSEC",
			zap.Uint("zone_id", zoneID),
			zap.String("pdns_zone_id", pdnsZoneID),
			zap.Error(err))
		return "", fmt.Errorf("enable DNSSEC: %w", err)
	}

	s.Logger().Info("DNSSEC enabled",
		zap.Uint("zone_id", zoneID))

	return dnskey, nil
}

// GetActiveDNSSECDS returns the SHA-256 DS RDATA (type 2) for the zone's
// currently-active signing key, computed live from PowerDNS cryptokey state.
// It is the on-the-fly source of the DS to display in dns-requirements and to
// verify on-chain, so no DS is persisted in the portal DB (a stored DS would
// go stale on key rotation). Returns ("", nil) when the zone has no active
// signing key; errors when multiple active keys exist (in-progress rollover).
func (s *DNSServiceDefault) GetActiveDNSSECDS(ctx context.Context, zoneID uint) (string, error) {
	if s.pdnsClient == nil {
		return "", fmt.Errorf("PowerDNS client not configured")
	}

	var pdnsZoneID string
	txErr := s.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var zone pluginDb.DNSZone
		if err := tx.First(&zone, zoneID).Error; err != nil {
			return err
		}
		if zone.PowerDNSZoneID == "" {
			return fmt.Errorf("zone %d has no PowerDNS zone ID", zoneID)
		}
		pdnsZoneID = zone.PowerDNSZoneID
		return nil
	})
	if txErr != nil {
		return "", txErr
	}

	ds, err := s.pdnsClient.GetActiveDNSKEYDS(ctx, pdnsZoneID)
	if err != nil {
		return "", fmt.Errorf("get active DNSKEY DS: %w", err)
	}
	return ds, nil
}
