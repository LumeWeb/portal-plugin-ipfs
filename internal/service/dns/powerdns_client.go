package dns

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/dns/powerdns"
	"go.uber.org/zap"
)

// Default PowerDNS server ID for single-server deployments
const defaultServerID = "localhost"

// PowerDNSClient wraps the generated PowerDNS client
type PowerDNSClient struct {
	client *powerdns.Client
	logger *core.Logger
}

// NewPowerDNSClient creates a new PowerDNS client wrapper
func NewPowerDNSClient(baseURL, apiKey string, logger *core.Logger) (*PowerDNSClient, error) {
	pdnsClient, err := powerdns.NewClient(baseURL, powerdns.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		req.Header.Set("X-API-Key", apiKey)
		return nil
	}))
	if err != nil {
		return nil, err
	}

	return &PowerDNSClient{
		client: pdnsClient,
		logger: logger,
	}, nil
}

// handleResponse processes HTTP response, checking status code and decoding JSON
// It safely closes the response body and returns an error if the status code is not 2xx
func handleResponse[T any](resp *http.Response) (*T, error) {
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		var zero T
		return &zero, fmt.Errorf("PowerDNS API returned status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var result T
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		var zero T
		return &zero, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// CreateZone creates a new zone in PowerDNS
func (c *PowerDNSClient) CreateZone(ctx context.Context, domain string, nameservers []string) (*powerdns.Zone, error) {
	// PowerDNS requires canonical zone names with trailing dots
	canonicalDomain := strings.TrimSuffix(domain, ".") + "."

	// Normalize nameservers to canonical form (with trailing dots)
	canonicalNameservers := make([]string, len(nameservers))
	for i, ns := range nameservers {
		canonicalNameservers[i] = strings.TrimSuffix(ns, ".") + "."
	}

	c.logger.Debug("Creating zone in PowerDNS",
		zap.String("domain", domain),
		zap.String("canonical_domain", canonicalDomain),
		zap.Strings("nameservers", nameservers),
		zap.Strings("canonical_nameservers", canonicalNameservers))

	zoneCreate := powerdns.ZoneCreate{
		Name:        canonicalDomain,
		Nameservers: &canonicalNameservers,
	}

	kind := powerdns.ZoneCreateKindNative
	zoneCreate.Kind = &kind

	resp, err := c.client.CreateZone(ctx, defaultServerID, zoneCreate)
	if err != nil {
		return nil, fmt.Errorf("failed to create zone: %w", err)
	}

	zone, err := handleResponse[powerdns.Zone](resp)
	if err != nil {
		return nil, err
	}

	if zone.Id == nil {
		return nil, fmt.Errorf("powerdns API returned zone with no ID for domain %q", domain)
	}

	c.logger.Info("Zone created in PowerDNS",
		zap.String("domain", domain),
		zap.String("zone_id", *zone.Id))

	return zone, nil
}

// GetZone retrieves a zone from PowerDNS
func (c *PowerDNSClient) GetZone(ctx context.Context, zoneID string) (*powerdns.Zone, error) {
	resp, err := c.client.GetZone(ctx, defaultServerID, zoneID)
	if err != nil {
		return nil, fmt.Errorf("failed to get zone: %w", err)
	}

	return handleResponse[powerdns.Zone](resp)
}

// UpdateZoneRRSets updates RRsets in a zone
func (c *PowerDNSClient) UpdateZoneRRSets(ctx context.Context, zoneID string, rrsets []powerdns.RRSet) error {
	zonePatch := powerdns.ZonePatch{
		Rrsets: &rrsets,
	}

	resp, err := c.client.UpdateZoneRRSets(ctx, defaultServerID, zoneID, zonePatch)
	if err != nil {
		return fmt.Errorf("failed to update zone: %w", err)
	}
	if resp != nil {
		defer resp.Body.Close()
	}

	c.logger.Info("Zone RRsets updated in PowerDNS",
		zap.String("zone_id", zoneID),
		zap.Int("rrsets_count", len(rrsets)))

	return nil
}

// DeleteZone deletes a zone from PowerDNS
func (c *PowerDNSClient) DeleteZone(ctx context.Context, zoneID string) error {
	resp, err := c.client.DeleteZone(ctx, defaultServerID, zoneID)
	if err != nil {
		return fmt.Errorf("failed to delete zone: %w", err)
	}
	if resp != nil {
		defer resp.Body.Close()
	}

	c.logger.Info("Zone deleted from PowerDNS",
		zap.String("zone_id", zoneID))

	return nil
}
