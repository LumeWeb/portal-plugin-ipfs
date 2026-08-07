package dns

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.lumeweb.com/ipfs-sdk/dnsname"
	"go.lumeweb.com/portal-plugin-ipfs/internal/dns/powerdns"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// Default PowerDNS server ID for single-server deployments
const defaultServerID = "localhost"

// PowerDNSClient wraps the generated PowerDNS client
type PowerDNSClient struct {
	client  *powerdns.Client
	logger  *core.Logger
	baseURL string
	apiKey  string

	// zoneLocks serializes the list-then-create window in EnableDNSSEC per zone
	// so two concurrent delegation builds cannot both POST a new KSK (TOCTOU).
	zoneLockMu sync.Mutex
	zoneLocks  map[string]*sync.Mutex
}

// zoneMu returns the per-zone mutex for a normalized zone id, creating it on
// first use. The map is never cleaned up; the number of distinct delegated
// zones is small and bounded by the deploy's domain count, so this is fine.
func (c *PowerDNSClient) zoneMu(zoneID string) *sync.Mutex {
	c.zoneLockMu.Lock()
	defer c.zoneLockMu.Unlock()
	if c.zoneLocks == nil {
		c.zoneLocks = make(map[string]*sync.Mutex)
	}
	mu, ok := c.zoneLocks[zoneID]
	if !ok {
		mu = &sync.Mutex{}
		c.zoneLocks[zoneID] = mu
	}
	return mu
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
		client:  pdnsClient,
		logger:  logger,
		baseURL: baseURL,
		apiKey:  apiKey,
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

// CreateZone creates a new zone in PowerDNS, or returns the existing zone if it already exists (409)
func (c *PowerDNSClient) CreateZone(ctx context.Context, domain string, nameservers []string) (*powerdns.Zone, error) {
	canonicalDomain := dnsname.EnsureFQDN(domain)

	canonicalNameservers := make([]string, len(nameservers))
	for i, ns := range nameservers {
		canonicalNameservers[i] = dnsname.EnsureFQDN(ns)
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
		if strings.Contains(err.Error(), "status 409") {
			c.logger.Info("Zone already exists in PowerDNS, fetching existing zone",
				zap.String("domain", domain))

			existingZone, getErr := c.GetZone(ctx, canonicalDomain)
			if getErr != nil {
				return nil, fmt.Errorf("zone already exists but failed to fetch it: %w", getErr)
			}
			if existingZone.Id == nil {
				return nil, fmt.Errorf("existing zone has no ID for domain %q", domain)
			}

			return existingZone, nil
		}
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
	if resp == nil {
		return fmt.Errorf("failed to update zone: nil response from PowerDNS")
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		c.logger.Warn("PowerDNS returned non-success status",
			zap.Int("status", resp.StatusCode),
			zap.String("zone_id", zoneID),
			zap.String("body", string(bodyBytes)))
		return fmt.Errorf("PowerDNS returned status %d", resp.StatusCode)
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
	if resp == nil {
		return fmt.Errorf("failed to delete zone: nil response from PowerDNS")
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		c.logger.Warn("PowerDNS returned non-success status",
			zap.Int("status", resp.StatusCode),
			zap.String("zone_id", zoneID),
			zap.String("body", string(bodyBytes)))
		return fmt.Errorf("PowerDNS returned status %d", resp.StatusCode)
	}

	c.logger.Info("Zone deleted from PowerDNS",
		zap.String("zone_id", zoneID))

	return nil
}

// EnableDNSSEC ensures a zone has DNSSEC enabled and returns the active KSK's
// DNSKEY record content.
//
// It is IDEMPOTENT: it lists the zone's existing cryptokeys and reuses an
// existing active KSK rather than POSTing a new one. Creating a fresh KSK on
// every call would rotate the zone's DNSSEC key each time delegation is built,
// stranding the previously-published DS (DNSSEC keys must not cycle). A new KSK
// is only created when the zone has no active KSK yet.
//
// The list-then-create window is serialized per zone (zoneMu) so concurrent
// delegation builds for the same zone cannot both observe "no active KSK" and
// both POST, which would mint two keys and reintroduce key cycling.
func (c *PowerDNSClient) EnableDNSSEC(ctx context.Context, zoneID string) (string, error) {
	// PowerDNS cryptokey API:
	//   GET  /api/v1/servers/:server/zones/:zone/cryptokeys  (list)
	//   POST /api/v1/servers/:server/zones/:zone/cryptokeys  (create)

	parts := strings.Split(zoneID, "/")
	apiZoneID := parts[len(parts)-1]
	if apiZoneID == "" {
		return "", fmt.Errorf("invalid zone ID: %s", zoneID)
	}

	// Serialize list+create for this zone so two concurrent builds can't both POST.
	zoneMu := c.zoneMu(apiZoneID)
	zoneMu.Lock()
	defer zoneMu.Unlock()

	base := strings.TrimSuffix(c.baseURL, "/")

	// 1) Reuse an existing active signing key if the zone already has DNSSEC
	// enabled. Match both "ksk" (split KSK/ZSK) and "csk" (Combined Signing Key)
	// modes so a CSK-configured zone is never handed a fresh key on re-delegation.
	existing, err := c.listCryptokeys(ctx, base, apiZoneID)
	if err != nil {
		return "", fmt.Errorf("list cryptokeys: %w", err)
	}
	var active []cryptokey
	for _, k := range existing {
		if k.Active && (k.KeyType == "ksk" || k.KeyType == "csk") && k.DNSKey != "" {
			active = append(active, k)
		}
	}
	switch len(active) {
	case 0:
		// No active signing key — fall through to create one below.
	case 1:
		c.logger.Info("DNSSEC already enabled on zone; reusing existing signing key",
			zap.String("zone_id", zoneID),
			zap.String("key_type", active[0].KeyType))
		return active[0].DNSKey, nil
	default:
		// Multiple active signing keys: we cannot confirm which one the on-chain
		// DS matches, so guess-free operation requires surfacing this rather than
		// republishing a DS for the wrong key (the very drift this fix targets).
		return "", fmt.Errorf("zone %s has %d active signing keys; cannot determine which matches the published DS; reconcile manually", zoneID, len(active))
	}

	// 2) No existing active KSK — create one. POSTing a cryptokey is what
	// generates a fresh DNSKEY, so we only do this when none exists.
	// PowerDNS picks the algorithm based on its config defaults; we don't force
	// bits/algorithm to avoid overriding the operator's backend configuration.
	reqBody := map[string]any{
		"keytype": "ksk",
		"active":  true,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal cryptokey request: %w", err)
	}

	url := fmt.Sprintf("%s/servers/%s/zones/%s/cryptokeys", base, defaultServerID, apiZoneID)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("create cryptokey request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.apiKey)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("cryptokey request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("PowerDNS cryptokey API returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		DNSKey string   `json:"dnskey"`
		DS     []string `json:"ds"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode cryptokey response: %w", err)
	}

	c.logger.Info("DNSSEC enabled on zone",
		zap.String("zone_id", zoneID),
		zap.Bool("has_ds", len(result.DS) > 0))

	return result.DNSKey, nil
}

// cryptokey is a PowerDNS cryptokey object (subset of the API response).
type cryptokey struct {
	ID      string `json:"id"`
	KeyType string `json:"keytype"`
	Active  bool   `json:"active"`
	DNSKey  string `json:"dnskey"`
}

// listCryptokeys returns the zone's existing cryptokeys via
// GET /servers/:server/zones/:zone/cryptokeys?details=true. The details=true
// query is required: without it PowerDNS returns only each key's id and omits
// the dnskey/ds content, which the idempotent reuse check depends on. A 404
// (zone has no cryptokeys yet) is treated as an empty list.
func (c *PowerDNSClient) listCryptokeys(ctx context.Context, base, apiZoneID string) ([]cryptokey, error) {
	url := fmt.Sprintf("%s/servers/%s/zones/%s/cryptokeys?details=true", base, defaultServerID, apiZoneID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("list cryptokeys request: %w", err)
	}
	req.Header.Set("X-API-Key", c.apiKey)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list cryptokeys failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("PowerDNS cryptokey list returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var keys []cryptokey
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		return nil, fmt.Errorf("decode cryptokey list: %w", err)
	}
	return keys, nil
}
