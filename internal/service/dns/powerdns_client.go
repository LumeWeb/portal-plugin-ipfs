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

	// Let PowerDNS own zone content generation: with only `nameservers` set,
	// it auto-generates both the SOA and NS records for the zone, manages the
	// SOA serial itself (SOA-EDIT-API defaults to DEFAULT), and applies a valid
	// RFC 1982 serial. We deliberately do NOT inject an explicit SOA RRSet here:
	// supplying our own serial fights PowerDNS's serial management (overflow and
	// wrap-around hazards) and is the wrong division of responsibility. The only
	// thing we correct afterwards is the SOA MNAME, which PowerDNS seeds with a
	// placeholder ("a.misconfigured.dns.server.invalid.").

	resp, err := c.client.CreateZone(ctx, defaultServerID, zoneCreate)
	if err != nil {
		return nil, fmt.Errorf("failed to create zone: %w", err)
	}

	zone, err := handleResponse[powerdns.Zone](resp)
	if err != nil {
		if strings.Contains(err.Error(), "status 409") {
			c.logger.Info("Zone already exists in PowerDNS, fetching existing zone",
				zap.String("domain", domain))

			// The zone already exists in PowerDNS and is not provably ours — it
			// may be a foreign or operator-managed zone. We must not mutate it
			// (the SOA MNAME correction is only safe on the fresh-create path,
			// where we just created the zone and know it is ours). Return it
			// untouched.
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

	// Correct the SOA MNAME that PowerDNS seeded with its placeholder
	// ("a.misconfigured.dns.server.invalid.") to the zone's primary nameserver.
	// This is strictly best-effort: the zone was already created successfully
	// and is provably portal-owned, and delegation is carried by the NS record
	// (whose primary we set) — the MNAME is a secondary authoritative pointer.
	// A transient GetZoneWithRRSets error or a rejected PATCH must neither fail
	// the create nor destroy the live zone over this cosmetic issue, so we log
	// a warning and always return the created zone. If the correction cannot
	// run here, the placeholder MNAME may persist; it is surfaced via the log
	// rather than by failing creation.
	if err := c.fixSOAMNAME(ctx, *zone.Id, canonicalDomain, canonicalNameservers); err != nil {
		c.logger.Warn("Failed to fix SOA MNAME after zone creation (best-effort)",
			zap.String("domain", canonicalDomain),
			zap.String("zone_id", *zone.Id),
			zap.Error(err))
	}

	c.logger.Info("Zone created in PowerDNS",
		zap.String("domain", domain),
		zap.String("zone_id", *zone.Id))

	return zone, nil
}

// fixSOAMNAME corrects the SOA MNAME of a zone that PowerDNS created with its
// placeholder owner ("a.misconfigured.dns.server.invalid.") to the primary
// authoritative nameserver. It reads the SOA PowerDNS generated, swaps only the
// MNAME field, and writes it back. All other SOA fields — most importantly the
// serial PowerDNS assigned — are preserved verbatim, so we never fabricate a
// serial (no RFC 1982 overflow or wrap-around hazards) and never fight
// PowerDNS's serial management. Subsequent serial bumps are handled by
// PowerDNS's SOA-EDIT-API=DEFAULT.
//
// This runs best-effort from CreateZone (both the fresh-create and the
// existing-zone recovery path). It only mutates zones still carrying the
// PowerDNS placeholder MNAME, so it can never rewrite a foreign or
// manually-managed zone's legitimate SOA MNAME.
//
// SOA content format: MNAME RNAME SERIAL REFRESH RETRY EXPIRE MINIMUM.
func (c *PowerDNSClient) fixSOAMNAME(ctx context.Context, zoneID, domain string, canonicalNameservers []string) error {
	if len(canonicalNameservers) == 0 || canonicalNameservers[0] == "" {
		// No nameserver to use as the MNAME; leave the zone as PowerDNS made it.
		return nil
	}
	mname := dnsname.EnsureFQDN(canonicalNameservers[0])

	zone, err := c.GetZoneWithRRSets(ctx, zoneID)
	if err != nil {
		return fmt.Errorf("get zone: %w", err)
	}
	return c.fixSOAMNAMEOnZone(ctx, zoneID, domain, mname, zone)
}

// fixSOAMNAMEOnZone performs the MNAME correction on an already-fetched zone.
// It is only called from the fresh-create path (fixSOAMNAME), where the zone
// was just created by this call and is therefore provably portal-owned — no
// ownership gate is needed. It returns nil (a no-op) if the zone has no rrsets,
// no usable MNAME, or its MNAME is already correct.
func (c *PowerDNSClient) fixSOAMNAMEOnZone(ctx context.Context, zoneID, domain, mname string, zone *powerdns.Zone) error {
	if zone == nil || zone.Rrsets == nil || mname == "" {
		return nil
	}

	for i := range *zone.Rrsets {
		rr := &(*zone.Rrsets)[i]
		if rr.Type != "SOA" || len(rr.Records) == 0 {
			continue
		}
		fields := strings.Fields(rr.Records[0].Content)
		if len(fields) < 1 {
			return fmt.Errorf("malformed SOA content %q", rr.Records[0].Content)
		}
		if fields[0] == mname {
			// MNAME already correct; nothing to do.
			return nil
		}

		// Preserve every field PowerDNS generated, swapping only the MNAME.
		fields[0] = mname
		content := strings.Join(fields, " ")
		ttl := 3600
		if rr.Ttl != nil {
			ttl = *rr.Ttl
		}

		soaRRSet := powerdns.RRSet{
			Name:       rr.Name,
			Type:       "SOA",
			Changetype: powerdns.REPLACE,
			Ttl:        &ttl,
			Records:    []powerdns.Record{{Content: content}},
		}
		if err := c.UpdateZoneRRSets(ctx, zoneID, []powerdns.RRSet{soaRRSet}); err != nil {
			return err
		}
		c.logger.Info("Corrected SOA MNAME for zone",
			zap.String("domain", domain),
			zap.String("mname", mname))
		return nil
	}

	return nil
}

// GetZone retrieves a zone from PowerDNS. It does not request rrsets, keeping
// the payload light for callers that only need the zone metadata (id, name,
// kind, serial). Callers that need the zone's records or SOA must use
// GetZoneWithRRSets.
func (c *PowerDNSClient) GetZone(ctx context.Context, zoneID string) (*powerdns.Zone, error) {
	resp, err := c.client.GetZone(ctx, defaultServerID, zoneID)
	if err != nil {
		return nil, fmt.Errorf("failed to get zone: %w", err)
	}

	return handleResponse[powerdns.Zone](resp)
}

// GetZoneWithRRSets retrieves a zone from PowerDNS including its rrsets.
// PowerDNS only returns a zone's rrsets when explicitly requested via
// ?rrsets=true; without it Rrsets is nil, which would make callers that need
// the SOA (e.g. fixSOAMNAME) silently no-op.
func (c *PowerDNSClient) GetZoneWithRRSets(ctx context.Context, zoneID string) (*powerdns.Zone, error) {
	resp, err := c.client.GetZone(ctx, defaultServerID, zoneID, func(_ context.Context, req *http.Request) error {
		q := req.URL.Query()
		q.Set("rrsets", "true")
		req.URL.RawQuery = q.Encode()
		return nil
	})
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

// GetActiveDNSKEYDS returns the SHA-256 DS RDATA (type 2, e.g.
// "60776 13 2 <hex>") for the zone's currently-active signing key, computed
// from the live PowerDNS cryptokey state. It is the on-the-fly source of the
// DS to publish/verify on-chain, so the portal never needs to persist a DS
// (which would go stale on key rotation).
//
// It returns ("", nil) when the zone has no active signing key (DNSSEC not
// enabled). When there are multiple active signing keys — an in-progress
// rollover — it returns an error rather than guessing which one is
// authoritative, mirroring the guard in EnableDNSSEC.
func (c *PowerDNSClient) GetActiveDNSKEYDS(ctx context.Context, zoneID string) (string, error) {
	parts := strings.Split(zoneID, "/")
	apiZoneID := parts[len(parts)-1]

	zoneMu := c.zoneMu(apiZoneID)
	zoneMu.Lock()
	defer zoneMu.Unlock()

	base := strings.TrimSuffix(c.baseURL, "/")
	existing, err := c.listCryptokeys(ctx, base, apiZoneID)
	if err != nil {
		return "", fmt.Errorf("list cryptokeys: %w", err)
	}

	var active []cryptokey
	for _, k := range existing {
		if k.Active && (k.KeyType == "ksk" || k.KeyType == "csk") {
			active = append(active, k)
		}
	}
	switch len(active) {
	case 0:
		return "", nil
	case 1:
		ds, err := sha256DSPresentation(active[0].DS)
		if err != nil {
			return "", fmt.Errorf("zone %s active signing key has no usable SHA-256 DS: %w", zoneID, err)
		}
		return ds, nil
	default:
		return "", fmt.Errorf("zone %s has %d active signing keys; cannot determine which matches the published DS; reconcile manually", zoneID, len(active))
	}
}

// sha256DSPresentation extracts the SHA-256 (digest type 2) DS RDATA
// presentation from a PowerDNS-returned DS list. PowerDNS returns one DS entry
// per digest type (e.g. "60776 13 2 <sha256>" and "60776 13 4 <sha512>"); we
// select the type-2 entry, which is what parent-zone queryDS comparison and
// on-chain DS publishing use.
func sha256DSPresentation(dss []string) (string, error) {
	for _, ds := range dss {
		fields := strings.Fields(ds)
		if len(fields) >= 3 && fields[2] == "2" {
			return ds, nil
		}
	}
	return "", fmt.Errorf("no SHA-256 DS (type 2) found")
}

// cryptokey is a PowerDNS cryptokey object (subset of the API response).
type cryptokey struct {
	// ID is the PowerDNS cryptokey id. PowerDNS returns it as a JSON number;
	// binding it as json.Number (rather than string) makes unmarshaling accept
	// the numeric form the API actually sends while remaining tolerant if a
	// version ever returns a quoted string.
	ID      json.Number `json:"id"`
	KeyType string      `json:"keytype"`
	Active  bool        `json:"active"`
	DNSKey  string      `json:"dnskey"`
	DS      []string    `json:"ds"`
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
