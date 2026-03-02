# DNSSEC Implementation Plan

## Overview

This document outlines the approach for adding DNSSEC (Domain Name System Security Extensions) support to the portal-plugin-ipfs DNS management system.

## PowerDNS DNSSEC API Capabilities

PowerDNS provides comprehensive DNSSEC management through its HTTP API:

### Available Endpoints

1. **Cryptokeys Endpoint**
   - `GET /servers/{server_id}/zones/{zone_id}/cryptokeys`
     - Retrieves all DNSSEC keys for a zone (excludes private keys)
     - Returns: ID, type (KSK/ZSK), active state, algorithm, bits, flags

   - `POST /servers/{server_id}/zones/{zone_id}/cryptokeys`
     - Creates new DNSSEC keys (can generate or import)
     - Supports KSK (Key Signing Key) and ZSK (Zone Signing Key)
     - Can specify algorithm, key size, or use defaults

   - `DELETE /servers/{server_id}/cryptokeys/{cryptokey_id}`
     - Deletes a specific DNSSEC key

2. **Zone DNSSEC Status**
   - `GET /servers/{server_id}/zones` with `dnssec=true` query parameter
     - Includes `dnssec` field in Zone objects (boolean indicating DNSSEC status)
     - Includes `edited_serial` field for DNSSEC operations

3. **Key Management**
   - Activate/deactivate keys via PUT to cryptokeys endpoint
   - Retrieve DS (Delegation Signer) records via metadata endpoint
   - Retrieve DNSKEY records

## Implementation Strategy

### Phase 1: PowerDNS Client Extensions

**File:** `internal/dns/powerdns/client.go`

Add DNSSEC-related methods:

```go
// GetZoneCryptoKeys retrieves DNSSEC keys for a zone
func (c *Client) GetZoneCryptoKeys(ctx context.Context, zoneID string) ([]CryptoKey, error)

// EnableDNSSEC enables DNSSEC for a zone
func (c *Client) EnableDNSSEC(ctx context.Context, zoneID string) ([]CryptoKey, error)

// DisableDNSSEC disables DNSSEC for a zone
func (c *Client) DisableDNSSEC(ctx context.Context, zoneID string) error

// ActivateKey activates a DNSSEC key
func (c *Client) ActivateKey(ctx context.Context, zoneID, keyID string) error

// DeactivateKey deactivates a DNSSEC key
func (c *Client) DeactivateKey(ctx context.Context, zoneID, keyID string) error
```

Define `CryptoKey` struct:
```go
type CryptoKey struct {
    ID          string
    Type        string  // "KSK" or "ZSK"
    Active      bool
    KeyType     string  // Algorithm name
    Bits        int
    Flags       int
    Inactive    bool
    Published   bool
}
```

### Phase 2: Service Layer Extensions

**File:** `internal/service/dns/dns_service_dnssec.go` (new file)

Add DNSSEC service methods:

```go
// GetZoneDNSSECStatus retrieves DNSSEC status for a zone
func (s *DNSService) GetZoneDNSSECStatus(ctx context.Context, zoneID uint) (*DNSSECStatus, error)

// EnableZoneDNSSEC enables DNSSEC for a zone
func (s *DNSService) EnableZoneDNSSEC(ctx context.Context, zoneID uint) (*DNSSECStatus, error)

// DisableZoneDNSSEC disables DNSSEC for a zone
func (s *DNSService) DisableZoneDNSSEC(ctx context.Context, zoneID uint) error

// RotateDNSSECKeys performs DNSSEC key rotation
func (s *DNSService) RotateDNSSECKeys(ctx context.Context, zoneID uint) (*DNSSECStatus, error)
```

Define `DNSSECStatus` struct:
```go
type DNSSECStatus struct {
    Enabled      bool
    KeyCount     int
    KSKCount     int
    ZSKCount     int
    LastKeyRollover time.Time
    Keys         []CryptoKey
}
```

### Phase 3: Database Schema Updates

**File:** `internal/db/dns_zone.go`

Add DNSSEC fields to `DNSZone` model:

```go
type DNSZone struct {
    // ... existing fields ...
    DNSSECEnabled       bool      `gorm:"column:dnssec_enabled"`
    DNSSECKSSCount      int       `gorm:"column:dnssec_ksk_count" sql:"DEFAULT:0"`
    DNSSECZSKCount      int       `gorm:"column:dnssec_zsk_count" sql:"DEFAULT:0"`
    LastDNSSECRollover  time.Time `gorm:"column:last_dnssec_rollover"`
}
```

**Migration files:**
- `internal/db/migrations/mysql/20260228_add_dnssec_fields.sql`
- `internal/db/migrations/sqlite/20260228_add_dnssec_fields.sql`

### Phase 4: API Layer Extensions

**File:** `internal/api/dto/dns.go`

Add DNSSEC DTOs:

```go
// DNSSECStatusResponse represents DNSSEC status response
type DNSSECStatusResponse struct {
    Enabled           bool      `json:"enabled"`
    KeyCount          int       `json:"key_count"`
    KSKCount          int       `json:"ksk_count"`
    ZSKCount          int       `json:"zsk_count"`
    LastKeyRollover   time.Time `json:"last_key_rollover,omitempty"`
    Keys              []DNSKey  `json:"keys"`
}

// DNSKey represents a DNSSEC key
type DNSKey struct {
    ID        string `json:"id"`
    Type      string `json:"type"`
    Active    bool   `json:"active"`
    Algorithm string `json:"algorithm"`
    Bits      int    `json:"bits"`
}

// DNSSECEnableRequest represents DNSSEC enable request
type DNSSECEnableRequest struct {
    Algorithm   string `json:"algorithm,omitempty"`   // Optional: override default
    KSKSize     int    `json:"ksk_size,omitempty"`    // Optional: override default
    ZSKSize     int    `json:"zsk_size,omitempty"`    // Optional: override default
}
```

**File:** `internal/api/api_dns.go`

Add DNSSEC handlers:

```go
// getZoneDNSSEC retrieves DNSSEC status for a zone
func (a *API) getZoneDNSSEC(c echo.Context) error

// enableZoneDNSSEC enables DNSSEC for a zone
func (a *API) enableZoneDNSSEC(c echo.Context) error

// disableZoneDNSSEC disables DNSSEC for a zone
func (a *API) disableZoneDNSSEC(c echo.Context) error

// rotateZoneDNSSECKeys performs DNSSEC key rotation
func (a *API) rotateZoneDNSSECKeys(c echo.Context) error
```

**File:** `internal/api/api.go`

Register DNSSEC routes:

```go
// DNSSEC routes
dnsSECRoutes := router.DefineRoutes(
    router.NewRoute(http.MethodGet, "/dns/zones/:id/dnssec", a.getZoneDNSSEC,
        router.WithAccess(core.ACCESS_USER_ROLE),
        router.WithSwagger(/* ... */),
    ),
    router.NewRoute(http.MethodPost, "/dns/zones/:id/dnssec", a.enableZoneDNSSEC,
        router.WithAccess(core.ACCESS_USER_ROLE),
        router.WithSwagger(/* ... */),
    ),
    router.NewRoute(http.MethodDelete, "/dns/zones/:id/dnssec", a.disableZoneDNSSEC,
        router.WithAccess(core.ACCESS_USER_ROLE),
        router.WithSwagger(/* ... */),
    ),
    router.NewRoute(http.MethodPost, "/dns/zones/:id/dnssec/rotate", a.rotateZoneDNSSECKeys,
        router.WithAccess(core.ACCESS_USER_ROLE),
        router.WithSwagger(/* ... */),
    ),
)
```

## Implementation Considerations

### Key Rotation Strategy

1. **Initial Key Generation:**
   - Generate one KSK and one ZSK when DNSSEC is enabled
   - KSK: 2048-bit or larger RSA (or ECDSA if preferred)
   - ZSK: 1024-bit RSA (or ECDSA if preferred)

2. **Key Rollover Process:**
   - Pre-publish new ZSK 30 days before activation
   - Activate new ZSK, wait for TTL expiration
   - Remove old ZSK after new one is fully distributed
   - KSK rollover is more complex (requires DS record updates at registrar)
   - Implement ZSK automatic rotation, KSK manual rotation

3. **Timing Recommendations:**
   - ZSK rotation: Every 90 days
   - KSK rotation: Every 1-2 years
   - Pre-publish period: 30 days
   - Retirement period: 30 days

### Security Considerations

1. **Private Key Protection:**
   - PowerDNS never exposes private keys via API
   - Keys are stored securely in PowerDNS database
   - Application never handles private key material

2. **Access Control:**
   - DNSSEC operations require `core.ACCESS_USER_ROLE`
   - Only zone owners can manage DNSSEC
   - Audit logging for all DNSSEC operations

3. **Validation:**
   - Verify DNSSEC status before enabling
   - Validate zone configuration before key generation
   - Check PowerDNS API permissions

### Error Handling

1. **PowerDNS API Errors:**
   - Handle 404 (zone not found)
   - Handle 422 (invalid parameters)
   - Handle 500 (internal server error)

2. **Service Errors:**
   - Handle zone not found errors
   - Handle DNSSEC already enabled errors
   - Handle DNSSEC not enabled errors for disable operations

3. **Validation Errors:**
   - Validate zone ownership
   - Validate algorithm and key size parameters
   - Validate zone is in active state

## Testing Strategy

### Unit Tests

- PowerDNS client DNSSEC methods
- Service layer DNSSEC operations
- DTO conversions

### Integration Tests

- DNSSEC enable/disable flow
- Key retrieval operations
- Key rotation process
- Error scenarios

### Manual Testing

- Verify DNSSEC status in PowerDNS UI
- Test DS record retrieval
- Verify DNS records are signed
- Test key rollover timing

## API Examples

### Get DNSSEC Status

```bash
curl -X GET http://localhost:8080/api/v1/dns/zones/123/dnssec \
  -H "Authorization: Bearer <token>"
```

Response:
```json
{
  "enabled": true,
  "key_count": 2,
  "ksk_count": 1,
  "zsk_count": 1,
  "last_key_rollover": "2026-02-28T00:00:00Z",
  "keys": [
    {
      "id": "12345",
      "type": "KSK",
      "active": true,
      "algorithm": "RSASHA256",
      "bits": 2048
    },
    {
      "id": "12346",
      "type": "ZSK",
      "active": true,
      "algorithm": "RSASHA256",
      "bits": 1024
    }
  ]
}
```

### Enable DNSSEC

```bash
curl -X POST http://localhost:8080/api/v1/dns/zones/123/dnssec \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"algorithm": "RSASHA256", "ksk_size": 2048, "zsk_size": 1024}'
```

### Disable DNSSEC

```bash
curl -X DELETE http://localhost:8080/api/v1/dns/zones/123/dnssec \
  -H "Authorization: Bearer <token>"
```

### Rotate Keys

```bash
curl -X POST http://localhost:8080/api/v1/dns/zones/123/dnssec/rotate \
  -H "Authorization: Bearer <token>"
```

## Future Enhancements

1. **Automatic ZSK Rotation:**
   - Scheduled task to rotate ZSK keys automatically
   - Configurable rotation interval
   - Notifications before rotation

2. **DS Record Management:**
   - Retrieve DS records for registrar submission
   - Track DS record submission status
   - Alert on expiring DS records

3. **DNSSEC Validation:**
   - Verify DNSSEC chain of trust
   - Validate DNS signatures
   - Report validation errors

4. **Audit Logging:**
   - Log all DNSSEC operations
   - Track key lifecycle events
   - Security event notifications

## Dependencies

- PowerDNS version 4.0 or higher
- Go 1.26 or higher
- Existing portal infrastructure
- PowerDNS API access with proper permissions

## References

- [PowerDNS DNSSEC Documentation](https://doc.powerdns.com/authoritative/dnssec/index.html)
- [PowerDNS API Cryptokeys](https://doc.powerdns.com/authoritative/http-api/cryptokey.html)
- [RFC 4035: DNS Security Extensions](https://www.rfc-editor.org/rfc/rfc4035)
- [DNSSEC Key Rollover Practices](https://www.iana.org/dnssec/dnssec-key-rollovers/)
