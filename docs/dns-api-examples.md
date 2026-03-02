# DNS API Examples

This document provides curl command examples for all DNS API endpoints.

## DNS Zones

### Create Zone
```bash
curl -X POST http://localhost:8080/api/dns/zones \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "example.com",
    "nameservers": ["ns1.example.com", "ns2.example.com"]
  }'
```

Response:
```json
{
  "id": 1,
  "domain": "example.com",
  "status": "pending_nameserver",
  "power_dns_zone_id": null,
  "user_id": 123,
  "created_at": "2026-02-28T02:00:00Z",
  "updated_at": "2026-02-28T02:00:00Z"
}
```

### List Zones
```bash
curl -X GET "http://localhost:8080/api/dns/zones?status=pending_nameserver&domain=example" \
  -H "Authorization: Bearer <token>"
```

Response:
```json
[
  {
    "id": 1,
    "domain": "example.com",
    "status": "pending_nameserver",
    "power_dns_zone_id": null,
    "user_id": 123,
    "created_at": "2026-02-28T02:00:00Z",
    "updated_at": "2026-02-28T02:00:00Z"
  }
]
```

### Get Zone
```bash
curl -X GET http://localhost:8080/api/dns/zones/1 \
  -H "Authorization: Bearer <token>"
```

Response:
```json
{
  "id": 1,
  "domain": "example.com",
  "status": "active",
  "power_dns_zone_id": "zone-123",
  "user_id": 123,
  "created_at": "2026-02-28T02:00:00Z",
  "updated_at": "2026-02-28T02:00:00Z"
}
```

### Update Zone
```bash
curl -X PUT http://localhost:8080/api/dns/zones/1 \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "example.com",
    "nameservers": ["ns1.example.com", "ns2.example.com", "ns3.example.com"]
  }'
```

Response:
```json
{
  "id": 1,
  "domain": "example.com",
  "status": "active",
  "power_dns_zone_id": "zone-123",
  "user_id": 123,
  "created_at": "2026-02-28T02:00:00Z",
  "updated_at": "2026-02-28T02:01:00Z"
}
```

### Delete Zone
```bash
curl -X DELETE http://localhost:8080/api/dns/zones/1 \
  -H "Authorization: Bearer <token>"
```

Response: `204 No Content`

### Validate Zone Nameservers
```bash
curl -X POST http://localhost:8080/api/dns/zones/1/validate \
  -H "Authorization: Bearer <token>"
```

Response:
```json
{
  "valid": true,
  "message": "Nameservers are valid",
  "nameservers": ["ns1.example.com", "ns2.example.com"],
  "checked_at": "2026-02-28T02:00:00Z"
}
```

### Get Zone Status
```bash
curl -X GET http://localhost:8080/api/dns/zones/1/status \
  -H "Authorization: Bearer <token>"
```

Response:
```json
{
  "status": "active",
  "nameservers_verified": true,
  "last_nameserver_check_at": "2026-02-28T02:00:00Z",
  "propagation_status": "complete"
}
```

## DNS Records

### List Records
```bash
curl -X GET "http://localhost:8080/api/dns/zones/1/records?type=A&name=www" \
  -H "Authorization: Bearer <token>"
```

Response:
```json
[
  {
    "id": 1,
    "zone_id": 1,
    "name": "www.example.com",
    "type": "A",
    "content": "192.0.2.1",
    "ttl": 3600,
    "disabled": false,
    "created_at": "2026-02-28T02:00:00Z",
    "updated_at": "2026-02-28T02:00:00Z"
  }
]
```

### Get Record
```bash
curl -X GET http://localhost:8080/api/dns/zones/1/records/1 \
  -H "Authorization: Bearer <token>"
```

Response:
```json
{
  "id": 1,
  "zone_id": 1,
  "name": "www.example.com",
  "type": "A",
  "content": "192.0.2.1",
  "ttl": 3600,
  "disabled": false,
  "created_at": "2026-02-28T02:00:00Z",
  "updated_at": "2026-02-28T02:00:00Z"
}
```

### Create Record
```bash
curl -X POST http://localhost:8080/api/dns/zones/1/records \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "www.example.com",
    "type": "A",
    "content": "192.0.2.1",
    "ttl": 3600,
    "disabled": false
  }'
```

Response:
```json
{
  "id": 1,
  "zone_id": 1,
  "name": "www.example.com",
  "type": "A",
  "content": "192.0.2.1",
  "ttl": 3600,
  "disabled": false,
  "created_at": "2026-02-28T02:00:00Z",
  "updated_at": "2026-02-28T02:00:00Z"
}
```

### Update Record
```bash
curl -X PUT http://localhost:8080/api/dns/zones/1/records/1 \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "www.example.com",
    "type": "A",
    "content": "192.0.2.2",
    "ttl": 7200,
    "disabled": false
  }'
```

Response:
```json
{
  "id": 1,
  "zone_id": 1,
  "name": "www.example.com",
  "type": "A",
  "content": "192.0.2.2",
  "ttl": 7200,
  "disabled": false,
  "created_at": "2026-02-28T02:00:00Z",
  "updated_at": "2026-02-28T02:01:00Z"
}
```

### Delete Record
```bash
curl -X DELETE http://localhost:8080/api/dns/zones/1/records/1 \
  -H "Authorization: Bearer <token>"
```

Response: `204 No Content`

### Bulk Records
```bash
curl -X POST http://localhost:8080/api/dns/zones/1/records/bulk \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      {
        "id": 1,
        "name": "www.example.com",
        "type": "A",
        "content": "192.0.2.1",
        "ttl": 3600
      },
      {
        "id": 2,
        "name": "mail.example.com",
        "type": "A",
        "content": "192.0.2.3",
        "ttl": 3600
      }
    ]
  }'
```

Response:
```json
[
  {
    "id": 1,
    "zone_id": 1,
    "name": "www.example.com",
    "type": "A",
    "content": "192.0.2.1",
    "ttl": 3600,
    "disabled": false,
    "created_at": "2026-02-28T02:00:00Z",
    "updated_at": "2026-02-28T02:01:00Z"
  },
  {
    "id": 2,
    "zone_id": 1,
    "name": "mail.example.com",
    "type": "A",
    "content": "192.0.2.3",
    "ttl": 3600,
    "disabled": false,
    "created_at": "2026-02-28T02:00:00Z",
    "updated_at": "2026-02-28T02:01:00Z"
  }
]
```

## Record Types

Supported DNS record types:
- `A` - IPv4 address
- `AAAA` - IPv6 address
- `CNAME` - Canonical name alias
- `MX` - Mail exchange
- `TXT` - Text records
- `NS` - Name server
- `SRV` - Service records
- `ALIAS` - Alias records

## Common Error Responses

### Unauthorized
```json
{
  "error": {
    "reason": "ErrUnauthorized",
    "details": "Access denied. Please check your credentials and try again."
  }
}
```

### Zone Not Found
```json
{
  "error": {
    "reason": "ErrZoneNotFound",
    "details": "DNS zone not found"
  }
}
```

### Record Not Found
```json
{
  "error": {
    "reason": "ErrRecordNotFound",
    "details": "DNS record not found"
  }
}
```

### Invalid Domain Format
```json
{
  "error": {
    "reason": "ErrInvalidDomainFormat",
    "details": "Invalid domain format"
  }
}
```

### Duplicate Record
```json
{
  "error": {
    "reason": "ErrDuplicateRecord",
    "details": "Duplicate DNS record"
  }
}
```
