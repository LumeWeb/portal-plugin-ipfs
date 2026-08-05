package core

import (
	"context"

	apiDTO "go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
)

const DNS_SERVICE = "ipfs.dns"

// DNSService defines the interface for managing DNS zones
type DNSService interface {
	core.Service
	core.Configurable

	// CreateZone creates a new DNS zone
	CreateZone(ctx context.Context, domain string, userID uint) (*pluginDb.DNSZone, error)

	// GetZone retrieves a zone by ID
	GetZone(ctx context.Context, zoneID uint) (*pluginDb.DNSZone, error)

	// GetZoneByDomain retrieves a zone by domain name
	GetZoneByDomain(ctx context.Context, domain string) (*pluginDb.DNSZone, error)

	// ListZones retrieves zones for a user with filtering, sorting, and pagination
	ListZones(ctx context.Context, filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.DNSZone, int64, error)

	// UpdateZone updates zone status
	UpdateZone(ctx context.Context, zoneID uint, status pluginDb.DNSZoneStatus) error

	// DeleteZone deletes a zone
	DeleteZone(ctx context.Context, zoneID uint) error

	// ValidateNameservers validates that domain's nameservers match approved list
	ValidateNameservers(ctx context.Context, zoneID uint) (bool, error)

	// CreateWebsiteDNSRecords creates initial DNS records for a new website.
	// websiteDomain is the full domain of the website (may differ from the zone's domain for subdomain websites).
	CreateWebsiteDNSRecords(ctx context.Context, zoneID uint, websiteDomain string, targetHash string, targetType pluginDb.WebsiteTargetType, validationToken string) error

	// UpdateWebsiteDNSRecords updates DNS records for a website.
	// websiteDomain is the full domain of the website (may differ from the zone's domain for subdomain websites).
	UpdateWebsiteDNSRecords(ctx context.Context, zoneID uint, websiteDomain string, targetHash string, targetType pluginDb.WebsiteTargetType) error

	// DeleteWebsiteDNSRecords removes DNS records for a website.
	// websiteDomain is the full domain of the website (may differ from the zone's domain for subdomain websites).
	DeleteWebsiteDNSRecords(ctx context.Context, zoneID uint, websiteDomain string) error

	// GetZoneRecords retrieves DNS records for a zone from PowerDNS
	// Returns list of DNSRecord DTOs representing PowerDNS RRSets with filtering applied
	GetZoneRecords(ctx context.Context, zoneID uint, filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*apiDTO.DNSRecord, int64, error)

	// GetRRSet retrieves a specific DNS RRSet by name and type from PowerDNS
	GetRRSet(ctx context.Context, zoneID uint, name string, recordType string) ([]*apiDTO.DNSRecord, error)

	// CreateDNSLinkRecord creates a DNSLink _dnslink.<domain> TXT record
	CreateDNSLinkRecord(ctx context.Context, zoneID uint, target string) error

	// CreateApexRecord creates an apex (root) record pointing to the gateway.
	// recordType is e.g. RecordTypeA (IP content for DNSSEC-signed alt-root)
	// or RecordTypeALIAS (gateway hostname content).
	CreateApexRecord(ctx context.Context, zoneID uint, recordType RecordType, content string) error

	// CreateRecord creates a new DNS record in PowerDNS via RRSet
	// name: the record name (e.g., "www")
	// recordType: the record type (e.g., "A", "CNAME")
	// content: the record content/value
	// ttl: time to live in seconds
	CreateRecord(ctx context.Context, zoneID uint, name string, recordType string, content string, ttl uint) (*apiDTO.DNSRecord, error)

	// UpdateRecord updates an existing DNS RRSet in PowerDNS
	// name: the record name
	// recordType: the record type
	// records: list of record contents to update
	UpdateRecord(ctx context.Context, zoneID uint, name string, recordType string, records []string, ttl uint) ([]*apiDTO.DNSRecord, error)

	// DeleteRecord deletes a DNS RRSet from PowerDNS
	DeleteRecord(ctx context.Context, zoneID uint, name string, recordType string) error

	// BulkDeleteRecords deletes multiple DNS records in a single PowerDNS API call
	BulkDeleteRecords(ctx context.Context, zoneID uint, userID uint, records []apiDTO.RecordIdentifier, dryRun bool) (*apiDTO.BulkDeleteResponse, error)

	// EnableDNSSEC enables DNSSEC on a zone and returns the DNSKEY record content.
	EnableDNSSEC(ctx context.Context, zoneID uint) (dnskey string, err error)
}
