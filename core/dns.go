package core

import (
	"context"

	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
)

const DNS_SERVICE = "ipfs.dns"

// DNSService defines the interface for managing DNS zones
type DNSService interface {
	core.Service

	// CreateZone creates a new DNS zone
	CreateZone(ctx context.Context, domain string, userID uint) (*pluginDb.DNSZone, error)

	// GetZone retrieves a zone by ID
	GetZone(ctx context.Context, zoneID uint) (*pluginDb.DNSZone, error)

	// GetZoneByDomain retrieves a zone by domain name
	GetZoneByDomain(ctx context.Context, domain string) (*pluginDb.DNSZone, error)

	// ListZones retrieves zones for a user
	ListZones(ctx context.Context, userID uint) ([]*pluginDb.DNSZone, error)

	// UpdateZone updates zone status
	UpdateZone(ctx context.Context, zoneID uint, status pluginDb.DNSZoneStatus) error

	// DeleteZone deletes a zone
	DeleteZone(ctx context.Context, zoneID uint) error

	// ValidateNameservers validates that domain's nameservers match approved list
	ValidateNameservers(ctx context.Context, zoneID uint) (bool, error)

	// UpdateWebsiteDNSRecords updates DNS records for a website
	UpdateWebsiteDNSRecords(ctx context.Context, zoneID uint, targetHash string, targetType string) error

	// DeleteWebsiteDNSRecords removes DNS records for a website
	DeleteWebsiteDNSRecords(ctx context.Context, zoneID uint) error
}
