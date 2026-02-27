package db

import (
	"fmt"
	"time"

	"go.lumeweb.com/portal-plugin-ipfs/internal/errors"
	"gorm.io/gorm"
)

// DNSZoneStatus represents the status of a DNS zone
type DNSZoneStatus string

const (
	DNSZoneStatusPendingNameserver DNSZoneStatus = "pending_nameserver"
	DNSZoneStatusActive             DNSZoneStatus = "active"
)

// validDNSZoneStatuses is a map of valid DNS zone statuses
var validDNSZoneStatuses = map[DNSZoneStatus]struct{}{
	DNSZoneStatusPendingNameserver: {},
	DNSZoneStatusActive:             {},
}

// DNSZone represents a DNS zone configuration in the database
type DNSZone struct {
	gorm.Model
	UserID                uint       `gorm:"not null"`
	Domain                string     `gorm:"not null"`
	Status                string     `gorm:"not null"` // pending_nameserver, active
	PowerDNSZoneID        string     `gorm:"column:powerdns_zone_id"`
	LastNameserverCheckAt *time.Time
	NameserversVerifiedAt *time.Time
}

func (D DNSZone) TableName() string {
	return "ipfs_dns_zones"
}

// BeforeSave hook to validate status
func (z *DNSZone) BeforeSave(tx *gorm.DB) error {
	// Validate status
	if _, ok := validDNSZoneStatuses[DNSZoneStatus(z.Status)]; !ok {
		return fmt.Errorf("%s: %s", errors.ErrInvalidZoneStatus, z.Status)
	}

	return nil
}

// IsPendingNameserver returns true if the zone is waiting for nameserver change
func (z *DNSZone) IsPendingNameserver() bool {
	return DNSZoneStatus(z.Status) == DNSZoneStatusPendingNameserver
}

// IsActive returns true if the zone is active
func (z *DNSZone) IsActive() bool {
	return DNSZoneStatus(z.Status) == DNSZoneStatusActive
}
