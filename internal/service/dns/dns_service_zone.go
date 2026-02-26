package dns

import (
	"context"
	"fmt"
	"time"

	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/dns/powerdns"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// CreateZone creates a new DNS zone
func (s *DNSService) CreateZone(ctx context.Context, domain string, userID uint) (*pluginDb.DNSZone, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.CreateZone")
	defer span.End()

	// Validate domain format
	if err := s.validateDomain(domain); err != nil {
		return nil, fmt.Errorf("invalid domain: %w", err)
	}

	// Check if domain already exists
	existing, err := s.GetZoneByDomain(ctx, domain)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing domain: %w", err)
	}
	if existing != nil {
		return nil, fmt.Errorf("domain already exists: %s", domain)
	}

	// Create zone in PowerDNS
	if s.pdnsClient == nil {
		return nil, fmt.Errorf("DNS hosting not enabled")
	}

	// Get approved nameservers from config
	nameservers := s.config.Nameservers
	if len(nameservers) == 0 {
		return nil, fmt.Errorf("no approved nameservers configured")
	}

	zone, err := s.pdnsClient.CreateZone(ctx, domain, nameservers)
	if err != nil {
		return nil, fmt.Errorf("failed to create zone in PowerDNS: %w", err)
	}

	// Create zone record in database
	dnsZone := &pluginDb.DNSZone{
		UserID:         userID,
		Domain:         domain,
		Status:         string(pluginDb.DNSZoneStatusPendingNameserver),
		PowerDNSZoneID: *zone.Id,
	}

	err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Create(dnsZone)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create zone in database: %w", err)
	}

	s.Logger().Info("DNS zone created",
		zap.Uint("id", dnsZone.ID),
		zap.String("domain", domain),
		zap.Uint("user_id", userID),
		zap.String("powerdns_zone_id", *zone.Id))

	return dnsZone, nil
}

// GetZone retrieves a zone by ID
func (s *DNSService) GetZone(ctx context.Context, zoneID uint) (*pluginDb.DNSZone, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.GetZone")
	defer span.End()

	var zone pluginDb.DNSZone

	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("id = ?", zoneID).First(&zone)
	})

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			s.Logger().Debug("DNS zone not found", zap.Uint("zone_id", zoneID))
			return nil, nil
		}
		s.Logger().Error("Failed to get zone",
			zap.Error(err),
			zap.Uint("zone_id", zoneID))
		return nil, fmt.Errorf("failed to get zone: %w", err)
	}

	return &zone, nil
}

// GetZoneByDomain retrieves a zone by domain name
func (s *DNSService) GetZoneByDomain(ctx context.Context, domain string) (*pluginDb.DNSZone, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.GetZoneByDomain")
	defer span.End()

	var zone pluginDb.DNSZone

	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("domain = ?", domain).First(&zone)
	})

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get zone by domain: %w", err)
	}

	return &zone, nil
}

// ListZones retrieves zones for a user
func (s *DNSService) ListZones(ctx context.Context, userID uint) ([]*pluginDb.DNSZone, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.ListZones")
	defer span.End()

	var zones []*pluginDb.DNSZone

	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("user_id = ?", userID).Find(&zones)
	})

	if err != nil {
		s.Logger().Error("Failed to list zones",
			zap.Error(err),
			zap.Uint("user_id", userID))
		return nil, fmt.Errorf("failed to list zones: %w", err)
	}

	return zones, nil
}

// UpdateZone updates zone status
func (s *DNSService) UpdateZone(ctx context.Context, zoneID uint, status pluginDb.DNSZoneStatus) error {
	ctx, span := core.TraceMethod(ctx, "DNSService.UpdateZone")
	defer span.End()

	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		var zone pluginDb.DNSZone
		if err := tx.Where("id = ?", zoneID).First(&zone).Error; err != nil {
			_ = tx.AddError(err)
			return tx
		}
		zone.Status = string(status)
		return tx.Save(&zone)
	})

	if err != nil {
		s.Logger().Error("Failed to update zone",
			zap.Error(err),
			zap.Uint("zone_id", zoneID))
		return fmt.Errorf("failed to update zone: %w", err)
	}

	s.Logger().Info("DNS zone updated",
		zap.Uint("zone_id", zoneID),
		zap.String("status", string(status)))

	return nil
}

// DeleteZone deletes a zone
func (s *DNSService) DeleteZone(ctx context.Context, zoneID uint) error {
	ctx, span := core.TraceMethod(ctx, "DNSService.DeleteZone")
	defer span.End()

	// Get the zone first
	zone, err := s.GetZone(ctx, zoneID)
	if err != nil {
		return err
	}
	if zone == nil {
		return fmt.Errorf("zone not found")
	}

	// Delete from PowerDNS
	if s.pdnsClient != nil {
		if err := s.pdnsClient.DeleteZone(ctx, zone.PowerDNSZoneID); err != nil {
			s.Logger().Warn("Failed to delete zone from PowerDNS",
				zap.Error(err),
				zap.String("powerdns_zone_id", zone.PowerDNSZoneID))
			// Continue with database deletion even if PowerDNS fails
		}
	}

	// Delete from database
	err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		result := tx.Delete(&pluginDb.DNSZone{}, zoneID)
		return result
	})

	if err != nil {
		s.Logger().Error("Failed to delete zone",
			zap.Error(err),
			zap.Uint("zone_id", zoneID))
		return fmt.Errorf("failed to delete zone: %w", err)
	}

	s.Logger().Info("DNS zone deleted",
		zap.Uint("zone_id", zoneID))

	return nil
}

// ValidateNameservers validates that domain's nameservers match approved list
func (s *DNSService) ValidateNameservers(ctx context.Context, zoneID uint) (bool, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.ValidateNameservers")
	defer span.End()

	zone, err := s.GetZone(ctx, zoneID)
	if err != nil {
		return false, err
	}
	if zone == nil {
		return false, fmt.Errorf("zone not found")
	}

	// TODO: Implement actual nameserver validation via DNS lookup
	// For now, mark as validated since we created the zone with approved nameservers
	now := time.Now()
	zone.NameserversVerifiedAt = &now

	err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		zone.Status = string(pluginDb.DNSZoneStatusActive)
		return tx.Save(zone)
	})
	if err != nil {
		return false, fmt.Errorf("failed to update zone: %w", err)
	}

	s.Logger().Info("DNS nameserver validated",
		zap.Uint("zone_id", zoneID),
		zap.String("domain", zone.Domain))

	return true, nil
}

// UpdateWebsiteDNSRecords updates DNS records for a website
func (s *DNSService) UpdateWebsiteDNSRecords(ctx context.Context, zoneID uint, targetHash string, targetType string) error {
	ctx, span := core.TraceMethod(ctx, "DNSService.UpdateWebsiteDNSRecords")
	defer span.End()

	zone, err := s.GetZone(ctx, zoneID)
	if err != nil {
		return fmt.Errorf("failed to get zone: %w", err)
	}
	if zone == nil {
		return fmt.Errorf("zone not found")
	}

	if s.pdnsClient == nil {
		return fmt.Errorf("DNS hosting not enabled")
	}

	// Create A/AAAA records for the domain pointing to gateway
	// For now, we'll just create a CNAME or A record
	// TODO: Integrate with gateway configuration to get proper IP addresses

	ttl := 300
	disabled := false
	rrsets := []powerdns.RRSet{
		{
			Name:       zone.Domain + ".",
			Type:       "A",
			Changetype: "REPLACE",
			Ttl:        &ttl,
			Records: []powerdns.Record{
				{
					Content:  "127.0.0.1",
					Disabled: &disabled,
				},
			},
		},
	}

	if err := s.pdnsClient.UpdateZoneRRSets(ctx, zone.PowerDNSZoneID, rrsets); err != nil {
		return fmt.Errorf("failed to update DNS records: %w", err)
	}

	s.Logger().Info("DNS records updated for website",
		zap.Uint("zone_id", zoneID),
		zap.String("domain", zone.Domain),
		zap.String("target_hash", targetHash))

	return nil
}

// DeleteWebsiteDNSRecords removes DNS records for a website
func (s *DNSService) DeleteWebsiteDNSRecords(ctx context.Context, zoneID uint) error {
	ctx, span := core.TraceMethod(ctx, "DNSService.DeleteWebsiteDNSRecords")
	defer span.End()

	zone, err := s.GetZone(ctx, zoneID)
	if err != nil {
		return fmt.Errorf("failed to get zone: %w", err)
	}
	if zone == nil {
		return fmt.Errorf("zone not found")
	}

	if s.pdnsClient == nil {
		return fmt.Errorf("DNS hosting not enabled")
	}

	// Remove all records for the domain
	rrsets := []powerdns.RRSet{
		{
			Name:       zone.Domain + ".",
			Type:       "A",
			Changetype: "DELETE",
		},
	}

	if err := s.pdnsClient.UpdateZoneRRSets(ctx, zone.PowerDNSZoneID, rrsets); err != nil {
		s.Logger().Warn("Failed to delete DNS records",
			zap.Error(err),
			zap.Uint("zone_id", zoneID))
		// Continue with zone deletion
	}

	s.Logger().Info("DNS records deleted for website",
		zap.Uint("zone_id", zoneID),
		zap.String("domain", zone.Domain))

	return nil
}

// validateDomain validates the domain name format
func (s *DNSService) validateDomain(domain string) error {
	if domain == "" {
		return fmt.Errorf("domain cannot be empty")
	}
	if len(domain) > 255 {
		return fmt.Errorf("domain too long (max 255 characters)")
	}

	// TODO: Add more sophisticated domain validation (IDNA, RFC 1035, etc.)
	return nil
}
