package dns

import (
	"context"
	"fmt"
	"strings"
	"time"

	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/dns/powerdns"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.lumeweb.com/queryutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// DNSLinkTarget represents a DNSLink target path
type DNSLinkTarget string

// CreateZone creates a new DNS zone
func (s *DNSServiceDefault) CreateZone(ctx context.Context, domain string, userID uint) (*pluginDb.DNSZone, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.CreateZone")
	defer span.End()

	// Validate domain format
	if err := s.validateDomain(domain); err != nil {
		return nil, fmt.Errorf("invalid domain: %w", err)
	}

	// Check if domain already exists in database (including soft-deleted)
	existing, err := s.GetZoneByDomain(ctx, domain)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing domain: %w", err)
	}
	if existing != nil {
		if existing.UserID != userID {
			return nil, fmt.Errorf("domain %q is already owned by another user", domain)
		}
		if existing.DeletedAt.Valid {
			if err := s.restoreSoftDeletedZone(ctx, existing, domain); err != nil {
				return nil, err
			}
		}
		return existing, nil
	}

	// Create zone in PowerDNS (idempotent: returns existing zone on 409)
	if s.pdnsClient == nil {
		return nil, fmt.Errorf("DNS hosting not enabled")
	}

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
		if isDuplicateKeyError(err) {
			s.Logger().Info("DNS zone already exists in database (concurrent create), fetching existing",
				zap.String("domain", domain))

			existing, getErr := s.GetZoneByDomain(ctx, domain)
			if getErr != nil {
				return nil, fmt.Errorf("concurrent zone creation detected but failed to fetch existing: %w", getErr)
			}
			if existing == nil {
				return nil, fmt.Errorf("concurrent zone creation detected but existing zone not found for domain %q", domain)
			}
			if existing.UserID != userID {
				return nil, fmt.Errorf("domain %q is already owned by another user", domain)
			}
			if existing.DeletedAt.Valid {
				if restoreErr := s.restoreSoftDeletedZone(ctx, existing, domain); restoreErr != nil {
					return nil, fmt.Errorf("concurrent zone creation detected but failed to restore soft-deleted zone: %w", restoreErr)
				}
			}
			return existing, nil
		}
		return nil, fmt.Errorf("failed to create zone in database: %w", err)
	}

	s.Logger().Debug("DNS zone created",
		zap.Uint("id", dnsZone.ID),
		zap.String("domain", domain),
		zap.Uint("user_id", userID),
		zap.String("powerdns_zone_id", *zone.Id))

	return dnsZone, nil
}

// GetZone retrieves a zone by ID
func (s *DNSServiceDefault) GetZone(ctx context.Context, zoneID uint) (*pluginDb.DNSZone, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.GetZone")
	defer span.End()

	var zone pluginDb.DNSZone

	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("id = ?", zoneID).First(&zone)
	})

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			s.Logger().Debug("DNS zone not found", zap.Uint("zone_id", zoneID))
			return nil, err
		}
		s.Logger().Error("Failed to get zone",
			zap.Error(err),
			zap.Uint("zone_id", zoneID))
		return nil, fmt.Errorf("failed to get zone: %w", err)
	}

	return &zone, nil
}

// GetZoneByDomain retrieves a zone by domain name, including soft-deleted zones
func (s *DNSServiceDefault) GetZoneByDomain(ctx context.Context, domain string) (*pluginDb.DNSZone, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.GetZoneByDomain")
	defer span.End()

	var zone pluginDb.DNSZone

	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Unscoped().Where("domain = ?", domain).First(&zone)
	})

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get zone by domain: %w", err)
	}

	return &zone, nil
}

// ListZones retrieves zones for a user with filtering, sorting, and pagination
func (s *DNSServiceDefault) ListZones(ctx context.Context, filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.DNSZone, int64, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.ListZones")
	defer span.End()

	var zones []*pluginDb.DNSZone
	var total int64

	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		query := tx.Model(&pluginDb.DNSZone{})

		// Apply filters, sorting, and pagination using queryutil helpers
		query = queryutil.ApplyFilters(query, filters, nil)
		query = queryutil.ApplySort(query, sorts)
		query = queryutil.ApplyPagination(query, pagination)

		// Count total matching records after applying filters (before pagination)
		countQuery := query.Session(&gorm.Session{})
		if err := countQuery.Count(&total).Error; err != nil {
			_ = tx.AddError(err)
			return tx
		}

		return query.Find(&zones)
	})

	if err != nil {
		s.Logger().Error("Failed to list zones",
			zap.Error(err))
		return nil, 0, fmt.Errorf("failed to list zones: %w", err)
	}

	return zones, total, nil
}

// UpdateZone updates zone status
func (s *DNSServiceDefault) UpdateZone(ctx context.Context, zoneID uint, status pluginDb.DNSZoneStatus) error {
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
func (s *DNSServiceDefault) DeleteZone(ctx context.Context, zoneID uint) error {
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
func (s *DNSServiceDefault) ValidateNameservers(ctx context.Context, zoneID uint) (bool, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.ValidateNameservers")
	defer span.End()

	zone, err := s.GetZone(ctx, zoneID)
	if err != nil {
		return false, err
	}
	if zone == nil {
		return false, fmt.Errorf("zone not found")
	}

	// Perform actual nameserver validation via DNS lookup
	// Query DNS for the domain's nameservers and compare against approved nameservers
	approvedNameservers := s.config.Nameservers
	if len(approvedNameservers) == 0 {
		return false, fmt.Errorf("no approved nameservers configured for validation")
	}

	// Lookup nameservers for the domain using DNS lookup interface
	dnsNameservers, err := s.dnsLookup.LookupNS(zone.Domain)
	if err != nil {
		// Update check timestamp even on failure to prevent retry loops
		now := time.Now()
		zone.LastNameserverCheckAt = &now
		_ = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			return tx.Save(zone)
		})
		return false, fmt.Errorf("failed to lookup nameservers for domain %s: %w", zone.Domain, err)
	}

	// Check if at least one approved nameserver is present in DNS response
	// Normalize by stripping trailing dots — net.LookupNS returns FQDNs with
	// trailing dots (e.g. "ns1.example.com.") but config values typically lack them.
	valid := false
	for _, approvedNS := range approvedNameservers {
		normalizedApproved := strings.TrimSuffix(approvedNS, ".")
		for _, dnsNS := range dnsNameservers {
			normalizedDNS := strings.TrimSuffix(dnsNS.Host, ".")
			if normalizedDNS == normalizedApproved {
				valid = true
				break
			}
		}
		if valid {
			break
		}
	}

	if !valid {
		now := time.Now()
		zone.LastNameserverCheckAt = &now
		_ = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			return tx.Save(zone)
		})
		return false, fmt.Errorf("no approved nameservers found in DNS for domain %s", zone.Domain)
	}

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

// buildTargetPath constructs the DNSLink target path based on target type.
// Per the dnslink spec (https://dnslink.info/), TXT record values must be
// prefixed with "dnslink=" (e.g., "dnslink=/ipns/<peerID>").
func buildTargetPath(targetHash string, targetType pluginDb.WebsiteTargetType) DNSLinkTarget {
	return DNSLinkTarget("dnslink=" + targetType.ToDNSLinkPath(targetHash))
}

// CreateWebsiteDNSRecords creates initial DNS records for a new website
func (s *DNSServiceDefault) CreateWebsiteDNSRecords(ctx context.Context, zoneID uint, targetHash string, targetType pluginDb.WebsiteTargetType, validationToken string) error {
	ctx, span := core.TraceMethod(ctx, "DNSService.CreateWebsiteDNSRecords")
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

	ttl := 300
	disabled := false
	targetPath := string(buildTargetPath(targetHash, targetType))

	rrsets := []powerdns.RRSet{
		{
			Name:       "_dnslink." + zone.Domain + ".",
			Type:       "TXT",
			Changetype: "REPLACE",
			Ttl:        &ttl,
			Records: []powerdns.Record{
				{
					Content:  formatTXTContent(targetPath),
					Disabled: &disabled,
				},
			},
		},
		{
			Name:       zone.Domain + ".",
			Type:       "TXT",
			Changetype: "REPLACE",
			Ttl:        &ttl,
			Records: []powerdns.Record{
				{
					Content:  formatTXTContent(validationToken),
					Disabled: &disabled,
				},
			},
		},
	}

	if s.config.GatewayDomain != "" {
		rrsets = append(rrsets, powerdns.RRSet{
			Name:       zone.Domain + ".",
			Type:       "ALIAS",
			Changetype: "REPLACE",
			Ttl:        &ttl,
			Records: []powerdns.Record{
				{
					Content:  s.config.GatewayDomain + ".",
					Disabled: &disabled,
				},
			},
		})
	}

	if err := s.pdnsClient.UpdateZoneRRSets(ctx, zone.PowerDNSZoneID, rrsets); err != nil {
		return fmt.Errorf("failed to create DNS records: %w", err)
	}

	s.Logger().Info("DNS records created for website",
		zap.Uint("zone_id", zoneID),
		zap.String("domain", zone.Domain),
		zap.String("target_hash", targetHash),
		zap.String("target_type", string(targetType)))

	return nil
}

// UpdateWebsiteDNSRecords updates DNS records for a website
func (s *DNSServiceDefault) UpdateWebsiteDNSRecords(ctx context.Context, zoneID uint, targetHash string, targetType pluginDb.WebsiteTargetType) error {
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

	ttl := 300
	disabled := false
	targetPath := string(buildTargetPath(targetHash, targetType))

	rrsets := []powerdns.RRSet{
		{
			Name:       "_dnslink." + zone.Domain + ".",
			Type:       "TXT",
			Changetype: "REPLACE",
			Ttl:        &ttl,
			Records: []powerdns.Record{
				{
					Content:  formatTXTContent(targetPath),
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
func (s *DNSServiceDefault) DeleteWebsiteDNSRecords(ctx context.Context, zoneID uint) error {
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

	rrsets := []powerdns.RRSet{
		{
			Name:       "_dnslink." + zone.Domain + ".",
			Type:       "TXT",
			Changetype: "DELETE",
		},
		{
			Name:       zone.Domain + ".",
			Type:       "TXT",
			Changetype: "DELETE",
		},
	}

	if s.config.GatewayDomain != "" {
		rrsets = append(rrsets, powerdns.RRSet{
			Name:       zone.Domain + ".",
			Type:       "ALIAS",
			Changetype: "DELETE",
		})
	}

	if err := s.pdnsClient.UpdateZoneRRSets(ctx, zone.PowerDNSZoneID, rrsets); err != nil {
		s.Logger().Warn("Failed to delete DNS records",
			zap.Error(err),
			zap.Uint("zone_id", zoneID))
		// Continue despite DNS cleanup failure
	}

	s.Logger().Info("DNS records deleted for website",
		zap.Uint("zone_id", zoneID),
		zap.String("domain", zone.Domain))

	return nil
}

// validateDomain validates the domain name format
func (s *DNSServiceDefault) validateDomain(domain string) error {
	if domain == "" {
		return fmt.Errorf("domain cannot be empty")
	}
	if len(domain) > 255 {
		return fmt.Errorf("domain too long (max 255 characters)")
	}

	trimmedDomain := strings.TrimSuffix(domain, ".")

	labels := strings.Split(trimmedDomain, ".")

	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 {
			return fmt.Errorf("domain label must be 1-63 characters")
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return fmt.Errorf("domain label cannot start or end with hyphen")
		}
		for _, c := range label {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-') {
				return fmt.Errorf("domain label contains invalid character: %c", c)
			}
		}
	}

	return nil
}

func (s *DNSServiceDefault) restoreSoftDeletedZone(ctx context.Context, zone *pluginDb.DNSZone, domain string) error {
	updates := map[string]interface{}{
		"deleted_at": nil,
		"status":     string(pluginDb.DNSZoneStatusPendingNameserver),
	}
	var newPowerDNSZoneID string
	if s.pdnsClient != nil {
		nameservers := s.config.Nameservers
		if len(nameservers) == 0 {
			return fmt.Errorf("no approved nameservers configured")
		}
		pdnsZone, pdnsErr := s.pdnsClient.CreateZone(ctx, domain, nameservers)
		if pdnsErr != nil {
			return fmt.Errorf("failed to recreate zone in PowerDNS: %w", pdnsErr)
		}
		newPowerDNSZoneID = *pdnsZone.Id
		updates["powerdns_zone_id"] = newPowerDNSZoneID
	}

	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Unscoped().Model(zone).Updates(updates)
	})
	if err != nil {
		if s.pdnsClient != nil && newPowerDNSZoneID != "" {
			if delErr := s.pdnsClient.DeleteZone(ctx, newPowerDNSZoneID); delErr != nil {
				s.Logger().Warn("Failed to clean up orphaned PowerDNS zone after DB restore failure",
					zap.Error(delErr), zap.String("powerdns_zone_id", newPowerDNSZoneID))
			}
		}
		return fmt.Errorf("failed to restore soft-deleted zone: %w", err)
	}
	zone.DeletedAt = gorm.DeletedAt{}
	zone.Status = string(pluginDb.DNSZoneStatusPendingNameserver)
	if newPowerDNSZoneID != "" {
		zone.PowerDNSZoneID = newPowerDNSZoneID
	}
	s.Logger().Info("Restored soft-deleted DNS zone",
		zap.Uint("id", zone.ID),
		zap.String("domain", domain))
	return nil
}

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "Duplicate entry") ||
		strings.Contains(msg, "duplicate key value")
}
