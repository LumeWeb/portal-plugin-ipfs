package dns

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	apiDTO "go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	powerdns "go.lumeweb.com/portal-plugin-ipfs/internal/dns/powerdns"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	"go.uber.org/zap"
)

// GetZoneRecords retrieves DNS records for a zone from PowerDNS
// Returns list of DNSRecord representing PowerDNS RRSets with filtering applied
func (s *DNSServiceDefault) GetZoneRecords(ctx context.Context, zoneID uint, filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*apiDTO.DNSRecord, int64, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.GetZoneRecords")
	defer span.End()

	zone, pdnsZone, err := s.getZoneWithPowerDNS(ctx, zoneID)
	if err != nil {
		return nil, 0, err
	}

	// Convert PowerDNS RRSets to DNSRecord struct using samber/lo
	var allRecords []*apiDTO.DNSRecord
	if pdnsZone.Rrsets != nil {
		allRecords = lo.FlatMap(*pdnsZone.Rrsets, func(rrset powerdns.RRSet, _ int) []*apiDTO.DNSRecord {
			if pluginCore.RecordType(rrset.Type).IsManagedByPowerDNS() || rrset.Records == nil || len(rrset.Records) == 0 {
				return []*apiDTO.DNSRecord{}
			}
			return lo.Map(rrset.Records, func(record powerdns.Record, _ int) *apiDTO.DNSRecord {
				return recordToDTOWithZoneID(rrset, record, zone.Domain, zoneID)
			})
		})
	}

	// Apply filters, sorting, and pagination in-memory
	filteredRecords := applyInMemoryFilters(allRecords, filters, sorts, pagination)

	total := int64(len(filteredRecords))

	// Apply pagination
	start, end := getPaginationRange(pagination, len(filteredRecords))
	paginatedRecords := lo.Slice(filteredRecords, start, end)

	return paginatedRecords, total, nil
}

// GetRRSet retrieves a specific DNS RRSet by name and type from PowerDNS
func (s *DNSServiceDefault) GetRRSet(ctx context.Context, zoneID uint, name string, recordType string) ([]*apiDTO.DNSRecord, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.GetRRSet")
	defer span.End()

	zone, pdnsZone, err := s.getZoneWithPowerDNS(ctx, zoneID)
	if err != nil {
		return nil, err
	}

	// Find matching RRSet
	fullName, err := buildFullName(name, zone.Domain)
	if err != nil {
		return nil, err
	}

	if pdnsZone.Rrsets != nil {
		matchingRRSet, found := lo.Find(*pdnsZone.Rrsets, func(rrset powerdns.RRSet) bool {
			return rrset.Name == fullName && rrset.Type == recordType
		})

		if found && matchingRRSet.Records != nil {
			records := lo.Map(matchingRRSet.Records, func(record powerdns.Record, _ int) *apiDTO.DNSRecord {
				return recordToDTOWithZoneID(matchingRRSet, record, zone.Domain, zoneID)
			})
			return records, nil
		}
	}

	return nil, fmt.Errorf("RRSet not found: %s %s", name, recordType)
}

// CreateRecord creates a new DNS record in PowerDNS via RRSet
func (s *DNSServiceDefault) CreateRecord(ctx context.Context, zoneID uint, name string, recordType string, content string, ttl uint) (*apiDTO.DNSRecord, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.CreateRecord")
	defer span.End()

	zone, _, err := s.getZoneWithPowerDNS(ctx, zoneID)
	if err != nil {
		return nil, err
	}

	fullName, err := buildFullName(name, zone.Domain)
	if err != nil {
		return nil, fmt.Errorf("invalid record name: %w", err)
	}

	pdnsContent := formatRecordContent(recordType, content)

	rrset := buildRRSet(fullName, recordType, powerdns.REPLACE, lo.ToPtr(int(ttl)), []powerdns.Record{{Content: pdnsContent}})

	err = s.pdnsClient.UpdateZoneRRSets(ctx, zone.PowerDNSZoneID, []powerdns.RRSet{rrset})
	if err != nil {
		s.Logger().Error("Failed to create RRSet in PowerDNS",
			zap.Error(err),
			zap.String("name", name),
			zap.String("type", recordType))
		return nil, fmt.Errorf("failed to create RRSet: %w", err)
	}

	s.Logger().Debug("DNS record created",
		zap.Uint("zone_id", zoneID),
		zap.String("name", name),
		zap.String("type", recordType))

	// Query the created record back to get complete data with all fields populated
	records, err := s.GetRRSet(ctx, zoneID, name, recordType)
	if err != nil {
		s.Logger().Error("Failed to retrieve created record",
			zap.Error(err),
			zap.Uint("zone_id", zoneID),
			zap.String("name", name),
			zap.String("type", recordType))
		return nil, fmt.Errorf("failed to retrieve created record: %w", err)
	}
	if len(records) == 0 {
		s.Logger().Error("No records found after creation",
			zap.Uint("zone_id", zoneID),
			zap.String("name", name),
			zap.String("type", recordType))
		return nil, fmt.Errorf("record not found after creation")
	}

	return records[0], nil
}

// UpdateRecord updates an existing DNS RRSet in PowerDNS
func (s *DNSServiceDefault) UpdateRecord(ctx context.Context, zoneID uint, name string, recordType string, records []string, ttl uint) ([]*apiDTO.DNSRecord, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.UpdateRecord")
	defer span.End()

	zone, _, err := s.getZoneWithPowerDNS(ctx, zoneID)
	if err != nil {
		return nil, err
	}

	fullName, err := buildFullName(name, zone.Domain)
	if err != nil {
		return nil, fmt.Errorf("invalid record name: %w", err)
	}

	pdnsRecords := lo.Map(records, func(r string, _ int) powerdns.Record {
		return powerdns.Record{Content: formatRecordContent(recordType, r)}
	})

	rrset := buildRRSet(fullName, recordType, powerdns.REPLACE, lo.ToPtr(int(ttl)), pdnsRecords)

	err = s.pdnsClient.UpdateZoneRRSets(ctx, zone.PowerDNSZoneID, []powerdns.RRSet{rrset})
	if err != nil {
		s.Logger().Error("Failed to update RRSet in PowerDNS",
			zap.Error(err),
			zap.String("name", name),
			zap.String("type", recordType))
		return nil, fmt.Errorf("failed to update RRSet: %w", err)
	}

	s.Logger().Debug("DNS record updated",
		zap.Uint("zone_id", zoneID),
		zap.String("name", name),
		zap.String("type", recordType))

	result := lo.Map(records, func(r string, _ int) *apiDTO.DNSRecord {
		return &apiDTO.DNSRecord{
			Name:     name,
			Type:     recordType,
			Content:  r,
			TTL:      ttl,
			Disabled: false,
		}
	})

	return result, nil
}

// DeleteRecord deletes a DNS RRSet from PowerDNS
func (s *DNSServiceDefault) DeleteRecord(ctx context.Context, zoneID uint, name string, recordType string) error {
	ctx, span := core.TraceMethod(ctx, "DNSService.DeleteRecord")
	defer span.End()

	zone, _, err := s.getZoneWithPowerDNS(ctx, zoneID)
	if err != nil {
		return err
	}

	fullName, err := buildFullName(name, zone.Domain)
	if err != nil {
		return fmt.Errorf("invalid record name: %w", err)
	}

	rrset := buildRRSet(fullName, recordType, powerdns.DELETE, nil, nil)

	err = s.pdnsClient.UpdateZoneRRSets(ctx, zone.PowerDNSZoneID, []powerdns.RRSet{rrset})
	if err != nil {
		s.Logger().Error("Failed to delete RRSet from PowerDNS",
			zap.Error(err),
			zap.String("name", name),
			zap.String("type", recordType))
		return fmt.Errorf("failed to delete RRSet: %w", err)
	}

	s.Logger().Debug("DNS record deleted",
		zap.Uint("zone_id", zoneID),
		zap.String("name", name),
		zap.String("type", recordType))

	return nil
}

// BulkDeleteRecords deletes multiple DNS records in a single PowerDNS API call
func (s *DNSServiceDefault) BulkDeleteRecords(ctx context.Context, zoneID uint, userID uint, records []apiDTO.RecordIdentifier, dryRun bool) (*apiDTO.BulkDeleteResponse, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.BulkDeleteRecords")
	defer span.End()

	zone, _, err := s.getZoneWithPowerDNS(ctx, zoneID)
	if err != nil {
		return nil, err
	}

	// Convert RecordIdentifiers to PowerDNS DELETE RRSet operations
	rrsets := make([]powerdns.RRSet, 0, len(records))
	for _, r := range records {
		fullName, err := buildFullName(r.Name, zone.Domain)
		if err != nil {
			return nil, fmt.Errorf("invalid record name %q: %w", r.Name, err)
		}
		rrsets = append(rrsets, buildRRSet(fullName, r.Type, powerdns.DELETE, nil, nil))
	}

	// If dryRun is true, return success results without actually deleting
	if dryRun {
		results := lo.Map(records, func(r apiDTO.RecordIdentifier, _ int) apiDTO.RecordResult {
			return apiDTO.RecordResult{
				Name:   r.Name,
				Type:   r.Type,
				Status: "success",
			}
		})
		s.Logger().Info("DNS bulk delete dry run",
			zap.Uint("user_id", userID),
			zap.Uint("zone_id", zoneID),
			zap.String("zone_domain", zone.Domain),
			zap.Int("record_count", len(records)),
			zap.Bool("dry_run", true))
		return &apiDTO.BulkDeleteResponse{Results: results}, nil
	}

	// Execute bulk delete using single PowerDNS API call
	err = s.pdnsClient.UpdateZoneRRSets(ctx, zone.PowerDNSZoneID, rrsets)

	// Transform records to results, capturing any errors
	results := lo.Map(records, func(r apiDTO.RecordIdentifier, _ int) apiDTO.RecordResult {
		if err != nil {
			return apiDTO.RecordResult{
				Name:   r.Name,
				Type:   r.Type,
				Status: "error",
				Error:  fmt.Sprintf("bulk delete failed: %v", err),
			}
		}
		return apiDTO.RecordResult{
			Name:   r.Name,
			Type:   r.Type,
			Status: "success",
		}
	})

	if err != nil {
		s.Logger().Error("Failed to bulk delete RRSets from PowerDNS",
			zap.Uint("user_id", userID),
			zap.Uint("zone_id", zoneID),
			zap.String("zone_domain", zone.Domain),
			zap.Int("record_count", len(records)),
			zap.Error(err))
		return &apiDTO.BulkDeleteResponse{Results: results}, nil
	}

	s.Logger().Info("DNS records bulk deleted successfully",
		zap.Uint("user_id", userID),
		zap.Uint("zone_id", zoneID),
		zap.String("zone_domain", zone.Domain),
		zap.Int("record_count", len(records)),
		zap.Bool("dry_run", false))

	return &apiDTO.BulkDeleteResponse{Results: results}, nil
}

func applyInMemoryFilters(records []*apiDTO.DNSRecord, filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) []*apiDTO.DNSRecord {
	if len(filters) == 0 && len(sorts) == 0 {
		return records
	}

	result := lo.Filter(records, func(record *apiDTO.DNSRecord, _ int) bool {
		for _, f := range filters {
			field := f.GetField()
			value := f.GetValue()
			operator := f.GetOperator()

			switch field {
			case "type":
				if !matchField(record.Type, value, operator) {
					return false
				}
			case "name":
				if !matchField(record.Name, value, operator) {
					return false
				}
			}
		}
		return true
	})

	// Apply sorting if specified
	if len(sorts) > 0 {
		sort.Slice(result, func(i, j int) bool {
			for _, s := range sorts {
				var valI, valJ string
				switch s.Field {
				case "name":
					valI, valJ = result[i].Name, result[j].Name
				case "type":
					valI, valJ = result[i].Type, result[j].Type
				case "content":
					valI, valJ = result[i].Content, result[j].Content
				default:
					continue
				}
				if valI != valJ {
					if s.Order == filter.OrderDesc {
						return valI > valJ
					}
					return valI < valJ
				}
			}
			return false
		})
	}

	return result
}

func matchField(field, value any, operator filter.Operator) bool {
	fieldStr, ok := field.(string)
	if !ok {
		return false
	}

	valueStr, ok := value.(string)
	if !ok {
		return false
	}

	switch operator {
	case filter.OpEq:
		return fieldStr == valueStr
	case filter.OpContains:
		return strings.Contains(strings.ToLower(fieldStr), strings.ToLower(valueStr))
	case filter.OpStartswith:
		return strings.HasPrefix(strings.ToLower(fieldStr), strings.ToLower(valueStr))
	case filter.OpEndswith:
		return strings.HasSuffix(strings.ToLower(fieldStr), strings.ToLower(valueStr))
	default:
		return fieldStr == valueStr
	}
}

func getPaginationRange(pagination queryutil.Pagination, total int) (start, end int) {
	start = lo.Ternary(pagination.Start == 0 && pagination.End == 0, 0, pagination.Start)
	end = lo.Ternary(pagination.Start == 0 && pagination.End == 0, total, lo.Min([]int{pagination.End, total}))
	return start, end
}

// ImportZoneFile imports DNS records from a BIND zone file into a zone.
// Supports three import modes: merge (add records, keep existing), replace (delete user records first), update (upsert behavior).
// When dryRun is true, the method parses and validates but doesn't make actual API calls.
func (s *DNSServiceDefault) ImportZoneFile(ctx context.Context, zoneID uint, zoneFileContent string, importMode apiDTO.ImportMode, dryRun bool) (*apiDTO.ImportZoneResponse, error) {
	ctx, span := core.TraceMethod(ctx, "DNSService.ImportZoneFile")
	defer span.End()

	response := &apiDTO.ImportZoneResponse{
		CreatedRecords: []apiDTO.CreatedRecord{},
		Errors:         []apiDTO.ImportZoneError{},
	}

	zone, pdnsZone, err := s.getZoneWithPowerDNS(ctx, zoneID)
	if err != nil {
		return response, fmt.Errorf("failed to get zone: %w", err)
	}

	entries, err := ParseZoneFile(zoneFileContent)
	if err != nil {
		response.Errors = append(response.Errors, apiDTO.ImportZoneError{
			Name:  "",
			Type:  "",
			Error: fmt.Sprintf("Failed to parse zone file: %v", err),
		})
		response.FailedCount++
		return response, fmt.Errorf("failed to parse zone file: %w", err)
	}

	// Filter out PowerDNS-managed records from existing zone
	existingUserRecords := []powerdns.RRSet{}
	if pdnsZone.Rrsets != nil {
		existingUserRecords = FilterUserManageableRecords(*pdnsZone.Rrsets)
	}

	// Build map of existing records for update mode
	existingRecordsMap := map[string]powerdns.RRSet{}
	for _, rrset := range existingUserRecords {
		key := fmt.Sprintf("%s:%s", rrset.Name, rrset.Type)
		existingRecordsMap[key] = rrset
	}

	// For replace mode, delete all existing user-manageable records first
	// This must happen BEFORE creating new records to avoid deleting records we just created
	if importMode == apiDTO.ImportModeReplace && !dryRun && len(existingUserRecords) > 0 {
		recordIdentifiers := lo.Map(existingUserRecords, func(rrset powerdns.RRSet, _ int) apiDTO.RecordIdentifier {
			return apiDTO.RecordIdentifier{
				Name: stripDomain(rrset.Name, zone.Domain),
				Type: rrset.Type,
			}
		})
		_, _ = s.BulkDeleteRecords(ctx, zoneID, zone.UserID, recordIdentifiers, false)
	}

	// Process each zone file entry
	for _, entry := range entries {
		recordType := string(entry.Type())
		name := string(entry.Domain())

		if pluginCore.RecordType(recordType).IsManagedByPowerDNS() {
			response.SkippedCount++
			continue
		}

		fullName, err := buildFullName(name, zone.Domain)
		if err != nil {
			response.Errors = append(response.Errors, apiDTO.ImportZoneError{
				Name:  name,
				Type:  recordType,
				Error: fmt.Sprintf("Invalid record name: %v", err),
			})
			response.FailedCount++
			continue
		}
		key := fmt.Sprintf("%s:%s", fullName, recordType)

		values := entry.Values()
		if len(values) == 0 || len(values[0]) == 0 {
			response.Errors = append(response.Errors, apiDTO.ImportZoneError{
				Name:  name,
				Type:  recordType,
				Error: "Record has no content",
			})
			response.FailedCount++
			continue
		}

		content := string(values[0][0])

		ttl := uint(3600)
		if entry.TTL() != nil {
			ttl = uint(*entry.TTL())
		}

		// Handle different import modes
		switch importMode {
		case apiDTO.ImportModeMerge:
			_, exists := existingRecordsMap[key]
			if exists {
				response.SkippedCount++
				continue
			}
			if dryRun {
				response.CreatedRecords = append(response.CreatedRecords, buildCreatedRecord(name, recordType, content, ttl))
				continue
			}
			_, err = s.CreateRecord(ctx, zoneID, name, recordType, content, ttl)
			if err != nil {
				response.Errors = append(response.Errors, buildImportZoneError(name, recordType, fmt.Errorf("failed to create record: %w", err)))
				response.FailedCount++
				continue
			}
			response.CreatedRecords = append(response.CreatedRecords, buildCreatedRecord(name, recordType, content, ttl))

		case apiDTO.ImportModeReplace:
			if dryRun {
				response.CreatedRecords = append(response.CreatedRecords, buildCreatedRecord(name, recordType, content, ttl))
				continue
			}
			_, err = s.CreateRecord(ctx, zoneID, name, recordType, content, ttl)
			if err != nil {
				response.Errors = append(response.Errors, buildImportZoneError(name, recordType, fmt.Errorf("failed to create record: %w", err)))
				response.FailedCount++
				continue
			}
			response.CreatedRecords = append(response.CreatedRecords, buildCreatedRecord(name, recordType, content, ttl))

		case apiDTO.ImportModeUpdate:
			_, exists := existingRecordsMap[key]
			if exists {
				if dryRun {
					response.CreatedRecords = append(response.CreatedRecords, buildCreatedRecord(name, recordType, content, ttl))
					continue
				}
				_, err = s.UpdateRecord(ctx, zoneID, name, recordType, []string{content}, ttl)
				if err != nil {
					response.Errors = append(response.Errors, buildImportZoneError(name, recordType, fmt.Errorf("failed to update record: %w", err)))
					response.FailedCount++
					continue
				}
				response.CreatedRecords = append(response.CreatedRecords, buildCreatedRecord(name, recordType, content, ttl))
			} else {
				if dryRun {
					response.CreatedRecords = append(response.CreatedRecords, buildCreatedRecord(name, recordType, content, ttl))
					continue
				}
				_, err = s.CreateRecord(ctx, zoneID, name, recordType, content, ttl)
				if err != nil {
					response.Errors = append(response.Errors, buildImportZoneError(name, recordType, fmt.Errorf("failed to create record: %w", err)))
					response.FailedCount++
					continue
				}
				response.CreatedRecords = append(response.CreatedRecords, buildCreatedRecord(name, recordType, content, ttl))
			}
		}
	}

	s.Logger().Info("DNS zone import completed",
		zap.Uint("zone_id", zoneID),
		zap.String("import_mode", string(importMode)),
		zap.Bool("dry_run", dryRun),
		zap.Int("created_count", len(response.CreatedRecords)),
		zap.Int("skipped_count", response.SkippedCount),
		zap.Int("failed_count", response.FailedCount))

	return response, nil
}
