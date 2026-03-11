package dns

import (
	"context"
	"fmt"
	"strings"

	"github.com/bwesterb/go-zonefile"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	apiDTO "go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	powerdns "go.lumeweb.com/portal-plugin-ipfs/internal/dns/powerdns"
	"go.uber.org/zap"
)

// getZoneWithPowerDNS retrieves a zone and validates it has a PowerDNS zone ID
func (s *DNSServiceDefault) getZoneWithPowerDNS(ctx context.Context, zoneID uint) (*pluginDb.DNSZone, *powerdns.Zone, error) {
	zone, err := s.GetZone(ctx, zoneID)
	if err != nil {
		s.Logger().Error("Failed to get zone", zap.Error(err), zap.Uint("zone_id", zoneID))
		return nil, nil, fmt.Errorf("failed to get zone: %w", err)
	}

	if zone.PowerDNSZoneID == "" {
		return nil, nil, fmt.Errorf("zone not properly initialized in PowerDNS")
	}

	pdnsZone, err := s.pdnsClient.GetZone(ctx, zone.PowerDNSZoneID)
	if err != nil {
		s.Logger().Error("Failed to get PowerDNS zone", zap.Error(err), zap.String("powerdns_zone_id", zone.PowerDNSZoneID))
		return nil, nil, fmt.Errorf("failed to get PowerDNS zone: %w", err)
	}

	return zone, pdnsZone, nil
}

// buildFullName constructs the full DNS record name from a name and domain
func buildFullName(name, domain string) string {
	if name != domain && !strings.HasSuffix(name, "."+domain) {
		return name + "." + domain
	}
	return name
}

// stripDomain removes the domain suffix from a full DNS name
func stripDomain(name, domain string) string {
	if strings.HasSuffix(name, "."+domain) {
		return strings.TrimSuffix(name, "."+domain)
	}
	return name
}

// pointerTo returns a pointer to the given value
func pointerTo[T any](v T) *T {
	return &v
}

// getTTL returns the TTL value from a pointer, or a default if nil
func getTTL(ttlPtr *int) int {
	if ttlPtr != nil {
		return *ttlPtr
	}
	return 3600 // Default TTL
}

// getDisabled returns the disabled status from a pointer, or false if nil
func getDisabled(disabledPtr *bool) bool {
	if disabledPtr != nil {
		return *disabledPtr
	}
	return false
}

// FilterUserManageableRecords filters out PowerDNS-managed records, returning only user-manageable records.
// This is used for bulk operations where users should only manage their own records (e.g., excluding SOA, NS).
func FilterUserManageableRecords(records []powerdns.RRSet) []powerdns.RRSet {
	return lo.Filter(records, func(rrset powerdns.RRSet, _ int) bool {
		return !pluginCore.RecordType(rrset.Type).IsManagedByPowerDNS()
	})
}

// ParseZoneFile parses a BIND zone file content string and returns the parsed resource records.
// It handles standard BIND zone file directives including $ORIGIN, $TTL, and $INCLUDE.
// The function returns a slice of zonefile.Entry (resource records) or an error if parsing fails.
//
// Parameters:
//   - content: The zone file content as a string
//
// Returns:
//   - []zonefile.Entry: Slice of parsed resource records
//   - error: Error if parsing fails, with descriptive message including line number if available
//
// Edge cases handled:
//   - Empty zone files: Returns error with descriptive message
//   - Invalid syntax: Returns error with descriptive message and line number
//   - Comments and blank lines: Automatically handled by the parser
//   - Multiple directives: All directives ($ORIGIN, $TTL, $INCLUDE) are processed
//
// Example usage:
//
//	records, err := ParseZoneFile(zoneContent)
//	if err != nil {
//	    return fmt.Errorf("failed to parse zone file: %w", err)
//	}
//	for _, record := range records {
//	    fmt.Printf("Type: %s, Domain: %s\n", record.Type(), record.Domain())
//	}
func ParseZoneFile(content string) ([]zonefile.Entry, error) {
	if content == "" {
		return nil, fmt.Errorf("zone file content is empty")
	}

	data := []byte(content)
	zf, err := zonefile.Load(data)
	if err != nil {
		if lineErr, ok := err.(interface{ LineNo() int }); ok {
			return nil, fmt.Errorf("parse error at line %d: %w", lineErr.LineNo(), err)
		}
		return nil, fmt.Errorf("failed to parse zone file: %w", err)
	}

	// Filter out control entries (directives like $ORIGIN, $TTL, $INCLUDE)
	// Control entries have no Type field or have a command type
	entries := lo.Filter(zf.Entries(), func(entry zonefile.Entry, _ int) bool {
		// Control entries have empty Type or start with $ in Domain
		entryType := entry.Type()
		entryDomain := entry.Domain()

		// Skip if type is empty (control directive)
		if len(entryType) == 0 {
			return false
		}

		// Skip if domain starts with $ (control directive)
		if len(entryDomain) > 0 && entryDomain[0] == '$' {
			return false
		}

		return true
	})

	if len(entries) == 0 {
		return nil, fmt.Errorf("no resource records found in zone file")
	}

	return entries, nil
}

// Constants for DNS operations
const DefaultTTL uint = 3600

// getDefaultTTL returns the provided TTL or the default TTL (3600 seconds)
func getDefaultTTL(ttl uint) uint {
	return lo.Ternary(ttl == 0, uint(DefaultTTL), ttl)
}

// GetDefaultTTL is the exported version of getDefaultTTL for use by other packages
func GetDefaultTTL(ttl uint) uint {
	return getDefaultTTL(ttl)
}

// buildRRSet creates a PowerDNS RRSet with the given parameters
func buildRRSet(name, recordType string, changetype powerdns.RRSetChangetype, ttl *int, records []powerdns.Record) powerdns.RRSet {
	return powerdns.RRSet{
		Changetype: changetype,
		Name:       name,
		Type:       recordType,
		Ttl:        ttl,
		Records:    records,
	}
}

// recordToDTO converts a PowerDNS RRSet and Record to an API DNSRecord DTO
// It strips the domain from the record name and extracts TTL and disabled status
func recordToDTO(rrset powerdns.RRSet, record powerdns.Record, zoneDomain string) *apiDTO.DNSRecord {
	return &apiDTO.DNSRecord{
		Name:     stripDomain(rrset.Name, zoneDomain),
		Type:     rrset.Type,
		Content:  record.Content,
		TTL:      uint(getTTL(rrset.Ttl)),
		Disabled: getDisabled(record.Disabled),
	}
}

// buildCreatedRecord creates a CreatedRecord response from zone file entry data
func buildCreatedRecord(name, recordType, content string, ttl uint) apiDTO.CreatedRecord {
	return apiDTO.CreatedRecord{
		Name:    name,
		Type:    recordType,
		Content: content,
		TTL:     ttl,
	}
}

// buildImportZoneError creates an ImportZoneError response with formatted error message
func buildImportZoneError(name, recordType string, err error) apiDTO.ImportZoneError {
	return apiDTO.ImportZoneError{
		Name:  name,
		Type:  recordType,
		Error: err.Error(),
	}
}
