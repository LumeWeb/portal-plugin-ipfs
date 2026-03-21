package dto

// DNS DTOs for DNS/Domains API
// This file contains request and response DTOs for DNS zone and record management

import (
	"time"

	"github.com/Oudwins/zog"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// Compile-time interface implementation assertions
var _ httputil.DTOResponse[any] = (*ValidationResponse)(nil)

// Zone DTOs

// ZoneRequest represents a request to create or update a DNS zone
type ZoneRequest struct {
	Domain      string `json:"domain"`
	Nameservers []string `json:"nameservers,omitempty"`
}

func (r ZoneRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Domain":      zog.String().Required().Min(1).Max(255),
		"Nameservers": zog.Slice(zog.String()).Optional(),
	})
}

func (r *ZoneRequest) ToModel() (*db.DNSZone, error) {
	zone := &db.DNSZone{
		Domain: r.Domain,
		Status: string(db.DNSZoneStatusPendingNameserver),
	}
	return zone, nil
}

var _ httputil.DTOValidator = (*ZoneRequest)(nil)

// ZoneResponse represents a DNS zone response
type ZoneResponse struct {
	ID             uint       `json:"id"`
	UserID         uint       `json:"user_id"`
	Domain         string     `json:"domain"`
	Status         string     `json:"status"`
	PowerDNSZoneID string     `json:"powerdns_zone_id,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (r *ZoneResponse) FromModel(model *db.DNSZone) error {
	r.ID = model.ID
	r.UserID = model.UserID
	r.Domain = model.Domain
	r.Status = model.Status
	if model.PowerDNSZoneID != "" {
		r.PowerDNSZoneID = model.PowerDNSZoneID
	}
	r.CreatedAt = model.CreatedAt
	r.UpdatedAt = model.UpdatedAt
	return nil
}

var _ httputil.DTOResponse[*db.DNSZone] = (*ZoneResponse)(nil)

// ZoneListResponse represents a DNS zone in a list response
type ZoneListResponse struct {
	ID             uint       `json:"id"`
	UserID         uint       `json:"user_id"`
	Domain         string     `json:"domain"`
	Status         string     `json:"status"`
	PowerDNSZoneID string     `json:"powerdns_zone_id,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (r *ZoneListResponse) FromModel(model *db.DNSZone) error {
	r.ID = model.ID
	r.UserID = model.UserID
	r.Domain = model.Domain
	r.Status = model.Status
	if model.PowerDNSZoneID != "" {
		r.PowerDNSZoneID = model.PowerDNSZoneID
	}
	r.CreatedAt = model.CreatedAt
	r.UpdatedAt = model.UpdatedAt
	return nil
}

// ZoneListRequest represents a request to list DNS zones with filters
type ZoneListRequest struct {
	Status string `json:"status,omitempty"` // Filter by status
	Domain string `json:"domain,omitempty"` // Filter by domain (partial match)
}

// ZoneListResponseResponse is a swagger-only DTO that represents the paginated response for DNS zones.
// It merges the generic queryutil.Response[*dto.ZoneListResponse] for OpenAPI documentation.
//
// This struct exists due to a TODO bug where queryutil.Response generics are not getting detected
// properly as an array type in the swagger documentation generation. By providing a concrete struct,
// we ensure the swagger docs correctly show the data field as an array of ZoneListResponse items.
//
// Note: This struct is only used for swagger documentation, not for actual encoding.
type ZoneListResponseResponse struct {
	Data  []ZoneListResponse `json:"data"`
	Total int64              `json:"total"`
}

func (r ZoneListRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Status": config.ZogStringLike[string]().OneOf([]string{
			string(db.DNSZoneStatusPendingNameserver),
			string(db.DNSZoneStatusActive),
		}).Optional(),
		"Domain": zog.String().Optional(),
	})
}

func (r ZoneListRequest) ToModel() (ZoneListRequest, error) {
	return r, nil
}

// DNSRecord represents a DNS record from PowerDNS
// This is a data structure for API responses, records are managed entirely by PowerDNS
type DNSRecord struct {
	ID        uint      `json:"id"`
	ZoneID    uint      `json:"zone_id"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	Content   string    `json:"content"`
	TTL       uint      `json:"ttl"`
	Disabled  bool      `json:"disabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Record DTOs

// RecordRequest represents a request to create or update a DNS record
type RecordRequest struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Content string `json:"content"`
	TTL     uint   `json:"ttl,omitempty"`
	Disabled bool  `json:"disabled,omitempty"`
}

func (r RecordRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Name":    zog.String().Required().Min(1).Max(255),
		"Type":    zog.String().Required().OneOf([]string{
			"A", "AAAA", "CNAME", "MX", "TXT", "NS", "SRV", "ALIAS",
		}),
		"Content": zog.String().Required().Min(1).Max(1024),
		"TTL":     zog.UintLike[uint]().Optional(),
		"Disabled": zog.Bool().Optional(),
	})
}

func (r *RecordRequest) ToModel() (*RecordRequest, error) {
	return r, nil
}

var _ httputil.DTOValidator = (*RecordRequest)(nil)

// RecordResponse represents a DNS record response
type RecordResponse struct {
	ID        uint      `json:"id"`
	ZoneID    uint      `json:"zone_id"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	Content   string    `json:"content"`
	TTL       uint      `json:"ttl"`
	Disabled  bool      `json:"disabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (r *RecordResponse) FromModel(model *DNSRecord) error {
	r.ID = model.ID
	r.ZoneID = model.ZoneID
	r.Name = model.Name
	r.Type = model.Type
	r.Content = model.Content
	r.TTL = model.TTL
	r.Disabled = model.Disabled
	r.CreatedAt = model.CreatedAt
	r.UpdatedAt = model.UpdatedAt
	return nil
}

// RecordListRequest represents a request to list DNS records with filters
type RecordListRequest struct {
	Type  string `json:"type,omitempty"`  // Filter by record type
	Name  string `json:"name,omitempty"`  // Filter by name (partial match)
	Limit uint   `json:"limit,omitempty"` // Maximum number of records to return
}

// RecordResponseResponse is a swagger-only DTO that represents the paginated response for DNS records.
// It merges the generic queryutil.Response[*dto.RecordResponse] for OpenAPI documentation.
//
// This struct exists due to a TODO bug where queryutil.Response generics are not getting detected
// properly as an array type in the swagger documentation generation. By providing a concrete struct,
// we ensure the swagger docs correctly show the data field as an array of RecordResponse items.
//
// Note: This struct is only used for swagger documentation, not for actual encoding.
type RecordResponseResponse struct {
	Data  []RecordResponse `json:"data"`
	Total int64            `json:"total"`
}

func (r RecordListRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Type": zog.String().Optional(),
		"Name": zog.String().Optional(),
	})
}

func (r RecordListRequest) ToModel() (RecordListRequest, error) {
	return r, nil
}

// RecordIdentifier represents a DNS record identifier for deletion operations
type RecordIdentifier struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// BulkRecordRequest represents a request to bulk create/update DNS records
type BulkRecordRequest struct {
	Records []RecordRequest `json:"records"`
}

func (r BulkRecordRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Records": zog.Slice(zog.Struct(zog.Shape{
			"Name":    zog.String().Required().Min(1).Max(255),
			"Type":    zog.String().Required().OneOf([]string{
				"A", "AAAA", "CNAME", "MX", "TXT", "NS", "SRV", "ALIAS",
			}),
			"Content": zog.String().Required().Min(1).Max(1024),
			"TTL":     zog.UintLike[uint]().Optional(),
			"Disabled": zog.Bool().Optional(),
		})).Required(),
	})
}

func (r BulkRecordRequest) ToModel() (BulkRecordRequest, error) {
	return r, nil
}

// BulkDeleteRequest represents a request to bulk delete DNS records
type BulkDeleteRequest struct {
	Records []RecordIdentifier `json:"records"`
	DryRun  bool               `json:"dry_run,omitempty"`
}

func (r BulkDeleteRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Records": zog.Slice(zog.Struct(zog.Shape{
			"Name": zog.String().Required().Min(1).Max(255),
			"Type": zog.String().Required().OneOf([]string{
				"A", "AAAA", "CNAME", "MX", "TXT", "NS", "SRV", "ALIAS",
			}),
		})).Required().Min(1),
		"DryRun": zog.Bool().Optional(),
	})
}

func (r BulkDeleteRequest) ToModel() (BulkDeleteRequest, error) {
	return r, nil
}

var _ httputil.DTOValidator = (*BulkDeleteRequest)(nil)

// ImportMode represents the import mode for DNS zone files
type ImportMode string

// DNS zone import mode constants
const (
	ImportModeMerge   ImportMode = "merge"   // Add records from zone file, keep existing records
	ImportModeReplace ImportMode = "replace" // Delete all user-manageable records first, then import new records
	ImportModeUpdate  ImportMode = "update"  // Upsert behavior (modify existing records, add new records)
)

// IsValid returns true if the import mode is a valid import mode
func (im ImportMode) IsValid() bool {
	switch im {
	case ImportModeMerge, ImportModeReplace, ImportModeUpdate:
		return true
	default:
		return false
	}
}

// ImportZoneRequest represents a request to import a DNS zone from BIND zone file content
type ImportZoneRequest struct {
	ZoneFileContent string `json:"zone_file_content"`
	ImportMode      string `json:"import_mode"`
	DryRun          bool   `json:"dry_run,omitempty"`
}

func (r ImportZoneRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"ZoneFileContent": zog.String().Required().Min(1).Max(10 * 1024 * 1024),
		"ImportMode":      zog.String().Required().OneOf([]string{string(ImportModeMerge), string(ImportModeReplace), string(ImportModeUpdate)}),
		"DryRun":          zog.Bool().Optional(),
	})
}

func (r *ImportZoneRequest) ToModel() (*ImportZoneRequest, error) {
	return r, nil
}

var _ httputil.DTOValidator = (*ImportZoneRequest)(nil)

// RecordResult represents the result of a single DNS record deletion
type RecordResult struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Status string `json:"status"` // "success" or "error"
	Error  string `json:"error,omitempty"`
}

// BulkDeleteResponse represents the response from a bulk delete operation
type BulkDeleteResponse struct {
	Results []RecordResult `json:"results"`
}

func (r BulkDeleteResponse) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Results": zog.Slice(zog.Struct(zog.Shape{
			"Name":   zog.String().Required().Min(1).Max(255),
			"Type":   zog.String().Required().OneOf([]string{
				"A", "AAAA", "CNAME", "MX", "TXT", "NS", "SRV", "ALIAS",
			}),
			"Status": zog.String().Required().OneOf([]string{"success", "error"}),
			"Error":  zog.String().Optional(),
		})).Required(),
	})
}

var _ httputil.DTOValidator = (*BulkDeleteResponse)(nil)

// ImportZoneError represents an error that occurred during zone import
type ImportZoneError struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Error string `json:"error"`
}

// CreatedRecord represents a DNS record that was successfully created during zone import
type CreatedRecord struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Content string `json:"content"`
	TTL     uint   `json:"ttl"`
}

// ImportZoneResponse represents the response from a DNS zone import operation
type ImportZoneResponse struct {
	CreatedRecords []CreatedRecord  `json:"created_records"`
	SkippedCount   int              `json:"skipped_count"`
	FailedCount    int              `json:"failed_count"`
	Errors         []ImportZoneError `json:"errors"`
}

func (r ImportZoneResponse) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"CreatedRecords": zog.Slice(zog.Struct(zog.Shape{
			"Name":    zog.String().Required().Min(1).Max(255),
			"Type":    zog.String().Required().OneOf([]string{
				"A", "AAAA", "CNAME", "MX", "TXT", "NS", "SRV", "ALIAS",
			}),
			"Content": zog.String().Required().Min(1).Max(1024),
			"TTL":     zog.UintLike[uint]().Optional(),
		})).Optional(),
		"SkippedCount": zog.IntLike[int]().Optional(),
		"FailedCount":  zog.IntLike[int]().Optional(),
		"Errors": zog.Slice(zog.Struct(zog.Shape{
			"Name":  zog.String().Required().Min(1).Max(255),
			"Type":  zog.String().Required().OneOf([]string{
				"A", "AAAA", "CNAME", "MX", "TXT", "NS", "SRV", "ALIAS",
			}),
			"Error": zog.String().Required().Min(1).Max(1024),
		})).Optional(),
	})
}

var _ httputil.DTOValidator = (*ImportZoneResponse)(nil)

// ZoneStatusResponse represents a DNS zone status response
type ZoneStatusResponse struct {
	Status                 string     `json:"status"`
	NameserversVerified    bool       `json:"nameservers_verified"`
	LastNameserverCheckAt *time.Time `json:"last_nameserver_check_at,omitempty"`
	NameserversVerifiedAt  *time.Time `json:"nameservers_verified_at,omitempty"`
}

func (r *ZoneStatusResponse) FromModel(model *db.DNSZone) error {
	r.Status = model.Status
	r.NameserversVerified = model.IsActive()
	r.LastNameserverCheckAt = model.LastNameserverCheckAt
	r.NameserversVerifiedAt = model.NameserversVerifiedAt
	return nil
}

// ValidationResponse represents a DNS validation response
type ValidationResponse struct {
	Valid      bool     `json:"valid"`
	Message    string   `json:"message"`
	Nameservers []string `json:"nameservers,omitempty"`
	CheckedAt  time.Time `json:"checked_at"`
}

func (r *ValidationResponse) FromModel(model any) error {
	return nil
}

// RecordListResponse represents a DNS record in a list response
type RecordListResponse struct {
	ID        uint      `json:"id"`
	ZoneID    uint      `json:"zone_id"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	Content   string    `json:"content"`
	TTL       uint      `json:"ttl"`
	Disabled  bool      `json:"disabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (r *RecordListResponse) FromModel(model any) error {
	if dnsRecord, ok := model.(*DNSRecord); ok {
		r.ID = dnsRecord.ID
		r.ZoneID = dnsRecord.ZoneID
		r.Name = dnsRecord.Name
		r.Type = dnsRecord.Type
		r.Content = dnsRecord.Content
		r.TTL = dnsRecord.TTL
		r.Disabled = dnsRecord.Disabled
		r.CreatedAt = dnsRecord.CreatedAt
		r.UpdatedAt = dnsRecord.UpdatedAt
	}
	return nil
}

// BulkRecordsResponse represents a bulk operation result for DNS records
type BulkRecordsResponse struct {
	Records []RecordResponse `json:"records"`
}

// FromModel implements DTOResponse[*BulkRecordsResponse] interface
// Since BulkRecordsResponse is already a response wrapper with populated data,
// this method does nothing (no conversion needed)
func (r *BulkRecordsResponse) FromModel(_ *BulkRecordsResponse) error {
	return nil
}

var _ httputil.DTOResponse[*BulkRecordsResponse] = (*BulkRecordsResponse)(nil)
