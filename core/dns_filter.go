package core

import (
	"context"

	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/queryutil/filter"
)

// RecordType represents a DNS record type
type RecordType string

// DNS record type constants
const (
	RecordTypeSOA     RecordType = "SOA"
	RecordTypeNS      RecordType = "NS"
	RecordTypeA       RecordType = "A"
	RecordTypeAAAA    RecordType = "AAAA"
	RecordTypeCNAME   RecordType = "CNAME"
	RecordTypeMX      RecordType = "MX"
	RecordTypeTXT     RecordType = "TXT"
	RecordTypeSRV     RecordType = "SRV"
	RecordTypePTR     RecordType = "PTR"
	RecordTypeCAA     RecordType = "CAA"
	RecordTypeSPF     RecordType = "SPF"
	RecordTypeTLSA    RecordType = "TLSA"
	RecordTypeSSHFP   RecordType = "SSHFP"
	RecordTypeHINFO   RecordType = "HINFO"
	RecordTypeRP      RecordType = "RP"
	RecordTypeDNAME   RecordType = "DNAME"
	RecordTypeNAPTR   RecordType = "NAPTR"
	RecordTypeLOC     RecordType = "LOC"
	RecordTypeDS      RecordType = "DS"
	RecordTypeDNSKEY  RecordType = "DNSKEY"
	RecordTypeRRSIG   RecordType = "RRSIG"
	RecordTypeNSEC    RecordType = "NSEC"
	RecordTypeNSEC3   RecordType = "NSEC3"
	RecordTypeOPT     RecordType = "OPT"
	RecordTypeANY     RecordType = "ANY"
	RecordTypeALIAS   RecordType = "ALIAS"
)

// IsManagedByPowerDNS returns true if the record type is managed by PowerDNS
// and should not be directly modified by users
func (rt RecordType) IsManagedByPowerDNS() bool {
	return rt == RecordTypeSOA || rt == RecordTypeNS
}

// IsValid returns true if the record type is a valid DNS record type
func (rt RecordType) IsValid() bool {
	switch rt {
	case RecordTypeSOA, RecordTypeNS, RecordTypeA, RecordTypeAAAA,
		RecordTypeCNAME, RecordTypeMX, RecordTypeTXT, RecordTypeSRV,
		RecordTypePTR, RecordTypeCAA, RecordTypeSPF, RecordTypeTLSA,
		RecordTypeSSHFP, RecordTypeHINFO, RecordTypeRP, RecordTypeDNAME,
		RecordTypeNAPTR, RecordTypeLOC, RecordTypeDS, RecordTypeDNSKEY,
		RecordTypeRRSIG, RecordTypeNSEC, RecordTypeNSEC3, RecordTypeOPT,
		RecordTypeANY, RecordTypeALIAS:
		return true
	default:
		return false
	}
}

// DNSRecordParser parses DNS record list requests into queryutil filters
type DNSRecordParser struct {
	ctx    context.Context
	filter dto.RecordListRequest
}

func NewDNSRecordParser(ctx context.Context, filter dto.RecordListRequest) *DNSRecordParser {
	return &DNSRecordParser{ctx: ctx, filter: filter}
}

func (p *DNSRecordParser) ParseFilters() ([]filter.CrudFilter, error) {
	var crudFilters []filter.CrudFilter

	if p.filter.Type != "" {
		crudFilters = append(crudFilters, filter.StringField("type").Eq(p.filter.Type))
	}

	if p.filter.Name != "" {
		crudFilters = append(crudFilters, filter.StringField("name").Contains(p.filter.Name))
	}

	return crudFilters, nil
}

func (p *DNSRecordParser) ParseSorts(_ *filter.SortConfig) ([]filter.Sort, error) {
	return []filter.Sort{
		{
			Field: "name",
			Order: filter.OrderAsc,
		},
	}, nil
}

func (p *DNSRecordParser) ParsePagination() (filter.Pagination, error) {
	return filter.NewPagination(0, int(p.filter.Limit))
}
