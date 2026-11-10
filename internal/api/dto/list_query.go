package dto

import (
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/queryutil/filter"
)

var (
	comparisonField = filter.FieldRule{Operators: append(filter.ComparableOperators(), filter.OpNull, filter.OpNnull)}
	textField       = filter.FieldRule{Operators: []filter.Operator{
		filter.OpEq, filter.OpNe, filter.OpIn, filter.OpNin, filter.OpNull, filter.OpNnull,
		filter.OpContains, filter.OpContainss, filter.OpNcontains, filter.OpNcontainss,
		filter.OpStartswith, filter.OpStartswiths, filter.OpNstartswith, filter.OpNstartswiths,
		filter.OpEndswith, filter.OpEndswiths, filter.OpNendswith, filter.OpNendswiths,
	}}
	booleanField = filter.FieldRule{Operators: []filter.Operator{filter.OpEq, filter.OpNe, filter.OpNull, filter.OpNnull}}
	cidField     = filter.FieldRule{Operators: []filter.Operator{filter.OpEq, filter.OpNe, filter.OpIn, filter.OpNin}, Convert: convertCIDFilter}
	pathField    = filter.FieldRule{Operators: []filter.Operator{filter.OpEq}, Convert: convertPathFilter}

	pinListPolicy = filter.QueryPolicy{
		Fields: map[string]filter.FieldRule{
			"id": comparisonField, "created_at": comparisonField, "updated_at": comparisonField,
			"name": textField, "status": textField, "cid": cidField,
		},
		SortFields: []string{"id", "created_at", "updated_at", "name", "status"},
	}
	IPNSKeyListPolicy = filter.QueryPolicy{
		Fields: map[string]filter.FieldRule{
			"id": comparisonField, "created_at": comparisonField, "last_published_at": comparisonField,
			"name": textField, "last_published_cid": textField,
		},
		SortFields: []string{"id", "name", "created_at", "last_published_at", "last_published_cid"},
	}
	FileListPolicy = filter.QueryPolicy{
		Fields: map[string]filter.FieldRule{
			"id": comparisonField, "type": comparisonField, "size": comparisonField, "depth": comparisonField,
			"created_at": comparisonField, "updated_at": comparisonField,
			"name": textField, "path": textField, "parent_path": pathField, "cid": cidField,
			"is_directory": booleanField, "is_orphan": booleanField,
		},
		SortFields: []string{"id", "name", "path", "parent_path", "type", "size", "depth", "is_directory", "is_orphan", "created_at", "updated_at"},
	}
	WebsiteListPolicy = filter.QueryPolicy{
		Fields: map[string]filter.FieldRule{
			"id": comparisonField, "created_at": comparisonField, "updated_at": comparisonField, "last_checked_at": comparisonField,
			"target_type": textField, "status": textField,
			"domain": {Operators: []filter.Operator{filter.OpEq}},
		},
		SortFields: []string{"id", "created_at", "updated_at", "last_checked_at", "target_type", "status"},
	}
	WebsiteDomainListPolicy = filter.QueryPolicy{
		Fields: map[string]filter.FieldRule{
			"id": comparisonField, "created_at": comparisonField, "updated_at": comparisonField,
			"ssl_issued_at": comparisonField, "ssl_last_updated_at": comparisonField,
			"domain": textField, "namespace": textField, "status": textField, "ssl_status": textField,
			"dns_hosting_enabled": booleanField,
		},
		SortFields: []string{"id", "domain", "namespace", "status", "ssl_status", "dns_hosting_enabled", "created_at", "updated_at", "ssl_issued_at", "ssl_last_updated_at"},
	}
	WorkspaceListPolicy = filter.QueryPolicy{
		Fields: map[string]filter.FieldRule{
			"id": comparisonField, "website_id": comparisonField, "platform_domain_id": comparisonField,
			"created_at": comparisonField, "updated_at": comparisonField,
			"label": textField, "status": textField,
		},
		SortFields: []string{"id", "website_id", "platform_domain_id", "label", "status", "created_at", "updated_at"},
	}
	DNSZoneListPolicy = filter.QueryPolicy{
		Fields: map[string]filter.FieldRule{
			"id": comparisonField, "created_at": comparisonField, "updated_at": comparisonField,
			"last_nameserver_check_at": comparisonField, "nameservers_verified_at": comparisonField,
			"domain": textField, "status": textField,
		},
		SortFields: []string{"id", "domain", "status", "created_at", "updated_at", "last_nameserver_check_at", "nameservers_verified_at"},
	}
	DNSRecordListPolicy = filter.QueryPolicy{
		Fields: map[string]filter.FieldRule{
			"name": {Operators: []filter.Operator{filter.OpEq, filter.OpContains, filter.OpStartswith, filter.OpEndswith}},
			"type": {Operators: []filter.Operator{filter.OpEq, filter.OpContains, filter.OpStartswith, filter.OpEndswith}},
		},
		SortFields: []string{"name", "type", "content"},
	}
	PlatformDomainListPolicy = filter.QueryPolicy{
		Fields: map[string]filter.FieldRule{
			"id": comparisonField, "created_at": comparisonField, "updated_at": comparisonField,
			"domain": textField, "namespace": textField, "enabled": booleanField,
		},
		SortFields: []string{"id", "domain", "namespace", "enabled", "created_at", "updated_at"},
	}
	EnabledPlatformDomainListPolicy = filter.QueryPolicy{}
	DirectoryListPolicy             = filter.QueryPolicy{
		Fields: map[string]filter.FieldRule{"parent_path": {Operators: []filter.Operator{filter.OpEq}, Convert: func(op filter.Operator, value any) (any, error) {
			if value == "" {
				value = "/"
			}
			return convertPathFilter(op, value)
		}}},
	}
	BreadcrumbListPolicy = filter.QueryPolicy{Fields: map[string]filter.FieldRule{"path": pathField}}
)

// PinListPolicy includes only the metadata paths supplied by the pin query.
// QueryPolicy remains responsible for validating the filter tree and operators.
func PinListPolicy(filters []filter.CrudFilter) filter.QueryPolicy {
	policy := filter.QueryPolicy{Fields: make(map[string]filter.FieldRule), SortFields: pinListPolicy.SortFields}
	for name, rule := range pinListPolicy.Fields {
		policy.Fields[name] = rule
	}
	pending := append([]filter.CrudFilter(nil), filters...)
	visited := make(map[*filter.ConditionalFilter]bool)
	for len(pending) > 0 {
		node := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		switch f := node.(type) {
		case *filter.LogicalFilter:
			if f != nil && strings.HasPrefix(f.Field(), "meta.") {
				policy.Fields[f.Field()] = filter.FieldRule{Operators: []filter.Operator{filter.OpEq}}
			}
		case *filter.ConditionalFilter:
			// Visit shared groups once so collecting paths cannot loop on cycles.
			if f != nil && !visited[f] {
				visited[f] = true
				pending = append(pending, f.Filters...)
			}
		}
	}
	return policy
}

func convertPathFilter(_ filter.Operator, value any) (any, error) {
	path, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("path must be text")
	}
	return ValidateFileManagerPath(path)
}

func convertCIDFilter(op filter.Operator, value any) (any, error) {
	if op == filter.OpIn || op == filter.OpNin {
		values, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("CID list required")
		}
		result := make([]any, len(values))
		for i, item := range values {
			var err error
			result[i], err = decodeFilterCID(item)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	return decodeFilterCID(value)
}

func decodeFilterCID(value any) ([]byte, error) {
	var c cid.Cid
	var err error
	switch v := value.(type) {
	case string:
		c, err = cid.Parse(v)
	case []byte:
		c, err = cid.Cast(v)
	default:
		return nil, fmt.Errorf("CID must be text or bytes")
	}
	if err != nil {
		return nil, fmt.Errorf("invalid CID")
	}
	return encoding.NormalizeCid(c).Bytes(), nil
}
