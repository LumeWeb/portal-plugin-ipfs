package dto

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

// ListQueryResource selects the public fields and operations of a list endpoint.
type ListQueryResource uint8

const (
	QueryPins ListQueryResource = iota + 1
	QueryIPNSKeys
	QueryFiles
	QueryWebsites
	QueryWebsiteDomains
	QueryWorkspaces
	QueryDNSZones
	QueryDNSRecords
	QueryPlatformDomains
	QueryEnabledPlatformDomains
	QueryDirectory
	QueryBreadcrumbs
)

type listQueryPolicy struct {
	policy       filter.QueryPolicy
	stringFields []string
}

var (
	// These narrower sets express endpoint contracts, not the library's full scalar set.
	textOps   = []filter.Operator{filter.OpEq, filter.OpNe, filter.OpIn, filter.OpNin, filter.OpNull, filter.OpNnull, filter.OpContains, filter.OpContainss, filter.OpNcontains, filter.OpNcontainss, filter.OpStartswith, filter.OpStartswiths, filter.OpNstartswith, filter.OpNstartswiths, filter.OpEndswith, filter.OpEndswiths, filter.OpNendswith, filter.OpNendswiths}
	boolOps   = []filter.Operator{filter.OpEq, filter.OpNe, filter.OpNull, filter.OpNnull}
	binaryOps = []filter.Operator{filter.OpEq, filter.OpNe, filter.OpIn, filter.OpNin}
)

// Definitions stay private: normalization must never mutate shared maps or slices.
var listQueryPolicies = map[ListQueryResource]listQueryPolicy{
	QueryPins: makeListPolicy("id created_at updated_at", "name status", "", "id created_at updated_at name status", map[string]filter.FieldRule{
		"cid": {Operators: binaryOps, Convert: convertListCID},
	}),
	QueryIPNSKeys: makeListPolicy("id created_at last_published_at", "name last_published_cid", "", "id name created_at last_published_at last_published_cid", nil),
	QueryFiles: makeListPolicy("id type size depth created_at updated_at", "name path", "is_directory is_orphan", "id name path parent_path type size depth is_directory is_orphan created_at updated_at", map[string]filter.FieldRule{
		"parent_path": {Operators: []filter.Operator{filter.OpEq}, Convert: convertRequiredListPath},
		"cid":         {Operators: binaryOps, Convert: convertListCID},
	}),
	QueryWebsites: makeListPolicy("id created_at updated_at last_checked_at", "target_type status", "", "id created_at updated_at last_checked_at target_type status", map[string]filter.FieldRule{
		"domain": {Operators: []filter.Operator{filter.OpEq}, Convert: convertListText},
	}),
	QueryWebsiteDomains: makeListPolicy("id created_at updated_at ssl_issued_at ssl_last_updated_at", "domain namespace status ssl_status", "dns_hosting_enabled", "id domain namespace status ssl_status dns_hosting_enabled created_at updated_at ssl_issued_at ssl_last_updated_at", nil),
	QueryWorkspaces:     makeListPolicy("id website_id platform_domain_id created_at updated_at", "label status", "", "id website_id platform_domain_id label status created_at updated_at", nil),
	QueryDNSZones:       makeListPolicy("id created_at updated_at last_nameserver_check_at nameservers_verified_at", "domain status", "", "id domain status created_at updated_at last_nameserver_check_at nameservers_verified_at", nil),
	QueryDNSRecords: {
		policy: filter.QueryPolicy{
			Fields: map[string]filter.FieldRule{
				"name": {Operators: []filter.Operator{filter.OpEq, filter.OpContains, filter.OpStartswith, filter.OpEndswith}, Convert: convertListText},
				"type": {Operators: []filter.Operator{filter.OpEq, filter.OpContains, filter.OpStartswith, filter.OpEndswith}, Convert: convertListText},
			},
			SortFields: []string{"name", "type", "content"},
		},
		stringFields: []string{"name", "type"},
	},
	QueryPlatformDomains:        makeListPolicy("id created_at updated_at", "domain namespace", "enabled", "id domain namespace enabled created_at updated_at", nil),
	QueryEnabledPlatformDomains: {},
	QueryDirectory: makeListPolicy("", "", "", "", map[string]filter.FieldRule{
		"parent_path": {Operators: []filter.Operator{filter.OpEq}, Convert: convertListPath},
	}),
	QueryBreadcrumbs: makeListPolicy("", "", "", "", map[string]filter.FieldRule{
		"path": {Operators: []filter.Operator{filter.OpEq}, Convert: convertRequiredListPath},
	}),
}

func makeListPolicy(ordered, text, boolean, sorts string, extra map[string]filter.FieldRule) listQueryPolicy {
	p := listQueryPolicy{policy: filter.QueryPolicy{Fields: filter.ComparableFieldRules(strings.Fields(ordered)...), SortFields: strings.Fields(sorts)}}
	for field, rule := range p.policy.Fields {
		// Nullable timestamps retain null predicates in addition to comparisons.
		rule.Operators = append(rule.Operators, filter.OpNull, filter.OpNnull)
		p.policy.Fields[field] = rule
	}
	for _, field := range strings.Fields(text) {
		p.policy.Fields[field] = filter.FieldRule{Operators: textOps, Convert: convertListText}
		p.stringFields = append(p.stringFields, field)
	}
	for _, field := range strings.Fields(boolean) {
		p.policy.Fields[field] = filter.FieldRule{Operators: boolOps, Convert: convertListBool}
	}
	for field, rule := range extra {
		p.policy.Fields[field] = rule
		p.stringFields = append(p.stringFields, field)
	}
	return p
}

// NormalizeListQuery validates before callers inspect filters or build queries.
// The zero pagination value remains available to internal all-record callers.
func NormalizeListQuery(resource ListQueryResource, filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]queryutil.CrudFilter, error) {
	definition, ok := listQueryPolicies[resource]
	if !ok {
		return nil, fmt.Errorf("%w: unknown list resource", filter.ErrInvalidQuery)
	}
	if pagination.Start < 0 || pagination.End < 0 || pagination.PageSize < 0 || pagination.PageSize > 1000 ||
		(pagination.Start > 0 && pagination.PageSize == 0 && pagination.End == 0) ||
		(pagination.End != 0 && (pagination.End <= pagination.Start || (pagination.PageSize != 0 && pagination.End-pagination.Start != pagination.PageSize))) {
		return nil, fmt.Errorf("%w: invalid pagination", filter.ErrInvalidQuery)
	}
	policy := definition.policy
	if resource == QueryPins {
		policy.Fields = make(map[string]filter.FieldRule, len(definition.policy.Fields))
		for name, rule := range definition.policy.Fields {
			policy.Fields[name] = rule
		}
	}
	count := 0
	if err := prepareListFilters(filters, resource, policy.Fields, &count); err != nil {
		return nil, err
	}
	normalized, err := policy.Normalize(filters, sorts)
	if err != nil {
		return nil, err
	}
	if resource == QueryDirectory || resource == QueryBreadcrumbs {
		if len(normalized) != 1 || len(normalized[0].GetFilters()) != 0 {
			return nil, fmt.Errorf("%w: one equality path is required", filter.ErrInvalidQuery)
		}
	}
	return normalized, nil
}

// prepareListFilters adds IPFS metadata rules and bounds query size. The node
// budget also bounds cyclic input; QueryPolicy owns shape and depth validation.
func prepareListFilters(filters []queryutil.CrudFilter, resource ListQueryResource, fields map[string]filter.FieldRule, count *int) error {
	for _, node := range filters {
		*count = *count + 1
		if *count > 256 {
			return fmt.Errorf("%w: too many filters", filter.ErrInvalidQuery)
		}
		switch f := node.(type) {
		case *filter.LogicalFilter:
			if f != nil && resource == QueryPins && strings.HasPrefix(f.Field(), "meta.") {
				path := strings.TrimPrefix(f.Field(), "meta.")
				if path == "" || len(path) > 255 || !utf8.ValidString(path) || strings.ContainsRune(path, 0) {
					return fmt.Errorf("%w: invalid metadata path", filter.ErrInvalidQuery)
				}
				for _, segment := range strings.Split(path, ".") {
					if segment == "" {
						return fmt.Errorf("%w: invalid metadata path", filter.ErrInvalidQuery)
					}
				}
				fields[f.Field()] = filter.FieldRule{Operators: []filter.Operator{filter.OpEq}, Convert: convertListText}
			}
		case *filter.ConditionalFilter:
			if f != nil {
				if err := prepareListFilters(f.Filters, resource, fields, count); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// ParseListQuery preserves text before coercion and returns an independent,
// validated tree. Converters accept their normalized values for service rechecks.
func ParseListQuery(resource ListQueryResource, r *http.Request) ([]queryutil.CrudFilter, []queryutil.Sort, queryutil.Pagination, error) {
	definition, ok := listQueryPolicies[resource]
	if !ok {
		return nil, nil, queryutil.Pagination{}, fmt.Errorf("%w: unknown list resource", filter.ErrInvalidQuery)
	}
	filters, sorts, pagination, err := parseListRequest(r, definition.stringFields)
	if err != nil {
		return nil, nil, pagination, fmt.Errorf("%w: %v", filter.ErrInvalidQuery, err)
	}
	filters, err = NormalizeListQuery(resource, filters, sorts, pagination)
	return filters, sorts, pagination, err
}

// The legacy parser constructs filters with panic-based value validation.
// Keep that behavior inside the request boundary, before any service call.
func parseListRequest(r *http.Request, stringFields []string) (filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination, err error) {
	defer func() {
		if recover() != nil {
			filters, sorts, pagination = nil, nil, queryutil.Pagination{}
			err = fmt.Errorf("%w: invalid filter value", filter.ErrInvalidQuery)
		}
	}()
	return queryutil.ParseRequestWithOptions(r, queryutil.WithStringFields(stringFields...))
}

func convertListText(op filter.Operator, value any) (any, error) {
	return convertListValue(op, value, func(value any) (any, error) {
		v := reflect.ValueOf(value)
		if v.Kind() != reflect.String {
			return nil, fmt.Errorf("text value required")
		}
		return v.String(), nil
	})
}

func convertListBool(op filter.Operator, value any) (any, error) {
	return convertListValue(op, value, func(value any) (any, error) {
		if _, ok := value.(bool); !ok {
			return nil, fmt.Errorf("boolean value required")
		}
		return value, nil
	})
}

func convertListPath(op filter.Operator, value any) (any, error) {
	path, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("path must be text")
	}
	if path == "" {
		path = "/"
	}
	return ValidateFileManagerPath(path)
}

func convertRequiredListPath(_ filter.Operator, value any) (any, error) {
	path, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("path must be text")
	}
	return ValidateFileManagerPath(path)
}

func convertListCID(op filter.Operator, value any) (any, error) {
	return convertListValue(op, value, func(value any) (any, error) {
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
	})
}

func convertListValue(op filter.Operator, value any, convert func(any) (any, error)) (any, error) {
	if op == filter.OpNull || op == filter.OpNnull {
		return value, nil
	}
	if op.RequiresArray() {
		values, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("list value required")
		}
		result := make([]any, len(values))
		for i, item := range values {
			var err error
			result[i], err = convert(item)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	return convert(value)
}
