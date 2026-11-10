package dto

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

func TestListPoliciesRejectUncheckedSelectors(t *testing.T) {
	for _, resource := range []ListQueryResource{QueryPins, QueryIPNSKeys, QueryFiles, QueryWebsites, QueryWebsiteDomains, QueryWorkspaces, QueryDNSZones, QueryDNSRecords, QueryPlatformDomains, QueryEnabledPlatformDomains, QueryDirectory, QueryBreadcrumbs} {
		t.Run(fmt.Sprint(resource), func(t *testing.T) {
			for _, field := range []string{"id) OR 1=1 --", "user_id", "private_key_encrypted", "proxy_password", "unknown"} {
				_, err := NormalizeListQuery(resource, []queryutil.CrudFilter{queryutil.Equal(field, 1)}, nil, queryutil.Pagination{})
				if !errors.Is(err, filter.ErrInvalidQuery) {
					t.Errorf("field %q: %v", field, err)
				}
			}
			_, err := NormalizeListQuery(resource, nil, []queryutil.Sort{{Field: "(SELECT 1)", Order: filter.OrderAsc}}, queryutil.Pagination{})
			if !errors.Is(err, filter.ErrInvalidQuery) {
				t.Errorf("sort expression: %v", err)
			}
		})
	}
}

func TestListPoliciesRejectMalformedTrees(t *testing.T) {
	var deep queryutil.CrudFilter = queryutil.Equal("id", 1)
	for i := 0; i < 34; i++ {
		deep = queryutil.NewConditionalFilter(filter.LogicalAnd, []queryutil.CrudFilter{deep})
	}
	cycle := &filter.ConditionalFilter{Operator: filter.LogicalAnd}
	cycle.Filters = []queryutil.CrudFilter{cycle}
	for _, node := range []queryutil.CrudFilter{nil, (*filter.LogicalFilter)(nil), (*filter.ConditionalFilter)(nil), cycle, deep, &filter.ConditionalFilter{Operator: filter.LogicalAnd}, &filter.ConditionalFilter{Operator: "unknown", Filters: []queryutil.CrudFilter{queryutil.Equal("id", 1)}}} {
		_, err := NormalizeListQuery(QueryFiles, []queryutil.CrudFilter{node}, nil, queryutil.Pagination{})
		if !errors.Is(err, filter.ErrInvalidQuery) {
			t.Fatalf("node %T: %v", node, err)
		}
	}
}

func TestListPoliciesKeepFieldSpecificOperators(t *testing.T) {
	for _, tc := range []struct {
		field string
		op    filter.Operator
		value any
		valid bool
	}{
		{"id", filter.OpLt, 2, true},
		{"created_at", filter.OpNull, nil, true},
		{"name", filter.OpContains, "one", true},
		{"id", filter.OpContains, "1", false},
		{"name", filter.OpGt, "2026-01-01T00:00:00Z", false},
	} {
		_, err := NormalizeListQuery(QueryFiles, []queryutil.CrudFilter{queryutil.NewLogicalFilter(tc.field, tc.op, tc.value)}, nil, queryutil.Pagination{})
		if tc.valid && err != nil {
			t.Errorf("%s %s: %v", tc.field, tc.op, err)
		}
		if !tc.valid && !errors.Is(err, filter.ErrInvalidQuery) {
			t.Errorf("accepted %s %s: %v", tc.field, tc.op, err)
		}
	}
}

func TestListQueryPreservesNumericNames(t *testing.T) {
	r := httptest.NewRequest("GET", "/?"+url.Values{"filters[name][eq]": {"000123"}}.Encode(), nil)
	fs, _, _, err := ParseListQuery(QueryIPNSKeys, r)
	if err != nil {
		t.Fatal(err)
	}
	if len(fs) != 1 || fs[0].GetValue() != "000123" {
		t.Fatalf("name was coerced: %#v", fs)
	}
}

func TestPinPolicyKeepsBinaryCIDsAndBoundMetadataPaths(t *testing.T) {
	c, err := cid.Parse("bafkreigh2akiscaildc36s5jt6a2uzq3wvh54m2vv7pzbnj3ge6xprywxm")
	if err != nil {
		t.Fatal(err)
	}
	original := []queryutil.CrudFilter{queryutil.In("cid", c.Bytes()), queryutil.Equal("meta.project-name", "0001")}
	fs, err := NormalizeListQuery(QueryPins, original, nil, queryutil.Pagination{})
	if err != nil {
		t.Fatal(err)
	}
	again, err := NormalizeListQuery(QueryPins, fs, nil, queryutil.Pagination{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fs[0].GetValue(), again[0].GetValue()) {
		t.Fatal("CID conversion is not idempotent")
	}
	if fs[1].GetValue() != "0001" || original[1].GetValue() != "0001" {
		t.Fatal("metadata value changed")
	}
}

func TestNavigationRequiresOneEqualityPath(t *testing.T) {
	for _, tc := range []struct {
		resource ListQueryResource
		field    string
	}{{QueryDirectory, "parent_path"}, {QueryBreadcrumbs, "path"}} {
		for _, fs := range [][]queryutil.CrudFilter{nil, {queryutil.Contains(tc.field, "/a")}, {queryutil.Equal(tc.field, "/a"), queryutil.Equal(tc.field, "/b")}} {
			_, err := NormalizeListQuery(tc.resource, fs, nil, queryutil.Pagination{})
			if !errors.Is(err, filter.ErrInvalidQuery) {
				t.Fatalf("accepted ambiguous navigation query: %v", err)
			}
		}
		fs, err := NormalizeListQuery(tc.resource, []queryutil.CrudFilter{queryutil.Equal(tc.field, "/0001")}, nil, queryutil.Pagination{})
		if err != nil || fs[0].GetValue() != "/0001" {
			t.Fatalf("valid path: %v", err)
		}
	}
}

type customListFilter struct{ *filter.LogicalFilter }

func TestListPoliciesRejectCustomNodesAndWideQueries(t *testing.T) {
	for _, fs := range [][]queryutil.CrudFilter{
		{&customListFilter{filter.NewLogicalFilter("id", filter.OpEq, 1)}},
		make([]queryutil.CrudFilter, 257),
	} {
		if len(fs) > 1 {
			for i := range fs {
				fs[i] = queryutil.Equal("id", 1)
			}
		}
		_, err := NormalizeListQuery(QueryFiles, fs, nil, queryutil.Pagination{})
		if !errors.Is(err, filter.ErrInvalidQuery) {
			t.Fatalf("unsupported tree: %v", err)
		}
	}
	if _, err := NormalizeListQuery(ListQueryResource(255), nil, nil, queryutil.Pagination{}); !errors.Is(err, filter.ErrInvalidQuery) {
		t.Fatalf("unknown resource: %v", err)
	}
}

func TestPinMetadataPoliciesAreRequestLocal(t *testing.T) {
	for i := 0; i < 20; i++ {
		field := fmt.Sprintf("meta.key%d", i)
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			fs, err := NormalizeListQuery(QueryPins, []queryutil.CrudFilter{queryutil.Equal(field, "x")}, nil, queryutil.Pagination{})
			if err != nil {
				t.Fatal(err)
			}
			if len(fs) != 1 || fs[0].GetField() != field || fs[0].GetValue() != "x" {
				t.Fatalf("metadata field changed: %#v", fs)
			}
		})
	}
}

func TestListPolicyRejectsInvalidPagination(t *testing.T) {
	for _, p := range []queryutil.Pagination{{Start: -1}, {PageSize: -1}, {PageSize: 1001}, {Start: 1}, {Start: 2, End: 1, PageSize: 1}} {
		_, err := NormalizeListQuery(QueryFiles, nil, nil, p)
		if !errors.Is(err, filter.ErrInvalidQuery) {
			t.Fatalf("accepted pagination %#v: %v", p, err)
		}
	}
	if _, err := NormalizeListQuery(QueryFiles, nil, nil, queryutil.Pagination{}); err != nil {
		t.Fatalf("internal all-record query: %v", err)
	}
}
