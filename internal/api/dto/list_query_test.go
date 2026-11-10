package dto

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

func TestListPoliciesRejectUncheckedSelectors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy filter.QueryPolicy
	}{
		{"pins", PinListPolicy(nil)}, {"keys", IPNSKeyListPolicy}, {"files", FileListPolicy},
		{"websites", WebsiteListPolicy}, {"domains", WebsiteDomainListPolicy}, {"workspaces", WorkspaceListPolicy},
		{"zones", DNSZoneListPolicy}, {"records", DNSRecordListPolicy}, {"platform_domains", PlatformDomainListPolicy},
		{"enabled_domains", EnabledPlatformDomainListPolicy}, {"directory", DirectoryListPolicy}, {"breadcrumbs", BreadcrumbListPolicy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, field := range []string{"id) OR 1=1 --", "user_id", "private_key_encrypted", "proxy_password", "unknown"} {
				_, err := tc.policy.Normalize([]queryutil.CrudFilter{queryutil.Equal(field, 1)}, nil)
				if !errors.Is(err, filter.ErrInvalidQuery) {
					t.Errorf("field %q: %v", field, err)
				}
			}
			_, err := tc.policy.Normalize(nil, []queryutil.Sort{{Field: "(SELECT 1)", Order: filter.OrderAsc}})
			if !errors.Is(err, filter.ErrInvalidQuery) {
				t.Errorf("sort expression: %v", err)
			}
		})
	}
}

func TestListPoliciesKeepFieldSpecificOperators(t *testing.T) {
	for _, tc := range []struct {
		field string
		op    filter.Operator
		value any
		valid bool
	}{
		{"id", filter.OpLt, 2, true}, {"created_at", filter.OpNull, nil, true},
		{"name", filter.OpContains, "one", true}, {"id", filter.OpContains, "1", false},
		{"name", filter.OpGt, "2026-01-01T00:00:00Z", false},
	} {
		_, err := FileListPolicy.Normalize([]queryutil.CrudFilter{queryutil.NewLogicalFilter(tc.field, tc.op, tc.value)}, nil)
		if tc.valid && err != nil {
			t.Errorf("%s %s: %v", tc.field, tc.op, err)
		}
		if !tc.valid && !errors.Is(err, filter.ErrInvalidQuery) {
			t.Errorf("accepted %s %s: %v", tc.field, tc.op, err)
		}
	}
}

func TestPinPolicyKeepsBinaryCIDsAndMetadataPaths(t *testing.T) {
	c, err := cid.Parse("bafkreigh2akiscaildc36s5jt6a2uzq3wvh54m2vv7pzbnj3ge6xprywxm")
	if err != nil {
		t.Fatal(err)
	}
	original := []queryutil.CrudFilter{queryutil.In("cid", c.Bytes()), queryutil.Or(queryutil.Equal("meta.project-name", "0001"))}
	fs, err := PinListPolicy(original).Normalize(original, nil)
	if err != nil {
		t.Fatal(err)
	}
	again, err := PinListPolicy(fs).Normalize(fs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fs[0].GetValue(), again[0].GetValue()) {
		t.Fatal("CID conversion is not idempotent")
	}
	if fs[1].GetFilters()[0].GetValue() != "0001" || original[1].GetFilters()[0].GetValue() != "0001" {
		t.Fatal("metadata value changed")
	}
}

func TestCIDPoliciesKeepTheirStorageRepresentations(t *testing.T) {
	v0, err := (cid.Prefix{Version: 0, Codec: cid.DagProtobuf, MhType: 0x12, MhLength: -1}).Sum([]byte("CID policy representation"))
	if err != nil {
		t.Fatal(err)
	}
	v1 := cid.NewCidV1(cid.DagProtobuf, v0.Hash())
	for _, op := range []filter.Operator{filter.OpEq, filter.OpNe, filter.OpIn, filter.OpNin} {
		input := any(v0.String())
		fileWant := any(v0.Bytes())
		pinWant := any(v1.Bytes())
		if op.RequiresArray() {
			input = []any{input}
			fileWant = []any{v0.Bytes()}
			pinWant = []any{v1.Bytes()}
		}
		original := []queryutil.CrudFilter{queryutil.NewLogicalFilter("cid", op, input)}
		files, err := FileListPolicy.Normalize(original, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(fileWant, files[0].GetValue()) {
			t.Errorf("%s file CID changed: got %v want %v", op, files[0].GetValue(), fileWant)
		}
		files, err = FileListPolicy.Normalize(files, nil)
		if err != nil || !reflect.DeepEqual(fileWant, files[0].GetValue()) {
			t.Fatalf("%s file recheck: %v", op, err)
		}
		pins, err := PinListPolicy(original).Normalize(original, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(pinWant, pins[0].GetValue()) {
			t.Errorf("%s pin CID not normalized", op)
		}
		pins, err = PinListPolicy(pins).Normalize(pins, nil)
		if err != nil || !reflect.DeepEqual(pinWant, pins[0].GetValue()) {
			t.Fatalf("%s pin recheck: %v", op, err)
		}
		if !reflect.DeepEqual(input, original[0].GetValue()) {
			t.Fatalf("%s mutated input", op)
		}
	}
}

func TestPinPolicyRejectsMalformedTrees(t *testing.T) {
	cycle := &filter.ConditionalFilter{Operator: filter.LogicalAnd}
	cycle.Filters = []queryutil.CrudFilter{cycle}
	for _, node := range []queryutil.CrudFilter{nil, (*filter.LogicalFilter)(nil), (*filter.ConditionalFilter)(nil), cycle} {
		fs := []queryutil.CrudFilter{node}
		if _, err := PinListPolicy(fs).Normalize(fs, nil); !errors.Is(err, filter.ErrInvalidQuery) {
			t.Fatalf("invalid tree: %v", err)
		}
	}
}

func TestPinMetadataPoliciesAreRequestLocal(t *testing.T) {
	for i := 0; i < 20; i++ {
		field := fmt.Sprintf("meta.key%d", i)
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			input := []queryutil.CrudFilter{queryutil.Equal(field, "x")}
			fs, err := PinListPolicy(input).Normalize(input, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(fs) != 1 || fs[0].GetField() != field || fs[0].GetValue() != "x" {
				t.Fatalf("metadata field changed: %#v", fs)
			}
		})
	}
}

func TestFileNavigationPoliciesNormalizePaths(t *testing.T) {
	fs, err := DirectoryListPolicy.Normalize([]queryutil.CrudFilter{queryutil.Equal("parent_path", "")}, nil)
	if err != nil || fs[0].GetValue() != "/" {
		t.Fatalf("root path: %v", err)
	}
	if _, err := BreadcrumbListPolicy.Normalize([]queryutil.CrudFilter{queryutil.Equal("path", "")}, nil); !errors.Is(err, filter.ErrInvalidQuery) {
		t.Fatalf("empty breadcrumb path: %v", err)
	}
	if _, err := FileListPolicy.Normalize([]queryutil.CrudFilter{queryutil.Contains("parent_path", "/a")}, nil); !errors.Is(err, filter.ErrInvalidQuery) {
		t.Fatalf("unsupported hierarchy operator: %v", err)
	}
}
