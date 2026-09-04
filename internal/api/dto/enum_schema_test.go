package dto

import (
	"testing"

	"go.lumeweb.com/queryutil"
)

func TestDomainResponse_FieldEnums(t *testing.T) {
	schema := queryutil.NewSchemaProvider().ForType(&DomainResponse{})
	enums := schema.FieldEnums()

	want := map[string][]string{
		"status":    {"draft", "records_generated", "waiting_delegation", "active", "self_hosted", "error", "onchain_managed"},
		"namespace": {"icann", "hns"},
	}
	for field, expected := range want {
		values, ok := enums[field]
		if !ok {
			t.Fatalf("expected enum field %q, not found in %v", field, enums)
		}
		if len(values) != len(expected) {
			t.Fatalf("%s: expected %d enum values, got %d (%v)", field, len(expected), len(values), values)
		}
		for i, v := range expected {
			if values[i] != v {
				t.Errorf("%s[%d]: expected %q, got %q", field, i, v, values[i])
			}
		}
	}
}

func TestSSLStatusInfo_FieldEnums(t *testing.T) {
	schema := queryutil.NewSchemaProvider().ForType(&SSLStatusInfo{})
	enums := schema.FieldEnums()

	values, ok := enums["status"]
	if !ok {
		t.Fatalf("expected enum field \"status\", not found in %v", enums)
	}

	expected := []string{"pending", "issuing", "ready", "failed"}
	if len(values) != len(expected) {
		t.Fatalf("expected %d enum values, got %d (%v)", len(expected), len(values), values)
	}
	for i, v := range expected {
		if values[i] != v {
			t.Errorf("status[%d]: expected %q, got %q", i, v, values[i])
		}
	}
}

func TestUploadResultResponse_FieldEnums(t *testing.T) {
	schema := queryutil.NewSchemaProvider().ForType(&UploadResultResponse{})
	enums := schema.FieldEnums()

	values, ok := enums["status"]
	if !ok {
		t.Fatalf("expected enum field \"status\", not found in %v", enums)
	}

	expected := []string{"pending", "processing", "completed", "failed", "duplicate"}
	if len(values) != len(expected) {
		t.Fatalf("expected %d enum values, got %d (%v)", len(expected), len(values), values)
	}
	for i, v := range expected {
		if values[i] != v {
			t.Errorf("status[%d]: expected %q, got %q", i, v, values[i])
		}
	}
}

func TestWebsiteChangeEvent_FieldEnums(t *testing.T) {
	schema := queryutil.NewSchemaProvider().ForType(&WebsiteChangeEvent{})
	enums := schema.FieldEnums()

	values, ok := enums["event_type"]
	if !ok {
		t.Fatalf("expected enum field \"event_type\", not found in %v", enums)
	}

	expected := []string{"published", "removed"}
	if len(values) != len(expected) {
		t.Fatalf("expected %d enum values, got %d (%v)", len(expected), len(values), values)
	}
	for i, v := range expected {
		if values[i] != v {
			t.Errorf("event_type[%d]: expected %q, got %q", i, v, values[i])
		}
	}
}
