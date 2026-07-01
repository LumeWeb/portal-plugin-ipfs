package dto

import (
	"testing"

	"go.lumeweb.com/queryutil"
)

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
