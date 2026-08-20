package dto

import (
	"encoding/json"
	"testing"

	"github.com/Oudwins/zog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/httputil"
)

// Test ImportZoneRequest DTO

func TestImportZoneRequest_Schema(t *testing.T) {
	t.Run("schema returns non-nil struct schema", func(t *testing.T) {
		req := ImportZoneRequest{}
		schema := req.Schema()

		require.NotNil(t, schema, "Schema() should return non-nil schema")
		assert.IsType(t, &zog.StructSchema{}, schema, "Schema() should return *zog.StructSchema")
	})
}

func TestImportZoneRequest_ImplementsInterfaces(t *testing.T) {
	t.Run("implements DTOValidator", func(t *testing.T) {
		var _ httputil.DTOValidator = (*ImportZoneRequest)(nil)
	})
}

func TestImportZoneRequest_ZogValidation(t *testing.T) {
	t.Run("valid request with all fields", func(t *testing.T) {
		zoneContent := "$ORIGIN example.com.\n$TTL 86400\n@ IN SOA ns1.example.com. admin.example.com. (1 3600 1800 604800 86400)\n@ IN NS ns1.example.com\n@ IN A 192.0.2.1"
		req := ImportZoneRequest{
			ZoneFileContent: zoneContent,
			ImportMode:      "merge",
			DryRun:          true,
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid request should pass validation")
	})

	t.Run("valid request without dry_run", func(t *testing.T) {
		zoneContent := "$ORIGIN example.com.\n$TTL 86400\n@ IN SOA ns1.example.com. admin.example.com. (1 3600 1800 604800 86400)\n@ IN NS ns1.example.com"
		req := ImportZoneRequest{
			ZoneFileContent: zoneContent,
			ImportMode:      "replace",
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid request without dry_run should pass validation")
	})

	t.Run("valid request with import_mode update", func(t *testing.T) {
		zoneContent := "$ORIGIN example.com.\n$TTL 86400\n@ IN SOA ns1.example.com. admin.example.com. (1 3600 1800 604800 86400)\n@ IN NS ns1.example.com"
		req := ImportZoneRequest{
			ZoneFileContent: zoneContent,
			ImportMode:      "update",
			DryRun:          false,
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid request with update mode should pass validation")
	})

	t.Run("invalid request with empty zone_file_content fails Min(1) validation", func(t *testing.T) {
		req := ImportZoneRequest{
			ZoneFileContent: "",
			ImportMode:      "merge",
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.NotNil(t, errs, "Request with empty zone file content should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid request with invalid import_mode fails OneOf validation", func(t *testing.T) {
		zoneContent := "$ORIGIN example.com.\n$TTL 86400\n@ IN SOA ns1.example.com. admin.example.com. (1 3600 1800 604800 86400)\n@ IN NS ns1.example.com"
		req := ImportZoneRequest{
			ZoneFileContent: zoneContent,
			ImportMode:      "invalid",
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.NotNil(t, errs, "Request with invalid import mode should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid request with zone_file_content exceeding 10MB fails Max(10MB) validation", func(t *testing.T) {
		largeContent := make([]byte, 10*1024*1024+1)
		for i := range largeContent {
			largeContent[i] = 'A'
		}
		req := ImportZoneRequest{
			ZoneFileContent: string(largeContent),
			ImportMode:      "merge",
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.NotNil(t, errs, "Request with zone file content exceeding 10MB should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})
}

func TestImportZoneRequest_ZoneFileContentValidation(t *testing.T) {
	t.Run("zone_file_content is required", func(t *testing.T) {
		req := ImportZoneRequest{
			ImportMode: "merge",
		}
		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.NotNil(t, errs, "Missing zone file content should fail validation")
	})

	t.Run("zone_file_content accepts valid BIND zone file", func(t *testing.T) {
		zoneContent := "$ORIGIN example.com.\n$TTL 86400\n@ IN SOA ns1.example.com. admin.example.com. (1 3600 1800 604800 86400)\n@ IN NS ns1.example.com\n@ IN A 192.0.2.1"
		req := ImportZoneRequest{
			ZoneFileContent: zoneContent,
			ImportMode:      "merge",
		}
		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid BIND zone file content should pass validation")
	})
}

func TestImportZoneRequest_ImportModeValidation(t *testing.T) {
	t.Run("import_mode accepts merge", func(t *testing.T) {
		req := ImportZoneRequest{
			ZoneFileContent: "test",
			ImportMode:      "merge",
		}
		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Import mode 'merge' should be valid")
	})

	t.Run("import_mode accepts replace", func(t *testing.T) {
		req := ImportZoneRequest{
			ZoneFileContent: "test",
			ImportMode:      "replace",
		}
		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Import mode 'replace' should be valid")
	})

	t.Run("import_mode accepts update", func(t *testing.T) {
		req := ImportZoneRequest{
			ZoneFileContent: "test",
			ImportMode:      "update",
		}
		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Import mode 'update' should be valid")
	})
}

func TestImportZoneRequest_DryRunValidation(t *testing.T) {
	t.Run("dry_run is optional", func(t *testing.T) {
		req := ImportZoneRequest{
			ZoneFileContent: "test",
			ImportMode:      "merge",
		}
		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Request without dry_run should pass validation")
	})
}

func TestImportZoneRequest_ToModel(t *testing.T) {
	t.Run("toModel returns same struct", func(t *testing.T) {
		req := ImportZoneRequest{
			ZoneFileContent: "test",
			ImportMode:      "merge",
			DryRun:          true,
		}

		model, err := req.ToModel()
		require.NoError(t, err)
		assert.Equal(t, req.ZoneFileContent, model.ZoneFileContent)
		assert.Equal(t, req.ImportMode, model.ImportMode)
		assert.Equal(t, req.DryRun, model.DryRun)
	})
}

// Test BulkDeleteRequest DTO

func TestBulkDeleteRequest_Schema(t *testing.T) {
	t.Run("schema returns non-nil struct schema", func(t *testing.T) {
		req := BulkDeleteRequest{}
		schema := req.Schema()

		require.NotNil(t, schema, "Schema() should return non-nil schema")
		assert.IsType(t, &zog.StructSchema{}, schema, "Schema() should return *zog.StructSchema")
	})
}

func TestBulkDeleteRequest_ImplementsInterfaces(t *testing.T) {
	t.Run("implements DTOValidator", func(t *testing.T) {
		var _ httputil.DTOValidator = (*BulkDeleteRequest)(nil)
	})
}

func TestBulkDeleteRequest_ZogValidation(t *testing.T) {
	t.Run("valid request with records and dry_run", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "www.example.com", Type: "A"},
				{Name: "mail.example.com", Type: "MX"},
			},
			DryRun: true,
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid request should pass validation")
	})

	t.Run("valid request with records only", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "www.example.com", Type: "A"},
			},
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid request without dry_run should pass validation")
	})

	t.Run("valid request with single record", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "example.com", Type: "CNAME"},
			},
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid request with single record should pass validation")
	})

	t.Run("invalid request with empty name fails Min(1) validation", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "", Type: "A"},
			},
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.NotNil(t, errs, "Request with empty name should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid request with invalid DNS type fails OneOf validation", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "test.example.com", Type: "INVALID"},
			},
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.NotNil(t, errs, "Request with invalid DNS type should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid request with missing records field fails Required validation", func(t *testing.T) {
		req := BulkDeleteRequest{}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.NotNil(t, errs, "Request with missing records should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid request with name exceeding 255 characters fails Max(255) validation", func(t *testing.T) {
		longName := ""
		for i := 0; i < 256; i++ {
			longName += "a"
		}
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: longName, Type: "A"},
			},
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.NotNil(t, errs, "Request with name exceeding 255 characters should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})
}

func TestBulkDeleteRequest_RecordsValidation(t *testing.T) {
	t.Run("records field is required", func(t *testing.T) {
		req := BulkDeleteRequest{}
		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.NotNil(t, errs, "Missing records field should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("records must have at least one element", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{},
		}
		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.NotNil(t, errs, "Empty records slice should fail Min(1) validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("records accepts multiple record identifiers", func(t *testing.T) {
		records := make([]RecordIdentifier, 10)
		for i := 0; i < 10; i++ {
			records[i] = RecordIdentifier{
				Name: "test.example.com",
				Type: "A",
			}
		}
		req := BulkDeleteRequest{
			Records: records,
		}
		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Request with 10 records should pass validation")
		assert.Equal(t, 10, len(req.Records))
	})
}

func TestBulkDeleteRequest_RecordIdentifierValidation(t *testing.T) {
	t.Run("valid record identifier with all allowed types", func(t *testing.T) {
		allowedTypes := []string{"A", "AAAA", "CNAME", "MX", "TXT", "NS", "SRV", "ALIAS"}
		for _, recordType := range allowedTypes {
			req := BulkDeleteRequest{
				Records: []RecordIdentifier{
					{Name: "test.example.com", Type: recordType},
				},
			}
			schema := req.Schema()
			errs := schema.Parse(req, &req)
			assert.Nil(t, errs, "Valid request with type %s should pass validation", recordType)
		}
	})

	t.Run("record identifier name is required", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Type: "A"},
			},
		}
		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.NotNil(t, errs, "Record with missing name should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("record identifier type is required", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "test.example.com"},
			},
		}
		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.NotNil(t, errs, "Record with missing type should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})
}

func TestBulkDeleteRequest_DryRunValidation(t *testing.T) {
	t.Run("dry_run is optional", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "test.example.com", Type: "A"},
			},
		}
		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Request without dry_run should pass validation")
	})

	t.Run("dry_run defaults to false when not provided", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "test.example.com", Type: "A"},
			},
		}
		assert.False(t, req.DryRun, "DryRun should default to false")
	})

	t.Run("dry_run can be set to true", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "test.example.com", Type: "A"},
			},
			DryRun: true,
		}
		assert.True(t, req.DryRun, "DryRun should be true")
	})

	t.Run("dry_run can be set to false", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "test.example.com", Type: "A"},
			},
			DryRun: false,
		}
		assert.False(t, req.DryRun, "DryRun should be false")
	})
}

func TestBulkDeleteRequest_ToModel(t *testing.T) {
	t.Run("toModel returns same struct", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "www.example.com", Type: "A"},
				{Name: "mail.example.com", Type: "MX"},
			},
			DryRun: true,
		}

		model, err := req.ToModel()
		require.NoError(t, err)
		assert.Equal(t, req.Records, model.Records)
		assert.Equal(t, req.DryRun, model.DryRun)
	})

	t.Run("toModel with empty records", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{},
			DryRun:  false,
		}

		model, err := req.ToModel()
		require.NoError(t, err)
		assert.Equal(t, req.Records, model.Records)
		assert.Equal(t, req.DryRun, model.DryRun)
	})
}

func TestBulkDeleteRequest_RealWorldScenarios(t *testing.T) {
	t.Run("scenario: delete single A record", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "www.example.com", Type: "A"},
			},
			DryRun: true,
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid single record delete should pass validation")
		assert.Len(t, req.Records, 1)
		assert.Equal(t, "www.example.com", req.Records[0].Name)
		assert.Equal(t, "A", req.Records[0].Type)
	})

	t.Run("scenario: delete multiple records of different types", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "www.example.com", Type: "A"},
				{Name: "www.example.com", Type: "AAAA"},
				{Name: "mail.example.com", Type: "MX"},
				{Name: "example.com", Type: "TXT"},
			},
			DryRun: false,
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid multiple records delete should pass validation")
		assert.Len(t, req.Records, 4)
	})

	t.Run("scenario: dry run validation before actual deletion", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "www.example.com", Type: "A"},
			},
			DryRun: true,
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid dry run request should pass validation")
		assert.True(t, req.DryRun, "DryRun should be true for validation")
	})

	t.Run("scenario: bulk delete entire subdomain", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "sub1.example.com", Type: "CNAME"},
				{Name: "sub2.example.com", Type: "CNAME"},
				{Name: "sub3.example.com", Type: "CNAME"},
			},
			DryRun: false,
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid bulk delete should pass validation")
		assert.Len(t, req.Records, 3)
	})
}

func TestRecordIdentifier(t *testing.T) {
	t.Run("record identifier struct is properly defined", func(t *testing.T) {
		ri := RecordIdentifier{
			Name: "test.example.com",
			Type: "A",
		}
		assert.Equal(t, "test.example.com", ri.Name)
		assert.Equal(t, "A", ri.Type)
	})

	t.Run("record identifier with CNAME type", func(t *testing.T) {
		ri := RecordIdentifier{
			Name: "www.example.com",
			Type: "CNAME",
		}
		assert.Equal(t, "www.example.com", ri.Name)
		assert.Equal(t, "CNAME", ri.Type)
	})

	t.Run("record identifier with MX type", func(t *testing.T) {
		ri := RecordIdentifier{
			Name: "mail.example.com",
			Type: "MX",
		}
		assert.Equal(t, "mail.example.com", ri.Name)
		assert.Equal(t, "MX", ri.Type)
	})

	t.Run("record identifier carries optional content and omits it when empty", func(t *testing.T) {
		ri := RecordIdentifier{Name: "@", Type: "TXT", Content: "v=spf1 include:mxroute.com -all"}
		b, err := json.Marshal(ri)
		assert.NoError(t, err)
		assert.Contains(t, string(b), `"content"`)

		empty := RecordIdentifier{Name: "@", Type: "TXT"}
		b, err = json.Marshal(empty)
		assert.NoError(t, err)
		assert.NotContains(t, string(b), "content")
	})
}

func TestBulkDeleteRequest_ComplexValidation(t *testing.T) {
	t.Run("valid request with mixed record types and dry_run", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "www.example.com", Type: "A"},
				{Name: "www.example.com", Type: "AAAA"},
				{Name: "mail.example.com", Type: "MX"},
				{Name: "example.com", Type: "TXT"},
				{Name: "ns1.example.com", Type: "NS"},
				{Name: "_sip._tcp.example.com", Type: "SRV"},
			},
			DryRun: true,
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid complex request should pass validation")
		assert.Len(t, req.Records, 6)
	})

	t.Run("request with maximum allowed record types", func(t *testing.T) {
		req := BulkDeleteRequest{
			Records: []RecordIdentifier{
				{Name: "www.example.com", Type: "A"},
				{Name: "www.example.com", Type: "AAAA"},
				{Name: "www.example.com", Type: "CNAME"},
				{Name: "mail.example.com", Type: "MX"},
				{Name: "example.com", Type: "TXT"},
				{Name: "ns1.example.com", Type: "NS"},
				{Name: "_sip._tcp.example.com", Type: "SRV"},
				{Name: "alias.example.com", Type: "ALIAS"},
			},
			DryRun: false,
		}

		schema := req.Schema()
		errs := schema.Parse(req, &req)
		assert.Nil(t, errs, "Valid request with all types should pass validation")
		assert.Len(t, req.Records, 8)
	})
}

// Test ImportZoneResponse DTO

func TestImportZoneResponse_Schema(t *testing.T) {
	t.Run("schema returns non-nil struct schema", func(t *testing.T) {
		resp := ImportZoneResponse{}
		schema := resp.Schema()

		require.NotNil(t, schema, "Schema() should return non-nil schema")
		assert.IsType(t, &zog.StructSchema{}, schema, "Schema() should return *zog.StructSchema")
	})
}

func TestImportZoneResponse_ImplementsInterfaces(t *testing.T) {
	t.Run("implements DTOValidator", func(t *testing.T) {
		var _ httputil.DTOValidator = (*ImportZoneResponse)(nil)
	})
}

func TestImportZoneResponse_ZogValidation(t *testing.T) {
	t.Run("valid response with all fields populated", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "www.example.com", Type: "A", Content: "192.168.1.1", TTL: 3600},
				{Name: "mail.example.com", Type: "MX", Content: "mail.example.com", TTL: 3600},
			},
			SkippedCount: 2,
			FailedCount:  1,
			Errors: []ImportZoneError{
				{Name: "test.example.com", Type: "A", Error: "Invalid IP address"},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Valid response should pass validation")
	})

	t.Run("valid response with only created records", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "www.example.com", Type: "A", Content: "192.168.1.1", TTL: 3600},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Valid response with only created records should pass validation")
	})

	t.Run("valid response with only errors", func(t *testing.T) {
		resp := ImportZoneResponse{
			Errors: []ImportZoneError{
				{Name: "test.example.com", Type: "A", Error: "Invalid IP address"},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Valid response with only errors should pass validation")
	})

	t.Run("valid response with only counts", func(t *testing.T) {
		resp := ImportZoneResponse{
			SkippedCount: 5,
			FailedCount:  3,
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Valid response with only counts should pass validation")
	})

	t.Run("valid empty response", func(t *testing.T) {
		resp := ImportZoneResponse{}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Empty response should pass validation")
	})

	t.Run("invalid created record with empty name fails Min(1) validation", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "", Type: "A", Content: "192.168.1.1", TTL: 3600},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Response with empty name should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid created record with invalid type fails OneOf validation", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "test.example.com", Type: "INVALID", Content: "192.168.1.1", TTL: 3600},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Response with invalid DNS type should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid created record with empty content fails Min(1) validation", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "test.example.com", Type: "A", Content: "", TTL: 3600},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Response with empty content should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid error with empty name fails Min(1) validation", func(t *testing.T) {
		resp := ImportZoneResponse{
			Errors: []ImportZoneError{
				{Name: "", Type: "A", Error: "Test error"},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Response with empty name should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid error with invalid type fails OneOf validation", func(t *testing.T) {
		resp := ImportZoneResponse{
			Errors: []ImportZoneError{
				{Name: "test.example.com", Type: "INVALID", Error: "Test error"},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Response with invalid DNS type should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid error with empty error message fails Min(1) validation", func(t *testing.T) {
		resp := ImportZoneResponse{
			Errors: []ImportZoneError{
				{Name: "test.example.com", Type: "A", Error: ""},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Response with empty error message should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid created record with name exceeding 255 characters fails Max(255) validation", func(t *testing.T) {
		longName := ""
		for i := 0; i < 256; i++ {
			longName += "a"
		}
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: longName, Type: "A", Content: "192.168.1.1", TTL: 3600},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Response with name exceeding 255 characters should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid error with name exceeding 255 characters fails Max(255) validation", func(t *testing.T) {
		longName := ""
		for i := 0; i < 256; i++ {
			longName += "a"
		}
		resp := ImportZoneResponse{
			Errors: []ImportZoneError{
				{Name: longName, Type: "A", Error: "Test error"},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Response with name exceeding 255 characters should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid created record with content exceeding 1024 characters fails Max(1024) validation", func(t *testing.T) {
		longContent := ""
		for i := 0; i < 1025; i++ {
			longContent += "a"
		}
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "test.example.com", Type: "A", Content: longContent, TTL: 3600},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Response with content exceeding 1024 characters should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("invalid error with error message exceeding 1024 characters fails Max(1024) validation", func(t *testing.T) {
		longError := ""
		for i := 0; i < 1025; i++ {
			longError += "a"
		}
		resp := ImportZoneResponse{
			Errors: []ImportZoneError{
				{Name: "test.example.com", Type: "A", Error: longError},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Response with error message exceeding 1024 characters should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})
}

func TestImportZoneResponse_CreatedRecordsValidation(t *testing.T) {
	t.Run("created_records field is optional", func(t *testing.T) {
		resp := ImportZoneResponse{}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Missing created_records field should pass validation")
	})

	t.Run("created_records accepts empty slice", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{},
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Empty created_records should pass validation")
	})

	t.Run("created_records accepts multiple records", func(t *testing.T) {
		records := make([]CreatedRecord, 10)
		for i := 0; i < 10; i++ {
			records[i] = CreatedRecord{
				Name:    "test.example.com",
				Type:    "A",
				Content: "192.168.1.1",
				TTL:     3600,
			}
		}
		resp := ImportZoneResponse{
			CreatedRecords: records,
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Response with 10 created records should pass validation")
		assert.Equal(t, 10, len(resp.CreatedRecords))
	})
}

func TestImportZoneResponse_ErrorsValidation(t *testing.T) {
	t.Run("errors field is optional", func(t *testing.T) {
		resp := ImportZoneResponse{}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Missing errors field should pass validation")
	})

	t.Run("errors accepts empty slice", func(t *testing.T) {
		resp := ImportZoneResponse{
			Errors: []ImportZoneError{},
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Empty errors should pass validation")
	})

	t.Run("errors accepts multiple errors", func(t *testing.T) {
		errors := make([]ImportZoneError, 10)
		for i := 0; i < 10; i++ {
			errors[i] = ImportZoneError{
				Name:  "test.example.com",
				Type:  "A",
				Error: "Test error",
			}
		}
		resp := ImportZoneResponse{
			Errors: errors,
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Response with 10 errors should pass validation")
		assert.Equal(t, 10, len(resp.Errors))
	})
}

func TestImportZoneResponse_CountsValidation(t *testing.T) {
	t.Run("skipped_count field is optional", func(t *testing.T) {
		resp := ImportZoneResponse{}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Missing skipped_count field should pass validation")
	})

	t.Run("failed_count field is optional", func(t *testing.T) {
		resp := ImportZoneResponse{}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Missing failed_count field should pass validation")
	})

	t.Run("counts accept zero values", func(t *testing.T) {
		resp := ImportZoneResponse{
			SkippedCount: 0,
			FailedCount:  0,
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Zero counts should pass validation")
	})

	t.Run("counts accept positive values", func(t *testing.T) {
		resp := ImportZoneResponse{
			SkippedCount: 5,
			FailedCount:  3,
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Positive counts should pass validation")
	})
}

func TestImportZoneResponse_CreatedRecordValidation(t *testing.T) {
	t.Run("valid created record with all allowed types", func(t *testing.T) {
		allowedTypes := []string{"A", "AAAA", "CNAME", "MX", "TXT", "NS", "SRV", "ALIAS"}
		for _, recordType := range allowedTypes {
			resp := ImportZoneResponse{
				CreatedRecords: []CreatedRecord{
					{Name: "test.example.com", Type: recordType, Content: "192.168.1.1", TTL: 3600},
				},
			}
			schema := resp.Schema()
			errs := schema.Parse(resp, &resp)
			assert.Nil(t, errs, "Valid created record with type %s should pass validation", recordType)
		}
	})

	t.Run("created record name is required", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Type: "A", Content: "192.168.1.1", TTL: 3600},
			},
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Created record with missing name should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("created record type is required", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "test.example.com", Content: "192.168.1.1", TTL: 3600},
			},
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Created record with missing type should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("created record content is required", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "test.example.com", Type: "A", TTL: 3600},
			},
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Created record with missing content should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("created record TTL is optional", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "test.example.com", Type: "A", Content: "192.168.1.1"},
			},
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Created record without TTL should pass validation")
	})
}

func TestImportZoneResponse_ImportZoneErrorValidation(t *testing.T) {
	t.Run("valid import zone error with all allowed types", func(t *testing.T) {
		allowedTypes := []string{"A", "AAAA", "CNAME", "MX", "TXT", "NS", "SRV", "ALIAS"}
		for _, recordType := range allowedTypes {
			resp := ImportZoneResponse{
				Errors: []ImportZoneError{
					{Name: "test.example.com", Type: recordType, Error: "Test error"},
				},
			}
			schema := resp.Schema()
			errs := schema.Parse(resp, &resp)
			assert.Nil(t, errs, "Valid error with type %s should pass validation", recordType)
		}
	})

	t.Run("import zone error name is required", func(t *testing.T) {
		resp := ImportZoneResponse{
			Errors: []ImportZoneError{
				{Type: "A", Error: "Test error"},
			},
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Error with missing name should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("import zone error type is required", func(t *testing.T) {
		resp := ImportZoneResponse{
			Errors: []ImportZoneError{
				{Name: "test.example.com", Error: "Test error"},
			},
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Error with missing type should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})

	t.Run("import zone error message is required", func(t *testing.T) {
		resp := ImportZoneResponse{
			Errors: []ImportZoneError{
				{Name: "test.example.com", Type: "A"},
			},
		}
		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.NotNil(t, errs, "Error with missing error message should fail validation")
		assert.NotEmpty(t, errs, "Should have validation errors")
	})
}

func TestImportZoneResponse_RealWorldScenarios(t *testing.T) {
	t.Run("scenario: successful import with all records created", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "www.example.com", Type: "A", Content: "192.168.1.1", TTL: 3600},
				{Name: "www.example.com", Type: "AAAA", Content: "2001:db8::1", TTL: 3600},
				{Name: "mail.example.com", Type: "MX", Content: "10 mail.example.com", TTL: 3600},
			},
			SkippedCount: 0,
			FailedCount:  0,
			Errors:       []ImportZoneError{},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Valid successful import response should pass validation")
		assert.Len(t, resp.CreatedRecords, 3)
		assert.Equal(t, 0, resp.SkippedCount)
		assert.Equal(t, 0, resp.FailedCount)
		assert.Len(t, resp.Errors, 0)
	})

	t.Run("scenario: import with mixed success, skip, and failure", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "www.example.com", Type: "A", Content: "192.168.1.1", TTL: 3600},
				{Name: "mail.example.com", Type: "MX", Content: "10 mail.example.com", TTL: 3600},
			},
			SkippedCount: 3,
			FailedCount:  1,
			Errors: []ImportZoneError{
				{Name: "invalid.example.com", Type: "A", Error: "Invalid IP address"},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Valid mixed result response should pass validation")
		assert.Len(t, resp.CreatedRecords, 2)
		assert.Equal(t, 3, resp.SkippedCount)
		assert.Equal(t, 1, resp.FailedCount)
		assert.Len(t, resp.Errors, 1)
	})

	t.Run("scenario: import with all failures", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{},
			SkippedCount:   0,
			FailedCount:    3,
			Errors: []ImportZoneError{
				{Name: "test1.example.com", Type: "A", Error: "Invalid IP address"},
				{Name: "test2.example.com", Type: "MX", Error: "Invalid priority"},
				{Name: "test3.example.com", Type: "CNAME", Error: "Invalid CNAME target"},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Valid all-failure response should pass validation")
		assert.Len(t, resp.CreatedRecords, 0)
		assert.Equal(t, 0, resp.SkippedCount)
		assert.Equal(t, 3, resp.FailedCount)
		assert.Len(t, resp.Errors, 3)
	})

	t.Run("scenario: import with only skipped records", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{},
			SkippedCount:   5,
			FailedCount:    0,
			Errors:         []ImportZoneError{},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Valid skip-only response should pass validation")
		assert.Equal(t, 5, resp.SkippedCount)
	})

	t.Run("scenario: empty zone file import", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{},
			SkippedCount:   0,
			FailedCount:    0,
			Errors:         []ImportZoneError{},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Valid empty import response should pass validation")
	})

	t.Run("scenario: large import with many records", func(t *testing.T) {
		createdRecords := make([]CreatedRecord, 100)
		for i := 0; i < 100; i++ {
			createdRecords[i] = CreatedRecord{
				Name:    "test.example.com",
				Type:    "A",
				Content: "192.168.1.1",
				TTL:     3600,
			}
		}
		resp := ImportZoneResponse{
			CreatedRecords: createdRecords,
			SkippedCount:   10,
			FailedCount:    5,
			Errors:         []ImportZoneError{},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Valid large import response should pass validation")
		assert.Len(t, resp.CreatedRecords, 100)
	})
}

func TestCreatedRecord(t *testing.T) {
	t.Run("created record struct is properly defined", func(t *testing.T) {
		record := CreatedRecord{
			Name:    "test.example.com",
			Type:    "A",
			Content: "192.168.1.1",
			TTL:     3600,
		}
		assert.Equal(t, "test.example.com", record.Name)
		assert.Equal(t, "A", record.Type)
		assert.Equal(t, "192.168.1.1", record.Content)
		assert.Equal(t, uint(3600), record.TTL)
	})

	t.Run("created record with A type", func(t *testing.T) {
		record := CreatedRecord{
			Name:    "www.example.com",
			Type:    "A",
			Content: "192.168.1.1",
			TTL:     3600,
		}
		assert.Equal(t, "www.example.com", record.Name)
		assert.Equal(t, "A", record.Type)
	})

	t.Run("created record with CNAME type", func(t *testing.T) {
		record := CreatedRecord{
			Name:    "www.example.com",
			Type:    "CNAME",
			Content: "example.com",
			TTL:     3600,
		}
		assert.Equal(t, "www.example.com", record.Name)
		assert.Equal(t, "CNAME", record.Type)
	})
}

func TestImportZoneError(t *testing.T) {
	t.Run("import zone error struct is properly defined", func(t *testing.T) {
		err := ImportZoneError{
			Name:  "test.example.com",
			Type:  "A",
			Error: "Invalid IP address",
		}
		assert.Equal(t, "test.example.com", err.Name)
		assert.Equal(t, "A", err.Type)
		assert.Equal(t, "Invalid IP address", err.Error)
	})

	t.Run("import zone error with validation error", func(t *testing.T) {
		err := ImportZoneError{
			Name:  "www.example.com",
			Type:  "A",
			Error: "Invalid IP address format",
		}
		assert.Equal(t, "www.example.com", err.Name)
		assert.Equal(t, "A", err.Type)
		assert.Equal(t, "Invalid IP address format", err.Error)
	})

	t.Run("import zone error with permission error", func(t *testing.T) {
		err := ImportZoneError{
			Name:  "example.com",
			Type:  "NS",
			Error: "Permission denied",
		}
		assert.Equal(t, "example.com", err.Name)
		assert.Equal(t, "NS", err.Type)
		assert.Equal(t, "Permission denied", err.Error)
	})
}

func TestImportZoneResponse_ComplexValidation(t *testing.T) {
	t.Run("valid response with all fields and multiple record types", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "www.example.com", Type: "A", Content: "192.168.1.1", TTL: 3600},
				{Name: "www.example.com", Type: "AAAA", Content: "2001:db8::1", TTL: 3600},
				{Name: "mail.example.com", Type: "MX", Content: "10 mail.example.com", TTL: 3600},
				{Name: "example.com", Type: "TXT", Content: "v=spf1 mx -all", TTL: 3600},
			},
			SkippedCount: 2,
			FailedCount:  1,
			Errors: []ImportZoneError{
				{Name: "invalid.example.com", Type: "A", Error: "Invalid IP address"},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Valid complex response should pass validation")
		assert.Len(t, resp.CreatedRecords, 4)
		assert.Equal(t, 2, resp.SkippedCount)
		assert.Equal(t, 1, resp.FailedCount)
		assert.Len(t, resp.Errors, 1)
	})

	t.Run("response with maximum allowed record types", func(t *testing.T) {
		resp := ImportZoneResponse{
			CreatedRecords: []CreatedRecord{
				{Name: "www.example.com", Type: "A", Content: "192.168.1.1", TTL: 3600},
				{Name: "www.example.com", Type: "AAAA", Content: "2001:db8::1", TTL: 3600},
				{Name: "www.example.com", Type: "CNAME", Content: "example.com", TTL: 3600},
				{Name: "mail.example.com", Type: "MX", Content: "10 mail.example.com", TTL: 3600},
				{Name: "example.com", Type: "TXT", Content: "v=spf1 mx -all", TTL: 3600},
				{Name: "ns1.example.com", Type: "NS", Content: "ns1.example.com", TTL: 3600},
				{Name: "_sip._tcp.example.com", Type: "SRV", Content: "10 60 5060 sip.example.com", TTL: 3600},
				{Name: "alias.example.com", Type: "ALIAS", Content: "example.com", TTL: 3600},
			},
		}

		schema := resp.Schema()
		errs := schema.Parse(resp, &resp)
		assert.Nil(t, errs, "Valid response with all types should pass validation")
		assert.Len(t, resp.CreatedRecords, 8)
	})
}
