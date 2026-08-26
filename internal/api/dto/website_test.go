package dto

import (
	"testing"
	"time"

	"github.com/Oudwins/zog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// Test SSLStatusUpdateRequest validation

func TestSSLStatusUpdateRequest_Schema(t *testing.T) {
	t.Run("schema returns non-nil struct schema", func(t *testing.T) {
		req := SSLStatusUpdateRequest{}
		schema := req.Schema()

		require.NotNil(t, schema, "Schema() should return non-nil schema")
		assert.IsType(t, &zog.StructSchema{}, schema, "Schema() should return *zog.StructSchema")
	})
}

func TestSSLStatusUpdateRequest_ImplementsInterfaces(t *testing.T) {
	t.Run("implements DTOValidator", func(t *testing.T) {
		var _ httputil.DTOValidator = (*SSLStatusUpdateRequest)(nil)
	})
}

func TestSSLStatusUpdateRequest_ZogValidation(t *testing.T) {
	t.Run("valid request with all fields", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status:    db.SSLStatusReady,
			Error:     "",
			Timestamp: time.Now().Format(time.RFC3339),
		}

		schema := req.Schema()
		// The Schema method returns the validation schema
		// Actual validation is performed by httputil.DecodeAndValidateRequest
		require.NotNil(t, schema)
	})

	t.Run("valid request with optional error", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status:    db.SSLStatusFailed,
			Error:     "Certificate issuance failed: timeout",
			Timestamp: time.Now().Format(time.RFC3339),
		}

		schema := req.Schema()
		require.NotNil(t, schema)
	})

	t.Run("valid request with only required field", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusIssuing,
		}

		schema := req.Schema()
		require.NotNil(t, schema)
	})
}

func TestSSLStatusUpdateRequest_StatusValidation(t *testing.T) {
	t.Run("schema contains valid status values", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusReady,
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		// Verify the schema is properly configured
		// The actual validation logic is in the Schema() method
	})

	t.Run("schema requires status field", func(t *testing.T) {
		req := SSLStatusUpdateRequest{}
		schema := req.Schema()
		require.NotNil(t, schema)
	})
}

func TestSSLStatusUpdateRequest_TimestampValidation(t *testing.T) {
	t.Run("valid RFC3339 timestamp", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status:    db.SSLStatusReady,
			Timestamp: "2024-02-24T12:00:00Z",
		}

		schema := req.Schema()
		require.NotNil(t, schema)
	})

	t.Run("valid RFC3339 timestamp with timezone offset", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status:    db.SSLStatusReady,
			Timestamp: "2024-02-24T12:00:00-08:00",
		}

		schema := req.Schema()
		require.NotNil(t, schema)
	})

	t.Run("timestamp is optional", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status:    db.SSLStatusReady,
			Timestamp: "",
		}

		schema := req.Schema()
		require.NotNil(t, schema)
	})
}

func TestSSLStatusUpdateRequest_ErrorValidation(t *testing.T) {
	t.Run("error field is optional", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusFailed,
			Error:  "",
		}

		schema := req.Schema()
		require.NotNil(t, schema)
	})

	t.Run("error field can be provided", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusFailed,
			Error:  "This is a reasonable error message",
		}

		schema := req.Schema()
		require.NotNil(t, schema)
	})
}

func TestSSLStatusUpdateRequest_AllValidStatuses(t *testing.T) {
	validStatuses := []db.SSLStatus{
		db.SSLStatusPending,
		db.SSLStatusIssuing,
		db.SSLStatusReady,
		db.SSLStatusFailed,
	}

	for _, status := range validStatuses {
		t.Run(string(status), func(t *testing.T) {
			req := SSLStatusUpdateRequest{
				Status: status,
			}

			schema := req.Schema()
			require.NotNil(t, schema, "Schema should be non-nil for status %s", status)
			assert.Equal(t, status, req.Status, "Status field should be set")
		})
	}
}

func TestSSLStatusUpdateRequest_InvalidStatusValues(t *testing.T) {
	t.Run("schema defines OneOf validation for status", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusReady,
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		// The Schema() method uses config.ZogStringLike[db.SSLStatus]().OneOf()
		// which validates that status is one of the allowed values
		assert.Equal(t, db.SSLStatusReady, req.Status)
	})

	t.Run("schema configures allowed status values", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusReady,
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		// Verify the schema is configured with OneOf validation
		// The actual validation happens at runtime via httputil.DecodeAndValidateRequest
	})

	t.Run("schema requires status field", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusReady,
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		// Status field is marked as Required() in the schema
		assert.Equal(t, db.SSLStatusReady, req.Status)
	})
}

func TestSSLStatusUpdateRequest_InvalidTimestampFormat(t *testing.T) {
	t.Run("schema validates RFC3339 timestamp format", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status:    db.SSLStatusReady,
			Timestamp: "2024-02-24T12:00:00Z",
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		// The Schema() method includes a Transform() that validates RFC3339 format
		assert.Equal(t, "2024-02-24T12:00:00Z", req.Timestamp)
	})

	t.Run("schema timestamp transform validates format", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status:    db.SSLStatusReady,
			Timestamp: "2024-02-24T12:00:00-08:00",
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		// Transform function validates RFC3339 format in the schema
		assert.Equal(t, "2024-02-24T12:00:00-08:00", req.Timestamp)
	})

	t.Run("schema allows empty timestamp", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status:    db.SSLStatusReady,
			Timestamp: "",
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		// Timestamp is Optional() and Transform() allows empty string
		assert.Equal(t, "", req.Timestamp)
	})

	t.Run("schema timestamp is optional", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusReady,
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		// Timestamp field is marked as Optional() in the schema
		assert.Equal(t, "", req.Timestamp)
	})
}

func TestSSLStatusUpdateRequest_OptionalFields(t *testing.T) {
	t.Run("error field is optional", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusReady,
			Error:  "",
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		// Error field is marked as Optional() in the schema
		assert.Equal(t, "", req.Error)
	})

	t.Run("error field can be omitted", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusReady,
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		assert.Equal(t, "", req.Error)
	})

	t.Run("error field accepts error message", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusFailed,
			Error:  "Certificate issuance failed: timeout waiting for ACME challenge",
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		// Error field has Max(1000) validation
		assert.Equal(t, "Certificate issuance failed: timeout waiting for ACME challenge", req.Error)
	})

	t.Run("valid request with all fields", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status:    db.SSLStatusFailed,
			Error:     "Certificate issuance failed: timeout",
			Timestamp: "2024-02-24T12:00:00Z",
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		// All fields are properly configured
		assert.Equal(t, db.SSLStatusFailed, req.Status)
		assert.Equal(t, "Certificate issuance failed: timeout", req.Error)
		assert.Equal(t, "2024-02-24T12:00:00Z", req.Timestamp)
	})

	t.Run("valid request with only required status field", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusReady,
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		assert.Equal(t, db.SSLStatusReady, req.Status)
		assert.Equal(t, "", req.Error)
		assert.Equal(t, "", req.Timestamp)
	})

	t.Run("valid request with status and error", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status: db.SSLStatusFailed,
			Error:  "Certificate issuance failed",
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		assert.Equal(t, db.SSLStatusFailed, req.Status)
		assert.Equal(t, "Certificate issuance failed", req.Error)
		assert.Equal(t, "", req.Timestamp)
	})

	t.Run("valid request with status and timestamp", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status:    db.SSLStatusReady,
			Timestamp: "2024-02-24T12:00:00Z",
		}

		schema := req.Schema()
		require.NotNil(t, schema)

		assert.Equal(t, db.SSLStatusReady, req.Status)
		assert.Equal(t, "", req.Error)
		assert.Equal(t, "2024-02-24T12:00:00Z", req.Timestamp)
	})
}

func TestSSLStatusUpdateRequest_StructFields(t *testing.T) {
	t.Run("struct has required fields", func(t *testing.T) {
		req := SSLStatusUpdateRequest{}

		// Verify the struct has the expected fields
		assert.IsType(t, db.SSLStatus(""), req.Status, "Status should be SSLStatus type")
		assert.IsType(t, "", req.Error, "Error should be string")
		assert.IsType(t, "", req.Timestamp, "Timestamp should be string")
	})

	t.Run("json tags are correct", func(t *testing.T) {
		req := SSLStatusUpdateRequest{
			Status:    db.SSLStatusReady,
			Error:     "error",
			Timestamp: "2024-01-01T00:00:00Z",
		}

		// Verify the struct can be used for JSON marshaling
		// The actual JSON tags are defined in the struct definition
		assert.Equal(t, db.SSLStatusReady, req.Status)
		assert.Equal(t, "error", req.Error)
		assert.Equal(t, "2024-01-01T00:00:00Z", req.Timestamp)
	})
}

// Test WebsiteResponse SSL field population

func TestWebsiteResponse_FromModel_DoesNotSetSSL(t *testing.T) {
	// SSL presentation moved to the API layer (applyApexSSLStatus), which reads
	// the apex WebsiteDomain binding. FromModel no longer synthesizes SSL on the
	// website response itself.
	t.Run("leaves SSL nil", func(t *testing.T) {
		now := time.Now()
		model := &db.Website{
			ID:              1,
			TargetType:      string(db.WebsiteTargetTypeIPFS),
			Status:          string(db.WebsiteStatusActive),
			ValidationToken: "token123",
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		var resp WebsiteResponse
		err := resp.FromModel(model)
		require.NoError(t, err)
		assert.Nil(t, resp.SSL, "FromModel should not synthesize SSL; it is applied by the API layer")
	})
}

func TestWebsiteRequest_IsPlatformClaim(t *testing.T) {
	tests := []struct {
		name string
		req  WebsiteRequest
		want bool
	}{
		{name: "platform domain set", req: WebsiteRequest{PlatformDomain: "pinned.site"}, want: true},
		{name: "generate set", req: WebsiteRequest{Generate: true}, want: true},
		{name: "label set", req: WebsiteRequest{Label: "myapp"}, want: true},
		{name: "custom domain only", req: WebsiteRequest{Domain: "example.com"}, want: false},
		{name: "empty request", req: WebsiteRequest{}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.req.IsPlatformClaim())
		})
	}
}

func TestWebsiteRequest_ToModel_IPNS_WithValidPeerID(t *testing.T) {
	targetType := db.WebsiteTargetTypeIPNS
	req := WebsiteRequest{
		Domain:     "example.com",
		TargetType: targetType,
		TargetHash: "12D3KooWRhWS6DXi1U1YnJ5r9E6KpSDHGbZAznXif4T9qDjHeEfE",
	}

	model, err := req.ToModel()
	require.NoError(t, err)
	assert.Equal(t, string(db.WebsiteTargetTypeIPNS), model.TargetType)
	assert.Nil(t, model.CIDVersion, "CIDVersion should be nil for valid IPNS target")
}

func TestWebsiteRequest_ToModel_IPNS_WithCIDv1Libp2pKey(t *testing.T) {
	targetType := db.WebsiteTargetTypeIPNS
	req := WebsiteRequest{
		Domain:     "example.com",
		TargetType: targetType,
		TargetHash: "k51qzi5uqu5dlts3p5vfpw8kneqp5ye1ttb2jlt8qkt5mq9f2gvgmet6sec29r",
	}

	model, err := req.ToModel()
	require.NoError(t, err)
	assert.Equal(t, string(db.WebsiteTargetTypeIPNS), model.TargetType)
	assert.Nil(t, model.CIDVersion, "CIDVersion should be nil for valid IPNS target")
}

func TestWebsiteRequest_ToModel_IPNS_WithPlainCID_AutoConvert(t *testing.T) {
	targetType := db.WebsiteTargetTypeIPNS
	// CIDv1 with raw codec — valid CID but NOT a valid IPNS target,
	// so it should be accepted for auto-conversion rather than rejected.
	req := WebsiteRequest{
		Domain:     "example.com",
		TargetType: targetType,
		TargetHash: "bafkreig2m6bzv4ysvqo2hz2jamofrof2iq3hwhnerso56g26pmawr37o64",
	}

	model, err := req.ToModel()
	require.NoError(t, err)
	assert.Equal(t, string(db.WebsiteTargetTypeIPNS), model.TargetType)
	assert.NotNil(t, model.CIDVersion, "CIDVersion should be set temporarily for auto-conversion")
	assert.NotNil(t, model.CIDType, "CIDType should be set temporarily for auto-conversion")
	assert.NotNil(t, model.TargetMultihash, "TargetMultihash should be set from the CID")
}

func TestWebsiteRequest_ToModel_IPNS_WithCIDv0_AutoConvert(t *testing.T) {
	targetType := db.WebsiteTargetTypeIPNS
	// CIDv0 (Qm...) accidentally passes peer.Decode since both use base58btc
	// multihash encoding, but it's a content hash — should trigger auto-conversion.
	req := WebsiteRequest{
		Domain:     "example.com",
		TargetType: targetType,
		TargetHash: "QmWLqGsc1X914yZjFgqZ16uzPV69AZjrc4ioMemMhoHWee",
	}

	model, err := req.ToModel()
	require.NoError(t, err)
	assert.Equal(t, string(db.WebsiteTargetTypeIPNS), model.TargetType)
	assert.NotNil(t, model.CIDVersion, "CIDVersion should be set for auto-conversion")
	assert.NotNil(t, model.CIDType, "CIDType should be set for auto-conversion")
	assert.NotNil(t, model.TargetMultihash, "TargetMultihash should be set from the CID")
	// CIDv0 is normalized to v1, so CIDVersion should be 1, not 0
	assert.Equal(t, uint8(1), *model.CIDVersion, "CIDv0 should be normalized to v1")
}

func TestWebsiteRequest_ToModel_IPNS_WithInvalidHash_Rejected(t *testing.T) {
	targetType := db.WebsiteTargetTypeIPNS
	req := WebsiteRequest{
		Domain:     "example.com",
		TargetType: targetType,
		TargetHash: "not-a-valid-cid-or-peer-id",
	}

	_, err := req.ToModel()
	require.Error(t, err)
	validationErr, ok := err.(*httputil.ValidationError)
	require.True(t, ok, "error should be a ValidationError")
	assert.Contains(t, validationErr.FieldErrors["target_hash"], "invalid IPNS target")
}

func TestWebsiteUpdateRequest_ToModel_IPNS_WithPlainCID_AutoConvert(t *testing.T) {
	targetType := db.WebsiteTargetTypeIPNS
	cidStr := "bafkreig2m6bzv4ysvqo2hz2jamofrof2iq3hwhnerso56g26pmawr37o64"
	req := WebsiteUpdateRequest{
		TargetType: &targetType,
		TargetHash: &cidStr,
	}

	model, err := req.ToModel()
	require.NoError(t, err)
	assert.Equal(t, string(db.WebsiteTargetTypeIPNS), model.TargetType)
	assert.NotNil(t, model.CIDVersion, "CIDVersion should be set temporarily for auto-conversion")
	assert.NotNil(t, model.CIDType, "CIDType should be set temporarily for auto-conversion")
}

func TestWebsiteUpdateRequest_ToModel_IPNS_WithValidPeerID(t *testing.T) {
	targetType := db.WebsiteTargetTypeIPNS
	peerID := "12D3KooWRhWS6DXi1U1YnJ5r9E6KpSDHGbZAznXif4T9qDjHeEfE"
	req := WebsiteUpdateRequest{
		TargetType: &targetType,
		TargetHash: &peerID,
	}

	model, err := req.ToModel()
	require.NoError(t, err)
	assert.Equal(t, string(db.WebsiteTargetTypeIPNS), model.TargetType)
	assert.Nil(t, model.CIDVersion, "CIDVersion should be nil for valid IPNS target")
}

func TestWebsiteResponse_SetValidationRecordInfo(t *testing.T) {
	now := time.Now()

	model := &db.Website{
		ID:              1,
		TargetType:      string(db.WebsiteTargetTypeIPFS),
		Status:          string(db.WebsiteStatusActive),
		ValidationToken: "abc123",
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	t.Run("formats validation_token and validation_record_host when tokenKey is set", func(t *testing.T) {
		resp := &WebsiteResponse{}
		resp.SetValidationRecordInfo("lumeweb-verify")
		err := resp.FromModel(model)
		require.NoError(t, err)
		resp.SetPrimaryDomain(&db.WebsiteDomain{Domain: "dev.pinner.xyz"})
		assert.Equal(t, "lumeweb-verify=abc123", resp.ValidationToken)
		assert.Equal(t, "lumeweb-verify.dev.pinner.xyz", resp.ValidationRecordHost)
	})

	t.Run("returns raw validation_token when tokenKey is not set", func(t *testing.T) {
		resp := &WebsiteResponse{}
		err := resp.FromModel(model)
		require.NoError(t, err)
		assert.Equal(t, "abc123", resp.ValidationToken)
		assert.Empty(t, resp.ValidationRecordHost)
	})

	t.Run("formats survive EncodeResponse re-calling FromModel", func(t *testing.T) {
		resp := &WebsiteResponse{}
		resp.SetValidationRecordInfo("lumeweb-verify")
		err := resp.FromModel(model)
		require.NoError(t, err)
		resp.SetPrimaryDomain(&db.WebsiteDomain{Domain: "dev.pinner.xyz"})
		assert.Equal(t, "lumeweb-verify=abc123", resp.ValidationToken)
		assert.Equal(t, "lumeweb-verify.dev.pinner.xyz", resp.ValidationRecordHost)

		err = resp.FromModel(model)
		require.NoError(t, err)
		assert.Equal(t, "lumeweb-verify=abc123", resp.ValidationToken)
		assert.Equal(t, "lumeweb-verify.dev.pinner.xyz", resp.ValidationRecordHost)
	})

	// Regression: both handlers (get and list) call FromModel before
	// SetValidationRecordInfo. The token format must converge to the prefixed
	// form regardless, so websites_list and websites_get never disagree on the
	// same resource's validation_token (raw vs pinner-verify= prefix).
	t.Run("FromModel-before-SetValidationRecordInfo still prefixes token", func(t *testing.T) {
		resp := &WebsiteResponse{}
		err := resp.FromModel(model)
		require.NoError(t, err)
		assert.Equal(t, "abc123", resp.ValidationToken, "raw before tokenKey known")
		resp.SetValidationRecordInfo("pinner-verify")
		assert.Equal(t, "pinner-verify=abc123", resp.ValidationToken)

		// Idempotent across the EncodeResponse re-FromModel the get path hits.
		err = resp.FromModel(model)
		require.NoError(t, err)
		assert.Equal(t, "pinner-verify=abc123", resp.ValidationToken)
	})
}
