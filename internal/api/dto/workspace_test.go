package dto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorkspaceRequest_WebsiteIDWireForm verifies the WebsiteID wire contract:
// a nil WebsiteID creates an unattached workspace and is omitted from the JSON,
// while a provided (including zero) ID is preserved faithfully. The Create path
// normalizes a nil OR <= 0 WebsiteID to unattached (omit semantics); a positive
// ID is attached. website_id 0 is therefore never treated as a required value
// or attached at creation.
func TestWorkspaceRequest_WebsiteIDWireForm(t *testing.T) {
	t.Run("nil website_id is omitted and means unattached", func(t *testing.T) {
		req := WorkspaceRequest{}
		raw, err := json.Marshal(req)
		require.NoError(t, err)
		assert.NotContains(t, string(raw), "website_id", "unattached request must not carry website_id")

		// The empty request must round-trip to a nil WebsiteID so the Create
		// path sees unattached, never website_id 0.
		var decoded WorkspaceRequest
		require.NoError(t, json.Unmarshal(raw, &decoded))
		assert.Nil(t, decoded.WebsiteID)
	})

	t.Run("explicit website_id is preserved", func(t *testing.T) {
		req := WorkspaceRequest{WebsiteID: new(int(42))}
		raw, err := json.Marshal(req)
		require.NoError(t, err)
		assert.JSONEq(t, `{"website_id":42}`, string(raw))

		var decoded WorkspaceRequest
		require.NoError(t, json.Unmarshal(raw, &decoded))
		require.NotNil(t, decoded.WebsiteID)
		assert.Equal(t, int(42), *decoded.WebsiteID)
	})

	t.Run("empty body decodes to nil website_id", func(t *testing.T) {
		var decoded WorkspaceRequest
		require.NoError(t, json.Unmarshal([]byte(`{}`), &decoded))
		assert.Nil(t, decoded.WebsiteID)
	})

	t.Run("explicit zero website_id is preserved (normalized to unattached by Create)", func(t *testing.T) {
		req := WorkspaceRequest{WebsiteID: new(int(0))}
		raw, err := json.Marshal(req)
		require.NoError(t, err)
		assert.JSONEq(t, `{"website_id":0}`, string(raw))

		var decoded WorkspaceRequest
		require.NoError(t, json.Unmarshal(raw, &decoded))
		require.NotNil(t, decoded.WebsiteID)
		assert.Equal(t, int(0), *decoded.WebsiteID, "DTO carries 0 faithfully; Create must treat it as unattached")
	})
}
