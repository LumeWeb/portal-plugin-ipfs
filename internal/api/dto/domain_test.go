package dto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"gorm.io/datatypes"
)

// TestDomainResponse_FromModel_HNS verifies a full HNS DelegationBundle is
// projected into the typed DNSDelegation shape with any stored DS entry
// stripped (the DS is derived live, not served from persisted data).
func TestDomainResponse_FromModel_HNS(t *testing.T) {
	delegation := datatypes.JSONMap{
		"mode": "delegated",
		"parent_records": []map[string]any{
			{"type": "NS", "value": "ns1.lumeweb,ns2.lumeweb"},
			{"type": "GLUE4", "ns": "ns1.lumeweb.", "address": "185.189.155.208"},
			{"type": "DS", "value": "lumeweb. 3600 IN DS 12345 13 2 <digest>"},
		},
		"authoritative_records": []map[string]any{
			{"type": "NS", "value": "ns1.lumeweb\nns2.lumeweb"},
			{"type": "TLSA", "value": "_443._tcp.lumeweb. 3600 IN TLSA 3 1 1 <hash>"},
		},
	}

	model := &pluginDb.WebsiteDomain{
		ID:             7,
		Domain:         "lumeweb",
		Namespace:      pluginDb.DomainNamespaceHNS,
		Status:         pluginDb.DomainStatusRecordsGenerated,
		ZoneName:       "lumeweb.",
		GatewayHost:    "gateway.lumeweb.com",
		DelegationData: delegation,
	}

	var resp DomainResponse
	err := resp.FromModel(model)
	require.NoError(t, err)

	assert.Equal(t, uint(7), resp.ID)
	assert.Equal(t, "lumeweb", resp.Domain)
	assert.Equal(t, "hns", resp.Namespace)
	assert.Equal(t, "records_generated", resp.Status)
	assert.Equal(t, "lumeweb.", resp.ZoneName)
	assert.Equal(t, "gateway.lumeweb.com", resp.GatewayHost)

	require.NotNil(t, resp.Delegation, "delegation should be populated for HNS")
	assert.Equal(t, "delegated", resp.Delegation.Mode)

	// No first-class DS field: the DS is a parent_records entry derived from
	// the live PowerDNS key (computed on the fly in dns-requirements), never a
	// stored/promoted value. A DS present in the stored delegation data is
	// stale/leftover and is stripped at read time.
	assert.NotContains(t, resp.Delegation.ParentRecords, "ds")

	// Parent records preserved (stored DS entry stripped); type/ns/address kept.
	require.Len(t, resp.Delegation.ParentRecords, 2)
	assert.Equal(t, "NS", resp.Delegation.ParentRecords[0].Type)
	assert.Equal(t, "GLUE4", resp.Delegation.ParentRecords[1].Type)
	assert.Equal(t, "ns1.lumeweb.", resp.Delegation.ParentRecords[1].NS)
	assert.Equal(t, "185.189.155.208", resp.Delegation.ParentRecords[1].Address)

	// Authoritative records preserved.
	require.Len(t, resp.Delegation.AuthoritativeRecords, 2)
	assert.Equal(t, "TLSA", resp.Delegation.AuthoritativeRecords[1].Type)
}

// TestDomainResponse_FromModel_ICANN verifies the ICANN nameservers shortcut is
// projected into the typed shape.
func TestDomainResponse_FromModel_ICANN(t *testing.T) {
	delegation := datatypes.JSONMap{
		"nameservers": []string{"ns1.example.com", "ns2.example.com"},
	}

	model := &pluginDb.WebsiteDomain{
		ID:             3,
		Domain:         "example.com",
		Namespace:      pluginDb.DomainNamespaceICANN,
		Status:         pluginDb.DomainStatusActive,
		ZoneName:       "example.com.",
		GatewayHost:    "gateway.example.com",
		DelegationData: delegation,
	}

	var resp DomainResponse
	err := resp.FromModel(model)
	require.NoError(t, err)

	require.NotNil(t, resp.Delegation)
	assert.Equal(t, []string{"ns1.example.com", "ns2.example.com"}, resp.Delegation.Nameservers)
	assert.Empty(t, resp.Delegation.ParentRecords)
}

// TestDomainResponse_FromModel_NoDelegation verifies core fields still populate
// when DelegationData is empty and no gateway host is set.
func TestDomainResponse_FromModel_NoDelegation(t *testing.T) {
	model := &pluginDb.WebsiteDomain{
		ID:        1,
		Domain:    "example.com",
		Namespace: pluginDb.DomainNamespaceICANN,
		Status:    pluginDb.DomainStatusDraft,
		ZoneName:  "example.com.",
	}

	var resp DomainResponse
	err := resp.FromModel(model)
	require.NoError(t, err)

	assert.Equal(t, uint(1), resp.ID)
	assert.Equal(t, "draft", resp.Status)
	assert.Nil(t, resp.Delegation, "delegation should be nil when no DelegationData")
	assert.Empty(t, resp.GatewayHost)
}
