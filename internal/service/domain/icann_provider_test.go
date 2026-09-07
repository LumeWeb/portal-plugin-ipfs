package domain

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
)

func TestICANNProvider_Protocol(t *testing.T) {
	p := NewICANNProvider([]string{"ns1.example.com."})
	assert.Equal(t, "icann", p.Protocol())
}

func TestICANNProvider_Validate(t *testing.T) {
	p := NewICANNProvider(nil)

	assert.NoError(t, p.Validate("example.com"))
	assert.NoError(t, p.Validate("www.example.com"))
	assert.Error(t, p.Validate(""))
	assert.Error(t, p.Validate("nodot"))
	// A dotted name is NOT ICANN merely because it is dotted; its final label
	// must be an IANA-registered ICANN TLD.
	assert.Error(t, p.Validate("blog.altroot"))
	assert.Error(t, p.Validate("foo.test-tld-not-real"))
}

func TestICANNProvider_BuildDelegation(t *testing.T) {
	p := NewICANNProvider([]string{"ns1.example.com.", "ns2.example.com."})
	result, err := p.BuildDelegation(context.Background(), 1, "example.com", &pluginDb.Website{}, nil)
	assert.NoError(t, err)

	var bundle ICANNDelegation
	require.NoError(t, json.Unmarshal(result, &bundle))
	assert.Contains(t, bundle.Nameservers, "ns1.example.com.")
}

func TestICANNProvider_Inspect(t *testing.T) {
	// ICANN names are never on-chain managed: Inspect is a static false.
	p := NewICANNProvider(nil)
	onchain, err := p.Inspect(context.Background(), "example.com")
	assert.NoError(t, err)
	assert.False(t, onchain)
}

// TestICANNProvider_InspectRouteAndBoolAgree preserves the existing Inspect
// contract while asserting the typed route identity: ICANN always resolves
// through standard DNS over the system-dns backend, measured (never assumed).
func TestICANNProvider_InspectRouteAndBoolAgree(t *testing.T) {
	p := NewICANNProvider(nil)
	obs, err := p.InspectRoute(context.Background(), "example.com")
	require.NoError(t, err)
	assert.Equal(t, domainpolicy.ResolutionRouteStandardDNS, obs.Route, "ICANN resolves via standard DNS")
	assert.Equal(t, domainpolicy.BackendSystemDNS, obs.Backend, "ICANN backend is system-dns")
	assert.False(t, obs.AssumedSource, "ICANN route is static, never assumed")

	// BOOL/TYPED PARITY: the compatibility Inspect bool must be exactly
	// OnChainManagedFromRoute(InspectRoute) for the same call.
	onchain, err := p.Inspect(context.Background(), "example.com")
	require.NoError(t, err)
	boolFromTyped, err := OnChainManagedFromRoute(obs, nil)
	require.NoError(t, err)
	assert.Equal(t, boolFromTyped, onchain, "Inspect must equal the typed adaptation of InspectRoute")
	assert.False(t, onchain, "ICANN is never on-chain managed")
}

func TestICANNProvider_VerifyDelegation(t *testing.T) {
	p := NewICANNProvider(nil)
	// ICANN ignores expectedDS entirely.
	verified, err := p.VerifyDelegation(context.Background(), "example.com", "")
	assert.NoError(t, err)
	assert.True(t, verified)
}

func TestICANNProvider_Nameservers(t *testing.T) {
	p := NewICANNProvider([]string{"ns1.example.com.", "ns2.example.com."})
	assert.Equal(t, []string{"ns1.example.com.", "ns2.example.com."}, p.Nameservers())

	empty := NewICANNProvider(nil)
	assert.Nil(t, empty.Nameservers())
}
