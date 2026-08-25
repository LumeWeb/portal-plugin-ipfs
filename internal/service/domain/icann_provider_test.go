package domain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
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

	bundle, ok := result.(ICANNDelegation)
	assert.True(t, ok)
	assert.Contains(t, bundle.Nameservers, "ns1.example.com.")
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
