package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
)


func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	mockProv := mocks.NewMockDomainProvider(t)
	mockProv.EXPECT().Protocol().Return("test").Maybe()

	r.Register(mockProv)
	assert.Equal(t, mockProv, r.Get("test"))
	assert.Nil(t, r.Get("missing"))
	assert.Equal(t, []string{"test"}, r.Names())
}

func TestRegistry_DuplicatePanics(t *testing.T) {
	r := NewRegistry()
	mockProv := mocks.NewMockDomainProvider(t)
	mockProv.EXPECT().Protocol().Return("test").Maybe()

	r.Register(mockProv)
	assert.Panics(t, func() {
		mockProv2 := mocks.NewMockDomainProvider(t)
		mockProv2.EXPECT().Protocol().Return("test").Maybe()
		r.Register(mockProv2)
	})
}

func TestNormalizeDomain(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain domain", "example.com", "example.com"},
		{"www prefix", "www.example.com", "example.com"},
		{"www subdomain", "www.sub.example.com", "sub.example.com"},
		{"uppercase with www", "WWW.Example.COM", "example.com"},
		{"mixed case no www", "Example.COM", "example.com"},
		{"only www", "www.com", "com"},
		{"deep www", "www.www.example.com", "www.example.com"},
		{"leading space", " www.example.com", "example.com"},
		{"trailing space", "www.example.com ", "example.com"},
		{"both spaces", " www.example.com ", "example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeDomain(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestHNSProvider_ApexRecordType(t *testing.T) {
	p := NewHNSProvider("", nil, TLSASource{})
	assert.Equal(t, pluginCore.RecordTypeA, p.ApexRecordType())
}

func TestICANNProvider_ApexRecordType(t *testing.T) {
	p := NewICANNProvider(nil)
	assert.Equal(t, pluginCore.RecordTypeALIAS, p.ApexRecordType())
}

func TestRegistry_NameserversFor(t *testing.T) {
	r := NewRegistry()
	r.Register(NewICANNProvider([]string{"ns1.icann.example.", "ns2.icann.example."}))
	r.Register(NewHNSProvider("", []string{"ns1.hns.example"}, TLSASource{}))

	ns, ok := r.NameserversFor("example.com")
	assert.True(t, ok)
	assert.Equal(t, []string{"ns1.icann.example.", "ns2.icann.example."}, ns)

	ns, ok = r.NameserversFor("lumeweb")
	assert.True(t, ok)
	assert.Equal(t, []string{"ns1.hns.example"}, ns)
}

func TestRegistry_LiveNameservers_RoutesByNamespace(t *testing.T) {
	// The DNS service routes NS resolution through the registry, which must
	// delegate to the namespace-appropriate provider: HNS domains to the HNS
	// provider (which requires the HNS-aware resolver, not the system one),
	// ICANN domains to the ICANN provider.
	r := NewRegistry()
	r.Register(NewICANNProvider([]string{"ns1.icann.example."}))
	r.Register(NewHNSProvider("", []string{"ns1.hns.example"}, TLSASource{}))

	var _ pluginCore.NameserverResolver = r

	// No HNS resolver configured -> HNS provider errors; proves the HNS
	// domain did not fall through to the system resolver / ICANN provider.
	_, err := r.LiveNameservers(t.Context(), "lumeweb")
	assert.ErrorContains(t, err, "HNS resolver")
}
