package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/icann-tlds"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
)

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	mockProv := mocks.NewMockDomainProvider(t)
	mockProv.EXPECT().Protocol().Return("test").Maybe()
	mockProv.EXPECT().Policy().Return(mockProviderPolicy()).Maybe()

	r.Register(mockProv)
	assert.Equal(t, mockProv, r.Get("test"))
	assert.Nil(t, r.Get("missing"))
	assert.Equal(t, []string{"test"}, r.Names())
}

// mockProviderPolicy returns a structurally valid ProviderPolicy so a mock
// provider passes the registration-time policy validation.
func mockProviderPolicy() pluginCore.ProviderPolicy {
	return pluginCore.ProviderPolicy{
		DNSSEC:         pluginCore.DNSSECNotRequired,
		TLSA:           pluginCore.TLSANotManaged,
		ApexRecordType: pluginCore.RecordTypeALIAS,
	}
}

func TestRegistry_DuplicatePanics(t *testing.T) {
	r := NewRegistry()
	mockProv := mocks.NewMockDomainProvider(t)
	mockProv.EXPECT().Protocol().Return("test").Maybe()
	mockProv.EXPECT().Policy().Return(mockProviderPolicy()).Maybe()

	r.Register(mockProv)
	assert.Panics(t, func() {
		mockProv2 := mocks.NewMockDomainProvider(t)
		mockProv2.EXPECT().Protocol().Return("test").Maybe()
		mockProv2.EXPECT().Policy().Return(mockProviderPolicy()).Maybe()
		r.Register(mockProv2)
	})
}

// TestRegistry_RegisterValidatesProviderPolicy verifies that provider
// registration rejects unknown policy enum values and invalid record types up
// front, so a broken capability can never silently reach a hosting-sensitive
// decision — while every relevant valid DNSSEC/TLSA combination (including
// DNSSEC-required with TLSA-disabled) registers cleanly.
func TestRegistry_RegisterValidatesProviderPolicy(t *testing.T) {
	newProv := func(ds pluginCore.DNSSECPolicy, tlsa pluginCore.TLSAPolicy, apex pluginCore.RecordType) *syntheticTestProvider {
		return &syntheticTestProvider{
			protocol: "synthetic",
			policy: pluginCore.ProviderPolicy{
				DNSSEC:         ds,
				TLSA:           tlsa,
				ApexRecordType: apex,
			},
		}
	}

	t.Run("invalid_dnssec_policy_rejected", func(t *testing.T) {
		r := NewRegistry()
		assert.Panics(t, func() {
			r.Register(newProv(99, pluginCore.TLSANotManaged, pluginCore.RecordTypeALIAS))
		})
	})

	t.Run("invalid_tlsa_policy_rejected", func(t *testing.T) {
		r := NewRegistry()
		assert.Panics(t, func() {
			r.Register(newProv(pluginCore.DNSSECNotRequired, 99, pluginCore.RecordTypeALIAS))
		})
	})

	t.Run("invalid_apex_record_type_rejected", func(t *testing.T) {
		r := NewRegistry()
		assert.Panics(t, func() {
			r.Register(newProv(pluginCore.DNSSECNotRequired, pluginCore.TLSANotManaged, pluginCore.RecordTypeTXT))
		})
	})

	t.Run("all_valid_policy_combinations_register", func(t *testing.T) {
		combos := []pluginCore.ProviderPolicy{
			{DNSSEC: pluginCore.DNSSECNotRequired, TLSA: pluginCore.TLSANotManaged, ApexRecordType: pluginCore.RecordTypeALIAS},
			{DNSSEC: pluginCore.DNSSECNotRequired, TLSA: pluginCore.TLSAManaged, ApexRecordType: pluginCore.RecordTypeA},
			{DNSSEC: pluginCore.DNSSECRequired, TLSA: pluginCore.TLSANotManaged, ApexRecordType: pluginCore.RecordTypeA},
			{DNSSEC: pluginCore.DNSSECRequired, TLSA: pluginCore.TLSAManaged, ApexRecordType: pluginCore.RecordTypeA},
		}
		for i, pol := range combos {
			i, pol := i, pol
			t.Run(fmt.Sprintf("combo_%d", i), func(t *testing.T) {
				r := NewRegistry()
				prov := newProv(pol.DNSSEC, pol.TLSA, pol.ApexRecordType)
				prov.protocol = fmt.Sprintf("synthetic-%d", i)
				require.NotPanics(t, func() { r.Register(prov) })
				assert.Equal(t, pol, r.Get(prov.Protocol()).Policy())
			})
		}
	})
}

func TestHNSProvider_RequiresDNSSEC_UsesManagedZoneTLSA_DeriveFromPolicy(t *testing.T) {
	// The legacy boolean adapters must derive from Policy() (single source of
	// truth), so they can never disagree with the policy.
	hns := NewHNSProvider("", nil, TLSASource{})
	assert.True(t, hns.RequiresDNSSEC())
	assert.True(t, hns.UsesManagedZoneTLSA())
	assert.Equal(t, pluginCore.DNSSECRequired, hns.Policy().DNSSEC)
	assert.Equal(t, pluginCore.TLSAManaged, hns.Policy().TLSA)

	icann := NewICANNProvider(nil)
	assert.False(t, icann.RequiresDNSSEC())
	assert.False(t, icann.UsesManagedZoneTLSA())
	assert.Equal(t, pluginCore.DNSSECNotRequired, icann.Policy().DNSSEC)
	assert.Equal(t, pluginCore.TLSANotManaged, icann.Policy().TLSA)
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

	ns, err := r.NameserversFor("example.com")
	require.NoError(t, err)
	assert.Equal(t, []string{"ns1.icann.example.", "ns2.icann.example."}, ns)

	ns, err = r.NameserversFor("lumeweb")
	require.NoError(t, err)
	assert.Equal(t, []string{"ns1.hns.example"}, ns)

	// Mutual exclusion: a dotted name under a non-ICANN TLD routes to HNS,
	// and an ICANN-TLD-suffixed name routes to ICANN.
	ns, err = r.NameserversFor("blog.altroot")
	require.NoError(t, err)
	assert.Equal(t, []string{"ns1.hns.example"}, ns)

	// A domain no provider validates reports ErrNoProviderForDomain so the
	// caller can fall back to a default resolution path.
	_, err = r.NameserversFor("bad..altroot")
	assert.ErrorIs(t, err, pluginCore.ErrNoProviderForDomain)
}

// failingProvider is a stub DomainProvider whose Validate returns a canned
// error, used to exercise registry error propagation.
type failingProvider struct {
	validate func(string) error
}

func (f failingProvider) Protocol() string { return "failing" }
func (f failingProvider) Policy() pluginCore.ProviderPolicy {
	return pluginCore.ProviderPolicy{
		DNSSEC:         pluginCore.DNSSECNotRequired,
		TLSA:           pluginCore.TLSANotManaged,
		ApexRecordType: pluginCore.RecordTypeALIAS,
	}
}
func (f failingProvider) UsesManagedZoneTLSA() bool { return false }
func (f failingProvider) RequiresDNSSEC() bool      { return false }
func (f failingProvider) ApexRecordType() pluginCore.RecordType {
	return pluginCore.RecordTypeALIAS
}
func (f failingProvider) Validate(domain string) error { return f.validate(domain) }
func (f failingProvider) Inspect(ctx context.Context, domain string) (bool, error) {
	return false, nil
}
func (f failingProvider) BuildDelegation(ctx context.Context, zoneID uint, domain string, website *pluginDb.Website, config json.RawMessage) (json.RawMessage, error) {
	return nil, nil
}
func (f failingProvider) VerifyDelegation(ctx context.Context, domain string, expectedDS string) (bool, error) {
	return true, nil
}
func (f failingProvider) Nameservers() []string { return []string{"ns1.failing.example"} }
func (f failingProvider) LiveNameservers(ctx context.Context, domain string) ([]string, error) {
	return nil, errors.New("not live")
}

// TestK1_Registry_PropagatesTLDListLoadFailure verifies that a Validate
// failure caused by an unloaded IANA root zone list is surfaced as
// ErrTLDListUnavailable (and NOT as "no provider"), while an ordinary
// rejection still reports ErrNoProviderForDomain.
func TestK1_Registry_PropagatesTLDListLoadFailure(t *testing.T) {
	r := NewRegistry()
	r.Register(failingProvider{validate: func(string) error {
		return fmt.Errorf("check IANA root zone list: %w", icann.ErrNotLoaded)
	}})

	// Load failure is NOT a namespace rejection: no silent fallback.
	_, err := r.NameserversFor("example.com")
	assert.ErrorIs(t, err, pluginCore.ErrTLDListUnavailable)

	_, err = r.LiveNameservers(context.Background(), "example.com")
	assert.ErrorIs(t, err, pluginCore.ErrTLDListUnavailable)
	// A live-lookup load failure must not be mistaken for a resolution
	// fallback trigger (ErrNoProviderForDomain).
	assert.NotErrorIs(t, err, pluginCore.ErrNoProviderForDomain)
}

// TestK1_Registry_RejectionIsNoProvider verifies that a Validate failure that
// is not a TLD-list load failure still counts as "no provider", preserving
// the caller fallback semantics.
func TestK1_Registry_RejectionIsNoProvider(t *testing.T) {
	r := NewRegistry()
	r.Register(failingProvider{validate: func(string) error {
		return errors.New("domain rejected by policy")
	}})

	_, err := r.NameserversFor("example.com")
	assert.ErrorIs(t, err, pluginCore.ErrNoProviderForDomain)
	assert.NotErrorIs(t, err, pluginCore.ErrTLDListUnavailable)
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
