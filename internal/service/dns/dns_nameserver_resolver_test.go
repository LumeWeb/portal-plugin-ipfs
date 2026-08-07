package dns

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
)

// stubNameserverResolver records which domain it was asked about, so tests
// can assert the DNS service delegates per-namespace routing to it.
type stubNameserverResolver struct {
	nsByDomain      map[string][]string
	liveByDomain    map[string][]string
	liveErrByDomain map[string]error
}

func (s *stubNameserverResolver) NameserversFor(domain string) ([]string, bool) {
	ns, ok := s.nsByDomain[domain]
	return ns, ok
}

func (s *stubNameserverResolver) LiveNameservers(ctx context.Context, domain string) ([]string, error) {
	if err := s.liveErrByDomain[domain]; err != nil {
		return nil, err
	}
	return s.liveByDomain[domain], nil
}

var _ pluginCore.NameserverResolver = (*stubNameserverResolver)(nil)

func TestNameserversForDomain_UsesResolver(t *testing.T) {
	resolver := &stubNameserverResolver{
		nsByDomain: map[string][]string{
			"example.com": {"ns1.icann.example.", "ns2.icann.example."},
			"lumeweb":     {"ns1.lumeweb.", "ns2.lumeweb."},
		},
	}
	svc := &DNSServiceDefault{nameserverResolver: resolver}

	// HNS domain resolves to the HNS nameservers via the interface.
	ns, err := svc.nameserversForDomain("lumeweb")
	require.NoError(t, err)
	assert.Equal(t, []string{"ns1.lumeweb.", "ns2.lumeweb."}, ns)

	// ICANN domain resolves to the ICANN nameservers via the interface.
	ns, err = svc.nameserversForDomain("example.com")
	require.NoError(t, err)
	assert.Equal(t, []string{"ns1.icann.example.", "ns2.icann.example."}, ns)
}

func TestNameserversForDomain_FallsBackToConfig(t *testing.T) {
	// Without an injected resolver the DNS service falls back to the ICANN
	// config nameservers, preserving prior behavior for deployments without
	// the provider registry wired up.
	svc := &DNSServiceDefault{
		config: &pluginConfig.DnsConfig{Nameservers: []string{"ns1.icann.example."}},
	}

	ns, err := svc.nameserversForDomain("example.com")
	require.NoError(t, err)
	assert.Equal(t, []string{"ns1.icann.example."}, ns)

	svc.config.Nameservers = nil
	_, err = svc.nameserversForDomain("example.com")
	assert.ErrorContains(t, err, "no approved nameservers configured")
}

func TestLiveNameservers_DelegatesToResolver(t *testing.T) {
	resolver := &stubNameserverResolver{
		liveByDomain:    map[string][]string{"lumeweb": {"ns1.lumeweb."}},
		liveErrByDomain: map[string]error{"broken": errors.New("resolver failed")},
	}
	svc := &DNSServiceDefault{nameserverResolver: resolver}

	live, err := svc.liveNameservers(context.Background(), "lumeweb")
	require.NoError(t, err)
	assert.Equal(t, []string{"ns1.lumeweb."}, live)

	_, err = svc.liveNameservers(context.Background(), "broken")
	assert.ErrorContains(t, err, "resolver failed")
}
