package domain

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

type DomainProvider interface {
	Protocol() string
	Validate(domain string) error
	BuildDelegation(ctx context.Context, zoneID uint, domain string, website *pluginDb.Website, config json.RawMessage) (any, error)
	VerifyDelegation(ctx context.Context, domain string, expectedDS string) (bool, error)
	// OnCertAvailable is called when a cert is pushed via /internal/dns/cert.
	// Providers can use it to update TLSA in delegation data or trigger
	// namespace-specific protocol updates. Can be nil-safe by returning nil.
	OnCertAvailable(ctx context.Context, domain string, certPEM string) error
	// UsesManagedZoneTLSA reports whether this provider's TLS certs translate
	// into a DANE TLSA record that the portal must publish into its
	// portal-managed authoritative PowerDNS zone (e.g. an alt-root namespace
	// whose zone the portal DNSSEC-signs). Providers that do not use DANE
	// (e.g. ICANN) return false so no spurious _443._tcp TLSA is published.
	UsesManagedZoneTLSA() bool
	// Nameservers returns the nameservers this provider publishes and
	// validates delegation against for its namespace. Alt-root providers
	// (e.g. HNS) return their own namespace-specific nameservers, which
	// differ from ICANN's; these are operator-provided, not user-provided.
	// Returns the namespace's nameservers, or nil/empty when none are set.
	Nameservers() []string
	// LiveNameservers returns the NS records currently served for `domain`,
	// resolved against the namespace-appropriate resolver (an alt-root
	// provider like HNS queries its HNS-aware resolver; ICANN uses the
	// system default resolver). Used to validate that a zone's namespace
	// delegation is live.
	LiveNameservers(ctx context.Context, domain string) ([]string, error)
	// ApexRecordType returns the DNS record type used for the zone apex.
	// DNSSEC-signed alt-root providers (e.g. HNS) must return RecordTypeA so
	// the apex is a real, signable RRset, which PowerDNS cannot provide for a
	// synthetic ALIAS/CNAME record at the apex. Providers whose apex is not
	// separately signed return RecordTypeALIAS.
	ApexRecordType() pluginCore.RecordType
}

type Registry struct {
	providers map[string]DomainProvider
}

func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]DomainProvider)}
}

func (r *Registry) Register(p DomainProvider) {
	key := p.Protocol()
	if _, exists := r.providers[key]; exists {
		panic("domain provider already registered for protocol: " + key)
	}
	r.providers[key] = p
}

func (r *Registry) Get(namespace string) DomainProvider {
	return r.providers[namespace]
}

func (r *Registry) Names() []string {
	return lo.Keys(r.providers)
}

// providerForDomain returns the provider whose namespace accepts `domain`,
// or nil when no registered provider validates it.
func (r *Registry) providerForDomain(domain string) DomainProvider {
	if r == nil {
		return nil
	}
	for _, ns := range r.Names() {
		prov := r.Get(ns)
		if prov != nil && prov.Validate(domain) == nil {
			return prov
		}
	}
	return nil
}

var _ pluginCore.NameserverResolver = (*Registry)(nil)

// NameserversFor returns the nameservers to publish for the given domain's
// namespace by delegating to the matching provider. The second return is
// false when no provider validates the domain.
func (r *Registry) NameserversFor(domain string) ([]string, bool) {
	prov := r.providerForDomain(domain)
	if prov == nil {
		return nil, false
	}
	return prov.Nameservers(), true
}

// LiveNameservers returns the live NS records for the domain resolved
// against the matching provider's namespace-appropriate resolver.
func (r *Registry) LiveNameservers(ctx context.Context, domain string) ([]string, error) {
	prov := r.providerForDomain(domain)
	if prov == nil {
		return nil, fmt.Errorf("no provider validates domain %q", domain)
	}
	return prov.LiveNameservers(ctx, domain)
}
