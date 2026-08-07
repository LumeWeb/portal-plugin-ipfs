package domain

import (
	"context"
	"encoding/json"

	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

type DomainProvider interface {
	Protocol() string
	Validate(domain string) error
	BuildDelegation(ctx context.Context, zoneID uint, domain string, website *pluginDb.Website, config json.RawMessage) (any, error)
	VerifyDelegation(ctx context.Context, domain string, delegationData json.RawMessage) (bool, error)
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
