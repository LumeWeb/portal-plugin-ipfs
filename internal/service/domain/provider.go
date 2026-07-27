package domain

import (
	"context"
	"encoding/json"

	"github.com/samber/lo"
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
