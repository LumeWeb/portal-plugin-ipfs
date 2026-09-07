package util

import (
	"context"
	"encoding/json"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	domainPolicyService "go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
)

// SyntheticDomainProvider is a configurable DomainProvider test double for
// exercising the provider capability matrix (arbitrary DNSSEC/TLSA/apex policy
// combinations) without depending on the concrete ICANN/HNS providers. It
// satisfies the domain package's DomainProvider interface; Policy() returns the
// configured policy, and the compatibility adapters derive from it.
type SyntheticDomainProvider struct {
	ProtocolName string
	PolicyValue  pluginCore.ProviderPolicy
	// VerifyDelegationFunc controls VerifyDelegation; when nil it reports
	// verified=true. Record calls here to assert whether verification was
	// invoked.
	VerifyDelegationFunc func(ctx context.Context, domain, expectedDS string) (bool, error)
	// InspectRouteFunc controls InspectRoute; when nil it reports a
	// standard-DNS route observation with the assumed-source marker, which
	// keeps Inspect() at its not-on-chain default. Tests override it to drive
	// typed route outcomes.
	InspectRouteFunc func(ctx context.Context, domain string) (domainpolicy.RouteObservation, error)
}

func (p *SyntheticDomainProvider) Protocol() string {
	if p.ProtocolName == "" {
		return "synthetic"
	}
	return p.ProtocolName
}

func (p *SyntheticDomainProvider) Validate(string) error { return nil }

// InspectRoute returns the configured typed route observation (or the
// assumed standard-DNS default). Inspect derives from it, so a cross-chain
// observation flip makes both outcomes agree by construction.
func (p *SyntheticDomainProvider) InspectRoute(ctx context.Context, domain string) (domainpolicy.RouteObservation, error) {
	if p.InspectRouteFunc != nil {
		return p.InspectRouteFunc(ctx, domain)
	}
	return domainpolicy.RouteObservation{
		Route:         domainpolicy.ResolutionRouteStandardDNS,
		Backend:       domainpolicy.BackendSystemDNS,
		AssumedSource: true,
	}, nil
}

func (p *SyntheticDomainProvider) Inspect(ctx context.Context, domain string) (bool, error) {
	return domainPolicyService.OnChainManagedFromRoute(p.InspectRoute(ctx, domain))
}

func (p *SyntheticDomainProvider) BuildDelegation(ctx context.Context, zoneID uint, domain string, website *pluginDb.Website, config json.RawMessage) (json.RawMessage, error) {
	return json.Marshal(map[string]any{"protocol": p.Protocol()})
}

func (p *SyntheticDomainProvider) VerifyDelegation(ctx context.Context, domain, expectedDS string) (bool, error) {
	if p.VerifyDelegationFunc != nil {
		return p.VerifyDelegationFunc(ctx, domain, expectedDS)
	}
	return true, nil
}

func (p *SyntheticDomainProvider) Policy() pluginCore.ProviderPolicy { return p.PolicyValue }

func (p *SyntheticDomainProvider) Nameservers() []string { return nil }

func (p *SyntheticDomainProvider) LiveNameservers(context.Context, string) ([]string, error) {
	return nil, nil
}

// UsesManagedZoneTLSA derives from the configured policy.
func (p *SyntheticDomainProvider) UsesManagedZoneTLSA() bool {
	return p.PolicyValue.TLSA == pluginCore.TLSAManaged
}

// RequiresDNSSEC derives from the configured policy.
func (p *SyntheticDomainProvider) RequiresDNSSEC() bool {
	return p.PolicyValue.DNSSEC == pluginCore.DNSSECRequired
}

// ApexRecordType derives from the configured policy.
func (p *SyntheticDomainProvider) ApexRecordType() pluginCore.RecordType {
	return p.PolicyValue.ApexRecordType
}
