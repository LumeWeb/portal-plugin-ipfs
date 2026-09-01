package util

import (
	"context"
	"encoding/json"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
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
	// verified=true. Record calls here to assert verification was (not) invoked.
	VerifyDelegationFunc func(ctx context.Context, domain, expectedDS string) (bool, error)
}

func (p *SyntheticDomainProvider) Protocol() string {
	if p.ProtocolName == "" {
		return "synthetic"
	}
	return p.ProtocolName
}

func (p *SyntheticDomainProvider) Validate(string) error { return nil }

func (p *SyntheticDomainProvider) Inspect(context.Context, string) (bool, error) { return false, nil }

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
