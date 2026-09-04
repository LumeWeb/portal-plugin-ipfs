package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"

	"go.lumeweb.com/icann-tlds"
)

// ICANNDelegation is the typed result for ICANN BuildDelegation.
// Replaces loose map[string]interface{}.
type ICANNDelegation struct {
	Nameservers []string `json:"nameservers"`
}
type ICANNProvider struct {
	nameservers []string
}

func NewICANNProvider(nameservers []string) *ICANNProvider {
	return &ICANNProvider{nameservers: nameservers}
}

func (p *ICANNProvider) Protocol() string {
	return "icann"
}

// Policy returns the ICANN hosting-capability policy: no managed-DNSSEC, no
// managed-zone DANE, and a synthetic ALIAS apex (not separately signed).
func (p *ICANNProvider) Policy() pluginCore.ProviderPolicy {
	return pluginCore.ProviderPolicy{
		DNSSEC:         pluginCore.DNSSECNotRequired,
		TLSA:           pluginCore.TLSANotManaged,
		ApexRecordType: pluginCore.RecordTypeALIAS,
	}
}

// RequiresDNSSEC derives from the policy: ICANN verifies delegation on NS
// visibility alone and does not require a live DS from the parent zone.
func (p *ICANNProvider) RequiresDNSSEC() bool {
	return p.Policy().DNSSEC == pluginCore.DNSSECRequired
}

// UsesManagedZoneTLSA derives from the policy: ICANN does not use DANE, so no
// TLSA record is published for ICANN domains.
func (p *ICANNProvider) UsesManagedZoneTLSA() bool {
	return p.Policy().TLSA == pluginCore.TLSAManaged
}

// ApexRecordType derives from the policy: ICANN apex is served through the
// gateway and is not separately DNSSEC-signed at the apex in our setup, so a
// synthetic ALIAS is acceptable.
func (p *ICANNProvider) ApexRecordType() pluginCore.RecordType {
	return p.Policy().ApexRecordType
}

func (p *ICANNProvider) Validate(domain string) error {
	domain = NormalizeDomain(domain)
	if domain == "" {
		return fmt.Errorf("domain is required")
	}
	if !strings.Contains(domain, ".") {
		return fmt.Errorf("ICANN domain must contain a dot")
	}
	// A domain is ICANN only if its final label is a TLD registered in the
	// IANA root zone list. The IANA list is the authoritative decision
	// procedure — a name is never ICANN merely because it is dotted.
	checkCtx, cancel := tldCheckCtx()
	defer cancel()
	isICANN, err := icann.IsICANN(checkCtx, domain)
	if err != nil {
		return fmt.Errorf("check IANA root zone list: %w", err)
	}
	if !isICANN {
		return fmt.Errorf("%q does not end in an ICANN TLD", domain)
	}
	return nil
}

// Inspect reports that ICANN names are never on-chain managed: the
// registry/registrar model lives out-of-band, so the portal always provisions
// a managed zone for them as normal.
func (p *ICANNProvider) Inspect(ctx context.Context, domain string) (bool, error) {
	return false, nil
}

func (p *ICANNProvider) BuildDelegation(ctx context.Context, zoneID uint,
	domain string, website *pluginDb.Website, config json.RawMessage) (json.RawMessage, error) {

	// Serialize at the provider boundary; the persisted JSON shape is the typed
	// ICANNDelegation.
	raw, err := json.Marshal(ICANNDelegation{
		Nameservers: p.nameservers,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal icann delegation: %w", err)
	}
	return raw, nil
}

func (p *ICANNProvider) VerifyDelegation(ctx context.Context, domain string,
	expectedDS string) (bool, error) {
	// ICANN domains do not use an alt-root delegation step.
	return true, nil
}

// Nameservers returns the ICANN nameservers configured for the namespace.
func (p *ICANNProvider) Nameservers() []string {
	return p.nameservers
}

// LiveNameservers returns the NS records currently served for `domain`,
// resolved against the system default resolver (ICANN names are visible to
// standard resolvers). Returns the hostnames without trailing dots.
func (p *ICANNProvider) LiveNameservers(ctx context.Context, domain string) ([]string, error) {
	nss, err := net.LookupNS(domain)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(nss))
	for _, ns := range nss {
		out = append(out, strings.TrimSuffix(ns.Host, "."))
	}
	return out, nil
}
