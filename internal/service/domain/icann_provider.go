package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
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

// ApexRecordType returns RecordTypeALIAS: ICANN apex is served through the
// gateway and is not separately DNSSEC-signed at the apex in our setup, so a
// synthetic ALIAS is acceptable.
func (p *ICANNProvider) ApexRecordType() pluginCore.RecordType {
	return pluginCore.RecordTypeALIAS
}

func (p *ICANNProvider) Validate(domain string) error {
	domain = NormalizeDomain(domain)
	if domain == "" {
		return fmt.Errorf("domain is required")
	}
	if !strings.Contains(domain, ".") {
		return fmt.Errorf("ICANN domain must contain a dot")
	}
	return nil
}

func (p *ICANNProvider) BuildDelegation(ctx context.Context, zoneID uint,
	domain string, website *pluginDb.Website, config json.RawMessage) (any, error) {

	return ICANNDelegation{
		Nameservers: p.nameservers,
	}, nil
}

func (p *ICANNProvider) VerifyDelegation(ctx context.Context, domain string,
	delegationData json.RawMessage) (bool, error) {
	// ICANN domains do not use an alt-root delegation step.
	return true, nil
}

// OnCertAvailable is a no-op for ICANN domains (no DANE/TLSA needed).
func (p *ICANNProvider) OnCertAvailable(ctx context.Context, domain string, certPEM string) error {
	return nil
}

// UsesManagedZoneTLSA reports that ICANN does not use DANE, so no TLSA record
// is published for ICANN domains.
func (p *ICANNProvider) UsesManagedZoneTLSA() bool {
	return false
}

// Nameservers returns the ICANN nameservers configured for the namespace.
func (p *ICANNProvider) Nameservers() []string {
	return p.nameservers
}
