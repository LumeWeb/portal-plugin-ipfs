package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// ICANNDelegation is the typed result for ICANN BuildDelegation.
// Replaces loose map[string]interface{}.
type ICANNDelegation struct {
	Nameservers  []string `json:"nameservers"`
	Instructions string   `json:"instructions"`
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
		Nameservers:  p.nameservers,
		Instructions: fmt.Sprintf("Configure these NS records at your registrar for %s", domain),
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