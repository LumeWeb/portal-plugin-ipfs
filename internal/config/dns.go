package config

import (
	"time"

	"go.lumeweb.com/portal/config"
)

var _ config.Defaults = (*DnsConfig)(nil)

// DnsConfig contains the configuration for the DNS hosting feature
type DnsConfig struct {
	// DNS hosting enabled/disabled
	Enabled bool `config:"enabled"`

	// PowerDNS configuration
	// NOTE: SOA serial propagation for API-managed zones works out of the box —
	// PowerDNS sets SOA-EDIT-API metadata to DEFAULT for API-created zones, which
	// auto-increments the serial on RRset edits so secondaries re-transfer. No
	// SOA-EDIT config is required. (Per-zone SOA-EDIT/SOA-EDIT-API are domain
	// metadata, not pdns.conf settings.)
	PowerDNSAPIURL string `config:"powerdns_api_url"`
	PowerDNSAPIKey string `config:"powerdns_api_key"`

	// Approved nameservers for validation
	Nameservers []string `config:"nameservers"`

	// HNS nameservers for the HNS namespace delegation/validation.
	// Alt-root namespaces (e.g. HNS) must delegate to nameservers that are
	// themselves members of that namespace, which differ from the ICANN
	// nameservers in Nameservers. Provided by the operator, not the user.
	HNSNameservers []string `config:"hns_nameservers"`

	// HNSResolver is the address (host:port) of an HNS-aware DNS resolver
	// used for HNS namespace:
	// - Delegation verification (NS lookup via HNSProvider)
	// - Full DNS validation: DNSLink + TXT token (via LiveResolver selection)
	// Different roots require different resolvers because alt-roots are not
	// visible to the system default resolver.
	HNSResolver string `config:"hns_resolver"`

	// Gateway domain for ALIAS records (auto-wiring)
	GatewayDomain string `config:"gateway_domain"`

	// GatewayIP is the IP address to publish as the apex A record for
	// DNSSEC-signed alt-root (e.g. HNS) zones. Alt-root apexes must be real
	// A records (not ALIAS) so they carry an RRSIG; PowerDNS cannot sign a
	// synthetic ALIAS at the apex. This should match where GatewayDomain
	// currently resolves. Keep it in sync when the gateway IP changes.
	GatewayIP string `config:"gateway_ip"`

	// Verification token key used as the subdomain label for validation TXT records
	VerificationTokenKey string `config:"verification_token_key"`

	// Nameserver validation job configuration
	NameserverValidationInterval time.Duration `config:"nameserver_validation_interval"`

	// DomainPolicyDNSLinkReconcilerEnabled routes DNSLink desired-state
	// When false (the default), the legacy scattered DNSLink writers remain
	// active and behavior is unchanged; when true, the internal/domainapp
	// reconciler — driven by the binding's domainpolicy plan and Diff effect
	// descriptors — is the single owner of DNSLink record writes and the
	// legacy writers defer to it. Feature flag for rollout/rollback only:
	// flipping it back to false restores the legacy path exactly.
	// NOTE: the key spells "dns_link" (not "dnslink") because the test and
	// platform config pipelines flatten the field name with CamelToSnake
	// ("DomainPolicyDNSLinkReconcilerEnabled" ->
	// "domain_policy_dns_link_reconciler_enabled") and then read values back
	// by the `config` tag — a "dnslink" spelling made the flag silently read
	// back FALSE even when set. The sibling repair flag already matched.
	DomainPolicyDNSLinkReconcilerEnabled bool `config:"domain_policy_dns_link_reconciler_enabled"`

	// DomainPolicyRepairReconcilerEnabled routes the remaining repair effect
	// families — expired challenge-token rotation (website validation flow)
	// and the DNSSEC ensure + SOA MNAME zone heal (domain verification flow)
	// — through the plan-driven domain-policy repair reconciler. When false (the default), the
	// legacy scattered repair paths (regenerateExpiredToken and
	// selfHealZone) remain active and behavior is unchanged; when true, the
	// internal/domainapp repair reconciler — driven by the binding's
	// domainpolicy plan and Diff effect descriptors — owns those effects and
	// the legacy paths defer to it for unrepresentable inputs. Sibling of
	// DomainPolicyDNSLinkReconcilerEnabled: flipping it back to false
	// restores the legacy path exactly.
	DomainPolicyRepairReconcilerEnabled bool `config:"domain_policy_repair_reconciler_enabled"`

	// DomainPolicyAxesBackfillEnabled enables the bounded application backfill
	// that derives and persists the independent policy-axis columns for
	// legacy website_domains rows: it locks one row at a time, maps it
	// through the legacy facts/profile mapper plus profile validation, writes
	// the mapped axes, and records the persisted error reconciliation status
	// (never guessed axis values) when the mapping fails. NOT auto-enabled:
	// the registered cron job no-ops while this is false (the default).
	// There is NO SQL backfill of ambiguous rows by design.
	// The tag spells "axes_backfill" so CamelToSnake flattening of the field
	// name matches the config pipeline read key.
	DomainPolicyAxesBackfillEnabled bool `config:"domain_policy_axes_backfill_enabled"`
}

func (c DnsConfig) Defaults() map[string]any {
	return map[string]any{
		"Enabled":                      false,
		"PowerDNSAPIURL":               "",
		"PowerDNSAPIKey":               "",
		"Nameservers":                  []string{},
		"HNSNameservers":               []string{},
		"HNSResolver":                  "",
		"GatewayDomain":                "",
		"GatewayIP":                    "",
		"VerificationTokenKey":         "lumeweb-verify",
		"NameserverValidationInterval": 5 * time.Minute,

		"DomainPolicyDNSLinkReconcilerEnabled": false,
		"DomainPolicyRepairReconcilerEnabled":  false,
		"DomainPolicyAxesBackfillEnabled":      false,
	}
}
