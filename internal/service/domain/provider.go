package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/samber/lo"
	"go.lumeweb.com/icann-tlds"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// tldCheckTimeout bounds the cold fetch of the IANA root zone list performed
// from Validate, which has no context of its own. The registry caches the
// list for the process lifetime, so after the first successful fetch this
// path is a pure in-memory lookup; the deadline only bounds a cold / retried
// fetch so a slow authority can never stall call paths unboundedly.
const tldCheckTimeout = 5 * time.Second

// tldCheckCtx returns a bounded context for IANA root zone list fetches.
func tldCheckCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), tldCheckTimeout)
}

// warmTLDList fetches the IANA root zone list once at startup so domain
// validation never races a cold network fetch on a bind or zone path. A
// failure here is non-fatal: Validate falls back to a bounded, single-flight
// fetch and load failures surface as ErrTLDListUnavailable rather than a
// silent namespace fallback.
func warmTLDList() {
	ctx, cancel := context.WithTimeout(context.Background(), tldCheckTimeout)
	defer cancel()
	_ = icann.Default().Refresh(ctx)
}

// DNSSECPolicy, TLSAPolicy, and ProviderPolicy live in the plugin core package
// (pluginCore) so they can be referenced without import cycles by consumers
// such as generated mocks; see core/provider_policy.go.

// CertificateProvider is the optional capability for providers whose namespaces
// translate TLS certificates into DANE/TLSA state (managed-zone DANE).
// Providers without DANE (e.g. ICANN) do not implement it, so certificate/DANE
// behavior is an explicit optional sub-interface rather than a mandatory no-op
// on every provider.
type CertificateProvider interface {
	// OnCertAvailable is called when a cert is pushed via /internal/dns/cert.
	// Providers can use it to update TLSA in delegation data or trigger
	// namespace-specific protocol updates. Returning nil is safe.
	OnCertAvailable(ctx context.Context, domain string, certPEM string) error
}

type DomainProvider interface {
	Protocol() string
	Validate(domain string) error
	// Inspect queries the domain's on-chain/registry state at bind time and
	// reports whether the name is managed on-chain (e.g. a Handshake HIP-5 name
	// whose NS record is a HIP-5 TX record pointing at an external contract).
	// An on-chain managed name serves its own DNS, so the portal must NOT
	// provision a PowerDNS zone for it and proves ownership via TXT token.
	// Providers without an on-chain concept (ICANN) always return false.
	// Detection is best-effort: HNS returns false when the resolver cannot
	// answer (name not yet registered, resolver unreachable), defaulting to
	// native so binding can proceed.
	Inspect(ctx context.Context, domain string) (onchainManaged bool, err error)
	// BuildDelegation returns the provider's typed delegation payload as a
	// json.RawMessage, so delegation never crosses the provider boundary as an
	// unconstrained any. Each provider marshals its own typed delegation
	// structure (e.g. HNS DelegationBundle, ICANN ICANNDelegation); the
	// persisted JSON shape is unchanged.
	BuildDelegation(ctx context.Context, zoneID uint, domain string, website *pluginDb.Website, config json.RawMessage) (json.RawMessage, error)
	VerifyDelegation(ctx context.Context, domain string, expectedDS string) (bool, error)
	// Policy returns the provider's immutable hosting-capability policy.
	Policy() pluginCore.ProviderPolicy
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
	// Deprecated compatibility adapters — their values MUST derive from
	// Policy() and are never independent sources of truth:

	// UsesManagedZoneTLSA reports whether this provider's TLS certs translate
	// into a DANE TLSA record that the portal must publish into its
	// portal-managed authoritative PowerDNS zone. Derived from Policy().TLSA.
	UsesManagedZoneTLSA() bool
	// RequiresDNSSEC reports whether this provider's delegation is confirmed
	// against a live DS record served by the parent zone (managed-DNSSEC
	// namespaces, e.g. HNS). Derived from Policy().DNSSEC. A provider could
	// DNSSEC-sign without DANE (and vice versa), so providers must never use
	// the TLSA capability as a proxy for DNSSEC.
	RequiresDNSSEC() bool
	// ApexRecordType returns the DNS record type used for the zone apex.
	// Derived from Policy().ApexRecordType.
	ApexRecordType() pluginCore.RecordType
}

type Registry struct {
	providers map[string]DomainProvider
}

func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]DomainProvider)}
}

// validatePolicy rejects unknown enum values and invalid record types at
// registration time (mirroring the duplicate-registration panic), so a broken
// capability can never silently reach a hosting-sensitive decision.
func validatePolicy(key string, pol pluginCore.ProviderPolicy) {
	switch pol.DNSSEC {
	case pluginCore.DNSSECNotRequired, pluginCore.DNSSECRequired:
	default:
		panic(fmt.Sprintf("domain provider %q registered with invalid DNSSEC policy value %d", key, pol.DNSSEC))
	}
	switch pol.TLSA {
	case pluginCore.TLSANotManaged, pluginCore.TLSAManaged:
	default:
		panic(fmt.Sprintf("domain provider %q registered with invalid TLSA policy value %d", key, pol.TLSA))
	}
	switch pol.ApexRecordType {
	case pluginCore.RecordTypeA, pluginCore.RecordTypeALIAS:
	default:
		panic(fmt.Sprintf("domain provider %q registered with invalid apex record type %q", key, pol.ApexRecordType))
	}
}

func (r *Registry) Register(p DomainProvider) {
	key := p.Protocol()
	if _, exists := r.providers[key]; exists {
		panic("domain provider already registered for protocol: " + key)
	}
	validatePolicy(key, p.Policy())
	r.providers[key] = p
}

func (r *Registry) Get(namespace string) DomainProvider {
	return r.providers[namespace]
}

func (r *Registry) Names() []string {
	keys := lo.Keys(r.providers)
	sort.Strings(keys)
	return keys
}

// providerForDomain returns the provider whose namespace accepts `domain`,
// or (nil, nil) when no registered provider validates it.
//
// Iteration is deterministic (Names returns sorted keys). When more than one
// provider accepts a dotted name (both ICANN's "must contain a dot" and HNS's
// "DNS-compliant labels" accept, e.g., "example.com"), ICANN wins for dotted
// names: an HNS name is typically single-label or a subdomain of a registered
// alt-root, and any dotted name is at least plausibly ICANN. The service layer
// (getNamespaceForDomain) further overrides this default by preferring a
// registered platform root's namespace when the domain descends from one.
//
// A Validate failure caused by an unloaded IANA root zone list
// (icann.ErrNotLoaded) is not a namespace rejection: if the providers cannot
// classify the domain at all, its namespace is unknown, and reporting "no
// provider" would silently misroute it (e.g. publish ICANN nameservers for an
// HNS zone). Such a failure is surfaced as ErrTLDListUnavailable so callers
// fail loudly, while genuine rejections still report "no provider".
func (r *Registry) providerForDomain(domain string) (DomainProvider, error) {
	if r == nil {
		return nil, nil
	}
	// Provider.Validate calls are mutually exclusive by construction: a domain
	// ends in an IANA ICANN TLD (ICANN) or it does not (HNS). Iterating the
	// deterministically-sorted namespaces therefore yields a single match, so
	// no tie-breaking is required.
	var listErr error
	for _, ns := range r.Names() {
		prov := r.Get(ns)
		if prov == nil {
			continue
		}
		if err := prov.Validate(domain); err != nil {
			if errors.Is(err, icann.ErrNotLoaded) {
				listErr = fmt.Errorf("%w in namespace %q: %w", pluginCore.ErrTLDListUnavailable, ns, err)
			}
			continue
		}
		return prov, nil
	}
	return nil, listErr
}

var _ pluginCore.NameserverResolver = (*Registry)(nil)

// NameserversFor returns the nameservers to publish for the given domain's
// namespace by delegating to the matching provider. It returns
// pluginCore.ErrNoProviderForDomain when no registered provider validates the
// domain (callers may fall back for unmatched domains), and
// ErrTLDListUnavailable when the IANA root zone list is not loaded (callers
// must not fall back: the domain's namespace is unknown).
func (r *Registry) NameserversFor(domain string) ([]string, error) {
	prov, listErr := r.providerForDomain(domain)
	if listErr != nil {
		return nil, listErr
	}
	if prov == nil {
		return nil, pluginCore.ErrNoProviderForDomain
	}
	return prov.Nameservers(), nil
}

// LiveNameservers returns the live NS records for the domain resolved
// against the matching provider's namespace-appropriate resolver. It returns
// pluginCore.ErrNoProviderForDomain when no registered provider validates
// the domain, so callers can fall back to a default resolution path, and
// ErrTLDListUnavailable when the IANA root zone list is not loaded (no
// fallback: the namespace is unknown).
func (r *Registry) LiveNameservers(ctx context.Context, domain string) ([]string, error) {
	prov, listErr := r.providerForDomain(domain)
	if listErr != nil {
		return nil, listErr
	}
	if prov == nil {
		return nil, pluginCore.ErrNoProviderForDomain
	}
	return prov.LiveNameservers(ctx, domain)
}
