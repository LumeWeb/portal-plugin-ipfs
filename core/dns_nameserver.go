package core

import "context"

// NameserverResolver resolves per-namespace nameserver and delegation
// behavior for a domain, so namespace-agnostic services (DNS zone
// provisioning and validation) can consult the right provider without
// namespace branching. It is implemented by the domain provider registry,
// which already distinguishes namespaces via their providers.
//
// Alt-root namespaces (e.g. HNS) resolve/validate their NS delegation
// against an HNS-aware resolver and publish their own namespace
// nameservers, which differ from ICANN's. Routing this through an interface
// keeps the DNS zone service free of `if namespace == hns` style logic.
type NameserverResolver interface {
	// NameserversFor returns the nameservers to publish for the given
	// domain's namespace (ICANN list for ICANN domains, HNS list for HNS
	// domains). The second return is false when no provider matches the
	// domain.
	NameserversFor(domain string) ([]string, bool)

	// LiveNameservers returns the live NS records currently served for the
	// domain, resolved against the namespace-appropriate resolver (the HNS
	// resolver for HNS domains, the system resolver otherwise). The caller
	// compares these against NameserversFor to confirm delegation.
	LiveNameservers(ctx context.Context, domain string) ([]string, error)
}
