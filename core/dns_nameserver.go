package core

import (
	"context"
	"errors"
)

// ErrNoProviderForDomain is returned by a NameserverResolver.LiveNameservers
// when no registered provider validates the given domain. Callers that fall
// back to a default resolution path for unmatched domains (mirroring
// NameserversFor's false return) can match this sentinel with errors.Is to
// continue their fallback.
var ErrNoProviderForDomain = errors.New("no provider validates domain")

// ErrTLDListUnavailable is returned when a provider could not classify a
// domain because the IANA root zone list is not loaded (the authority was
// unreachable). This is a transient infrastructure failure, not a namespace
// rejection: unlike ErrNoProviderForDomain, callers must fail loudly instead
// of falling back to another namespace's defaults (e.g. publishing ICANN
// nameservers for an HNS zone), because the domain's namespace is unknown.
var ErrTLDListUnavailable = errors.New("iana root zone list unavailable")

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
	// domains). It returns ErrNoProviderForDomain when no provider matches
	// the domain (callers may fall back for unmatched domains), and
	// ErrTLDListUnavailable when the IANA root zone list is not loaded
	// (callers must not fall back: the domain's namespace is unknown).
	NameserversFor(domain string) ([]string, error)

	// LiveNameservers returns the live NS records currently served for the
	// domain, resolved against the namespace-appropriate resolver (the HNS
	// resolver for HNS domains, the system resolver otherwise). It returns
	// ErrNoProviderForDomain when no registered provider validates the
	// domain. The caller compares these against NameserversFor to confirm
	// delegation.
	LiveNameservers(ctx context.Context, domain string) ([]string, error)
}
