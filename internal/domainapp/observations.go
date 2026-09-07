// Package domainapp is the application layer between the pure domain policy
// model (internal/domainpolicy) and the services that observe DNS and trust
// relationships (internal/service/*). It owns the observation-collection
// ports for the validation flows, collects only the observations the
// binding's gates require (preserving today's short-circuiting), delegates
// the gate outcomes to domainpolicy.Evaluate, and converts the evaluation
// into the service boundary's core.ValidationCheck checks and validation
// reason codes — byte-for-byte identical to the prior imperative
// validation paths of the website and domain-verification flows.
//
// Evaluation here is read and diagnostic only. The effectful repair steps —
// DNSSEC/SOA self-heal and expired-token rotation in particular — remain on
// the legacy service paths, and the domain-verification flow's
// route-conversion branch keeps its effects; only the passive gate outcomes
// are centralized here.
package domainapp

import (
	"context"

	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
)

// --- DNSLink ----------------------------------------------------------------

// DNSLinkCollector is the DNSLink observation port: the adapters the website
// service wraps around its DNS resolver. Ports return domainpolicy values
// plus the minimal transport annotations needed to reproduce today's
// diagnostics — never DB models or API DTOs.
type DNSLinkCollector interface {
	// CollectDNSLink observes the DNSLink record for domain. Record-class
	// absence (NXDOMAIN) is reported in the returned value — today's
	// "missing" case. Every other lookup failure is returned as an error and
	// aborts the validation flow exactly as today's "DNS lookup failed" path
	// does.
	CollectDNSLink(ctx context.Context, domain string) (DNSLinkObserved, error)
}

// DNSLinkObserved is the DNSLink observation plus the transport annotation
// that separates the missing case from a mismatching record.
type DNSLinkObserved struct {
	// Observation carries the candidate the legacy matching logic reports as
	// "found": the first ipfs link (normalized), else the first ipns link;
	// empty when the record is absent or carries no usable link. The gate
	// passes when this equals the expected target path.
	Observation domainpolicy.DNSLinkObservation
	// NXDOMAIN reports record-class absence (the whole name does not
	// resolve), as opposed to a present-but-mismatching record.
	NXDOMAIN bool
}

// --- Challenge TXT -----------------------------------------------------------

// TokenTXTCollector is the challenge-TXT observation port: the adapter the
// website service wraps around its DNS TXT lookup. It returns the raw records
// at the request label; matching the expected "<key>=<token>" content is
// domainapp's (pure, tested) job, mirroring today's contains-based check.
type TokenTXTCollector interface {
	// CollectTokenTXT returns the TXT records at fqdn. A lookup failure must
	// be returned as an error: today it aborts the validation flow with the
	// wrapped "DNS TXT lookup failed" error.
	CollectTokenTXT(ctx context.Context, fqdn string) ([]string, error)
}

// --- On-chain TLSA -----------------------------------------------------------

// ChainTLSACollector is the on-chain TLSA observation port: the adapter the
// website service wraps around the delegated-domain service's stored-DANE vs
// live-TLSA comparison (DelegatedDomainService.ValidateOnChainTLSA today).
type ChainTLSACollector interface {
	// CollectChainTLSA observes the binding's live on-chain TLSA against its
	// portal-stored DANE identity. Transport/unavailability failures (an
	// unconfigured or unreachable resolver, a failed DANE-record load) are
	// returned as errors; the evaluator degrades them to today's sanitized
	// "resolver unavailable" outcome, never a raw 500.
	CollectChainTLSA(ctx context.Context, domain string) (ChainTLSAObserved, error)
}

// ChainTLSAObserved is the TLSA observation plus the legacy client-facing
// diagnostics the gate conversion echoes unchanged.
type ChainTLSAObserved struct {
	// Observation reports whether the live record was found AND matches the
	// stored identity (today's ok), with the live rdata as its value.
	Observation domainpolicy.TLSAObservation
	// Detail is the legacy per-gate message (pass detail, missing-report, or
	// mismatch description) — reproduced verbatim.
	Detail string
	// Expected is the normalized stored DANE identity echoed on failure.
	Expected string
}

// --- Delegation NS/DS --------------------------------------------------------

// DelegationNSCollector observes the live NS (and, where the authority
// requires DNSSEC, DS) delegation of a binding domain — performed inside
// provider.VerifyDelegation today. The port is defined now so both flows'
// port sets are complete; the domain-verification flow adopts it when its
// remaining branches stop carrying effects, since VerifyDomain's
// DNSSEC/SOA self-heal and route-conversion effects stay on the legacy paths
// until then. Ports may remain unused until that migration.
type DelegationNSCollector interface {
	CollectDelegation(ctx context.Context, domain string) (DelegationObserved, error)
}

// DelegationObserved is the observed delegation evidence.
type DelegationObserved struct {
	// NS is the observed NS delegation of the binding.
	NS domainpolicy.NSObservation
	// DS is the observed DS delegating the zone's DNSSEC chain, when the
	// authority requires one today; nil when not checked.
	DS *domainpolicy.DSObservation
}

// --- Platform trust ----------------------------------------------------------

// PlatformTrustCollector observes the operator-trust relationship for a
// platform subdomain (DelegatedDomainService.ValidatePlatformBinding performs
// the shared-validator checks today). The same adoption note as
// DelegationNSCollector applies.
type PlatformTrustCollector interface {
	CollectPlatformTrust(ctx context.Context, domain string) (domainpolicy.PlatformTrustObservation, error)
}

// --- Collector bundle --------------------------------------------------------

// Collectors bundles the observation-collection adapters one validation flow
// may use. A nil collector means its gate may not be requested; requesting a
// gate whose collector is not wired fails closed with an error rather than
// inventing an observation.
type Collectors struct {
	DNSLink   DNSLinkCollector
	TokenTXT  TokenTXTCollector
	ChainTLSA ChainTLSACollector
}
