package core

// DNSSECPolicy captures whether a namespace provider's delegation is confirmed
// against a live DS record served by the parent zone (managed-DNSSEC namespaces,
// e.g. HNS) or on NS visibility alone (e.g. ICANN).
type DNSSECPolicy uint8

const (
	DNSSECNotRequired DNSSECPolicy = iota
	DNSSECRequired
)

// TLSAPolicy captures whether a provider publishes a DANE TLSA record into the
// portal-managed authoritative zone (DANE-capable alt-root namespaces, e.g.
// HNS). DNSSEC and TLSA are independent capabilities: a provider may DNSSEC-sign
// without DANE (and vice versa), so one must never be used as a proxy for the
// other.
type TLSAPolicy uint8

const (
	TLSANotManaged TLSAPolicy = iota
	TLSAManaged
)

// ProviderPolicy is the immutable hosting-capability set of a namespace
// provider. Every capability decision (DNSSEC gating, managed-zone TLSA
// publication, apex record type) derives from it. The per-method boolean and
// record-type adapters (RequiresDNSSEC, UsesManagedZoneTLSA, ApexRecordType)
// are transient compatibility shims over this single source of truth and must
// not become additional sources of truth.
//
// It lives in this (leaf) core package — alongside RecordType — so any
// consumer, including generated mocks, can reference it without importing the
// service package that owns the providers.
type ProviderPolicy struct {
	DNSSEC         DNSSECPolicy
	TLSA           TLSAPolicy
	ApexRecordType RecordType
}
