package domainpolicy

// ZoneDNSSECState describes the observed DNSSEC signing state of a zone.
type ZoneDNSSECState int

const (
	// ZoneDNSSECStateUnknown is the zero value; it fails closed (unknown
	// state produces no effects, never a forced enable).
	ZoneDNSSECStateUnknown ZoneDNSSECState = iota
	ZoneDNSSECStateEnabled
	ZoneDNSSECStateDisabled
	// ZoneDNSSECStateIndeterminate means the check could not be completed
	// (for example key rollover with multiple active keys); it never
	// authorizes a weakening action.
	ZoneDNSSECStateIndeterminate
)

// String returns a stable diagnostic name for the DNSSEC state.
func (s ZoneDNSSECState) String() string {
	switch s {
	case ZoneDNSSECStateEnabled:
		return "enabled"
	case ZoneDNSSECStateDisabled:
		return "disabled"
	case ZoneDNSSECStateIndeterminate:
		return "indeterminate"
	default:
		return "unknown"
	}
}

// Valid reports whether the state is a known, non-unknown value.
func (s ZoneDNSSECState) Valid() bool {
	switch s {
	case ZoneDNSSECStateEnabled, ZoneDNSSECStateDisabled, ZoneDNSSECStateIndeterminate:
		return true
	default:
		return false
	}
}

// DNSLinkObservation is the observed DNSLink TXT record content.
type DNSLinkObservation struct {
	// Value is the found DNSLink path (e.g. "/ipfs/<cid>"); empty when the
	// record is absent.
	Value string
}

// Found reports whether a DNSLink record was observed.
func (o DNSLinkObservation) Found() bool { return o.Value != "" }

// TXTObservation is the observed TXT record at the challenge label.
type TXTObservation struct {
	// Value is the found TXT record content; empty when absent.
	Value string
	// Expired reports that the validation token has expired (the website
	// lifecycle sets this; it triggers the challenge-rotation repair).
	Expired bool
}

// NSObservation is the observed NS delegation of the binding.
type NSObservation struct {
	// Found reports whether NS records were observed.
	Found bool
	// Nameservers are the observed NS targets.
	Nameservers []string
}

// DSObservation is the observed DS delegating the zone's DNSSEC chain.
type DSObservation struct {
	// Found reports whether a DS record was observed.
	Found bool
	// Value is the observed DS record content.
	Value string
}

// TLSAObservation is the observed TLSA record for the binding's DANE
// identity (on-chain zone data for chain bindings, the portal zone otherwise).
type TLSAObservation struct {
	// Found reports whether a TLSA record was observed.
	Found bool
	// Value is the observed TLSA record content.
	Value string
}

// PlatformTrustObservation is the observed operator-trust relationship for a
// platform subdomain (the shared-validator checks ValidatePlatformBinding
// performs today).
type PlatformTrustObservation struct {
	// Trusted reports whether the full operator-trust relationship holds.
	Trusted bool
	// Reason is a stable diagnostic when the trust relationship fails.
	Reason string
}

// ZoneDNSSECObservation is the observed DNSSEC signing state of the zone.
type ZoneDNSSECObservation struct {
	State ZoneDNSSECState
}

// SOAMNAMEObservation is the observed SOA MNAME of the binding's zone. The
// portal repairs a mismatching MNAME today (selfHealZone); Diff expresses
// that as an EnsureSOAMNAME effect.
type SOAMNAMEObservation struct {
	// Found reports whether the SOA record was observed.
	Found bool
	// Current is the observed MNAME.
	Current string
	// MatchesPortalMNAME reports whether the MNAME matches the portal's
	// expected server identity.
	MatchesPortalMNAME bool
}

// ZonePresenceObservation is the observed zone infrastructure for the
// binding.
type ZonePresenceObservation struct {
	// Present reports whether a portal/operator zone exists.
	Present bool
	// Allocation is the observed allocation.
	Allocation ZoneAllocation
}

// RecordObservation is one DNS record observed in the binding's zone. It is
// the input Diff compares against the plan's record intents.
type RecordObservation struct {
	// Kind is the observed record type.
	Kind RecordKind
	// Name is the record label relative to the binding's zone apex.
	Name string
	// Value is the observed record content.
	Value string
	// Ownership names who owns the observed record.
	Ownership RecordOwnership
}

// RouteObservation is the typed result of route discovery: which route the
// name resolves through, which backend serves it, and whether the source was
// assumed rather than measured. Route discovery returns it as a typed result
// so backend identity survives; a bare Inspect boolean would discard it.
type RouteObservation struct {
	// Route is the discovered resolution route (unknown when not probed).
	Route ResolutionRoute
	// Backend is the discovered resolution backend.
	Backend BackendID
	// AssumedSource reports whether the route source was assumed (inferred
	// from persisted state) rather than measured live.
	AssumedSource bool
}

// ObservationSet collects everything the evaluators may look at. Every field
// is optional: an absent observation simply fails its gate closed in
// Evaluate and produces no effect in Diff. The set must contain no unknown
// enum values; Diff rejects such a set instead of guessing.
type ObservationSet struct {
	Route         RouteObservation
	DNSLink       *DNSLinkObservation
	ChallengeTXT  *TXTObservation
	NS            *NSObservation
	DS            *DSObservation
	TLSA          *TLSAObservation
	PlatformTrust *PlatformTrustObservation
	ZoneDNSSEC    *ZoneDNSSECObservation
	SOAMNAME      *SOAMNAMEObservation
	Zone          *ZonePresenceObservation
	Records       []RecordObservation
}

// Validate checks that the set carries no unknown enum values. It is used by
// both Evaluate and Diff so unknown observations can never quietly produce
// results or effects.
func (o ObservationSet) Validate() error {
	if o.Route.Route != ResolutionRouteUnknown {
		if _, err := NewResolutionRoute(o.Route.Route); err != nil {
			return err
		}
	}
	if o.Route.Backend != EmptyBackendID {
		if _, err := NewBackendID(o.Route.Backend); err != nil {
			return err
		}
	}
	if o.ZoneDNSSEC != nil && !o.ZoneDNSSEC.State.Valid() {
		return newInvalid("observations", "unknown zone DNSSEC state %d", int(o.ZoneDNSSEC.State))
	}
	if o.Zone != nil && o.Zone.Present {
		if _, err := NewZoneAllocation(o.Zone.Allocation); err != nil {
			return err
		}
	}
	for _, record := range o.Records {
		if _, err := NewRecordKind(record.Kind); err != nil {
			return err
		}
		if _, err := NewRecordOwnership(record.Ownership); err != nil {
			return err
		}
	}
	return nil
}
