package domainpolicy

// RecordKind identifies the type of DNS record a plan intends to manage.
type RecordKind int

const (
	RecordKindUnknown RecordKind = iota
	// RecordKindDNSLink is the DNSLink TXT record (_dnslink.<domain>).
	RecordKindDNSLink
	// RecordKindChallengeTXT is the portal-account challenge TXT record.
	RecordKindChallengeTXT
	// RecordKindApexA is the apex A record.
	RecordKindApexA
	// RecordKindApexALIAS is the apex ALIAS record.
	RecordKindApexALIAS
	// RecordKindTLSA is the DANE TLSA record.
	RecordKindTLSA
	// RecordKindNS is an NS delegation record.
	RecordKindNS
	// RecordKindDS is a DNSSEC DS delegation record.
	RecordKindDS
	// RecordKindSOA is the SOA record (used for MNAME observation/repair).
	RecordKindSOA
)

// String returns a stable diagnostic name for the record kind.
func (r RecordKind) String() string {
	switch r {
	case RecordKindDNSLink:
		return "dnslink"
	case RecordKindChallengeTXT:
		return "challenge-txt"
	case RecordKindApexA:
		return "apex-a"
	case RecordKindApexALIAS:
		return "apex-alias"
	case RecordKindTLSA:
		return "tlsa"
	case RecordKindNS:
		return "ns"
	case RecordKindDS:
		return "ds"
	case RecordKindSOA:
		return "soa"
	default:
		return "unknown"
	}
}

// Valid reports whether the record kind is a known, non-unknown value.
func (r RecordKind) Valid() bool {
	switch r {
	case RecordKindDNSLink, RecordKindChallengeTXT, RecordKindApexA, RecordKindApexALIAS, RecordKindTLSA, RecordKindNS, RecordKindDS, RecordKindSOA:
		return true
	default:
		return false
	}
}

// NewRecordKind validates and returns the given record kind, rejecting
// unknown values.
func NewRecordKind(r RecordKind) (RecordKind, error) {
	if !r.Valid() {
		return RecordKindUnknown, newInvalid("record kind", "unknown value %d", int(r))
	}
	return r, nil
}

// RecordOwnership identifies which party or system owns a record in a plan.
// Ownership determines who may write or delete the record and whether
// shared-zone protection applies.
type RecordOwnership int

const (
	RecordOwnershipUnknown RecordOwnership = iota
	// RecordOwnershipBindingContent is the record binding content to the
	// domain (the DNSLink record). It remains binding-content-owned even in
	// delegated zones.
	RecordOwnershipBindingContent
	// RecordOwnershipBindingSecurity is a security record owned by the
	// binding (e.g. TLSA for a managed-zone DANE identity).
	RecordOwnershipBindingSecurity
	// RecordOwnershipDelegation is a delegation record (NS/DS) whose
	// lifecycle is tied to the delegation decision.
	RecordOwnershipDelegation
	// RecordOwnershipZoneInfrastructure is a record owned by the zone
	// rather than by a binding (e.g. SOA infrastructure state).
	RecordOwnershipZoneInfrastructure
	// RecordOwnershipOperator is a record controlled by the human operator
	// of a binding.
	RecordOwnershipOperator
)

// String returns a stable diagnostic name for the record ownership.
func (r RecordOwnership) String() string {
	switch r {
	case RecordOwnershipBindingContent:
		return "binding-content"
	case RecordOwnershipBindingSecurity:
		return "binding-security"
	case RecordOwnershipDelegation:
		return "delegation"
	case RecordOwnershipZoneInfrastructure:
		return "zone-infrastructure"
	case RecordOwnershipOperator:
		return "operator"
	default:
		return "unknown"
	}
}

// Valid reports whether the record ownership is a known, non-unknown value.
func (r RecordOwnership) Valid() bool {
	switch r {
	case RecordOwnershipBindingContent, RecordOwnershipBindingSecurity, RecordOwnershipDelegation, RecordOwnershipZoneInfrastructure, RecordOwnershipOperator:
		return true
	default:
		return false
	}
}

// NewRecordOwnership validates and returns the given record ownership,
// rejecting unknown values.
func NewRecordOwnership(r RecordOwnership) (RecordOwnership, error) {
	if !r.Valid() {
		return RecordOwnershipUnknown, newInvalid("record ownership", "unknown value %d", int(r))
	}
	return r, nil
}

// RecordIntent is the desired state of a single DNS record, expressed as
// pure values only. It carries no network identity (no zone ID, no RRSet ID)
// and no database identity (no row keys): those live in adapter and
// executor layers that consume intents.
type RecordIntent struct {
	// Kind is the type of record intended.
	Kind RecordKind
	// Name is the DNS label relative to the binding's zone apex (e.g.
	// "_dnslink" or the apex itself). It never carries a FQDN suffix.
	Name string
	// Value is the desired record content.
	Value string
	// TTL is the desired record TTL in seconds; zero means "caller's
	// default".
	TTL uint32
	// Ownership names who owns the record.
	Ownership RecordOwnership
}

// NewRecordIntent validates and returns a RecordIntent, rejecting unknown
// kinds and ownerships and empty names and values.
func NewRecordIntent(kind RecordKind, name, value string, ttl uint32, ownership RecordOwnership) (RecordIntent, error) {
	if !kind.Valid() {
		return RecordIntent{}, newInvalid("record intent", "unknown record kind %d", int(kind))
	}
	if name == "" {
		return RecordIntent{}, newInvalid("record intent", "empty record name")
	}
	if value == "" {
		return RecordIntent{}, newInvalid("record intent", "empty record value")
	}
	if !ownership.Valid() {
		return RecordIntent{}, newInvalid("record intent", "unknown record ownership %d", int(ownership))
	}
	return RecordIntent{
		Kind:      kind,
		Name:      name,
		Value:     value,
		TTL:       ttl,
		Ownership: ownership,
	}, nil
}

// Valid reports whether the intent is well-formed, matching what
// NewRecordIntent accepts.
func (r RecordIntent) Valid() bool {
	return r.Kind.Valid() && r.Name != "" && r.Value != "" && r.Ownership.Valid()
}
