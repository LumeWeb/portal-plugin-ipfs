package domainpolicy

// EffectKind identifies an effect an executor may apply. This package
// produces descriptors only: no effect is ever executed here.
type EffectKind int

const (
	EffectKindUnknown EffectKind = iota
	// EffectKindCreateZone creates the portal/operator zone for the binding.
	EffectKindCreateZone
	// EffectKindReuseZone adopts an existing zone for the binding.
	EffectKindReuseZone
	// EffectKindWriteRecord writes or replaces one DNS record.
	EffectKindWriteRecord
	// EffectKindDeleteRecord deletes one DNS record; the descriptor names the
	// record's owner so shared records are never deleted by mistake.
	EffectKindDeleteRecord
	// EffectKindEnsureDNSSEC enables/repairs zone DNSSEC.
	EffectKindEnsureDNSSEC
	// EffectKindEnsureSOAMNAME repairs the zone SOA MNAME.
	EffectKindEnsureSOAMNAME
	// EffectKindRotateChallenge rotates an expired validation token.
	EffectKindRotateChallenge
	// EffectKindReportRouteDrift reports a route change; policy marks the
	// binding as requiring reconciliation and a separate command performs any
	// conversion — verification never converts implicitly.
	EffectKindReportRouteDrift
)

// String returns a stable diagnostic name for the effect kind.
func (e EffectKind) String() string {
	switch e {
	case EffectKindCreateZone:
		return "create-zone"
	case EffectKindReuseZone:
		return "reuse-zone"
	case EffectKindWriteRecord:
		return "write-record"
	case EffectKindDeleteRecord:
		return "delete-record"
	case EffectKindEnsureDNSSEC:
		return "ensure-dnssec"
	case EffectKindEnsureSOAMNAME:
		return "ensure-soa-mname"
	case EffectKindRotateChallenge:
		return "rotate-challenge"
	case EffectKindReportRouteDrift:
		return "report-route-drift"
	default:
		return "unknown"
	}
}

// Valid reports whether the effect kind is a known, non-unknown value.
func (e EffectKind) Valid() bool {
	switch e {
	case EffectKindCreateZone, EffectKindReuseZone, EffectKindWriteRecord, EffectKindDeleteRecord,
		EffectKindEnsureDNSSEC, EffectKindEnsureSOAMNAME, EffectKindRotateChallenge, EffectKindReportRouteDrift:
		return true
	default:
		return false
	}
}

// Effect is one idempotent effect descriptor: what an executor would do,
// with the stable identity material (idempotency key, profile, binding,
// record owner) needed to run it safely and repeat it. Effects are pure
// data; nothing in this package executes one.
type Effect struct {
	// Kind is the effect's kind.
	Kind EffectKind
	// IdempotencyKey is the stable identity of the effect: repeating the same
	// plan against the same observations yields the same key, so an executor
	// can dedupe and retry safely.
	IdempotencyKey string
	// Reason is a stable human-readable diagnostic.
	Reason string
	// Zone is set for zone effects (create/reuse).
	Zone *ZoneIntent
	// Record is set for record effects. For deletions the record's Ownership
	// names the record's owner so shared records are protected.
	Record *PlannedRecord
	// KeyMaterial is the stable material the idempotency key was derived
	// from, so an executor can recompute the key independently.
	KeyMaterial string
}
