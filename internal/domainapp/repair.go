package domainapp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	"go.uber.org/zap"
)

// ErrRepairNotReconciled reports that the plan-driven repair reconciler
// could not derive — or
// refuses to apply — the requested repair from the binding's plan and
// observations (no plan, a plan whose profile declares no such repair, a
// plan/observed divergence, or an unavailable zone reference).
//
// It is the repair-family sibling of ErrDNSLinkNotReconciled: callers
// must treat it as "fall back to the legacy repair path with a loud log".
// The reconciler never authorizes a divergence from legacy behavior, and an
// error here means the flagged path has written NOTHING yet (pre-write
// authorization fails closed before any token generation, persistence, or
// DNS mutation), so the legacy fallback can proceed verbatim.
var ErrRepairNotReconciled = errors.New("domainapp: repair not representable by the binding plan")

// RepairExecutor is the write-side port for the repair effect families:
// zone heals (DNSSEC ensure, SOA MNAME) and challenge rotation. Every method
// is an idempotent adaptation of the existing legacy write (the domain's
// EnableDNSSEC / EnsureSOAMNAME / CreateWebsiteValidationRecord /
// CreateDNSLinkRecord adapters); the executor layer decides WETHER and WHICH
// effects run, derived exclusively from plan/Diff.
//
// Failure semantics per effect family (legacy parity, see ApplyRepairEffects):
//   - EnableZoneDNSSEC: fatal — a managed zone without a working signing key
//     must fail the verification (fail-closed DS doctrine).
//   - EnsureZoneSOAMNAME: best-effort — logged, never raised.
//   - challenge/DNSLink record writes on the rotation path: best-effort —
//     logged; the token is already persisted, and callers must never
//     double-write after a reconciled write failed.
type RepairExecutor interface {
	// WriteDNSLinkRecord creates or replaces the DNSLink TXT record for
	// domain (owner `_dnslink.<domain>`, content `dnslink=<target>`,
	// TTL 300) in zone zoneID. Same semantics as
	// EffectExecutor.WriteDNSLinkRecord.
	WriteDNSLinkRecord(ctx context.Context, zoneID uint, domain string, target string) error
	// WriteChallengeRecord creates or replaces the website validation TXT
	// record at <token-key>.<domain>, tokenRecord carrying the full
	// "<key>=<token>" content for the FRESHLY PERSISTED token. Adapters must
	// reproduce the legacy validation-record write byte-for-byte.
	WriteChallengeRecord(ctx context.Context, zoneID uint, domain string, tokenRecord string) error
	// EnableZoneDNSSEC enables (or repairs) DNSSEC signing on zone zoneID.
	// Idempotent: reuses an active key, mints one only when none exists.
	EnableZoneDNSSEC(ctx context.Context, zoneID uint) error
	// EnsureZoneSOAMNAME idempotently corrects the zone SOA MNAME to
	// nameservers[0], no-op'ing when it is already correct.
	EnsureZoneSOAMNAME(ctx context.Context, zoneID uint, domain string, nameservers []string) error
}

// ChallengeRotationInput is one expired-challenge-rotation reconciliation.
type ChallengeRotationInput struct {
	// Plan is the binding's current-behavior domainpolicy plan. Nil fails
	// closed: no plan, no rotation through the flagged path.
	Plan *domainpolicy.Plan
	// Domain is the binding's FQDN (the challenge record's parent name).
	Domain string
	// ZoneID is the portal-managed zone the challenge record lives in. Zero
	// is valid only when the plan's challenge record is owner-published.
	ZoneID uint
	// ChallengeExpired is the caller-observed token expiry (the website
	// lifecycle's IsExpired()); a false value fails closed — rotation is
	// only ever driven by an observed expiry.
	ChallengeExpired bool
	// DesiredDNSLinkPath is the service-derived DNSLink target path
	// (WebsiteTargetType.ToDNSLinkPath(TargetHash())). When the plan's
	// DNSLink intent diverges, the rotation fails closed (ErrRepairNotReconciled)
	// exactly like the DNSLink reconciler — the flagged path never
	// silently substitutes a target.
	DesiredDNSLinkPath string
	// TokenRecord is the full "<key>=<token>" content of the freshly
	// persisted token. Preview calls (AuthorizeChallengeRotation) may leave
	// it empty; applying a rotate-challenge effect without it fails closed.
	TokenRecord string
}

// ChallengeRotationResult is the outcome of one rotation reconciliation.
type ChallengeRotationResult struct {
	// PublicationLocus is the plan's destination for the challenge record:
	// portal-managed zones are written by the portal, owner-published ones
	// are the owner's duty (no portal write).
	PublicationLocus domainpolicy.PublicationLocus
	// Applied lists the effects the executor applied (challenge write, plus
	// any DNSLink re-assert). Empty for owner-published challenges.
	Applied []domainpolicy.Effect
	// Deferred lists effects reported but never executed (zone lifecycle,
	// unrepresentable placeholder record writes); no zone effect executes here.
	Deferred []string
	// OwnerInstruction is non-empty when the challenge record is
	// owner-published: the owner republishes it; the portal only rotated the
	// persisted token.
	OwnerInstruction string
	// Effects is the full ordered effect list Diff produced (diagnostics).
	Effects []domainpolicy.Effect
}

// AuthorizeChallengeRotation performs the pure fail-closed pre-write check
// for a challenge rotation: is this rotation representable by the plan, and
// where would the record be written? It performs no I/O and writes nothing,
// so callers may run it BEFORE generating or persisting a fresh token and
// fall back to the legacy rotation verbatim when it errors (with a loud
// log). It returns the plan's challenge-record publication locus.
func AuthorizeChallengeRotation(input ChallengeRotationInput) (domainpolicy.PublicationLocus, error) {
	if input.Plan == nil {
		return domainpolicy.PublicationLocusNone, fmt.Errorf("%w: no binding plan (nil)", ErrRepairNotReconciled)
	}
	plan := *input.Plan
	if input.Domain == "" {
		return domainpolicy.PublicationLocusNone, fmt.Errorf("%w: empty binding name", ErrRepairNotReconciled)
	}
	if !input.ChallengeExpired {
		return domainpolicy.PublicationLocusNone, fmt.Errorf("%w: rotation for %q requested without an observed token expiry", ErrRepairNotReconciled, input.Domain)
	}
	intent, found := planRecordIntent(plan, domainpolicy.RecordKindChallengeTXT)
	if !found {
		return domainpolicy.PublicationLocusNone, fmt.Errorf("%w: plan %q declares no challenge-TXT intent for %q", ErrRepairNotReconciled, plan.ProfileID.String(), input.Domain)
	}
	if !hasRepair(plan, domainpolicy.RepairKindRotateChallenge) {
		return domainpolicy.PublicationLocusNone, fmt.Errorf("%w: plan %q for %q declares no rotate-challenge repair", ErrRepairNotReconciled, plan.ProfileID.String(), input.Domain)
	}
	// The plan's challenge label must equal the token key of the record the
	// caller would write — a placeholders-free, byte-level plan/runtime parity
	// check, mirroring the DNSLink reconciler: never silently substitute.
	if input.TokenRecord != "" {
		key, _, _ := strings.Cut(input.TokenRecord, "=")
		if key != intent.Intent.Name {
			return domainpolicy.PublicationLocusNone, fmt.Errorf("%w: plan challenge label %q for %q diverges from the service token key %q", ErrRepairNotReconciled, intent.Intent.Name, input.Domain, key)
		}
	}
	// DNSLink target parity, mirroring the DNSLink reconciler's fail-closed
	// plan/service divergence guard.
	if dnsLink, ok := planRecordIntent(plan, domainpolicy.RecordKindDNSLink); ok &&
		input.DesiredDNSLinkPath != "" &&
		dnsLink.Intent.Value != input.DesiredDNSLinkPath {
		return domainpolicy.PublicationLocusNone, fmt.Errorf("%w: plan DNSLink value %q for %q diverges from the service-derived target %q",
			ErrRepairNotReconciled, dnsLink.Intent.Value, input.Domain, input.DesiredDNSLinkPath)
	}
	if intent.Destination == domainpolicy.PublicationLocusPortalZone && input.ZoneID == 0 {
		return domainpolicy.PublicationLocusNone, fmt.Errorf("%w: plan %q for %q publishes the challenge in the portal zone, but the binding holds no zone reference", ErrRepairNotReconciled, plan.ProfileID.String(), input.Domain)
	}
	return intent.Destination, nil
}

// ReconcileChallengeRotation derives the rotation's effects from the plan and
// the observed expiry, then applies them through the RepairExecutor. It is
// the single production entry point for expired-challenge rotation:
//
//   - owner-published challenge (e.g. ICANN owner-hosted): the portal ONLY
//     rotates the persisted token; the record republish duty is returned as
//     an owner instruction and no executor call is made.
//   - portal-managed challenge: Dry Diff yields the rotate-challenge effect
//     (from the observed expiry), the challenge record write (covered by the
//     rotation command — the fresh token is the runtime operand), and the
//     DNSLink re-assert write (legacy rotation rewrote it). Unrepresentable
//     placeholders (apex ALIAS content) and zone-lifecycle effects are
//     reported as deferred — never silently executed.
//
// Callers must have persisted the fresh token BEFORE calling this (the
// explicit application command order of the plan: generate → persist →
// reconcile), so a failed reconciled write is logged and the operation
// converges on a later pass; there is no double-write fallback.
func ReconcileChallengeRotation(ctx context.Context, input ChallengeRotationInput, executor RepairExecutor, logger *zap.Logger) (ChallengeRotationResult, error) {
	result := ChallengeRotationResult{Deferred: []string{}}
	// Pure pre-write authorization; nothing has been written yet, so an
	// error here still allows the caller's legacy fallback.
	locus, err := AuthorizeChallengeRotation(input)
	if err != nil {
		return result, err
	}
	result.PublicationLocus = locus
	plan := *input.Plan

	if locus != domainpolicy.PublicationLocusPortalZone {
		result.OwnerInstruction = fmt.Sprintf(
			"republish challenge TXT %s.%s = %s (owner-side publication; the portal has rotated the persisted token only)",
			planRecordIntentOrEmpty(plan, domainpolicy.RecordKindChallengeTXT).Intent.Name, input.Domain, input.TokenRecord)
		logRotation(logger, plan, input, "owner publication duty (no portal write)", result)
		return result, nil
	}

	// Observations: the live challenge record is KNOWN stale (the caller
	// observed the expiry), so it is reported as expired and its content is
	// irrelevant to the decision. The DNSLink record is deliberately left
	// unobserved: the legacy rotation rewrote it unconditionally, and Diff
	// then derives the same idempotent write. The zone is present when the
	// binding references one (suppressing the erroneous create-zone effect
	// the shared one-zone topology would otherwise produce) — zone lifecycle
	// itself is never executed here.
	obs := domainpolicy.ObservationSet{
		ChallengeTXT: &domainpolicy.TXTObservation{Value: "", Expired: true},
	}
	if input.ZoneID != 0 {
		obs.Zone = &domainpolicy.ZonePresenceObservation{Present: true, Allocation: plan.Zone.Allocation}
	}

	effects, derr := domainpolicy.Diff(plan, obs)
	if derr != nil {
		return result, fmt.Errorf("%w: diff rejected the plan/observations for %q: %s", ErrRepairNotReconciled, input.Domain, derr)
	}
	result.Effects = effects

	applied, deferred, aerr := ApplyRepairEffects(ctx, RepairApplyInput{
		Domain:          input.Domain,
		ZoneID:          input.ZoneID,
		ChallengeRecord: input.TokenRecord,
	}, effects, executor, logger)
	result.Applied = applied
	result.Deferred = deferred
	logRotation(logger, plan, input, rotationOutcome(result), result)
	return result, aerr
}

// ZoneHealInput is one zone-invariant repair reconciliation (DNSSEC signing
// key, SOA MNAME) for a portal-managed zone.
type ZoneHealInput struct {
	// Plan is the binding's current-behavior domainpolicy plan. Nil fails
	// closed (the caller falls back to the legacy self-heal verbatim).
	Plan *domainpolicy.Plan
	// Domain is the binding's FQDN.
	Domain string
	// ZoneID is the portal-managed zone to heal. Zero fails closed: legacy
	// self-heal only ever ran against a referenced portal zone.
	ZoneID uint
	// ZoneDNSSEC is the observed signing state (nil when the plan does not
	// require DNSSEC or the read was indeterminate); it gates the
	// ensure-dnssec effect.
	ZoneDNSSEC *domainpolicy.ZoneDNSSECObservation
	// SOAMNAME is the observed SOA MNAME (nil only when the zone's SOA could
	// not be observed AND the caller has decided to skip the repair).
	SOAMNAME *domainpolicy.SOAMNAMEObservation
	// Nameservers are the provider's approved nameservers; nameservers[0] is
	// the portal MNAME the SOA is ensured against.
	Nameservers []string
}

// ZoneHealResult is the outcome of one zone heal.
type ZoneHealResult struct {
	// Applied lists the executed repair effects (ensure-dnssec and/or
	// ensure-soa-mname).
	Applied []domainpolicy.Effect
	// Deferred lists effects reported but never executed (zone lifecycle and
	// unrepresentable record writes; zone transitions are never executed).
	Deferred []string
	// Effects is the full ordered effect list Diff produced (diagnostics).
	Effects []domainpolicy.Effect
	// DNSSECEnsured reports whether an ensure-dnssec effect was applied: the
	// caller MUST then re-read the live DS and fail closed when it is still
	// empty (the fail-closed DS doctrine of the legacy self-heal).
	DNSSECEnsured bool
}

// ReconcileZoneHeal derives the portal-zone invariant repairs from the
// binding's plan and the heal observations and applies them through
// the RepairExecutor:
//
//   - ensure-dnssec: only an observed Disabled signing state (and a plan
//     declaring the repair) authorizes EnableDNSSEC; an error is FATAL
//     (legacy parity — a managed zone that cannot be signed must fail
//     verification).
//   - ensure-soa-mname: only an observed mismatching MNAME authorizes the
//     repair; an error is best-effort (logged, never raised), exactly like
//     the legacy self-heal.
//
// The heal never rewrites binding records: the DNSLink intent is observed as
// matching (suppressing its write effect), and placeholder-valued
// apex/challenge record intents Diff would emit are reported as deferred —
// never executed.
func ReconcileZoneHeal(ctx context.Context, input ZoneHealInput, executor RepairExecutor, logger *zap.Logger) (ZoneHealResult, error) {
	result := ZoneHealResult{Deferred: []string{}}
	if input.Plan == nil {
		return result, fmt.Errorf("%w: no binding plan (nil)", ErrRepairNotReconciled)
	}
	plan := *input.Plan
	if input.Domain == "" {
		return result, fmt.Errorf("%w: empty binding name", ErrRepairNotReconciled)
	}
	if input.ZoneID == 0 {
		return result, fmt.Errorf("%w: zone heal for %q with no portal zone reference", ErrRepairNotReconciled, input.Domain)
	}

	// Records: DNSLink is observed as matching the plan so Diff derives no
	// write for it (the heal never rewrites binding records, exactly like the
	// legacy self-heal). Apex/challenge placeholders stay unobserved and are
	// deferred by the executor (loud), never executed.
	obs := domainpolicy.ObservationSet{Records: []domainpolicy.RecordObservation{}}
	if dnsLink, ok := planRecordIntent(plan, domainpolicy.RecordKindDNSLink); ok &&
		dnsLink.Destination == domainpolicy.PublicationLocusPortalZone {
		obs.Records = append(obs.Records, domainpolicy.RecordObservation{
			Kind:      dnsLink.Intent.Kind,
			Name:      dnsLink.Intent.Name,
			Value:     dnsLink.Intent.Value,
			Ownership: dnsLink.Intent.Ownership,
		})
	}
	// Zone: present with the plan's observed allocation — the heal repairs an
	// EXISTING zone and must not adopt or create one.
	obs.Zone = &domainpolicy.ZonePresenceObservation{Present: true, Allocation: plan.Zone.Allocation}
	if input.ZoneDNSSEC != nil {
		obs.ZoneDNSSEC = input.ZoneDNSSEC
	}
	obs.SOAMNAME = input.SOAMNAME

	effects, derr := domainpolicy.Diff(plan, obs)
	if derr != nil {
		return result, fmt.Errorf("%w: diff rejected the plan/observations for %q: %s", ErrRepairNotReconciled, input.Domain, derr)
	}
	result.Effects = effects

	applied, deferred, aerr := ApplyRepairEffects(ctx, RepairApplyInput{
		Domain:       input.Domain,
		ZoneID:       input.ZoneID,
		Nameservers:  input.Nameservers,
		ZoneHealFlow: true,
	}, effects, executor, logger)
	result.Applied = applied
	result.Deferred = deferred
	for _, effect := range applied {
		if effect.Kind == domainpolicy.EffectKindEnsureDNSSEC {
			result.DNSSECEnsured = true
		}
	}
	logHeal(logger, plan, input, healOutcome(result), result)
	return result, aerr
}

// RepairApplyInput carries the runtime operands the executor families need
// beyond the effect descriptors.
type RepairApplyInput struct {
	// Domain is the binding's FQDN (record owner parent).
	Domain string
	// ZoneID is the portal-managed zone the effects run against.
	ZoneID uint
	// ChallengeRecord is the full "<key>=<token>" content of the freshly
	// persisted token. Required when any rotate-challenge effect is expected.
	ChallengeRecord string
	// Nameservers is the approved NS list; nameservers[0] is the SOA MNAME
	// target. Required when any ensure-soa-mname effect is expected.
	Nameservers []string
	// ZoneHealFlow marks heal-flow application (runs on every verification):
	// placeholder record writes Diff derives for the apex/challenge intents
	// are expected non-actions there (the legacy self-heal never repaired
	// records either) and are reported at Debug instead of Warn.
	ZoneHealFlow bool
	// ReportOnlyItems, when true, reports skipped placeholder record effects
	// in the deferred list at Debug level (zone-heal flows, which run on
	// every verification); when false they are logged loudly at Warn
	// (calendar rotation flows, where skipping a legacy write is notable).
	ReportOnlyItems bool
}

// ApplyRepairEffects filters plan-derived effect descriptors to the
// repair families and applies them through the RepairExecutor. It is the
// executor half of the repair reconciler; the decision of WHICH effects run
// belongs entirely to domainpolicy.Diff.
//
// Family scope:
//
//   - EffectKindRotateChallenge: requires RepairApplyInput.ChallengeRecord
//     (fail closed without it); applied as the validation-record write for
//     the fresh token. Record-write failures are best-effort (logged, never
//     double-written) — legacy parity for regenerateExpiredToken.
//   - EffectKindEnsureDNSSEC: applied as EnableZoneDNSSEC; an error is fatal
//     (legacy parity: a managed zone that cannot be signed fails the flow).
//   - EffectKindEnsureSOAMNAME: applied as EnsureZoneSOAMNAME; an error is
//     best-effort (logged, never raised) — legacy parity.
//   - EffectKindWriteRecord: only DNSLink writes execute (the rotation's
//     unconditional re-assert; failures best-effort). A challenge write whose
//     intent value is the plan's placeholder is covered by the rotation
//     command and reported as such. All other record kinds (apex placeholders
//     among them) are unrepresentable: deferred, never executed.
//   - EffectKindCreateZone / ReuseZone / DeleteRecord / ReportRouteDrift are
//     reported as deferred and never executed (zone lifecycle and route
//     conversion are never executed here).
//   - an unknown effect kind fails closed with an error.
func ApplyRepairEffects(ctx context.Context, input RepairApplyInput, effects []domainpolicy.Effect, executor RepairExecutor, logger *zap.Logger) (applied []domainpolicy.Effect, deferred []string, err error) {
	applied = []domainpolicy.Effect{}
	deferred = []string{}

	// failClosed reports whether an out-of-family effect should abort the
	// application (no executor wired) instead of merely being reported.
	failClosed := func(effect domainpolicy.Effect) error {
		return fmt.Errorf("domainapp: repair effect %q for %q cannot be applied: no repair executor wired", effect.IdempotencyKey, input.Domain)
	}

	challengeRecordHandled := false
	for _, effect := range effects {
		switch effect.Kind {
		case domainpolicy.EffectKindRotateChallenge:
			if input.ChallengeRecord == "" {
				return applied, deferred, fmt.Errorf("%w: rotate-challenge effect %q for %q has no persisted token record to reconcile", ErrRepairNotReconciled, effect.IdempotencyKey, input.Domain)
			}
			if executor == nil {
				return applied, deferred, failClosed(effect)
			}
			if err := executor.WriteChallengeRecord(ctx, input.ZoneID, input.Domain, input.ChallengeRecord); err != nil {
				// Best-effort (legacy parity: the token is persisted; the
				// record write failure only logs and never triggers a
				// double-write fallback).
				logEffectIssue(logger, input, effect, "challenge record write failed (best-effort; token already persisted)", err)
				continue
			}
			applied = append(applied, effect)
			challengeRecordHandled = true
			logEffectApplied(logger, input, effect, "rotated challenge record")

		case domainpolicy.EffectKindEnsureDNSSEC:
			if executor == nil {
				return applied, deferred, failClosed(effect)
			}
			if err := executor.EnableZoneDNSSEC(ctx, input.ZoneID); err != nil {
				return applied, deferred, fmt.Errorf("failed to ensure DNSSEC for %s zone %d (effect %s): %w", input.Domain, input.ZoneID, effect.IdempotencyKey, err)
			}
			applied = append(applied, effect)
			logEffectApplied(logger, input, effect, "ensured zone DNSSEC")

		case domainpolicy.EffectKindEnsureSOAMNAME:
			if executor == nil {
				return applied, deferred, failClosed(effect)
			}
			if err := executor.EnsureZoneSOAMNAME(ctx, input.ZoneID, input.Domain, input.Nameservers); err != nil {
				// Best-effort (legacy parity: the SOA MNAME is a secondary
				// authoritative pointer).
				logEffectIssue(logger, input, effect, "SOA MNAME heal failed (best-effort)", err)
				continue
			}
			applied = append(applied, effect)
			logEffectApplied(logger, input, effect, "ensured SOA MNAME")

		case domainpolicy.EffectKindWriteRecord:
			if effect.Record == nil {
				return applied, deferred, fmt.Errorf("%w: write-record effect %q for %q carries no record intent", ErrRepairNotReconciled, effect.IdempotencyKey, input.Domain)
			}
			switch {
			case effect.Record.Intent.Kind == domainpolicy.RecordKindDNSLink:
				if executor == nil {
					return applied, deferred, failClosed(effect)
				}
				if err := executor.WriteDNSLinkRecord(ctx, input.ZoneID, input.Domain, effect.Record.Intent.Value); err != nil {
					// Best-effort on the rotation path (legacy parity).
					logEffectIssue(logger, input, effect, "DNSLink re-assert write failed (best-effort)", err)
					continue
				}
				applied = append(applied, effect)
				logEffectApplied(logger, input, effect, "re-asserted DNSLink record")
			case effect.Record.Intent.Kind == domainpolicy.RecordKindChallengeTXT &&
				isRecordPlaceholder(effect.Record.Intent.Kind, effect.Record.Intent.Value):
				// The validation token is dynamic — the plan carries a
				// placeholder, so the placeholder write is unrepresentable.
				// On the rotation path the fresh content is written by the
				// rotate-challenge command above, which is authoritative for
				// the record; nothing else executes here.
				deferred = append(deferred, effect.Kind.String()+" "+effect.KeyMaterial)
				logRecordCoveredOrDeferred(logger, input, effect, challengeRecordHandled || input.ChallengeRecord != "")
			case isRecordPlaceholder(effect.Record.Intent.Kind, effect.Record.Intent.Value):
				// A placeholder-valued record intent (e.g. the apex ALIAS
				// content, derived from portal config, not from the plan) is
				// unrepresentable in the pure model: deferred and never
				// executed — the legacy lifecycle writers re-assert it. During
				// zone heals this matches legacy parity (the self-heal never
				// repaired records), so it is a Debug note, not a warning.
				deferred = append(deferred, effect.Kind.String()+" "+effect.KeyMaterial)
				if input.ZoneHealFlow {
					logZoneEffectDeferred(logger, input, effect)
				} else {
					logEffectDeferred(logger, input, effect)
				}
			default:
				deferred = append(deferred, effect.Kind.String()+" "+effect.KeyMaterial)
				logEffectDeferred(logger, input, effect)
			}

		case domainpolicy.EffectKindCreateZone, domainpolicy.EffectKindReuseZone,
			domainpolicy.EffectKindDeleteRecord, domainpolicy.EffectKindReportRouteDrift:
			// Zone lifecycle and route drift belong to an explicit transition
			// command; report, never execute. This is expected
			// sequencing, not a divergence: Debug, not Warn.
			deferred = append(deferred, effect.Kind.String()+" "+effect.KeyMaterial)
			logZoneEffectDeferred(logger, input, effect)

		default:
			return applied, deferred, fmt.Errorf("%w: unknown effect kind %d (%s) for %s", ErrRepairNotReconciled, int(effect.Kind), effect.Kind.String(), input.Domain)
		}
	}
	return applied, deferred, nil
}

// isRecordPlaceholder reports whether the record intent's value is the pure
// model's placeholder for its kind (dynamic runtime content — the validation
// token and the apex ALIAS target — that the plan does not materialize).
// Placeholder-valued intents are descriptors only: assigning them real
// content is the executor command's business (the rotation token record),
// and any other placeholder write stays with the legacy lifecycle writers.
func isRecordPlaceholder(kind domainpolicy.RecordKind, value string) bool {
	switch kind {
	case domainpolicy.RecordKindChallengeTXT:
		return value == domainpolicy.ChallengeValuePlaceholder
	case domainpolicy.RecordKindApexALIAS:
		return value == domainpolicy.ApexALIASPlaceholder
	default:
		return false
	}
}

// planRecordIntent returns the plan's record intent of the given kind.
func planRecordIntent(plan domainpolicy.Plan, kind domainpolicy.RecordKind) (domainpolicy.PlannedRecord, bool) {
	for _, planned := range plan.Records {
		if planned.Intent.Kind == kind {
			return planned, true
		}
	}
	return domainpolicy.PlannedRecord{}, false
}

// planRecordIntentOrEmpty is planRecordIntent without the ok flag.
func planRecordIntentOrEmpty(plan domainpolicy.Plan, kind domainpolicy.RecordKind) domainpolicy.PlannedRecord {
	p, _ := planRecordIntent(plan, kind)
	return p
}

// hasRepair reports whether the plan declares the given repair kind.
func hasRepair(plan domainpolicy.Plan, kind domainpolicy.RepairKind) bool {
	for _, repair := range plan.Repairs {
		if repair.Kind == kind {
			return true
		}
	}
	return false
}

func rotationOutcome(r ChallengeRotationResult) string {
	if r.OwnerInstruction != "" {
		return "owner publication duty (no portal write)"
	}
	if len(r.Applied) == 0 {
		return "no-op"
	}
	return "effects applied"
}

func healOutcome(r ZoneHealResult) string {
	if len(r.Applied) == 0 {
		return "no-op (zone invariants hold)"
	}
	return "effects applied"
}

func logRotation(logger *zap.Logger, plan domainpolicy.Plan, input ChallengeRotationInput, outcome string, result ChallengeRotationResult) {
	if logger == nil {
		return
	}
	logger.Debug("challenge rotation reconcile",
		zap.String("profile", plan.ProfileID.String()),
		zap.String("domain", input.Domain),
		zap.Uint("zone_id", input.ZoneID),
		zap.String("locus", result.PublicationLocus.String()),
		zap.String("outcome", outcome),
		zap.Strings("deferred_effects", result.Deferred))
}

func logHeal(logger *zap.Logger, plan domainpolicy.Plan, input ZoneHealInput, outcome string, result ZoneHealResult) {
	if logger == nil {
		return
	}
	logger.Debug("zone heal reconcile",
		zap.String("profile", plan.ProfileID.String()),
		zap.String("domain", input.Domain),
		zap.Uint("zone_id", input.ZoneID),
		zap.String("outcome", outcome),
		zap.Strings("deferred_effects", result.Deferred))
}

func logEffectApplied(logger *zap.Logger, input RepairApplyInput, effect domainpolicy.Effect, note string) {
	if logger != nil {
		logger.Info("repair effect applied",
			zap.String("domain", input.Domain),
			zap.Uint("zone_id", input.ZoneID),
			zap.String("effect", effect.Kind.String()),
			zap.String("idempotency_key", effect.IdempotencyKey),
			zap.String("reason", effect.Reason),
			zap.String("note", note))
	}
}

func logEffectIssue(logger *zap.Logger, input RepairApplyInput, effect domainpolicy.Effect, note string, err error) {
	if logger != nil {
		logger.Warn("repair effect write failed",
			zap.String("domain", input.Domain),
			zap.Uint("zone_id", input.ZoneID),
			zap.String("effect", effect.Kind.String()),
			zap.String("idempotency_key", effect.IdempotencyKey),
			zap.String("note", note),
			zap.Error(err))
	}
}

// logZoneEffectDeferred reports a zone-lifecycle/route effect the repair
// families intentionally leave to the explicit transition command.
func logZoneEffectDeferred(logger *zap.Logger, input RepairApplyInput, effect domainpolicy.Effect) {
	if logger != nil {
		logger.Debug("repair reconcile deferred a zone-lifecycle/route effect; the explicit transition command owns it",
			zap.String("domain", input.Domain),
			zap.Uint("zone_id", input.ZoneID),
			zap.String("effect", effect.Kind.String()),
			zap.String("material", effect.KeyMaterial))
	}
}

// logRecordCoveredOrDeferred reports a placeholder challenge write that the
// rotate-challenge command owns: covered callsides log at Debug, uncovered
// ones at Warn (nothing wrote it).
func logRecordCoveredOrDeferred(logger *zap.Logger, input RepairApplyInput, effect domainpolicy.Effect, covered bool) {
	if logger == nil {
		return
	}
	if covered {
		logger.Debug("challenge record write covered by the rotate-challenge command",
			zap.String("domain", input.Domain),
			zap.Uint("zone_id", input.ZoneID),
			zap.String("idempotency_key", effect.IdempotencyKey))
		return
	}
	logEffectDeferred(logger, input, effect)
}

func logEffectDeferred(logger *zap.Logger, input RepairApplyInput, effect domainpolicy.Effect) {
	if logger != nil {
		logger.Warn("plan-driven repair deferred an unrepresentable effect; the legacy lifecycle path owns it — report this for follow-up",
			zap.String("domain", input.Domain),
			zap.Uint("zone_id", input.ZoneID),
			zap.String("effect", effect.Kind.String()),
			zap.String("material", effect.KeyMaterial))
	}
}
