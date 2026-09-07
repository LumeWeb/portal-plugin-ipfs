package domainapp

import (
	"context"
	"errors"
	"fmt"

	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	"go.uber.org/zap"
)

// ErrDNSLinkNotReconciled reports that the plan-driven DNSLink reconciler
// could not derive — or refuses to apply — the binding's DNSLink desired
// state from the given inputs (no plan, a plan carrying no DNSLink record
// intent, a plan/service-target divergence, a plan whose authority contradicts
// the zone reference, or an unknown publication locus).
//
// Callers must treat it as "fall back to the legacy DNSLink writer path with
// a loud log": the reconciler never authorizes a divergence from the legacy
// behavior, and an error here means the flagged path leaves the operation
// untouched (the fallback caller performs the legacy write).
var ErrDNSLinkNotReconciled = errors.New("domainapp: DNSLink desired state not representable by the binding plan")

// DNSLinkReconcileInput is one DNSLink desired-state reconciliation operation.
type DNSLinkReconcileInput struct {
	// Plan is the binding's current-behavior domainpolicy plan (from
	// DelegatedDomainService.CurrentBindingPlan). It is the DESIRED-STATE
	// OWNER of the reconciliation: effects are derived from it, never from
	// call-site heuristics. A nil plan fails closed.
	Plan *domainpolicy.Plan
	// Domain is the binding's FQDN — the DNSLink record owner
	// (`_dnslink.<domain>`) and the observation target.
	Domain string
	// ZoneID is the portal-managed zone the portal authority writes DNSLink
	// into. Zero is valid only when the plan's DNSLink publication locus is
	// not the portal zone (owner-dns / chain authority).
	ZoneID uint
	// DesiredTarget is the desired DNSLink path derived from the website's
	// persisted target at the service boundary
	// (db.WebsiteTargetType.ToDNSLinkPath(website.TargetHash())). It must
	// equal the plan's DNSLink record intent value; a divergence is fail
	// closed (ErrDNSLinkNotReconciled) with a loud log — the reconciler never
	// silently substitutes one for the other.
	DesiredTarget string
}

// DNSLinkReconcileResult is the outcome of one reconciliation.
type DNSLinkReconcileResult struct {
	// Applied lists the DNSLink-record effects the executor applied
	// (create/update/delete). Empty when the live record already matches.
	Applied []domainpolicy.Effect
	// Deferred lists effect kinds Diff produced that are outside the DNSLink
	// record-write family (zone create/reuse, DNSSEC, SOA, challenge
	// rotation) — reported, never executed; they stay on the legacy writers.
	Deferred []string
	// RouteDrift is non-empty when Diff reported a route-drift finding for
	// the binding (a report only; conversion remains a separate command).
	RouteDrift string
	// OwnerInstruction is non-empty when the plan's DNSLink publication
	// locus is owner DNS or chain: the owner publishes the record; the
	// portal performs no PowerDNS write for it.
	OwnerInstruction string
	// Effects is the full ordered effect list Diff produced (diagnostics).
	Effects []domainpolicy.Effect
}

// NoOp reports whether reconciliation performed no write (the live record
// already matched, the binding needs no portal write, or the publication is
// an owner-side duty).
func (r DNSLinkReconcileResult) NoOp() bool {
	return len(r.Applied) == 0
}

// ReconcileDNSLink applies the plan's DNSLink desired state for one binding.
// It is the single production entry point for DNSLink record reconciliation:
// the binding plan owns the desired state, domainpolicy.Diff owns the
// effect derivation, and only the DNSLink record-write family is executed
// through the EffectExecutor.
//
// Behavior:
//
//   - fail closed: a nil plan, an empty domain/target, a plan without a
//     DNSLink record intent, a plan-intent/service-target divergence, or a
//     portal-zone intent without a zone reference returns
//     ErrDNSLinkNotReconciled (wrapped with the reason) and no write.
//   - owner/chain authority: the plan carries the DNSLink record at the
//     owner-dns or chain publication locus; the result carries an owner
//     instruction and no write (and no executor call) is made.
//   - no-op comparison: the live DNSLink record is observed through the
//     collector (the same observation adapter the validation flow uses, so
//     NXDOMAIN classification and candidate normalization are identical
//     across flag states); a live record already carrying the desired target
//     yields no effect. An observation TRANSPORT failure does not block the
//     repair — exactly like the legacy reconcile path, the reconciler
//     proceeds as if unobserved and lets the idempotent write converge.
//   - observe == nil is permitted only for first-publication writes (the
//     bind path, whose legacy semantics wrote unconditionally): Diff then
//     sees the record as unobserved and deterministically yields the
//     idempotent write. Callers observing live state must pass a collector.
//
// The result is deterministic: repeating the call with the same plan,
// observation, and executor outcome yields the same effect list in the same
// order (domainpolicy.Diff idempotency).
func ReconcileDNSLink(ctx context.Context, input DNSLinkReconcileInput, observe DNSLinkCollector, executor EffectExecutor, logger *zap.Logger) (DNSLinkReconcileResult, error) {
	result := DNSLinkReconcileResult{Deferred: []string{}}

	if input.Plan == nil {
		return result, fmt.Errorf("%w: no binding plan (nil)", ErrDNSLinkNotReconciled)
	}
	plan := *input.Plan
	if input.Domain == "" {
		return result, fmt.Errorf("%w: empty binding name", ErrDNSLinkNotReconciled)
	}
	if input.DesiredTarget == "" {
		return result, fmt.Errorf("%w: empty desired target for %q", ErrDNSLinkNotReconciled, input.Domain)
	}

	// The plan's DNSLink record intent is the desired-state owner.
	intent, found := planDNSLinkIntent(plan)
	if !found {
		return result, fmt.Errorf("%w: plan %q carries no DNSLink record intent for %q", ErrDNSLinkNotReconciled, plan.ProfileID.String(), input.Domain)
	}
	if intent.Intent.Value != input.DesiredTarget {
		if logger != nil {
			logger.Warn("plan/legacy divergence (DNSLink target): flagged reconciler deferring to the legacy writer",
				zap.String("domain", input.Domain),
				zap.String("profile", plan.ProfileID.String()),
				zap.String("plan_target", intent.Intent.Value),
				zap.String("service_target", input.DesiredTarget))
		}
		return result, fmt.Errorf("%w: plan DNSLink value %q for %q diverges from the service-derived target %q", ErrDNSLinkNotReconciled, intent.Intent.Value, input.Domain, input.DesiredTarget)
	}

	// Publication locus decides who writes. Owner-dns and chain loci are
	// publication duties of the owner or the chain: the portal returns the
	// instruction and performs no PowerDNS write.
	switch intent.Destination {
	case domainpolicy.PublicationLocusPortalZone:
		if plan.Authority != domainpolicy.AuthorityLocusPortalZone && plan.Authority != domainpolicy.AuthorityLocusOperatorZone {
			return result, fmt.Errorf("%w: plan %q for %q publishes DNSLink in the portal zone under %s authority",
				ErrDNSLinkNotReconciled, plan.ProfileID.String(), input.Domain, plan.Authority.String())
		}
		if input.ZoneID == 0 {
			return result, fmt.Errorf("%w: plan %q for %q publishes DNSLink in the portal zone, but the binding holds no zone reference",
				ErrDNSLinkNotReconciled, plan.ProfileID.String(), input.Domain)
		}
	case domainpolicy.PublicationLocusOwnerDNS, domainpolicy.PublicationLocusChain:
		result.OwnerInstruction = ownerInstruction(input.Domain, input.DesiredTarget, intent.Destination)
		logReconcile(logger, plan, input, "owner publication duty (no portal write)", result, "")
		return result, nil
	default:
		return result, fmt.Errorf("%w: plan %q for %q carries an unknown DNSLink publication locus %d",
			ErrDNSLinkNotReconciled, plan.ProfileID.String(), input.Domain, int(intent.Destination))
	}

	// Observe the live record for the no-op comparison. Transport failures do
	// not block the repair (legacy parity: the reconcile wrote unconditionally
	// when the live record was unreadable); the idempotent write converges.
	current := ""
	if observe != nil {
		collected, err := observe.CollectDNSLink(ctx, input.Domain)
		if err != nil {
			if logger != nil {
				logger.Debug("DNSLink reconcile: live observation failed; writing unconditionally (legacy parity)",
					zap.String("domain", input.Domain), zap.Error(err))
			}
		} else if !collected.NXDOMAIN {
			current = collected.Observation.Value
		}
	} else {
		if logger != nil {
			logger.Debug("DNSLink reconcile: no observation collector wired (first-publication write); deriving effects from an unobserved record",
				zap.String("domain", input.Domain))
		}
	}

	obs := domainpolicy.ObservationSet{Route: domainpolicy.RouteObservation{}}
	if current != "" {
		obs.Records = []domainpolicy.RecordObservation{{
			Kind:      intent.Intent.Kind,
			Name:      intent.Intent.Name,
			Value:     current,
			Ownership: intent.Intent.Ownership,
		}}
	}

	effects, err := domainpolicy.Diff(plan, obs)
	if err != nil {
		return result, fmt.Errorf("%w: diff rejected the plan/observations for %q: %s", ErrDNSLinkNotReconciled, input.Domain, err)
	}
	result.Effects = effects

	applied, deferred, err := ApplyDNSLinkEffects(ctx, input.Domain, input.ZoneID, effects, executor, logger)
	if err != nil {
		return result, err
	}
	result.Applied = applied
	result.Deferred = deferred
	for _, effect := range effects {
		if effect.Kind == domainpolicy.EffectKindReportRouteDrift {
			result.RouteDrift = effect.Reason
		}
	}
	logReconcile(logger, plan, input, reconcileOutcome(result), result, current)
	return result, nil
}

// planDNSLinkIntent returns the plan's DNSLink record intent, if the plan
// declares one.
func planDNSLinkIntent(plan domainpolicy.Plan) (domainpolicy.PlannedRecord, bool) {
	for _, planned := range plan.Records {
		if planned.Intent.Kind == domainpolicy.RecordKindDNSLink {
			return planned, true
		}
	}
	return domainpolicy.PlannedRecord{}, false
}

// ownerInstruction is the stable owner-side publication instruction returned
// for owner-dns and chain loci: what to publish and where, no portal write.
func ownerInstruction(domain, target string, destination domainpolicy.PublicationLocus) string {
	locus := "owner DNS"
	if destination == domainpolicy.PublicationLocusChain {
		locus = "the chain's zone data"
	}
	return fmt.Sprintf("publish DNSLink TXT _dnslink.%s = dnslink=%s in %s (owner-side publication; the portal does not write it)", domain, target, locus)
}

// reconcileOutcome renders the effect/no-op classification for diagnostics.
func reconcileOutcome(r DNSLinkReconcileResult) string {
	if r.OwnerInstruction != "" {
		return "owner publication duty (no portal write)"
	}
	if r.NoOp() {
		return "no-op (live DNSLink already matches the desired target)"
	}
	return "effects applied"
}

// logReconcile emits the plan/profile, desired target, live value, and
// effect/no-op classification. Record values are DNS paths (no secrets).
func logReconcile(logger *zap.Logger, plan domainpolicy.Plan, input DNSLinkReconcileInput, outcome string, result DNSLinkReconcileResult, current string) {
	if logger == nil {
		return
	}
	logger.Debug("DNSLink desired-state reconcile",
		zap.String("profile", plan.ProfileID.String()),
		zap.Uint("profile_version", uint(plan.ProfileVersion)),
		zap.String("domain", input.Domain),
		zap.Uint("zone_id", input.ZoneID),
		zap.String("target_kind", plan.Target.Kind.String()),
		zap.String("desired_target", input.DesiredTarget),
		zap.String("live_target", current),
		zap.String("outcome", outcome),
		zap.String("owner_instruction", result.OwnerInstruction),
		zap.Strings("deferred_effects", result.Deferred))
}
