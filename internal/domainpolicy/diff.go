package domainpolicy

import "sort"

// Diff compares the plan against the observed state and returns the ordered
// list of effect descriptors an executor may apply. It is pure,
// deterministic, and fail closed:
//
//   - it returns descriptors only; no effect executes in this package;
//   - it is deterministic: repeating the call against the same inputs returns
//     an identical effect list (same order, same idempotency keys);
//   - every effect carries stable idempotency material;
//   - an observation set with unknown enum values yields an error and no
//     effects; unknown or absent observations never invent effects;
//   - deletion effects name their record's owner, and only portal/operator
//     authority with a zone intent can produce record deletions;
//   - route differences produce a ReportRouteDrift descriptor only — policy
//     marks the binding as requiring reconciliation, and conversion remains a
//     separate explicit command.
func Diff(plan Plan, obs ObservationSet) ([]Effect, error) {
	if err := validatePlanUsable(plan); err != nil {
		return nil, err
	}
	if err := obs.Validate(); err != nil {
		return nil, err
	}

	var effects []Effect
	appendEffect := func(effect Effect) {
		effects = append(effects, effect)
	}

	// 1. Route drift: a measured route different from the plan's route is
	// reported, never converted implicitly.
	if obs.Route.Route != ResolutionRouteUnknown && obs.Route.Route != plan.Route {
		appendEffect(Effect{
			Kind: EffectKindReportRouteDrift,
			IdempotencyKey: effectKey(plan, "route-drift",
				plan.Route.String()+"->"+obs.Route.Route.String(), string(obs.Route.Backend)),
			KeyMaterial: "route " + plan.Route.String() + " -> " + obs.Route.Route.String() +
				" (backend " + string(obs.Route.Backend) + ", assumed=" +
				boolText(obs.Route.AssumedSource) + ")",
			Reason: "binding " + plan.Name + " now resolves via route " + obs.Route.Route.String() +
				" (backend " + string(obs.Route.Backend) + "); expected " + plan.Route.String(),
		})
	}

	zoneIntent := plan.Zone
	hasZoneIntent := zoneIntent.Allocation == ZoneAllocationDedicated || zoneIntent.Allocation == ZoneAllocationSharedParent

	// 2. Zone provisioning and adoption: a plan with a zone intent either
	// creates the zone or adopts the observed one.
	if hasZoneIntent {
		if obs.Zone != nil && obs.Zone.Present {
			appendEffect(Effect{
				Kind:           EffectKindReuseZone,
				IdempotencyKey: effectKey(plan, "reuse-zone", zoneIntent.Allocation.String()),
				KeyMaterial:    "zone " + zoneIntent.Allocation.String(),
				Zone:           zonePtr(zoneIntent),
				Reason:         "binding " + plan.Name + " adopts its existing " + zoneIntent.Allocation.String() + " zone",
			})
		} else {
			appendEffect(Effect{
				Kind:           EffectKindCreateZone,
				IdempotencyKey: effectKey(plan, "create-zone", zoneIntent.Allocation.String()),
				KeyMaterial:    "zone " + zoneIntent.Allocation.String(),
				Zone:           zonePtr(zoneIntent),
				Reason:         "binding " + plan.Name + " requires a new " + zoneIntent.Allocation.String() + " zone",
			})
		}
	}

	// 3. Record writes: planned records in the portal/operator zone are
	// reconciled against observed records. Records in owner-dns or chain
	// loci are portal publication duties of the owner or chain — the portal
	// never writes them, so they yield no write effects.
	expected := make(map[string]PlannedRecord, len(plan.Records))
	for _, planned := range plan.Records {
		if planned.Destination != PublicationLocusPortalZone {
			continue
		}
		key := recordKey(planned.Intent)
		expected[key] = planned
		if observed, ok := lookupObservedRecord(obs, planned.Intent.Kind, planned.Intent.Name); !ok || observed.Value != planned.Intent.Value {
			appendEffect(Effect{
				Kind: EffectKindWriteRecord,
				IdempotencyKey: effectKey(plan, "write-record",
					planned.Intent.Kind.String(), planned.Intent.Name, hashValue(planned.Intent.Value)),
				KeyMaterial: "record " + planned.Intent.Kind.String() + " " + planned.Intent.Name +
					" = " + hashValue(planned.Intent.Value) +
					" (owner " + planned.Intent.Ownership.String() + ")",
				Record: plannedPtr(planned),
				Reason: "binding " + plan.Name + " requires " + planned.Intent.Kind.String() +
					" record " + planned.Intent.Name,
			})
		}
	}

	// 4. Record deletions: only bindings with portal/operator authority over
	// a zone may delete records from it, and a deletion always names the
	// record's owner. Binding-content records (DNSLink) are preserved by the
	// current code paths and never deleted here without an intent change.
	if hasZoneIntent {
		for _, observed := range sortObservedRecords(obs) {
			if !observed.Ownership.Valid() {
				continue
			}
			key := recordKey(RecordIntent{
				Kind:      observed.Kind,
				Name:      observed.Name,
				Value:     observed.Value,
				Ownership: observed.Ownership,
			})
			if _, planned := expected[key]; planned {
				continue
			}
			switch observed.Ownership {
			case RecordOwnershipBindingContent, RecordOwnershipBindingSecurity, RecordOwnershipDelegation:
				appendEffect(Effect{
					Kind: EffectKindDeleteRecord,
					IdempotencyKey: effectKey(plan, "delete-record",
						observed.Kind.String(), observed.Name, hashValue(observed.Value)),
					KeyMaterial: "record " + observed.Kind.String() + " " + observed.Name +
						" = " + hashValue(observed.Value) +
						" (owner " + observed.Ownership.String() + ")",
					Record: &PlannedRecord{
						Intent: RecordIntent{
							Kind:      observed.Kind,
							Name:      observed.Name,
							Value:     observed.Value,
							Ownership: observed.Ownership,
						},
						Destination: PublicationLocusPortalZone,
					},
					Reason: "binding " + plan.Name + " no longer requires " + observed.Kind.String() +
						" record " + observed.Name + " (owner " + observed.Ownership.String() + ")",
				})
			}
		}
	}

	// 5. DNSSEC repair: only an observed Disabled state confirms the zone
	// needs DNSSEC. Unknown or indeterminate states produce no effect.
	hasDNSSECRepair := containsRepair(plan, RepairKindEnsureDNSSEC)
	if hasDNSSECRepair && obs.ZoneDNSSEC != nil && obs.ZoneDNSSEC.State == ZoneDNSSECStateDisabled {
		appendEffect(Effect{
			Kind:           EffectKindEnsureDNSSEC,
			IdempotencyKey: effectKey(plan, "ensure-dnssec"),
			KeyMaterial:    "dnssec",
			Reason:         "binding " + plan.Name + "'s zone requires DNSSEC signing",
		})
	}

	// 6. SOA MNAME repair: only an observed mismatching MNAME triggers.
	hasSOARepair := containsRepair(plan, RepairKindEnsureSOAMNAME)
	if hasSOARepair && obs.SOAMNAME != nil && obs.SOAMNAME.Found && !obs.SOAMNAME.MatchesPortalMNAME {
		appendEffect(Effect{
			Kind:           EffectKindEnsureSOAMNAME,
			IdempotencyKey: effectKey(plan, "ensure-soa-mname", hashValue(obs.SOAMNAME.Current)),
			KeyMaterial:    "soa-mname " + hashValue(obs.SOAMNAME.Current),
			Reason:         "binding " + plan.Name + "'s zone SOA MNAME does not match the portal server",
		})
	}

	// 7. Challenge rotation: only an observed expired token rotates. The
	// challenge record above covers the plain write; rotation carries its own
	// effect so the executor knows a fresh token must be generated.
	if containsRepair(plan, RepairKindRotateChallenge) && obs.ChallengeTXT != nil && obs.ChallengeTXT.Expired {
		appendEffect(Effect{
			Kind:           EffectKindRotateChallenge,
			IdempotencyKey: effectKey(plan, "rotate-challenge", ChallengeRecordLabel),
			KeyMaterial:    "challenge " + ChallengeRecordLabel,
			Reason:         "binding " + plan.Name + "'s validation token has expired",
		})
	}

	return effects, nil
}

// validatePlanUsable re-checks the identifying fields of a plan so a
// zero-value Plan fails closed before any effect could be derived.
func validatePlanUsable(plan Plan) error {
	if _, err := NewProfileID(plan.ProfileID); err != nil {
		return err
	}
	if _, err := NewProfileVersion(int(plan.ProfileVersion)); err != nil {
		return err
	}
	if plan.Name == "" {
		return newInvalid("diff", "empty binding name")
	}
	if !plan.Target.Valid() {
		return newInvalid("diff", "invalid content target for %q", plan.Name)
	}
	if _, err := NewAuthorityLocus(plan.Authority); err != nil {
		return err
	}
	if _, err := NewResolutionRoute(plan.Route); err != nil {
		return err
	}
	if _, err := NewZoneAllocation(plan.Zone.Allocation); err != nil {
		return err
	}
	if _, err := NewSecurityPlan(plan.DNSSEC.Requirement, plan.DNSSEC.Provisioner, plan.DNSSEC.Publication, plan.DNSSEC.Verification); err != nil {
		return newInvalid("diff", "invalid DNSSEC plan: %s", err)
	}
	if _, err := NewSecurityPlan(plan.DANE.Requirement, plan.DANE.Provisioner, plan.DANE.Publication, plan.DANE.Verification); err != nil {
		return newInvalid("diff", "invalid DANE plan: %s", err)
	}
	return nil
}

// containsRepair reports whether the plan declares the given repair.
func containsRepair(plan Plan, kind RepairKind) bool {
	for _, repair := range plan.Repairs {
		if repair.Kind == kind {
			return true
		}
	}
	return false
}

// lookupObservedRecord finds the observed record matching a planned record's
// kind and name. The first match wins; current code maintains at most one
// live record per (kind, name).
func lookupObservedRecord(obs ObservationSet, kind RecordKind, name string) (RecordObservation, bool) {
	for _, record := range obs.Records {
		if record.Kind == kind && record.Name == name {
			return record, true
		}
	}
	return RecordObservation{}, false
}

// sortObservedRecords returns the observed records in a stable order
// (kind, then name, then value) so deletion effects are deterministic.
func sortObservedRecords(obs ObservationSet) []RecordObservation {
	out := append([]RecordObservation(nil), obs.Records...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Value < out[j].Value
	})
	return out
}

// recordKey is the stable identity of a record within the plan.
func recordKey(intent RecordIntent) string {
	return intent.Kind.String() + "|" + intent.Name
}

// hashValue renders a value into the key material. Values here are short
// DNS-safe strings (paths, placeholders); a length-bounded encoding keeps
// keys stable and readable without any crypto.
func hashValue(value string) string {
	const maxKeyMaterial = 64
	if len(value) <= maxKeyMaterial {
		return value
	}
	return value[:maxKeyMaterial]
}

// effectKey derives the stable idempotency key for an effect.
func effectKey(plan Plan, effect string, parts ...string) string {
	key := string(plan.ProfileID) + "/" + plan.Name + "/" + effect
	for _, part := range parts {
		key += "/" + part
	}
	return key
}

// zonePtr returns a pointer copy of a zone intent so effects cannot be
// aliased to the plan.
func zonePtr(intent ZoneIntent) *ZoneIntent {
	copy := intent
	return &copy
}

// plannedPtr returns a deep pointer copy of a planned record.
func plannedPtr(planned PlannedRecord) *PlannedRecord {
	copy := planned
	return &copy
}

// boolText renders a stable boolean for key material and diagnostics.
func boolText(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
