package domainpolicy

import (
	"reflect"
	"testing"
)

// emptyObservations is the observation set of a freshly provisioned binding:
// no zone, no records, nothing measured.
func emptyObservations() ObservationSet {
	return ObservationSet{}
}

// recordsFromPlan returns observed records matching a plan's record intents
// exactly (the reconciled steady state).
func recordsFromPlan(plan Plan) []RecordObservation {
	var records []RecordObservation
	for _, planned := range plan.Records {
		if planned.Destination != PublicationLocusPortalZone {
			// Owner-dns and chain records are observed where the owner or
			// chain publishes them; a plan-scoped zone observation sees only
			// portal-zone records.
			continue
		}
		records = append(records, RecordObservation{
			Kind:      planned.Intent.Kind,
			Name:      planned.Intent.Name,
			Value:     planned.Intent.Value,
			Ownership: planned.Intent.Ownership,
		})
	}
	return records
}

// observationsWithZoneAndRecords builds a reconciled observation set for the
// plan: zone present, records matching, DNSSEC enabled, MNAME matching.
func observationsWithZoneAndRecords(t *testing.T, plan Plan, allocation ZoneAllocation) ObservationSet {
	t.Helper()
	obs := goodObservations()
	obs.Zone = &ZonePresenceObservation{Present: true, Allocation: allocation}
	obs.Records = recordsFromPlan(plan)
	return obs
}

func effectKinds(effects []Effect) []EffectKind {
	kinds := make([]EffectKind, 0, len(effects))
	for _, effect := range effects {
		kinds = append(kinds, effect.Kind)
	}
	return kinds
}

func TestDiffZoneProvisioning(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDICANNPortal)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan, err := planFor(t, profile)
	if err != nil {
		t.Fatalf("PlanBinding: %s", err)
	}

	t.Run("no zone creates one", func(t *testing.T) {
		effects, err := Diff(plan, emptyObservations())
		if err != nil {
			t.Fatalf("Diff: %s", err)
		}
		if len(effects) == 0 || effects[0].Kind != EffectKindCreateZone {
			t.Fatalf("first effect = %v, want create-zone", effectKinds(effects))
		}
		if effects[0].Zone == nil || effects[0].Zone.Allocation != ZoneAllocationDedicated {
			t.Fatalf("create-zone effect missing zone intent: %+v", effects[0])
		}
		// Followed by the DNS record writes for the missing records.
		kinds := effectKinds(effects)
		want := []EffectKind{EffectKindCreateZone, EffectKindWriteRecord, EffectKindWriteRecord, EffectKindWriteRecord}
		if !reflect.DeepEqual(kinds, want) {
			t.Fatalf("effects = %v, want %v", kinds, want)
		}
	})

	t.Run("existing zone is reused", func(t *testing.T) {
		effects, err := Diff(plan, observationsWithZoneAndRecords(t, plan, ZoneAllocationDedicated))
		if err != nil {
			t.Fatalf("Diff: %s", err)
		}
		kinds := effectKinds(effects)
		// Exactly the reuse effect: records match, DNSSEC is not required
		// for ICANN, MNAME matches, token fresh.
		if !reflect.DeepEqual(kinds, []EffectKind{EffectKindReuseZone}) {
			t.Fatalf("effects = %v, want only reuse-zone", kinds)
		}
	})
}

// TestDiffSharedParentTopologyReusesParentZone encodes the observed
// one-zone topology (DelegatedDomainService.resolveManagedZone): a subdomain
// of a portal-managed binding reuses the parent's zone, so a shared-parent
// plan bound from shared-parent facts diffs to a reuse-zone effect carrying
// the shared-parent intent — never a create-zone.
func TestDiffSharedParentTopologyReusesParentZone(t *testing.T) {
	cases := []struct {
		id         ProfileID
		name       string
		zoneIntent ZoneIntent
	}{
		{
			// ICANN portal profile: subdomain binding inside the parent's zone.
			id:         ProfileIDICANNPortal,
			name:       "docs.example.xyz",
			zoneIntent: ZoneIntent{Authority: AuthorityLocusPortalZone, Allocation: ZoneAllocationSharedParent},
		},
		{
			// Namebase-style child: a subdomain of a managed HNS name.
			id:         ProfileIDHNSPortalNamebaseChild,
			name:       "docs.sub.altroot",
			zoneIntent: ZoneIntent{Authority: AuthorityLocusPortalZone, Allocation: ZoneAllocationSharedParent},
		},
	}
	for _, tc := range cases {
		t.Run(tc.id.String(), func(t *testing.T) {
			profile, ok := DefaultRegistry().Lookup(tc.id)
			if !ok {
				t.Fatalf("profile %q is not registered", tc.id.String())
			}
			facts := factsForProfile(t, profile)
			facts.Name = tc.name
			facts.ZonePresent = true
			facts.ZoneAllocation = ZoneAllocationSharedParent
			plan, err := PlanBinding(profile, facts)
			if err != nil {
				t.Fatalf("PlanBinding with shared-parent zone facts: %s", err)
			}
			if !reflect.DeepEqual(plan.Zone, tc.zoneIntent) {
				t.Fatalf("zone intent = %+v, want %+v", plan.Zone, tc.zoneIntent)
			}

			// Reconciled parent zone: only a reuse effect, carrying the
			// shared-parent intent; no create-zone anywhere.
			effects, err := Diff(plan, observationsWithZoneAndRecords(t, plan, ZoneAllocationSharedParent))
			if err != nil {
				t.Fatalf("Diff: %s", err)
			}
			kinds := effectKinds(effects)
			if len(kinds) == 0 || kinds[0] != EffectKindReuseZone {
				t.Fatalf("first effect = %v, want reuse-zone", kinds)
			}
			if effects[0].Zone == nil || effects[0].Zone.Allocation != ZoneAllocationSharedParent {
				t.Fatalf("reuse effect missing shared-parent zone intent: %+v", effects[0])
			}
			for _, kind := range kinds {
				if kind == EffectKindCreateZone {
					t.Fatalf("shared-parent topology produced a create-zone effect: %v", kinds)
				}
			}
		})
	}
}

func TestDiffStaleRecordIsRewritten(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDICANNPortal)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan, err := planFor(t, profile)
	if err != nil {
		t.Fatalf("PlanBinding: %s", err)
	}
	obs := observationsWithZoneAndRecords(t, plan, ZoneAllocationDedicated)
	for i := range obs.Records {
		if obs.Records[i].Kind == RecordKindDNSLink {
			obs.Records[i].Value = "/ipfs/stalecid"
		}
	}
	effects, err := Diff(plan, obs)
	if err != nil {
		t.Fatalf("Diff: %s", err)
	}
	sawWrite := false
	for _, effect := range effects {
		if effect.Kind == EffectKindWriteRecord && effect.Record != nil &&
			effect.Record.Intent.Kind == RecordKindDNSLink &&
			effect.Record.Intent.Value == plan.Target.DNSLinkPath() {
			sawWrite = true
		}
		if effect.Kind == EffectKindDeleteRecord && effect.Record != nil &&
			effect.Record.Intent.Kind == RecordKindDNSLink {
			t.Fatalf("a stale DNSLink must be rewritten, never deleted")
		}
	}
	if !sawWrite {
		t.Fatalf("stale DNSLink produced no write effect: %v", effectKinds(effects))
	}
}

func TestDiffDeletionNamesOwner(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDICANNPortal)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan, err := planFor(t, profile)
	if err != nil {
		t.Fatalf("PlanBinding: %s", err)
	}
	obs := observationsWithZoneAndRecords(t, plan, ZoneAllocationDedicated)
	// A leftover managed-zone TLSA from a previous policy: not planned by the
	// ICANN profile, so the plan observes it as no longer required.
	obs.Records = append(obs.Records, RecordObservation{
		Kind:      RecordKindTLSA,
		Name:      TLSARecordName,
		Value:     TLSAValuePlaceholder,
		Ownership: RecordOwnershipBindingSecurity,
	})
	effects, err := Diff(plan, obs)
	if err != nil {
		t.Fatalf("Diff: %s", err)
	}
	var deletion *Effect
	for i := range effects {
		if effects[i].Kind == EffectKindDeleteRecord {
			deletion = &effects[i]
		}
	}
	if deletion == nil {
		t.Fatalf("stray TLSA produced no deletion effect: %v", effectKinds(effects))
	}
	if deletion.Record == nil || deletion.Record.Intent.Ownership != RecordOwnershipBindingSecurity {
		t.Fatalf("deletion effect must name the record owner: %+v", deletion)
	}
	if deletion.IdempotencyKey == "" || deletion.KeyMaterial == "" {
		t.Fatalf("deletion effect must carry idempotency material: %+v", deletion)
	}
}

func TestDiffDNSSECRepairs(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDHNSPortalNative)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan, err := planFor(t, profile)
	if err != nil {
		t.Fatalf("PlanBinding: %s", err)
	}

	t.Run("disabled zone state triggers ensure", func(t *testing.T) {
		obs := observationsWithZoneAndRecords(t, plan, ZoneAllocationDedicated)
		obs.ZoneDNSSEC = &ZoneDNSSECObservation{State: ZoneDNSSECStateDisabled}
		effects, err := Diff(plan, obs)
		if err != nil {
			t.Fatalf("Diff: %s", err)
		}
		found := false
		for _, effect := range effects {
			if effect.Kind == EffectKindEnsureDNSSEC {
				found = true
			}
		}
		if !found {
			t.Fatalf("disabled zone DNSSEC produced no ensure effect: %v", effectKinds(effects))
		}
	})

	t.Run("unknown state is rejected and produces no effect", func(t *testing.T) {
		obs := observationsWithZoneAndRecords(t, plan, ZoneAllocationDedicated)
		obs.ZoneDNSSEC = &ZoneDNSSECObservation{State: ZoneDNSSECStateUnknown}
		effects, err := Diff(plan, obs)
		if err == nil {
			t.Fatalf("unknown DNSSEC state must be rejected")
		}
		if len(effects) != 0 {
			t.Fatalf("unknown DNSSEC state must produce no effects: %+v", effects)
		}
	})

	t.Run("indeterminate state produces no effect", func(t *testing.T) {
		obs := observationsWithZoneAndRecords(t, plan, ZoneAllocationDedicated)
		obs.ZoneDNSSEC = &ZoneDNSSECObservation{State: ZoneDNSSECStateIndeterminate}
		effects, err := Diff(plan, obs)
		if err != nil {
			t.Fatalf("Diff: %s", err)
		}
		for _, effect := range effects {
			if effect.Kind == EffectKindEnsureDNSSEC {
				t.Fatalf("indeterminate DNSSEC state must not produce an ensure effect")
			}
		}
	})

	t.Run("unobserved state produces no effect", func(t *testing.T) {
		obs := observationsWithZoneAndRecords(t, plan, ZoneAllocationDedicated)
		obs.ZoneDNSSEC = nil
		effects, err := Diff(plan, obs)
		if err != nil {
			t.Fatalf("Diff: %s", err)
		}
		for _, effect := range effects {
			if effect.Kind == EffectKindEnsureDNSSEC {
				t.Fatalf("unobserved DNSSEC state must not produce an ensure effect")
			}
		}
	})
}

func TestDiffSOAMNAMERepair(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDICANNPortal)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan, err := planFor(t, profile)
	if err != nil {
		t.Fatalf("PlanBinding: %s", err)
	}

	t.Run("mismatching MNAME triggers repair", func(t *testing.T) {
		obs := observationsWithZoneAndRecords(t, plan, ZoneAllocationDedicated)
		obs.SOAMNAME = &SOAMNAMEObservation{Found: true, Current: "ns1.legacy.example", MatchesPortalMNAME: false}
		effects, err := Diff(plan, obs)
		if err != nil {
			t.Fatalf("Diff: %s", err)
		}
		found := false
		for _, effect := range effects {
			if effect.Kind == EffectKindEnsureSOAMNAME {
				found = true
			}
		}
		if !found {
			t.Fatalf("mismatching SOA MNAME produced no repair effect: %v", effectKinds(effects))
		}
	})

	t.Run("matching MNAME and missing SOA produce no effect", func(t *testing.T) {
		obs := observationsWithZoneAndRecords(t, plan, ZoneAllocationDedicated)
		effects, err := Diff(plan, obs)
		if err != nil {
			t.Fatalf("Diff: %s", err)
		}
		for _, effect := range effects {
			if effect.Kind == EffectKindEnsureSOAMNAME {
				t.Fatalf("matching MNAME must not produce a repair effect")
			}
		}
		obs.SOAMNAME = nil
		effects, err = Diff(plan, obs)
		if err != nil {
			t.Fatalf("Diff: %s", err)
		}
		for _, effect := range effects {
			if effect.Kind == EffectKindEnsureSOAMNAME {
				t.Fatalf("unobserved SOA must not produce a repair effect")
			}
		}
	})
}

func TestDiffChallengeRotation(t *testing.T) {
	for _, id := range []ProfileID{ProfileIDICANNPortal, ProfileIDICANNOwner} {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		plan, err := planFor(t, profile)
		if err != nil {
			t.Fatalf("PlanBinding: %s", err)
		}

		t.Run(id.String()+"/expired token rotates", func(t *testing.T) {
			obs := emptyObservations()
			obs.ChallengeTXT = &TXTObservation{Value: ChallengeRecordLabel + "=old", Expired: true}
			effects, err := Diff(plan, obs)
			if err != nil {
				t.Fatalf("Diff: %s", err)
			}
			found := false
			for _, effect := range effects {
				if effect.Kind == EffectKindRotateChallenge {
					found = true
				}
			}
			if !found {
				t.Fatalf("expired token produced no rotation effect: %v", effectKinds(effects))
			}
		})

		t.Run(id.String()+"/fresh token does not rotate", func(t *testing.T) {
			obs := emptyObservations()
			obs.ChallengeTXT = &TXTObservation{Value: ChallengeRecordLabel + "=fresh", Expired: false}
			effects, err := Diff(plan, obs)
			if err != nil {
				t.Fatalf("Diff: %s", err)
			}
			for _, effect := range effects {
				if effect.Kind == EffectKindRotateChallenge {
					t.Fatalf("fresh token must not rotate")
				}
			}
		})
	}

	t.Run("HNS profiles never rotate a challenge", func(t *testing.T) {
		for _, id := range []ProfileID{ProfileIDHNSPortalNative, ProfileIDHNSOwnerNative, ProfileIDHNSChainEthereum} {
			profile, ok := DefaultRegistry().Lookup(id)
			if !ok {
				t.Fatalf("profile %q is not registered", id.String())
			}
			plan, err := planFor(t, profile)
			if err != nil {
				t.Fatalf("PlanBinding: %s", err)
			}
			obs := emptyObservations()
			obs.ChallengeTXT = &TXTObservation{Value: "x", Expired: true}
			effects, err := Diff(plan, obs)
			if err != nil {
				t.Fatalf("Diff: %s", err)
			}
			for _, effect := range effects {
				if effect.Kind == EffectKindRotateChallenge {
					t.Fatalf("HNS profile %q must not rotate a challenge (TXT is skipped today)", id.String())
				}
			}
		}
	})
}

func TestDiffRouteDrift(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDHNSOwnerNative)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan, err := planFor(t, profile)
	if err != nil {
		t.Fatalf("PlanBinding: %s", err)
	}
	obs := emptyObservations()
	obs.Route = RouteObservation{Route: ResolutionRouteCrossChain, Backend: BackendEthereum, AssumedSource: false}
	effects, err := Diff(plan, obs)
	if err != nil {
		t.Fatalf("Diff: %s", err)
	}
	if len(effects) != 1 || effects[0].Kind != EffectKindReportRouteDrift {
		t.Fatalf("route drift effects = %v, want exactly one report-route-drift", effectKinds(effects))
	}
	if effects[0].IdempotencyKey == "" {
		t.Fatalf("route drift effect must carry idempotency material")
	}
}

func TestDiffChainProfileProducesNoPowerDNSEffects(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDHNSChainEthereum)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan, err := planFor(t, profile)
	if err != nil {
		t.Fatalf("PlanBinding: %s", err)
	}

	t.Run("empty observations produce no effects", func(t *testing.T) {
		effects, err := Diff(plan, emptyObservations())
		if err != nil {
			t.Fatalf("Diff: %s", err)
		}
		if len(effects) != 0 {
			t.Fatalf("chain profile diff produced effects %+v; chain authority with no zone must produce no PowerDNS effects", effects)
		}
	})

	t.Run("on-chain observations produce no portal write effects", func(t *testing.T) {
		obs := goodObservations()
		obs.Zone = nil
		effects, err := Diff(plan, obs)
		if err != nil {
			t.Fatalf("Diff: %s", err)
		}
		for _, effect := range effects {
			if effect.Kind == EffectKindCreateZone || effect.Kind == EffectKindReuseZone || effect.Kind == EffectKindWriteRecord || effect.Kind == EffectKindDeleteRecord {
				t.Fatalf("chain profile produced PowerDNS effect %+v", effect)
			}
		}
	})
}

func TestDiffIsDeterministic(t *testing.T) {
	for _, id := range currentProfileIDs {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		plan, err := planFor(t, profile)
		if err != nil {
			t.Fatalf("PlanBinding(%s): %s", id.String(), err)
		}
		for name, obs := range map[string]ObservationSet{
			"empty":      emptyObservations(),
			"reconciled": observationsWithZoneAndRecords(t, plan, profile.ZoneAllocation),
			"drifted":    {Route: RouteObservation{Route: ResolutionRouteCrossChain, Backend: BackendEthereum}},
		} {
			first, err := Diff(plan, obs)
			if err != nil {
				t.Fatalf("Diff(%s/%s): %s", id.String(), name, err)
			}
			second, err := Diff(plan, obs)
			if err != nil {
				t.Fatalf("Diff(%s/%s): %s", id.String(), name, err)
			}
			if !reflect.DeepEqual(first, second) {
				t.Fatalf("Diff(%s/%s) is not deterministic", id.String(), name)
			}
		}
	}
}

func TestDiffIdempotencyMaterialOnEveryEffect(t *testing.T) {
	for _, id := range currentProfileIDs {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		plan, err := planFor(t, profile)
		if err != nil {
			t.Fatalf("PlanBinding: %s", err)
		}
		obs := emptyObservations()
		obs.ZoneDNSSEC = &ZoneDNSSECObservation{State: ZoneDNSSECStateDisabled}
		obs.SOAMNAME = &SOAMNAMEObservation{Found: true, MatchesPortalMNAME: false}
		obs.ChallengeTXT = &TXTObservation{Value: "expired", Expired: true}
		obs.Route = RouteObservation{Route: ResolutionRouteCrossChain, Backend: BackendEthereum}
		effects, err := Diff(plan, obs)
		if err != nil {
			t.Fatalf("Diff(%s): %s", id.String(), err)
		}
		for _, effect := range effects {
			if !effect.Kind.Valid() {
				t.Fatalf("effect with unknown kind: %+v", effect)
			}
			if effect.IdempotencyKey == "" || effect.KeyMaterial == "" || effect.Reason == "" {
				t.Fatalf("effect %+v lacks stable idempotency material", effect)
			}
		}
		// Key independence: distinct effects within one plan carry distinct
		// keys.
		seen := make(map[string]bool)
		for _, effect := range effects {
			if seen[effect.IdempotencyKey] {
				t.Fatalf("duplicate idempotency key %q for profile %s", effect.IdempotencyKey, id.String())
			}
			seen[effect.IdempotencyKey] = true
		}
	}
}

func TestDiffRejectsUnknownInputs(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDICANNPortal)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan, err := planFor(t, profile)
	if err != nil {
		t.Fatalf("PlanBinding: %s", err)
	}

	cases := []struct {
		name string
		obs  ObservationSet
	}{
		{
			name: "unknown route",
			obs: func() ObservationSet {
				obs := emptyObservations()
				obs.Route = RouteObservation{Route: ResolutionRoute(99), Backend: BackendEthereum}
				return obs
			}(),
		},
		{
			name: "unknown record kind",
			obs: func() ObservationSet {
				obs := emptyObservations()
				obs.Records = []RecordObservation{{Kind: RecordKind(99), Name: "x", Ownership: RecordOwnershipBindingContent}}
				return obs
			}(),
		},
		{
			name: "unknown record ownership",
			obs: func() ObservationSet {
				obs := emptyObservations()
				obs.Records = []RecordObservation{{Kind: RecordKindDNSLink, Name: "_dnslink", Value: "v", Ownership: RecordOwnership(99)}}
				return obs
			}(),
		},
		{
			name: "unknown zone DNSSEC state on a present zone",
			obs: func() ObservationSet {
				obs := emptyObservations()
				obs.Zone = &ZonePresenceObservation{Present: true, Allocation: ZoneAllocation(99)}
				return obs
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			effects, err := Diff(plan, tc.obs)
			if err == nil {
				t.Fatalf("Diff accepted unknown observations")
			}
			if len(effects) != 0 {
				t.Fatalf("Diff must produce no effects for unknown observations: %+v", effects)
			}
		})
	}

	t.Run("zero-value plan", func(t *testing.T) {
		effects, err := Diff(Plan{}, emptyObservations())
		if err == nil {
			t.Fatalf("Diff accepted a zero-value plan")
		}
		if len(effects) != 0 {
			t.Fatalf("Diff must produce no effects for an invalid plan: %+v", effects)
		}
	})
}

// TestDiffUnknownFactsProduceNoEffects ties the "no unknown or unresolved
// input produces effects" invariant to PlanBinding: when PlanBinding rejects
// the pair there is no plan, and without a plan Diff cannot run at all.
func TestDiffUnknownFactsProduceNoEffects(t *testing.T) {
	for _, id := range currentProfileIDs {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		facts := factsForProfile(t, profile)
		facts.Lifecycle = LifecycleUnknown
		_, err := PlanBinding(profile, facts)
		if err == nil {
			t.Fatalf("PlanBinding(%s) accepted unknown lifecycle", id.String())
		}
		facts.Lifecycle = LifecycleProvisioning
		facts.DiscoveredRoute = ResolutionRouteUnknown
		if _, err := PlanBinding(profile, facts); err == nil {
			t.Fatalf("PlanBinding(%s) accepted an unresolved route", id.String())
		}
	}
}
