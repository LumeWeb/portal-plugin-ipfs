package domainpolicy

import (
	"reflect"
	"testing"
)

// countGate reports how many gates of a kind a plan has in a flow.
func countGate(plan Plan, flow Flow, kind GateKind) int {
	count := 0
	for _, gate := range plan.Gates {
		if gate.Flow == flow && gate.Kind == kind {
			count++
		}
	}
	return count
}

// TestInvariantExactlyOneDNSLinkIntentAndGatePerWebsiteValidationProfile
// encodes the universal invariant: every Pinner-served website profile gates
// on exactly one DNSLink check and carries exactly one DNSLink record intent.
func TestInvariantExactlyOneDNSLinkIntentAndGatePerWebsiteValidationProfile(t *testing.T) {
	for _, id := range currentProfileIDs {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		plan, err := planFor(t, profile)
		if err != nil {
			t.Fatalf("PlanBinding(%s): %s", id.String(), err)
		}
		if got := countGate(plan, FlowWebsiteValidation, GateDNSLink); got != 1 {
			t.Fatalf("profile %s has %d website DNSLink gates, want exactly 1", id.String(), got)
		}
		dnsLinkRecords := 0
		for _, record := range plan.Records {
			if record.Intent.Kind == RecordKindDNSLink {
				dnsLinkRecords++
			}
		}
		if dnsLinkRecords != 1 {
			t.Fatalf("profile %s has %d DNSLink record intents, want exactly 1", id.String(), dnsLinkRecords)
		}
	}
}

// TestInvariantManagedNativeHNSPublishesTLSAWithoutLiveTLSAGate asserts the
// current managed-zone DANE behavior: the profile declares portal TLSA
// publication into its signed zone (record intent) but has NO live TLSA
// activation gate in either flow — the live activation policy is an explicit
// product decision that is open today. This test characterizes current
// behavior; it does not assert that the gap is desirable.
func TestInvariantManagedNativeHNSPublishesTLSAWithoutLiveTLSAGate(t *testing.T) {
	for _, id := range []ProfileID{ProfileIDHNSPortalNative, ProfileIDHNSPortalNamebaseChild, ProfileIDPlatformHNSNative} {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		plan, err := planFor(t, profile)
		if err != nil {
			t.Fatalf("PlanBinding(%s): %s", id.String(), err)
		}
		hasTLSARecord := false
		for _, record := range plan.Records {
			if record.Intent.Kind == RecordKindTLSA && record.Destination == PublicationLocusPortalZone {
				hasTLSARecord = true
			}
		}
		if !hasTLSARecord {
			t.Fatalf("managed native HNS profile %s must publish TLSA into its managed zone", id.String())
		}
		if got := countGate(plan, FlowWebsiteValidation, GateTLSA) + countGate(plan, FlowDomainVerification, GateTLSA); got != 0 {
			t.Fatalf("managed native HNS profile %s must not activate a live TLSA gate today (open product decision); got %d", id.String(), got)
		}
		if profile.DANE.Requirement == RequirementNotApplicable {
			t.Fatalf("managed native HNS profile %s must declare a DANE plan (publication, no live verification)", id.String())
		}
	}
}

// TestCurrentSelfHostedHNSGapNoDSOrTLSAEnforcement names the current
// self-hosted HNS behavior as an explicit, documented gap: today DNSLink is
// the only effective validation gate (TXT and portal delegation are skipped
// via UsesDelegationForOwnership), DNSSEC is not enforced, and owner TLSA
// publication plus live TLSA validation are not enforced. The profile adds
// neither a DS nor a live TLSA gate. Closing this gap requires a separate
// approved policy change; corrected variants live only in test fixtures with
// separate IDs.
func TestCurrentSelfHostedHNSGapNoDSOrTLSAEnforcement(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDHNSOwnerNative)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan, err := planFor(t, profile)
	if err != nil {
		t.Fatalf("PlanBinding: %s", err)
	}
	if !reflect.DeepEqual(countGate(plan, FlowWebsiteValidation, GateDSDelegation), 0) {
		t.Fatalf("self-hosted HNS must not add a DS gate today (documented gap)")
	}
	if !reflect.DeepEqual(countGate(plan, FlowWebsiteValidation, GateTLSA), 0) {
		t.Fatalf("self-hosted HNS must not add a live TLSA gate today (documented gap)")
	}
	if !reflect.DeepEqual(countGate(plan, FlowDomainVerification, GateDSDelegation)+countGate(plan, FlowDomainVerification, GateTLSA), 0) {
		t.Fatalf("self-hosted HNS must not add DS or TLSA gates to VerifyDomain today (documented gap)")
	}
	if got := len(plan.Gates); got != 1 {
		t.Fatalf("self-hosted HNS currently gates on DNSLink alone; got %d gates", got)
	}
	if plan.DNSSEC.Requirement != RequirementNotApplicable {
		t.Fatalf("self-hosted HNS DNSSEC is not enforced today")
	}
	if plan.DANE.Requirement == RequirementNotApplicable {
		t.Fatalf("self-hosted HNS still bootstraps a DANE identity; the gap is live enforcement, not DANE as a concept")
	}
}

// TestICANNOwnershipUsesTxtGate asserts that the ICANN ownership proof
// remains the challenge TXT gate in both current ICANN profiles.
func TestICANNOwnershipUsesTxtGate(t *testing.T) {
	for _, id := range []ProfileID{ProfileIDICANNPortal, ProfileIDICANNOwner} {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		if !containsGateKind(profile.Ownership.Gates(), GateChallengeTXT) {
			t.Fatalf("profile %s ownership must keep the challenge TXT gate", id.String())
		}
	}
}

func containsGateKind(kinds []GateKind, kind GateKind) bool {
	for _, candidate := range kinds {
		if candidate == kind {
			return true
		}
	}
	return false
}

// TestChainWebsiteProfilesRequireTLSAAndProduceNoPowerDNSEffects asserts that
// every chain website profile requires a live TLSA gate for the website flow
// and that planning plus diffing one never yields a zone create/reuse, record
// write/delete, DNSSEC, SOA, challenge, or PowerDNS effect.
func TestChainWebsiteProfilesRequireTLSAAndProduceNoPowerDNSEffects(t *testing.T) {
	chainProfileIDs := []ProfileID{ProfileIDHNSChainEthereum}
	for _, id := range chainProfileIDs {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		if got := countGate(planMust(t, profile), FlowWebsiteValidation, GateTLSA); got != 1 {
			t.Fatalf("chain profile %s must require the TLSA website gate; got %d", id.String(), got)
		}
		facts := factsForProfile(t, profile)
		facts.ZonePresent = true
		if _, err := PlanBinding(profile, facts); err == nil {
			t.Fatalf("chain profile %s must reject portal zones", id.String())
		}
		// No record intent may target the portal zone, and diffing from a
		// fully-missing observation set must produce no effects.
		plan := planMust(t, profile)
		for _, record := range plan.Records {
			if record.Destination == PublicationLocusPortalZone {
				t.Fatalf("chain profile %s produced a portal-zone record intent %+v", id.String(), record)
			}
		}
		effects, err := Diff(plan, emptyObservations())
		if err != nil {
			t.Fatalf("Diff(%s): %s", id.String(), err)
		}
		if len(effects) != 0 {
			t.Fatalf("chain profile %s produced PowerDNS effects %+v", id.String(), effects)
		}
	}
}

func planMust(t *testing.T, profile Profile) Plan {
	t.Helper()
	plan, err := planFor(t, profile)
	if err != nil {
		t.Fatalf("PlanBinding(%s): %s", profile.ID.String(), err)
	}
	return plan
}

// TestCorrectedProfilesAreTestOnlyAndViewable asserts that intended corrected
// profiles can exist only as test fixtures with separate IDs: they are not in
// the default registry and cannot be selected by runtime compatibility code.
// It also documents the fail-closed boundary: the current profile grammar
// cannot even encode a corrected self-hosted HNS profile (live TLSA outside
// chain authority), so closing that gap requires extending the model, not
// flipping a flag.
func TestCorrectedProfilesAreTestOnlyAndViewable(t *testing.T) {
	t.Run("corrected self-hosted HNS is rejected by the current grammar", func(t *testing.T) {
		_, err := NewProfile(ProfileSpec{
			ID:             "test.hns.owner.corrected-v1",
			Version:        ProfileVersion1,
			NamingSystem:   NamingSystemHNS,
			Route:          ResolutionRouteHNSRoot,
			Backend:        BackendHNSRoot,
			Authority:      AuthorityLocusOwnerDNS,
			ZoneAllocation: ZoneAllocationNone,
			Ownership:      gateAllOf(GateDNSLink, GateTLSA),
			DNSSEC:         mustSecurity(t, RequirementRequired, ActorOwner, PublicationLocusOwnerDNS, VerificationModeResolveDNS),
			DANE:           mustSecurity(t, RequirementRequired, ActorOwner, PublicationLocusOwnerDNS, VerificationModeResolveDNS),
			AllowedTargets: []TargetKind{TargetKindIPFS, TargetKindIPNS},
			WebsiteGates:   []GateSpec{{Kind: GateDNSLink}, {Kind: GateTLSA}},
			DomainGates:    []GateSpec{},
		})
		if err == nil {
			t.Fatalf("the current profile grammar must reject live TLSA enforcement outside chain authority; extending it is an approved policy change, not an option")
		}
	})

	t.Run("encodable corrected fixtures stay out of the default registry", func(t *testing.T) {
		// A corrected ICANN portal profile would drop the challenge TXT gate
		// and require portal-signed DNSSEC.
		corrected, err := NewProfile(ProfileSpec{
			ID:             "test.icann.portal.corrected-v1",
			Version:        ProfileVersion1,
			NamingSystem:   NamingSystemICANN,
			Route:          ResolutionRouteStandardDNS,
			Backend:        BackendSystemDNS,
			Authority:      AuthorityLocusPortalZone,
			ZoneAllocation: ZoneAllocationDedicated,
			Ownership:      gateAllOf(GateDNSLink, GateNSDelegation),
			DNSSEC:         mustSecurity(t, RequirementRequired, ActorPortal, PublicationLocusPortalZone, VerificationModeResolveDNS),
			DANE:           mustNotApplicable(),
			AllowedTargets: []TargetKind{TargetKindIPFS, TargetKindIPNS},
			WebsiteGates:   []GateSpec{{Kind: GateDNSLink}, {Kind: GateNSDelegation}},
			DomainGates:    []GateSpec{{Kind: GateNSDelegation}},
		})
		if err != nil {
			t.Fatalf("construct corrected test fixture: %s", err)
		}
		if _, ok := DefaultRegistry().Lookup(corrected.ID); ok {
			t.Fatalf("test-only corrected profile %q must never be registered in the default registry", corrected.ID.String())
		}
		// The default registry must contain exactly the current-behavior IDs:
		// no fixture can slip into runtime profile selection.
		if got := DefaultRegistry().IDs(); !reflect.DeepEqual(got, currentProfileIDs) {
			t.Fatalf("default registry IDs = %v, want exactly the current-behavior IDs", got)
		}
	})
}

// TestEvaluateNeverProducesEffects asserts the structural invariant: Evaluate
// returns only evaluations (no effect descriptors exist in its output type)
// and never triggers Diff pathways. Diff is the only effect producer.
func TestEvaluateNeverProducesEffects(t *testing.T) {
	evalType := reflect.TypeOf(Evaluation{})
	if evalType.NumField() == 0 {
		t.Fatalf("Evaluation has no fields")
	}
	for i := 0; i < evalType.NumField(); i++ {
		field := evalType.Field(i)
		if effectDoes(field.Type) {
			t.Fatalf("Evaluation field %s carries effect-typed data; Evaluate must never produce effects", field.Name)
		}
	}
}

func effectDoes(t reflect.Type) bool {
	if t.Name() == "Effect" || t.Name() == "EffectKind" {
		return true
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Array, reflect.Ptr:
		return effectDoes(t.Elem())
	default:
		return false
	}
}

// TestDiffIsIdempotentAcrossReconciledStates asserts that repeating Diff with
// the same inputs returns the identical effect list (including order and
// idempotency keys), both before and after reconciliation would have applied.
func TestDiffIsIdempotentAcrossReconciledStates(t *testing.T) {
	for _, id := range currentProfileIDs {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		plan := planMust(t, profile)
		states := map[string]ObservationSet{
			"empty":      emptyObservations(),
			"reconciled": observationsWithZoneAndRecords(t, plan, profile.ZoneAllocation),
		}
		for name, obs := range states {
			first, err := Diff(plan, obs)
			if err != nil {
				t.Fatalf("Diff(%s/%s): %s", id.String(), name, err)
			}
			second, err := Diff(plan, obs)
			if err != nil {
				t.Fatalf("Diff(%s/%s): %s", id.String(), name, err)
			}
			if !reflect.DeepEqual(first, second) {
				t.Fatalf("Diff(%s/%s) repeated is not identical", id.String(), name)
			}
		}
	}
}

// TestDeletionEffectsNameTheirRecordOwner asserts that every delete-record
// effect carries the owner convenience: Record(Intent.Ownership) populated,
// so shared-zone records can never be deleted anonymously.
func TestDeletionEffectsNameTheirRecordOwner(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDHNSPortalNative)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan := planMust(t, profile)
	obs := observationsWithZoneAndRecords(t, plan, ZoneAllocationDedicated)
	// A stale delegation-owned record that no plan requires anymore.
	obs.Records = append(obs.Records, RecordObservation{
		Kind:      RecordKindDS,
		Name:      "@",
		Value:     "stale-ds",
		Ownership: RecordOwnershipDelegation,
	})
	effects, err := Diff(plan, obs)
	if err != nil {
		t.Fatalf("Diff: %s", err)
	}
	found := false
	for _, effect := range effects {
		if effect.Kind != EffectKindDeleteRecord {
			continue
		}
		found = true
		if effect.Record == nil {
			t.Fatalf("delete effect %+v carries no record", effect)
		}
		if !effect.Record.Intent.Ownership.Valid() {
			t.Fatalf("delete effect %+v does not name its record owner", effect)
		}
		if effect.Record.Intent.Ownership != RecordOwnershipDelegation {
			t.Fatalf("delete effect ownership = %s, want delegation", effect.Record.Intent.Ownership.String())
		}
	}
	if !found {
		t.Fatalf("stale delegation record produced no delete effect: %v", effectKinds(effects))
	}
}

// TestOperatorOwnedRecordsAreNeverDeleted asserts the shared-zone protection
// implied by the deletion ownership rules: zone-infrastructure and operator
// records are not binding-owned deletables.
func TestOperatorOwnedRecordsAreNeverDeleted(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDICANNPortal)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan := planMust(t, profile)
	for _, ownership := range []RecordOwnership{RecordOwnershipZoneInfrastructure, RecordOwnershipOperator} {
		obs := observationsWithZoneAndRecords(t, plan, ZoneAllocationDedicated)
		obs.Records = append(obs.Records, RecordObservation{
			Kind:      RecordKindNS,
			Name:      "@",
			Value:     "ns1.legacy.example",
			Ownership: ownership,
		})
		effects, err := Diff(plan, obs)
		if err != nil {
			t.Fatalf("Diff: %s", err)
		}
		for _, effect := range effects {
			if effect.Kind == EffectKindDeleteRecord {
				t.Fatalf("record owned by %s must never be deleted by binding diffing", ownership.String())
			}
		}
	}
}
