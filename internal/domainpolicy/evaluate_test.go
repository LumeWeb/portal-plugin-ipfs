package domainpolicy

import (
	"reflect"
	"testing"
)

// goodObservations returns an observation set in which every observation a
// current plan could need is present and passing.
func goodObservations() ObservationSet {
	return ObservationSet{
		DNSLink:       &DNSLinkObservation{Value: "/ipfs/bafkqtestcid"},
		ChallengeTXT:  &TXTObservation{Value: ChallengeRecordLabel + "=fixture-not-a-real-token"},
		NS:            &NSObservation{Found: true, Nameservers: []string{"ns1.portal.example"}},
		DS:            &DSObservation{Found: true, Value: "ds-data"},
		TLSA:          &TLSAObservation{Found: true, Value: TLSAValuePlaceholder},
		PlatformTrust: &PlatformTrustObservation{Trusted: true},
		ZoneDNSSEC:    &ZoneDNSSECObservation{State: ZoneDNSSECStateEnabled},
		SOAMNAME:      &SOAMNAMEObservation{Found: true, Current: "mname", MatchesPortalMNAME: true},
		Zone:          &ZonePresenceObservation{Present: true, Allocation: ZoneAllocationDedicated},
	}
}

func factsGoodObservations(t *testing.T, profile Profile) (Plan, ObservationSet) {
	t.Helper()
	plan, err := planFor(t, profile)
	if err != nil {
		t.Fatalf("PlanBinding: %s", err)
	}
	obs := goodObservations()
	if profile.ZoneAllocation == ZoneAllocationSharedParent {
		obs.Zone = &ZonePresenceObservation{Present: true, Allocation: ZoneAllocationSharedParent}
	}
	return plan, obs
}

func planFor(t *testing.T, profile Profile) (Plan, error) {
	t.Helper()
	return PlanBinding(profile, factsForProfile(t, profile))
}

// TestEvaluateCurrentWebsiteFlowPerProfile asserts the current website
// ValidateDNS gate matrix per profile: which gates run, in which order, and
// whether they are live or trivially passing — encoded from the code paths in
// WebsiteServiceDefault.ValidateDNS.
func TestEvaluateCurrentWebsiteFlowPerProfile(t *testing.T) {
	cases := []struct {
		id      ProfileID
		results []GateResult
	}{
		{
			// ValidateDNS: DNSLink always; challenge TXT for ICANN; the
			// delegation check trivially passes for ICANN (VerifyDelegation
			// returns true without an NS lookup).
			id: ProfileIDICANNPortal,
			results: []GateResult{
				{Kind: GateDNSLink, OK: true, Expected: "/ipfs/bafkqtestcid", Found: "/ipfs/bafkqtestcid"},
				{Kind: GateChallengeTXT, OK: true, Found: ChallengeRecordLabel + "=fixture-not-a-real-token"},
				{Kind: GateNSDelegation, OK: true, Found: "not checked (trivial pass today)"},
			},
		},
		{
			// Owner-hosted ICANN: DNSLink and challenge TXT; no delegation.
			id: ProfileIDICANNOwner,
			results: []GateResult{
				{Kind: GateDNSLink, OK: true, Expected: "/ipfs/bafkqtestcid", Found: "/ipfs/bafkqtestcid"},
				{Kind: GateChallengeTXT, OK: true, Found: ChallengeRecordLabel + "=fixture-not-a-real-token"},
			},
		},
		{
			// Native managed HNS: DNSLink, live NS, live DS; no TXT.
			id: ProfileIDHNSPortalNative,
			results: []GateResult{
				{Kind: GateDNSLink, OK: true, Expected: "/ipfs/bafkqtestcid", Found: "/ipfs/bafkqtestcid"},
				{Kind: GateNSDelegation, OK: true, Found: "ns1.portal.example"},
				{Kind: GateDSDelegation, OK: true, Found: "ds-data"},
			},
		},
		{
			id: ProfileIDHNSPortalNamebaseChild,
			results: []GateResult{
				{Kind: GateDNSLink, OK: true, Expected: "/ipfs/bafkqtestcid", Found: "/ipfs/bafkqtestcid"},
				{Kind: GateNSDelegation, OK: true, Found: "ns1.portal.example"},
				{Kind: GateDSDelegation, OK: true, Found: "ds-data"},
			},
		},
		{
			// Self-hosted HNS (current gap): DNSLink only; TXT and delegation
			// are skipped and no DS/TLSA enforcement exists.
			id: ProfileIDHNSOwnerNative,
			results: []GateResult{
				{Kind: GateDNSLink, OK: true, Expected: "/ipfs/bafkqtestcid", Found: "/ipfs/bafkqtestcid"},
			},
		},
		{
			// HIP-5 Ethereum: DNSLink plus live on-chain TLSA; no TXT, no
			// delegation, no DNSSEC.
			id: ProfileIDHNSChainEthereum,
			results: []GateResult{
				{Kind: GateDNSLink, OK: true, Expected: "/ipfs/bafkqtestcid", Found: "/ipfs/bafkqtestcid"},
				{Kind: GateTLSA, OK: true, Found: TLSAValuePlaceholder},
			},
		},
		{
			// Platform subdomain: DNSLink plus platform trust (via the
			// delegation slot); no user TXT.
			id: ProfileIDPlatformICANN,
			results: []GateResult{
				{Kind: GateDNSLink, OK: true, Expected: "/ipfs/bafkqtestcid", Found: "/ipfs/bafkqtestcid"},
				{Kind: GatePlatformTrust, OK: true, Found: "trusted"},
			},
		},
		{
			id: ProfileIDPlatformHNSNative,
			results: []GateResult{
				{Kind: GateDNSLink, OK: true, Expected: "/ipfs/bafkqtestcid", Found: "/ipfs/bafkqtestcid"},
				{Kind: GatePlatformTrust, OK: true, Found: "trusted"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.id.String(), func(t *testing.T) {
			profile, ok := DefaultRegistry().Lookup(tc.id)
			if !ok {
				t.Fatalf("profile %q is not registered", tc.id.String())
			}
			plan, obs := factsGoodObservations(t, profile)
			evaluation, err := Evaluate(plan, FlowWebsiteValidation, obs)
			if err != nil {
				t.Fatalf("Evaluate: %s", err)
			}
			if !evaluation.Passed {
				t.Fatalf("website flow with complete passing observations must pass: %+v", evaluation.Results)
			}
			if evaluation.Flow != FlowWebsiteValidation {
				t.Fatalf("evaluation flow = %s", evaluation.Flow.String())
			}
			for i, want := range tc.results {
				if i >= len(evaluation.Results) {
					t.Fatalf("missing evaluation result %d: got %+v", i, evaluation.Results)
				}
				got := evaluation.Results[i]
				if got.Kind != want.Kind || got.OK != want.OK {
					t.Fatalf("result %d = %+v, want kind %s OK=%v", i, got, want.Kind.String(), want.OK)
				}
				if want.Expected != "" && got.Expected != want.Expected {
					t.Fatalf("result %d expected = %q, want %q", i, got.Expected, want.Expected)
				}
			}
			if len(evaluation.Results) != len(tc.results) {
				t.Fatalf("result count = %d, want %d: %+v", len(evaluation.Results), len(tc.results), evaluation.Results)
			}
		})
	}
}

// TestEvaluateMissingObservationsFailClosed asserts that any observation a
// live gate needs, when absent, fails that gate with Found "unobserved".
func TestEvaluateMissingObservationsFailClosed(t *testing.T) {
	absent := func(obs ObservationSet, drop func(*ObservationSet)) ObservationSet {
		drop(&obs)
		return obs
	}
	cases := []struct {
		id   ProfileID
		flow Flow
		gate GateKind
		drop func(*ObservationSet)
	}{
		{ProfileIDICANNPortal, FlowWebsiteValidation, GateDNSLink, func(o *ObservationSet) { o.DNSLink = nil }},
		{ProfileIDICANNPortal, FlowWebsiteValidation, GateChallengeTXT, func(o *ObservationSet) { o.ChallengeTXT = nil }},
		{ProfileIDICANNOwner, FlowWebsiteValidation, GateChallengeTXT, func(o *ObservationSet) { o.ChallengeTXT = nil }},
		{ProfileIDHNSPortalNative, FlowWebsiteValidation, GateNSDelegation, func(o *ObservationSet) { o.NS = nil }},
		{ProfileIDHNSPortalNative, FlowWebsiteValidation, GateDSDelegation, func(o *ObservationSet) { o.DS = nil }},
		{ProfileIDHNSPortalNative, FlowDomainVerification, GateDSDelegation, func(o *ObservationSet) { o.DS = nil }},
		{ProfileIDHNSPortalNative, FlowDomainVerification, GateNSDelegation, func(o *ObservationSet) { o.NS = nil }},
		{ProfileIDHNSChainEthereum, FlowWebsiteValidation, GateTLSA, func(o *ObservationSet) { o.TLSA = nil }},
		{ProfileIDPlatformHNSNative, FlowWebsiteValidation, GatePlatformTrust, func(o *ObservationSet) { o.PlatformTrust = nil }},
		{ProfileIDPlatformHNSNative, FlowDomainVerification, GatePlatformTrust, func(o *ObservationSet) { o.PlatformTrust = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.id.String()+"/"+tc.gate.String(), func(t *testing.T) {
			profile, ok := DefaultRegistry().Lookup(tc.id)
			if !ok {
				t.Fatalf("profile %q is not registered", tc.id.String())
			}
			plan, _ := factsGoodObservations(t, profile)
			obs := absent(goodObservations(), tc.drop)
			evaluation, err := Evaluate(plan, tc.flow, obs)
			if err != nil {
				t.Fatalf("Evaluate: %s", err)
			}
			found := false
			for _, result := range evaluation.Results {
				if result.Kind == tc.gate {
					found = true
					if result.OK {
						t.Fatalf("gate %s must fail without its observation", tc.gate.String())
					}
					if result.Found != "unobserved" {
						t.Fatalf("gate %s found = %q, want %q", tc.gate.String(), result.Found, "unobserved")
					}
				}
			}
			if !found {
				t.Fatalf("gate %s not evaluated in flow %s", tc.gate.String(), tc.flow.String())
			}
			if evaluation.Passed {
				t.Fatalf("flow must not pass when gate %s lacks its observation", tc.gate.String())
			}
		})
	}
}

// TestEvaluateNeverMixesFlows asserts that a plan's evaluators keep the two
// flows separate: the website flow never emits a domain gate and vice versa.
// The domain VerifyDomain flow emits no DNSLink, TXT, or TLSA gate; the
// website ValidateDNS flow emits no DNSSEC (zone-signing) gate.
func TestEvaluateNeverMixesFlows(t *testing.T) {
	for _, id := range currentProfileIDs {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		plan, obs := factsGoodObservations(t, profile)
		website, err := Evaluate(plan, FlowWebsiteValidation, obs)
		if err != nil {
			t.Fatalf("Evaluate(%s): %s", id.String(), err)
		}
		domain, err := Evaluate(plan, FlowDomainVerification, obs)
		if err != nil {
			t.Fatalf("Evaluate(%s): %s", id.String(), err)
		}
		for _, result := range website.Results {
			if result.Flow != FlowWebsiteValidation {
				t.Fatalf("website evaluation leaked a %s gate", result.Flow.String())
			}
			switch result.Kind {
			case GateDSDelegation:
				if !isDelegationDSGateAllowed(id, result) {
					t.Fatalf("website flow for %s must not emit a DNSSEC chain gate it does not perform today", id.String())
				}
			case GatePlatformTrust:
				// Platform trust appears in the website flow only for
				// platform profiles.
				if profile.Authority != AuthorityLocusOperatorZone {
					t.Fatalf("platform trust gate in website flow for non-platform profile %s", id.String())
				}
			}
		}
		for _, result := range domain.Results {
			if result.Flow != FlowDomainVerification {
				t.Fatalf("domain evaluation leaked a %s gate", result.Flow.String())
			}
			switch result.Kind {
			case GateDNSLink, GateChallengeTXT, GateTLSA:
				t.Fatalf("domain VerifyDomain flow emitted gate %s; it emits no DNSLink, TXT, or TLSA gate", result.Kind.String())
			}
		}
		// The website flow emits no zone-signing (DNSSEC) gate: the current
		// ValidateDNS runs DNSLink, TXT, delegation, TLSA — never a DNSSEC
		// self-heal gate (that lives in the VerifyDomain delegation check).
		for _, result := range website.Results {
			if result.Kind == GateDSDelegation && !isDelegationDSGateAllowed(id, result) {
				t.Fatalf("website flow for %s must not emit a DNSSEC gate", id.String())
			}
		}
	}
}

// isDelegationDSGateAllowed reports whether an HNS managed profile's website
// flow DS gate is the delegation-carrying check (checkDelegation ->
// VerifyDomain -> live DS), which the current code does perform — it is a
// delegation chain check, not a zone-DNSSEC self-heal gate.
func isDelegationDSGateAllowed(id ProfileID, result GateResult) bool {
	return (id == ProfileIDHNSPortalNative || id == ProfileIDHNSPortalNamebaseChild) &&
		result.Flow == FlowWebsiteValidation
}

// TestEvaluateFailingValuesReportsExpectedAndFound asserts per-gate
// expected/found diagnostics on failures.
func TestEvaluateFailingValuesReportsExpectedAndFound(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDICANNPortal)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan, _ := factsGoodObservations(t, profile)
	obs := goodObservations()
	obs.DNSLink = &DNSLinkObservation{Value: "/ipfs/stalecid"}
	evaluation, err := Evaluate(plan, FlowWebsiteValidation, obs)
	if err != nil {
		t.Fatalf("Evaluate: %s", err)
	}
	for _, result := range evaluation.Results {
		if result.Kind == GateDNSLink {
			if result.OK {
				t.Fatalf("stale DNSLink must fail the gate")
			}
			if result.Expected != "/ipfs/bafkqtestcid" || result.Found != "/ipfs/stalecid" {
				t.Fatalf("expected/found = %q/%q", result.Expected, result.Found)
			}
			return
		}
	}
	t.Fatalf("DNSLink gate not evaluated")
}

// TestEvaluateRejectsUnknownInputs asserts fail-closed behavior on unknown
// flows and observation sets.
func TestEvaluateRejectsUnknownInputs(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDICANNPortal)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	plan, _ := factsGoodObservations(t, profile)

	if _, err := Evaluate(plan, FlowUnknown, goodObservations()); err == nil {
		t.Fatalf("unknown flow accepted")
	}

	badSet := goodObservations()
	badSet.Records = []RecordObservation{{Kind: RecordKind(99), Name: "x", Ownership: RecordOwnershipBindingContent}}
	if _, err := Evaluate(plan, FlowWebsiteValidation, badSet); err == nil {
		t.Fatalf("observation set with unknown record kind accepted")
	}

	badRoute := goodObservations()
	badRoute.Route = RouteObservation{Route: ResolutionRoute(99)}
	if _, err := Evaluate(plan, FlowWebsiteValidation, badRoute); err == nil {
		t.Fatalf("observation set with unknown route accepted")
	}

	if _, err := Evaluate(Plan{}, FlowWebsiteValidation, goodObservations()); err == nil {
		t.Fatalf("zero-value plan accepted")
	}
}

// TestEvaluateIsDeterministicAndPure asserts that repeated evaluation is
// identical and that the observation set is never mutated.
func TestEvaluateIsDeterministicAndPure(t *testing.T) {
	for _, id := range currentProfileIDs {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		plan, _ := factsGoodObservations(t, profile)
		for _, flow := range []Flow{FlowWebsiteValidation, FlowDomainVerification} {
			obs := goodObservations()
			before := snapshotObservations(obs)
			first, err := Evaluate(plan, flow, obs)
			if err != nil {
				t.Fatalf("Evaluate(%s/%s): %s", id.String(), flow.String(), err)
			}
			second, err := Evaluate(plan, flow, obs)
			if err != nil {
				t.Fatalf("Evaluate(%s/%s): %s", id.String(), flow.String(), err)
			}
			if !reflect.DeepEqual(first, second) {
				t.Fatalf("Evaluate(%s/%s) is not deterministic", id.String(), flow.String())
			}
			after := snapshotObservations(obs)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("Evaluate(%s/%s) mutated the observation set", id.String(), flow.String())
			}
		}
	}
}

func snapshotObservations(obs ObservationSet) map[string]string {
	snapshot := make(map[string]string)
	if obs.DNSLink != nil {
		snapshot["dnslink"] = obs.DNSLink.Value
	}
	if obs.ChallengeTXT != nil {
		snapshot["challenge"] = obs.ChallengeTXT.Value
	}
	if obs.NS != nil {
		snapshot["ns"] = joinNameservers(obs.NS.Nameservers)
	}
	if obs.DS != nil {
		snapshot["ds"] = obs.DS.Value
	}
	if obs.TLSA != nil {
		snapshot["tlsa"] = obs.TLSA.Value
	}
	if obs.PlatformTrust != nil {
		snapshot["platform"] = boolText(obs.PlatformTrust.Trusted)
	}
	if obs.ZoneDNSSEC != nil {
		snapshot["dnssec"] = obs.ZoneDNSSEC.State.String()
	}
	if obs.SOAMNAME != nil {
		snapshot["soa"] = obs.SOAMNAME.Current
	}
	if obs.Zone != nil {
		snapshot["zone"] = boolText(obs.Zone.Present)
	}
	return snapshot
}
