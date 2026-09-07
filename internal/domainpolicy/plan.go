package domainpolicy

// ZoneIntent is the plan's zone requirement: which authority holds the zone
// and how it is allocated. A plan for owner or chain authority has a None
// zone intent; portal and operator authority require a non-None intent.
type ZoneIntent struct {
	// Authority is the authority locus the zone belongs to.
	Authority AuthorityLocus
	// Allocation is the required zone allocation.
	Allocation ZoneAllocation
}

// PlannedRecord is a record intent from the profile materialized for one
// binding: the concrete DNS record the plan declares plus the publication
// locus the portal may write it to.
type PlannedRecord struct {
	// Intent is the concrete record intent.
	Intent RecordIntent
	// Destination is where the record is published.
	Destination PublicationLocus
}

// Gate is one evaluated gate in a plan: what evidence is checked, in which
// flow, what it expects, and whether today's code passes it without a live
// check. A plan carries the gates of both flows; Evaluate selects by flow.
type Gate struct {
	// Kind is the gate's kind.
	Kind GateKind
	// Flow is the validation flow the gate belongs to.
	Flow Flow
	// Expected is the stable human-readable expectation for diagnostics.
	Expected string
	// TriviallyPasses reports that today's code returns success without a
	// live observation for this gate. It is characterization data; Evaluate
	// reports such gates as passing with a "not checked" finding.
	TriviallyPasses bool
}

// Plan is the full pure intent for one binding under one profile version:
// authority, zone requirement, record intents, per-flow gates, repair
// intents, and the security plans. PlanBinding produces it deterministically
// from a profile and facts; Evaluate and Diff consume it without I/O and
// without mutation.
type Plan struct {
	// ProfileID names the profile that produced the plan.
	ProfileID ProfileID
	// ProfileVersion is the profile version the plan was built from.
	ProfileVersion ProfileVersion
	// Name is the binding the plan was built for.
	Name string
	// Target is the desired content target.
	Target ContentTarget
	// Authority is the binding's authority locus.
	Authority AuthorityLocus
	// Route is the resolution route the plan was built for.
	Route ResolutionRoute
	// Backend is the resolution backend the plan was built for.
	Backend BackendID
	// Zone is the zone requirement.
	Zone ZoneIntent
	// Records are the materialized record intents.
	Records []PlannedRecord
	// Gates are the per-flow gates, in per-flow order.
	Gates []Gate
	// Repairs are the repairs the plan may produce effects for.
	Repairs []RepairIntent
	// DNSSEC is the DNSSEC duty.
	DNSSEC SecurityPlan
	// DANE is the DANE security plan.
	DANE SecurityPlan
}

// HasGate reports whether the plan requires the given gate in the given
// validation flow. This is the single read accessor the service adapters use
// to select which validation checks run: a check runs exactly when the plan
// carries its gate in the flow the check belongs to, never because a
// namespace string or status comparison happened at the call site.
func (p Plan) HasGate(flow Flow, kind GateKind) bool {
	for _, g := range p.Gates {
		if g.Flow == flow && g.Kind == kind {
			return true
		}
	}
	return false
}

// HasWebsiteGate reports whether the plan requires the given gate in the
// website-validation flow (WebsiteService ValidateDNS ordering).
func (p Plan) HasWebsiteGate(kind GateKind) bool {
	return p.HasGate(FlowWebsiteValidation, kind)
}

// HasDomainGate reports whether the plan requires the given gate in the
// domain-verification flow (DelegatedDomainService VerifyDomain ordering).
func (p Plan) HasDomainGate(kind GateKind) bool {
	return p.HasGate(FlowDomainVerification, kind)
}

// PlanBinding validates that the profile and facts describe the same binding
// class and materializes the pure plan. It rejects:
//
//   - unknown or inconsistent profiles and facts (empty name, unknown enums,
//     invalid targets, a target kind the profile does not allow);
//   - mismatched pairs: a profile-version mismatch, a route/backend the
//     profile does not encode, platform roots on non-platform profiles or
//     their absence on platform profiles, a hosting request that contradicts
//     the authority, and a chain authority meeting a present zone;
//   - a portal/operator profile whose zone allocation does not match the
//     facts, i.e. portal authority without a zone intent;
//   - unresolved facts that would authorize writes (for example an unknown
//     lifecycle, or portal record publication without a resolved route).
//
// PlanBinding performs no I/O and mutates nothing.
func PlanBinding(profile Profile, facts BindingFacts) (Plan, error) {
	if err := validateProfileUsable(profile); err != nil {
		return Plan{}, err
	}
	if err := validateFactsForProfile(profile, facts); err != nil {
		return Plan{}, err
	}

	zoneAllocation := profile.ZoneAllocation
	if profile.Authority == AuthorityLocusPortalZone || profile.Authority == AuthorityLocusOperatorZone {
		// Portal/operator plans carry the OBSERVED topology so the effect
		// diff can distinguish adopting a shared parent zone from
		// provisioning a dedicated one; PlanBinding has already validated the
		// observed allocation against the profile's permitted set.
		zoneAllocation = facts.ZoneAllocation
	}
	plan := Plan{
		ProfileID:      profile.ID,
		ProfileVersion: profile.Version,
		Name:           facts.Name,
		Target:         facts.Target,
		Authority:      profile.Authority,
		Route:          profile.Route,
		Backend:        profile.Backend,
		Zone: ZoneIntent{
			Authority:  profile.Authority,
			Allocation: zoneAllocation,
		},
		Records: materializeRecords(profile, facts),
		Gates:   materializeGates(profile, facts),
		Repairs: append([]RepairIntent(nil), profile.Repairs...),
		DNSSEC:  profile.DNSSEC,
		DANE:    profile.DANE,
	}
	return plan, nil
}

// validateProfileUsable re-checks the identifying fields of a profile so a
// zero-value or partially populated Profile fails closed even before the
// full profile invariants are consulted (profiles built by NewProfile always
// pass).
func validateProfileUsable(profile Profile) error {
	if _, err := NewProfileID(profile.ID); err != nil {
		return err
	}
	if _, err := NewProfileVersion(int(profile.Version)); err != nil {
		return err
	}
	if _, err := NewBackendID(profile.Backend); err != nil {
		return err
	}
	if _, err := NewNamingSystem(profile.NamingSystem); err != nil {
		return err
	}
	if _, err := NewResolutionRoute(profile.Route); err != nil {
		return err
	}
	if _, err := NewAuthorityLocus(profile.Authority); err != nil {
		return err
	}
	if _, err := NewZoneAllocation(profile.ZoneAllocation); err != nil {
		return err
	}
	if profile.Ownership.IsEmpty() {
		return newInvalid("plan", "profile %q has an empty ownership expression", profile.ID.String())
	}
	return nil
}

// validateFactsForProfile rejects every profile/facts mismatch that would let
// a plan authorize writes the binding state does not support.
func validateFactsForProfile(profile Profile, facts BindingFacts) error {
	if facts.Name == "" {
		return newInvalid("plan", "empty binding name")
	}
	if !facts.Lifecycle.Valid() {
		return newInvalid("plan", "unknown lifecycle %d for %q", int(facts.Lifecycle), facts.Name)
	}
	if _, err := NewHostingRequest(facts.RequestedHosting); err != nil {
		return err
	}
	if !facts.PolicyVersion.Valid() {
		return newInvalid("plan", "unknown policy version %d for %q", int(facts.PolicyVersion), facts.Name)
	}
	if facts.PolicyVersion != profile.Version {
		return newInvalid("plan", "profile version mismatch: profile %q is v%s, facts select v%s", profile.ID.String(), profile.Version.String(), facts.PolicyVersion.String())
	}
	if !facts.Target.Valid() {
		return newInvalid("plan", "invalid content target for %q", facts.Name)
	}
	if !profile.AllowsTarget(facts.Target.Kind) {
		return newInvalid("plan", "profile %q does not allow target kind %s", profile.ID.String(), facts.Target.Kind.String())
	}
	if _, err := NewResolutionRoute(facts.DiscoveredRoute); err != nil {
		return err
	}
	if _, err := NewBackendID(facts.DiscoveredBackend); err != nil {
		return err
	}
	if facts.DiscoveredRoute != profile.Route {
		return newInvalid("plan", "route mismatch: profile %q encodes route %s, facts report %s", profile.ID.String(), profile.Route.String(), facts.DiscoveredRoute.String())
	}
	if facts.DiscoveredBackend != profile.Backend {
		return newInvalid("plan", "backend mismatch: profile %q encodes backend %q, facts report %q", profile.ID.String(), profile.Backend.String(), facts.DiscoveredBackend.String())
	}

	platformProfile := profile.Authority == AuthorityLocusOperatorZone
	if platformProfile && facts.PlatformRootID == nil {
		return newInvalid("plan", "profile %q requires platform ownership (PlatformRootID) for %q", profile.ID.String(), facts.Name)
	}
	if !platformProfile && facts.PlatformRootID != nil {
		return newInvalid("plan", "non-platform profile %q cannot hold platform ownership for %q", profile.ID.String(), facts.Name)
	}

	switch profile.Authority {
	case AuthorityLocusPortalZone, AuthorityLocusOperatorZone:
		if facts.RequestedHosting != HostingRequestPortal {
			return newInvalid("plan", "authority %s requires a portal hosting request; facts request %s", profile.Authority.String(), facts.RequestedHosting.String())
		}
		if !profile.AllowsZoneAllocation(facts.ZoneAllocation) {
			return newInvalid("plan", "zone allocation mismatch: profile %q permits %s%s, facts report %s", profile.ID.String(), profile.ZoneAllocation.String(), allowedPhrase(profile.ExtraZoneAllocations), facts.ZoneAllocation.String())
		}
		// A shared-parent zone intent can only ADOPT the parent's zone, never
		// provision a new one: sharing a zone that does not exist is a
		// fail-closed incoherence (a fresh subdomain whose parent zone is
		// absent gets its own dedicated zone instead).
		if facts.ZoneAllocation == ZoneAllocationSharedParent && !facts.ZonePresent {
			return newInvalid("plan", "shared-parent allocation for %q requires the parent zone to be present", facts.Name)
		}
	case AuthorityLocusOwnerDNS:
		if facts.RequestedHosting != HostingRequestOwner {
			return newInvalid("plan", "owner-dns authority requires an owner hosting request; facts request %s", facts.RequestedHosting.String())
		}
		if facts.ZonePresent {
			return newInvalid("plan", "owner-dns authority for %q cannot hold a portal zone", facts.Name)
		}
	case AuthorityLocusChain:
		// Chain authority serves the binding's DNS itself; a portal zone
		// under chain authority is data incoherence and never authorizes
		// portal DNS work (an on-chain binding carrying a stray zone ID).
		if facts.ZonePresent {
			return newInvalid("plan", "chain authority for %q cannot hold a portal zone", facts.Name)
		}
		if facts.RequestedHosting != HostingRequestOwner {
			return newInvalid("plan", "chain authority requires an owner hosting request; facts request %s", facts.RequestedHosting.String())
		}
	}

	// A portal or operator plan authorizes DNS writes; unresolved zone state
	// (present with a none/unknown allocation) must never reach that point.
	if profile.Authority == AuthorityLocusPortalZone || profile.Authority == AuthorityLocusOperatorZone {
		if !facts.ZoneAllocation.Valid() {
			return newInvalid("plan", "authority %s requires a valid zone allocation for %q; got unknown", authorityPhrase(profile.Authority), facts.Name)
		}
		if facts.ZonePresent && facts.ZoneAllocation == ZoneAllocationNone {
			return newInvalid("plan", "zone present with no allocation for %s authority on %q", authorityPhrase(profile.Authority), facts.Name)
		}
	}
	return nil
}

// authorityPhrase returns a stable phrase for diagnostics.
func authorityPhrase(a AuthorityLocus) string {
	if a.Valid() {
		return a.String()
	}
	return "unknown"
}

// allowedPhrase renders the extra permitted zone allocations for diagnostics.
func allowedPhrase(extras []ZoneAllocation) string {
	if len(extras) == 0 {
		return ""
	}
	out := ""
	for _, extra := range extras {
		out += " + " + extra.String()
	}
	return out
}

// materializeRecords converts profile record specs into concrete record
// intents for the binding.
func materializeRecords(profile Profile, facts BindingFacts) []PlannedRecord {
	records := make([]PlannedRecord, 0, len(profile.Records))
	for _, spec := range profile.Records {
		value := spec.FixedValue
		if spec.ValueFromTarget {
			value = facts.Target.DNSLinkPath()
		}
		intent, err := NewRecordIntent(spec.Kind, spec.Name, value, spec.TTL, spec.Ownership)
		if err != nil {
			// Unreachable for profiles built by NewProfile, which validates
			// every record spec; fail closed regardless, but never panic.
			continue
		}
		records = append(records, PlannedRecord{Intent: intent, Destination: spec.Destination})
	}
	return records
}

// materializeGates converts the profile's per-flow gate sequences into plan
// gates with stable expectations, preserving each flow's ordering.
func materializeGates(profile Profile, facts BindingFacts) []Gate {
	gates := make([]Gate, 0, len(profile.WebsiteGates)+len(profile.DomainGates))
	add := func(spec GateSpec, flow Flow) {
		gates = append(gates, Gate{
			Kind:            spec.Kind,
			Flow:            flow,
			Expected:        gateExpectation(spec.Kind, facts),
			TriviallyPasses: spec.TriviallyPasses,
		})
	}
	for _, spec := range profile.WebsiteGates {
		add(spec, FlowWebsiteValidation)
	}
	for _, spec := range profile.DomainGates {
		add(spec, FlowDomainVerification)
	}
	return gates
}

// gateExpectation returns the stable expected-value text for a gate. The
// challenge token itself is dynamic (it rotates); its expectation is the
// presence of the challenge TXT record, not a constant value.
func gateExpectation(kind GateKind, facts BindingFacts) string {
	switch kind {
	case GateDNSLink:
		return facts.Target.DNSLinkPath()
	case GateChallengeTXT:
		return "challenge TXT record present at " + ChallengeRecordLabel + "." + facts.Name
	case GateNSDelegation:
		return "NS delegation visible for " + facts.Name
	case GateDSDelegation:
		return "DS record present for " + facts.Name
	case GateTLSA:
		return "TLSA record present at " + TLSARecordName + "." + facts.Name
	case GatePlatformTrust:
		return "platform trust relationship holds for " + facts.Name
	case GatePartnerAttestation:
		return "partner attestation holds for " + facts.Name
	default:
		return ""
	}
}
