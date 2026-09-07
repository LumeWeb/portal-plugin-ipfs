package domainpolicy

import (
	"reflect"
	"testing"
)

// factsForProfile builds facts consistent with the given profile: the
// hosting request and zone presence follow the profile's authority, and the
// route/backend match what the profile encodes.
func factsForProfile(t *testing.T, profile Profile) BindingFacts {
	t.Helper()
	name := "example.com"
	if profile.NamingSystem == NamingSystemHNS {
		name = "example.hnsdata.com"
	}
	request := HostingRequestPortal
	if profile.Authority == AuthorityLocusOwnerDNS || profile.Authority == AuthorityLocusChain {
		request = HostingRequestOwner
	}
	var platformRoot *uint
	if profile.Authority == AuthorityLocusOperatorZone {
		root := uint(42)
		platformRoot = &root
	}
	target, err := NewContentTarget(TargetKindIPFS, "bafkqtestcid")
	if err != nil {
		t.Fatalf("NewContentTarget: %s", err)
	}
	zonePresent := profile.Authority == AuthorityLocusPortalZone || profile.Authority == AuthorityLocusOperatorZone
	return BindingFacts{
		Name:              name,
		Lifecycle:         LifecycleProvisioning,
		RequestedHosting:  request,
		PlatformRootID:    platformRoot,
		ZonePresent:       zonePresent,
		ZoneAllocation:    profile.ZoneAllocation,
		DiscoveredRoute:   profile.Route,
		DiscoveredBackend: profile.Backend,
		Target:            target,
		PolicyVersion:     profile.Version,
	}
}

// TestPlanBindingForEveryCurrentProfile asserts the pure plan shape for the
// current-behavior matrix: authority, zone requirement, record values, gate
// sequences per flow, and repairs.
func TestPlanBindingForEveryCurrentProfile(t *testing.T) {
	cases := []struct {
		id             ProfileID
		zoneIntent     ZoneIntent
		records        int
		websiteGateSeq []GateKind
		domainGateSeq  []GateKind
		trivialWebsite map[GateKind]bool
		trivialDomain  map[GateKind]bool
		repairs        []RepairKind
	}{
		{
			id:             ProfileIDICANNPortal,
			zoneIntent:     ZoneIntent{Authority: AuthorityLocusPortalZone, Allocation: ZoneAllocationDedicated},
			records:        3, // DNSLink, apex ALIAS, challenge TXT
			websiteGateSeq: []GateKind{GateDNSLink, GateChallengeTXT, GateNSDelegation},
			domainGateSeq:  []GateKind{GateNSDelegation},
			trivialWebsite: map[GateKind]bool{GateNSDelegation: true},
			trivialDomain:  map[GateKind]bool{GateNSDelegation: true},
			repairs:        []RepairKind{RepairKindRotateChallenge, RepairKindEnsureSOAMNAME},
		},
		{
			id:             ProfileIDICANNOwner,
			zoneIntent:     ZoneIntent{Authority: AuthorityLocusOwnerDNS, Allocation: ZoneAllocationNone},
			records:        2, // DNSLink and challenge TXT, both owner-published
			websiteGateSeq: []GateKind{GateDNSLink, GateChallengeTXT},
			domainGateSeq:  nil, // VerifyDomain short-circuits (NotApplicable)
			repairs:        []RepairKind{RepairKindRotateChallenge},
		},
		{
			id:             ProfileIDHNSPortalNative,
			zoneIntent:     ZoneIntent{Authority: AuthorityLocusPortalZone, Allocation: ZoneAllocationDedicated},
			records:        3, // DNSLink, A apex, TLSA
			websiteGateSeq: []GateKind{GateDNSLink, GateNSDelegation, GateDSDelegation},
			domainGateSeq:  []GateKind{GateDSDelegation, GateNSDelegation},
			repairs:        []RepairKind{RepairKindEnsureDNSSEC, RepairKindEnsureSOAMNAME},
		},
		{
			id:             ProfileIDHNSPortalNamebaseChild,
			zoneIntent:     ZoneIntent{Authority: AuthorityLocusPortalZone, Allocation: ZoneAllocationDedicated},
			records:        3,
			websiteGateSeq: []GateKind{GateDNSLink, GateNSDelegation, GateDSDelegation},
			domainGateSeq:  []GateKind{GateDSDelegation, GateNSDelegation},
			repairs:        []RepairKind{RepairKindEnsureDNSSEC, RepairKindEnsureSOAMNAME},
		},
		{
			id:             ProfileIDHNSOwnerNative,
			zoneIntent:     ZoneIntent{Authority: AuthorityLocusOwnerDNS, Allocation: ZoneAllocationNone},
			records:        2, // owner-published DNSLink and TLSA
			websiteGateSeq: []GateKind{GateDNSLink},
			domainGateSeq:  nil,
			repairs:        nil,
		},
		{
			id:             ProfileIDHNSChainEthereum,
			zoneIntent:     ZoneIntent{Authority: AuthorityLocusChain, Allocation: ZoneAllocationNone},
			records:        2, // chain DNSLink and TLSA
			websiteGateSeq: []GateKind{GateDNSLink, GateTLSA},
			domainGateSeq:  nil,
			repairs:        nil,
		},
		{
			id:             ProfileIDPlatformICANN,
			zoneIntent:     ZoneIntent{Authority: AuthorityLocusOperatorZone, Allocation: ZoneAllocationSharedParent},
			records:        2, // DNSLink, apex ALIAS (ICANN roots have no TLSA)
			websiteGateSeq: []GateKind{GateDNSLink, GatePlatformTrust},
			domainGateSeq:  []GateKind{GatePlatformTrust},
			repairs:        []RepairKind{RepairKindEnsureSOAMNAME},
		},
		{
			id:             ProfileIDPlatformHNSNative,
			zoneIntent:     ZoneIntent{Authority: AuthorityLocusOperatorZone, Allocation: ZoneAllocationSharedParent},
			records:        3, // DNSLink, A apex, TLSA from the HNS root policy
			websiteGateSeq: []GateKind{GateDNSLink, GatePlatformTrust},
			domainGateSeq:  []GateKind{GatePlatformTrust},
			repairs:        []RepairKind{RepairKindEnsureSOAMNAME, RepairKindEnsureDNSSEC},
		},
	}
	for _, tc := range cases {
		t.Run(tc.id.String(), func(t *testing.T) {
			profile, ok := DefaultRegistry().Lookup(tc.id)
			if !ok {
				t.Fatalf("profile %q is not registered", tc.id.String())
			}
			facts := factsForProfile(t, profile)
			plan, err := PlanBinding(profile, facts)
			if err != nil {
				t.Fatalf("PlanBinding: %s", err)
			}
			if plan.ProfileID != profile.ID || plan.ProfileVersion != profile.Version {
				t.Fatalf("plan identity = %q v%s, want %q v%s",
					plan.ProfileID.String(), plan.ProfileVersion.String(),
					profile.ID.String(), profile.Version.String())
			}
			if plan.Name != facts.Name {
				t.Fatalf("plan name = %q, want %q", plan.Name, facts.Name)
			}
			if !reflect.DeepEqual(plan.Zone, tc.zoneIntent) {
				t.Fatalf("zone intent = %+v, want %+v", plan.Zone, tc.zoneIntent)
			}
			if len(plan.Records) != tc.records {
				t.Fatalf("record intents = %d (%+v), want %d", len(plan.Records), plan.Records, tc.records)
			}
			gotWebsite := make([]GateKind, 0, len(tc.websiteGateSeq))
			for _, gate := range plan.Gates {
				if gate.Flow == FlowWebsiteValidation {
					gotWebsite = append(gotWebsite, gate.Kind)
					if gate.TriviallyPasses != tc.trivialWebsite[gate.Kind] {
						t.Fatalf("gate %s trivially-passes = %v, want %v", gate.Kind.String(), gate.TriviallyPasses, tc.trivialWebsite[gate.Kind])
					}
				}
			}
			if !reflect.DeepEqual(gotWebsite, tc.websiteGateSeq) {
				t.Fatalf("website gate sequence = %v, want %v", gotWebsite, tc.websiteGateSeq)
			}
			gotDomain := make([]GateKind, 0, len(tc.domainGateSeq))
			for _, gate := range plan.Gates {
				if gate.Flow == FlowDomainVerification {
					gotDomain = append(gotDomain, gate.Kind)
				}
			}
			if len(gotDomain) != 0 && !reflect.DeepEqual(gotDomain, tc.domainGateSeq) {
				t.Fatalf("domain gate sequence = %v, want %v", gotDomain, tc.domainGateSeq)
			}
			if len(gotDomain) == 0 && len(tc.domainGateSeq) != 0 {
				t.Fatalf("domain gate sequence is empty, want %v", tc.domainGateSeq)
			}
			repairs := make([]RepairKind, 0, len(plan.Repairs))
			for _, repair := range plan.Repairs {
				repairs = append(repairs, repair.Kind)
			}
			if len(repairs) != len(tc.repairs) {
				t.Fatalf("repairs = %v, want %v", repairs, tc.repairs)
			}
			for i, kind := range tc.repairs {
				if repairs[i] != kind {
					t.Fatalf("repairs = %v, want %v", repairs, tc.repairs)
				}
			}
			// DNSLink intents carry the target-derived value byte-for-byte.
			for _, record := range plan.Records {
				if record.Intent.Kind == RecordKindDNSLink {
					if record.Intent.Value != facts.Target.DNSLinkPath() {
						t.Fatalf("DNSLink intent value = %q, want %q", record.Intent.Value, facts.Target.DNSLinkPath())
					}
				}
			}
		})
	}
}

// TestPlanBindingSharedParentSubdomainCarriesSharedZoneIntent encodes the
// observed one-zone topology (DelegatedDomainService.resolveManagedZone): a
// subdomain of a portal-managed binding reuses the parent's zone, so the
// profiles that cover subdomain bindings bind SharedParent facts and the
// plan's zone intent carries the observed shared-parent allocation.
func TestPlanBindingSharedParentSubdomainCarriesSharedZoneIntent(t *testing.T) {
	cases := []struct {
		id   ProfileID
		name string
	}{
		{id: ProfileIDICANNPortal, name: "docs.example.xyz"},
		{id: ProfileIDHNSPortalNamebaseChild, name: "docs.sub.altroot"},
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
			want := ZoneIntent{Authority: AuthorityLocusPortalZone, Allocation: ZoneAllocationSharedParent}
			if !reflect.DeepEqual(plan.Zone, want) {
				t.Fatalf("zone intent = %+v, want %+v", plan.Zone, want)
			}
		})
	}

	t.Run("shared-parent facts without a live parent zone fail closed", func(t *testing.T) {
		profile, ok := DefaultRegistry().Lookup(ProfileIDICANNPortal)
		if !ok {
			t.Fatalf("profile is not registered")
		}
		facts := factsForProfile(t, profile)
		facts.ZoneAllocation = ZoneAllocationSharedParent
		facts.ZonePresent = false
		if _, err := PlanBinding(profile, facts); err == nil {
			t.Fatalf("shared-parent facts with no parent zone present must be rejected")
		}
	})

	t.Run("dedicated topology keeps a dedicated zone intent", func(t *testing.T) {
		profile, ok := DefaultRegistry().Lookup(ProfileIDICANNPortal)
		if !ok {
			t.Fatalf("profile is not registered")
		}
		facts := factsForProfile(t, profile)
		plan, err := PlanBinding(profile, facts)
		if err != nil {
			t.Fatalf("PlanBinding: %s", err)
		}
		if plan.Zone.Allocation != ZoneAllocationDedicated {
			t.Fatalf("zone allocation = %s, want dedicated", plan.Zone.Allocation.String())
		}
	})
}

// TestPlanBindingRejections covers every mismatched profile/facts pair the
// docs name, plus unknown facts that would otherwise authorize writes.
func TestPlanBindingRejections(t *testing.T) {
	lookup := func(t *testing.T, id ProfileID) Profile {
		t.Helper()
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		return profile
	}
	mutate := func(base BindingFacts, f func(*BindingFacts)) BindingFacts {
		facts := base
		f(&facts)
		return facts
	}

	cases := []struct {
		name    string
		profile ProfileID
		mutator func(*BindingFacts)
	}{
		{
			name:    "chain authority with a zone",
			profile: ProfileIDHNSChainEthereum,
			mutator: func(f *BindingFacts) {
				f.ZonePresent = true
				f.ZoneAllocation = ZoneAllocationDedicated
			},
		},
		{
			name:    "owner authority with a zone",
			profile: ProfileIDHNSOwnerNative,
			mutator: func(f *BindingFacts) { f.ZonePresent = true },
		},
		{
			name:    "portal profile with an owner hosting request",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) { f.RequestedHosting = HostingRequestOwner },
		},
		{
			name:    "owner profile with a portal hosting request",
			profile: ProfileIDICANNOwner,
			mutator: func(f *BindingFacts) { f.RequestedHosting = HostingRequestPortal },
		},
		{
			name:    "profile version mismatch",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) { f.PolicyVersion = ProfileVersion(2) },
		},
		{
			name:    "unknown policy version",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) { f.PolicyVersion = ProfileVersion(0) },
		},
		{
			name:    "route mismatch",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) { f.DiscoveredRoute = ResolutionRouteCrossChain },
		},
		{
			name:    "unknown discovered route",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) { f.DiscoveredRoute = ResolutionRouteUnknown },
		},
		{
			name:    "backend mismatch",
			profile: ProfileIDHNSPortalNative,
			mutator: func(f *BindingFacts) { f.DiscoveredBackend = BackendEthereum },
		},
		{
			name:    "empty discovered backend",
			profile: ProfileIDHNSPortalNative,
			mutator: func(f *BindingFacts) { f.DiscoveredBackend = EmptyBackendID },
		},
		{
			name:    "unknown lifecycle",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) { f.Lifecycle = LifecycleUnknown },
		},
		{
			name:    "unknown hosting request",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) { f.RequestedHosting = HostingRequestUnknown },
		},
		{
			name:    "empty binding name",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) { f.Name = "" },
		},
		{
			name:    "invalid target",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) { f.Target = ContentTarget{Kind: TargetKindIPFS, Value: ""} },
		},
		{
			name:    "unknown target kind",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) { f.Target = ContentTarget{Kind: TargetKind(99), Value: "x"} },
		},
		{
			name:    "portal profile without a valid zone allocation",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) { f.ZoneAllocation = ZoneAllocationUnknown },
		},
		{
			name:    "zone present with no allocation",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) { f.ZoneAllocation = ZoneAllocationNone },
		},
		{
			// Native HNS names are single-label TLDs: they can never share a
			// parent zone, so the native profile permits the dedicated
			// allocation only (the namebase-child profile covers subdomain
			// topologies).
			name:    "profile forbids the observed zone allocation",
			profile: ProfileIDHNSPortalNative,
			mutator: func(f *BindingFacts) { f.ZoneAllocation = ZoneAllocationSharedParent },
		},
		{
			name:    "platform profile without platform ownership",
			profile: ProfileIDPlatformICANN,
			mutator: func(f *BindingFacts) { f.PlatformRootID = nil },
		},
		{
			name:    "non-platform profile with platform ownership",
			profile: ProfileIDICANNPortal,
			mutator: func(f *BindingFacts) {
				root := uint(7)
				f.PlatformRootID = &root
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			profile := lookup(t, tc.profile)
			facts := factsForProfile(t, profile)
			if _, err := PlanBinding(profile, mutate(facts, tc.mutator)); err == nil {
				t.Fatalf("PlanBinding accepted mismatched facts for %q", tc.profile.String())
			}
		})
	}

	t.Run("target kind not allowed by profile", func(t *testing.T) {
		restrictive, err := NewProfile(ProfileSpec{
			ID:             "test.icann.restrictive.current-v1",
			Version:        ProfileVersion1,
			NamingSystem:   NamingSystemICANN,
			Route:          ResolutionRouteStandardDNS,
			Backend:        BackendSystemDNS,
			Authority:      AuthorityLocusOwnerDNS,
			ZoneAllocation: ZoneAllocationNone,
			Ownership:      gateAllOf(GateDNSLink),
			DNSSEC:         mustSecurity(t, RequirementNotApplicable, ActorUnknown, PublicationLocusNone, VerificationModeNone),
			DANE:           mustSecurity(t, RequirementNotApplicable, ActorUnknown, PublicationLocusNone, VerificationModeNone),
			AllowedTargets: []TargetKind{TargetKindIPFS},
			WebsiteGates:   []GateSpec{{Kind: GateDNSLink}},
			DomainGates:    []GateSpec{},
		})
		if err != nil {
			t.Fatalf("construct restrictive profile: %s", err)
		}
		facts := factsForProfile(t, restrictive)
		facts.Target = mustTarget(t, TargetKindIPNS, "peerid")
		if _, err := PlanBinding(restrictive, facts); err == nil {
			t.Fatalf("PlanBinding accepted a target kind the profile does not allow")
		}
	})
}

func mustSecurity(t *testing.T, requirement Requirement, provisioner Actor, publication PublicationLocus, verification VerificationMode) SecurityPlan {
	t.Helper()
	plan, err := NewSecurityPlan(requirement, provisioner, publication, verification)
	if err != nil {
		t.Fatalf("mustSecurity: %s", err)
	}
	return plan
}

func mustTarget(t *testing.T, kind TargetKind, value string) ContentTarget {
	t.Helper()
	target, err := NewContentTarget(kind, value)
	if err != nil {
		t.Fatalf("mustTarget: %s", err)
	}
	return target
}

// TestPlanBindingAuthoritativeChainZone checks the stray-zone rule: an
// on-chain binding carrying a zone reference is data incoherence that must
// never authorize portal DNS work.
func TestPlanBindingAuthoritativeChainZone(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDHNSChainEthereum)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	facts := factsForProfile(t, profile)
	facts.ZonePresent = true
	if _, err := PlanBinding(profile, facts); err == nil {
		t.Fatalf("chain authority with a zone must be rejected")
	}
}

// TestPlanBindingGateExpectations checks that the DNSLink gate expects the
// target-derived path and the challenge gate expects record presence.
func TestPlanBindingGateExpectations(t *testing.T) {
	profile, ok := DefaultRegistry().Lookup(ProfileIDICANNPortal)
	if !ok {
		t.Fatalf("profile is not registered")
	}
	facts := factsForProfile(t, profile)
	plan, err := PlanBinding(profile, facts)
	if err != nil {
		t.Fatalf("PlanBinding: %s", err)
	}
	for _, gate := range plan.Gates {
		switch gate.Kind {
		case GateDNSLink:
			if gate.Expected != facts.Target.DNSLinkPath() {
				t.Fatalf("DNSLink expectation = %q, want %q", gate.Expected, facts.Target.DNSLinkPath())
			}
		case GateChallengeTXT:
			want := "challenge TXT record present at " + ChallengeRecordLabel + "." + facts.Name
			if gate.Expected != want {
				t.Fatalf("challenge expectation = %q, want %q", gate.Expected, want)
			}
		}
	}
}

// TestPlanBindingIsDeterministic asserts that repeated planning yields an
// identical plan.
func TestPlanBindingIsDeterministic(t *testing.T) {
	for _, id := range currentProfileIDs {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		facts := factsForProfile(t, profile)
		first, err := PlanBinding(profile, facts)
		if err != nil {
			t.Fatalf("PlanBinding(%s): %s", id.String(), err)
		}
		second, err := PlanBinding(profile, facts)
		if err != nil {
			t.Fatalf("PlanBinding(%s): %s", id.String(), err)
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("PlanBinding(%s) is not deterministic", id.String())
		}
	}
}
