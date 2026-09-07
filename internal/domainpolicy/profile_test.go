package domainpolicy

import (
	"reflect"
	"testing"
)

// currentProfileIDs lists the expected built-in profile IDs in sorted order.
// The default registry must contain exactly these; anything else registered
// there would silently change runtime policy selection.
var currentProfileIDs = []ProfileID{
	ProfileIDHNSChainEthereum,
	ProfileIDHNSOwnerNative,
	ProfileIDHNSPortalNamebaseChild,
	ProfileIDHNSPortalNative,
	ProfileIDICANNOwner,
	ProfileIDICANNPortal,
	ProfileIDPlatformHNSNative,
	ProfileIDPlatformICANN,
}

func TestDefaultRegistryHoldsExactlyTheCurrentProfiles(t *testing.T) {
	ids := DefaultRegistry().IDs()
	if !reflect.DeepEqual(ids, currentProfileIDs) {
		t.Fatalf("default registry IDs = %v, want %v", ids, currentProfileIDs)
	}
}

func TestCurrentProfilesShape(t *testing.T) {
	cases := []struct {
		id          ProfileID
		naming      NamingSystem
		route       ResolutionRoute
		backend     BackendID
		authority   AuthorityLocus
		zone        ZoneAllocation
		extra       []ZoneAllocation
		description string
	}{
		{
			id:          ProfileIDICANNPortal,
			naming:      NamingSystemICANN,
			route:       ResolutionRouteStandardDNS,
			backend:     BackendSystemDNS,
			authority:   AuthorityLocusPortalZone,
			zone:        ZoneAllocationDedicated,
			extra:       []ZoneAllocation{ZoneAllocationSharedParent},
			description: "ICANN Pinner-managed DNS gets a dedicated zone; subdomains reuse the parent's zone",
		},
		{
			id:          ProfileIDICANNOwner,
			naming:      NamingSystemICANN,
			route:       ResolutionRouteStandardDNS,
			backend:     BackendSystemDNS,
			authority:   AuthorityLocusOwnerDNS,
			zone:        ZoneAllocationNone,
			description: "ICANN owner-hosted DNS holds no zone",
		},
		{
			id:          ProfileIDHNSPortalNative,
			naming:      NamingSystemHNS,
			route:       ResolutionRouteHNSRoot,
			backend:     BackendHNSRoot,
			authority:   AuthorityLocusPortalZone,
			zone:        ZoneAllocationDedicated,
			description: "managed native HNS uses a dedicated signed PowerDNS zone (single-label apex; no parent zone to share)",
		},
		{
			id:          ProfileIDHNSPortalNamebaseChild,
			naming:      NamingSystemHNS,
			route:       ResolutionRouteHNSRoot,
			backend:     BackendHNSRoot,
			authority:   AuthorityLocusPortalZone,
			zone:        ZoneAllocationDedicated,
			extra:       []ZoneAllocation{ZoneAllocationSharedParent},
			description: "Namebase-style delegated HNS gets its own full zone; subdomains reuse the parent's zone",
		},
		{
			id:          ProfileIDHNSOwnerNative,
			naming:      NamingSystemHNS,
			route:       ResolutionRouteHNSRoot,
			backend:     BackendHNSRoot,
			authority:   AuthorityLocusOwnerDNS,
			zone:        ZoneAllocationNone,
			description: "self-hosted native HNS holds no zone",
		},
		{
			id:          ProfileIDHNSChainEthereum,
			naming:      NamingSystemHNS,
			route:       ResolutionRouteCrossChain,
			backend:     BackendEthereum,
			authority:   AuthorityLocusChain,
			zone:        ZoneAllocationNone,
			description: "HIP-5 Ethereum HNS is served by chain authority with no zone",
		},
		{
			id:          ProfileIDPlatformICANN,
			naming:      NamingSystemICANN,
			route:       ResolutionRouteStandardDNS,
			backend:     BackendPowerDNS,
			authority:   AuthorityLocusOperatorZone,
			zone:        ZoneAllocationSharedParent,
			description: "ICANN platform subdomains share the operator root zone",
		},
		{
			id:          ProfileIDPlatformHNSNative,
			naming:      NamingSystemHNS,
			route:       ResolutionRouteHNSRoot,
			backend:     BackendPowerDNS,
			authority:   AuthorityLocusOperatorZone,
			zone:        ZoneAllocationSharedParent,
			description: "HNS platform subdomains share the operator root zone",
		},
	}
	for _, tc := range cases {
		t.Run(tc.id.String(), func(t *testing.T) {
			profile, ok := DefaultRegistry().Lookup(tc.id)
			if !ok {
				t.Fatalf("profile %q is not registered", tc.id.String())
			}
			if profile.ID != tc.id {
				t.Fatalf("ID = %q, want %q", profile.ID.String(), tc.id.String())
			}
			if profile.Version != ProfileVersion1 {
				t.Fatalf("version = %s, want 1", profile.Version.String())
			}
			if profile.NamingSystem != tc.naming {
				t.Fatalf("naming system = %s, want %s", profile.NamingSystem.String(), tc.naming.String())
			}
			if profile.Route != tc.route {
				t.Fatalf("route = %s, want %s", profile.Route.String(), tc.route.String())
			}
			if profile.Backend != tc.backend {
				t.Fatalf("backend = %q, want %q", profile.Backend.String(), tc.backend.String())
			}
			if profile.Authority != tc.authority {
				t.Fatalf("authority = %s, want %s", profile.Authority.String(), tc.authority.String())
			}
			if profile.ZoneAllocation != tc.zone {
				t.Fatalf("zone allocation = %s, want %s", profile.ZoneAllocation.String(), tc.zone.String())
			}
			if len(profile.ExtraZoneAllocations) != len(tc.extra) {
				t.Fatalf("extra zone allocations = %v, want %v", profile.ExtraZoneAllocations, tc.extra)
			}
			for _, extra := range tc.extra {
				if !containsZoneAllocation(profile.ExtraZoneAllocations, extra) {
					t.Fatalf("extra zone allocations = %v, want %v", profile.ExtraZoneAllocations, tc.extra)
				}
				if !profile.AllowsZoneAllocation(extra) {
					t.Fatalf("profile must permit extra zone allocation %s", extra.String())
				}
			}
			if !profile.AllowsZoneAllocation(profile.ZoneAllocation) {
				t.Fatalf("profile must always permit its canonical zone allocation %s", profile.ZoneAllocation.String())
			}
			if !profile.AllowsTarget(TargetKindIPFS) || !profile.AllowsTarget(TargetKindIPNS) {
				t.Fatalf("every current profile serves IPFS and IPNS targets")
			}
			if profile.Ownership.IsEmpty() {
				t.Fatalf("ownership expression is empty")
			}
		})
	}
}

func TestICANNProfilesKeepTxtChallengeGate(t *testing.T) {
	for _, id := range []ProfileID{ProfileIDICANNPortal, ProfileIDICANNOwner} {
		profile, ok := DefaultRegistry().Lookup(id)
		if !ok {
			t.Fatalf("profile %q is not registered", id.String())
		}
		found := false
		for _, g := range profile.WebsiteGates {
			if g.Kind == GateChallengeTXT {
				found = true
			}
		}
		if !found {
			t.Fatalf("profile %q must keep the challenge TXT gate in both current ICANN profiles", id.String())
		}
	}
}

func TestNewProfileRejections(t *testing.T) {
	base, err := icannPortalProfile()
	if err != nil {
		t.Fatalf("construct base profile: %s", err)
	}
	baseSpec := ProfileSpec{
		ID:             base.ID,
		Version:        base.Version,
		NamingSystem:   base.NamingSystem,
		Route:          base.Route,
		Backend:        base.Backend,
		Authority:      base.Authority,
		ZoneAllocation: base.ZoneAllocation,
		Ownership:      base.Ownership,
		DNSSEC:         base.DNSSEC,
		DANE:           base.DANE,
		AllowedTargets: base.AllowedTargets,
		WebsiteGates:   base.WebsiteGates,
		DomainGates:    base.DomainGates,
		Repairs:        base.Repairs,
		Records:        base.Records,
	}
	mutate := func(f func(*ProfileSpec)) ProfileSpec {
		spec := baseSpec
		f(&spec)
		return spec
	}
	unknownRoute := ResolutionRoute(99)
	unknownAuthority := AuthorityLocus(99)
	unknownZone := ZoneAllocation(99)
	unknownNaming := NamingSystem(99)
	unknownTarget := TargetKind(99)
	unknownGate := GateKind(99)

	cases := []struct {
		name string
		spec ProfileSpec
	}{
		{name: "empty ID", spec: mutate(func(s *ProfileSpec) { s.ID = EmptyProfileID })},
		{name: "zero version", spec: mutate(func(s *ProfileSpec) { s.Version = ProfileVersion(0) })},
		{name: "negative version", spec: mutate(func(s *ProfileSpec) { s.Version = ProfileVersion(-1) })},
		{name: "empty backend", spec: mutate(func(s *ProfileSpec) { s.Backend = EmptyBackendID })},
		{name: "unknown naming system", spec: mutate(func(s *ProfileSpec) { s.NamingSystem = unknownNaming })},
		{name: "unknown route", spec: mutate(func(s *ProfileSpec) { s.Route = unknownRoute })},
		{name: "unknown authority", spec: mutate(func(s *ProfileSpec) { s.Authority = unknownAuthority })},
		{name: "unknown zone allocation", spec: mutate(func(s *ProfileSpec) { s.ZoneAllocation = unknownZone })},
		{name: "unknown extra zone allocation", spec: mutate(func(s *ProfileSpec) { s.ExtraZoneAllocations = []ZoneAllocation{unknownZone} })},
		{name: "none extra zone allocation", spec: mutate(func(s *ProfileSpec) { s.ExtraZoneAllocations = []ZoneAllocation{ZoneAllocationNone} })},
		{
			name: "extra zone allocation duplicate of the primary",
			spec: mutate(func(s *ProfileSpec) { s.ExtraZoneAllocations = []ZoneAllocation{ZoneAllocationDedicated} }),
		},
		{
			name: "duplicated extra zone allocations",
			spec: mutate(func(s *ProfileSpec) {
				s.ExtraZoneAllocations = []ZoneAllocation{ZoneAllocationSharedParent, ZoneAllocationSharedParent}
			}),
		},
		{
			name: "extra zone allocation on owner authority",
			spec: mutate(func(s *ProfileSpec) {
				s.Authority = AuthorityLocusOwnerDNS
				s.ZoneAllocation = ZoneAllocationNone
				s.ExtraZoneAllocations = []ZoneAllocation{ZoneAllocationSharedParent}
			}),
		},
		{
			name: "empty ownership",
			spec: mutate(func(s *ProfileSpec) { s.Ownership = ProofExpression{} }),
		},
		{
			name: "empty allowed targets",
			spec: mutate(func(s *ProfileSpec) { s.AllowedTargets = nil }),
		},
		{
			name: "unknown allowed target",
			spec: mutate(func(s *ProfileSpec) { s.AllowedTargets = []TargetKind{unknownTarget} }),
		},
		{
			name: "duplicate allowed targets",
			spec: mutate(func(s *ProfileSpec) { s.AllowedTargets = []TargetKind{TargetKindIPFS, TargetKindIPFS} }),
		},
		{
			name: "invalid DNSSEC plan",
			spec: mutate(func(s *ProfileSpec) {
				s.DNSSEC = SecurityPlan{Requirement: RequirementRequired}
			}),
		},
		{
			name: "invalid DANE plan",
			spec: mutate(func(s *ProfileSpec) {
				s.DANE = SecurityPlan{Requirement: RequirementRequired, Provisioner: ActorPortal, Publication: PublicationLocusNone}
			}),
		},
		{
			name: "unknown website gate",
			spec: mutate(func(s *ProfileSpec) {
				s.WebsiteGates = []GateSpec{{Kind: GateDNSLink}, {Kind: unknownGate}}
			}),
		},
		{
			name: "duplicate website gate",
			spec: mutate(func(s *ProfileSpec) {
				s.WebsiteGates = []GateSpec{{Kind: GateDNSLink}, {Kind: GateChallengeTXT}, {Kind: GateChallengeTXT}}
			}),
		},
		{
			name: "no DNSLink website gate",
			spec: mutate(func(s *ProfileSpec) {
				s.WebsiteGates = []GateSpec{{Kind: GateChallengeTXT}}
			}),
		},
		{
			name: "two DNSLink website gates",
			spec: mutate(func(s *ProfileSpec) {
				s.WebsiteGates = []GateSpec{{Kind: GateDNSLink}, {Kind: GateDNSLink}}
			}),
		},
		{
			name: "chain authority with a zone",
			spec: mutate(func(s *ProfileSpec) {
				s.Authority = AuthorityLocusChain
				s.ZoneAllocation = ZoneAllocationDedicated
				s.Ownership = gateAllOf(GateDNSLink, GateTLSA)
				s.DANE = mustDANE(t, RequirementRequired, ActorOwner, PublicationLocusChain, VerificationModeResolveHNS)
			}),
		},
		{
			name: "portal authority without a zone intent",
			spec: mutate(func(s *ProfileSpec) { s.ZoneAllocation = ZoneAllocationNone }),
		},
		{
			name: "owner authority with a zone",
			spec: mutate(func(s *ProfileSpec) {
				s.Authority = AuthorityLocusOwnerDNS
				s.ZoneAllocation = ZoneAllocationSharedParent
			}),
		},
		{
			name: "chain authority without the cross-chain route",
			spec: mutate(func(s *ProfileSpec) {
				s.Authority = AuthorityLocusChain
				s.ZoneAllocation = ZoneAllocationNone
				s.Route = ResolutionRouteStandardDNS
			}),
		},
		{
			name: "TLSA ownership gate outside chain authority",
			spec: mutate(func(s *ProfileSpec) {
				s.Ownership = gateAllOf(GateDNSLink, GateTLSA)
				s.DANE = mustDANE(t, RequirementOptional, ActorPortal, PublicationLocusPortalZone, VerificationModeNone)
				s.WebsiteGates = []GateSpec{{Kind: GateDNSLink}, {Kind: GateTLSA}}
			}),
		},
		{
			name: "TLSA website gate with not-applicable DANE",
			spec: mutate(func(s *ProfileSpec) {
				s.Ownership = gateAllOf(GateDNSLink, GateTLSA)
				s.DANE = mustDANE(t, RequirementNotApplicable, ActorUnknown, PublicationLocusNone, VerificationModeNone)
				s.WebsiteGates = []GateSpec{{Kind: GateDNSLink}, {Kind: GateTLSA}}
			}),
		},
		{
			name: "required DANE with chain authority but no TLSA gate",
			spec: mutate(func(s *ProfileSpec) {
				s.Authority = AuthorityLocusChain
				s.ZoneAllocation = ZoneAllocationNone
				s.Ownership = gateAllOf(GateDNSLink)
				s.DANE = mustDANE(t, RequirementRequired, ActorOwner, PublicationLocusChain, VerificationModeResolveHNS)
			}),
		},
		{
			name: "challenge TXT gate outside ICANN",
			spec: mutate(func(s *ProfileSpec) {
				s.NamingSystem = NamingSystemHNS
				s.Ownership = gateAllOf(GateDNSLink, GateChallengeTXT)
			}),
		},
		{
			name: "platform trust gate outside operator authority",
			spec: mutate(func(s *ProfileSpec) {
				s.Ownership = gateAllOf(GatePlatformTrust, GateDNSLink)
				s.DomainGates = []GateSpec{{Kind: GatePlatformTrust}}
			}),
		},
		{
			name: "partner attestation gate",
			spec: mutate(func(s *ProfileSpec) {
				s.Ownership = gateAllOf(GateDNSLink, GatePartnerAttestation)
				s.DomainGates = []GateSpec{{Kind: GatePartnerAttestation}}
			}),
		},
		{
			name: "invalid record spec",
			spec: mutate(func(s *ProfileSpec) {
				spec, _ := NewPlannedRecordSpec(RecordKindDNSLink, "_dnslink", PublicationLocusPortalZone, RecordOwnershipBindingContent, 0, true, "")
				spec.Name = ""
				s.Records = []PlannedRecordSpec{spec}
			}),
		},
		{
			name: "invalid repair intent",
			spec: mutate(func(s *ProfileSpec) {
				s.Repairs = []RepairIntent{{Kind: RepairKindUnknown, Flow: FlowWebsiteValidation}}
			}),
		},
		{
			name: "unknown domain gate",
			spec: mutate(func(s *ProfileSpec) {
				s.DomainGates = []GateSpec{{Kind: unknownGate}}
			}),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewProfile(tc.spec); err == nil {
				t.Fatalf("NewProfile accepted an invalid profile")
			}
		})
	}
}

func mustDANE(t *testing.T, requirement Requirement, provisioner Actor, publication PublicationLocus, verification VerificationMode) SecurityPlan {
	t.Helper()
	plan, err := NewSecurityPlan(requirement, provisioner, publication, verification)
	if err != nil {
		t.Fatalf("mustDANE: %s", err)
	}
	return plan
}

func TestNewProfileCopiesSlices(t *testing.T) {
	profile, err := icannPortalProfile()
	if err != nil {
		t.Fatalf("construct profile: %s", err)
	}
	registered, ok := DefaultRegistry().Lookup(ProfileIDICANNPortal)
	if !ok {
		t.Fatalf("profile %q is not registered", ProfileIDICANNPortal.String())
	}
	registeredGates := append([]GateSpec(nil), registered.WebsiteGates...)
	registeredTargets := append([]TargetKind(nil), registered.AllowedTargets...)
	registeredRecords := append([]PlannedRecordSpec(nil), registered.Records...)
	// Mutating the local profile must not touch the registered one: NewProfile
	// deep-copies the specification slices.
	profile.WebsiteGates[0] = GateSpec{Kind: GateTLSA}
	profile.AllowedTargets[0] = TargetKind(99)
	profile.Records[0].Name = "mutated"
	if !reflect.DeepEqual(registered.WebsiteGates, registeredGates) ||
		!reflect.DeepEqual(registered.AllowedTargets, registeredTargets) ||
		!reflect.DeepEqual(registered.Records, registeredRecords) {
		t.Fatalf("mutating a locally constructed profile changed the registered profile")
	}
}

func TestRegistryRejections(t *testing.T) {
	profile, err := icannOwnerProfile()
	if err != nil {
		t.Fatalf("construct profile: %s", err)
	}

	t.Run("duplicate ID", func(t *testing.T) {
		registry := NewRegistry()
		if err := registry.Register(profile); err != nil {
			t.Fatalf("first registration: %s", err)
		}
		if err := registry.Register(profile); err == nil {
			t.Fatalf("duplicate registration was accepted")
		}
	})

	t.Run("zero version", func(t *testing.T) {
		registry := NewRegistry()
		bad := profile
		bad.Version = ProfileVersion(0)
		if err := registry.Register(bad); err == nil {
			t.Fatalf("zero-version profile was accepted")
		}
	})

	t.Run("empty backend", func(t *testing.T) {
		registry := NewRegistry()
		bad := profile
		bad.Backend = EmptyBackendID
		if err := registry.Register(bad); err == nil {
			t.Fatalf("empty-backend profile was accepted")
		}
	})

	t.Run("zero-value profile", func(t *testing.T) {
		registry := NewRegistry()
		if err := registry.Register(Profile{}); err == nil {
			t.Fatalf("zero-value profile was accepted")
		}
	})

	t.Run("lookup on empty registry", func(t *testing.T) {
		registry := NewRegistry()
		if _, ok := registry.Lookup(ProfileIDICANNPortal); ok {
			t.Fatalf("empty registry returned a profile")
		}
	})
}

func TestRegistryIDsAreSorted(t *testing.T) {
	registry := NewRegistry()
	profiles, err := CurrentProfiles()
	if err != nil {
		t.Fatalf("CurrentProfiles: %s", err)
	}
	for _, profile := range profiles {
		if err := registry.Register(profile); err != nil {
			t.Fatalf("register %q: %s", profile.ID.String(), err)
		}
	}
	ids := registry.IDs()
	for i := 1; i < len(ids); i++ {
		if ids[i-1] >= ids[i] {
			t.Fatalf("registry IDs are not sorted: %v", ids)
		}
	}
}

func TestDerivePlatformProfile(t *testing.T) {
	hnsRoot, ok := DefaultRegistry().Lookup(ProfileIDHNSPortalNative)
	if !ok {
		t.Fatalf("root profile is not registered")
	}
	derived, err := DerivePlatformProfile(hnsRoot, "test.platform.hns.current-v1", BackendPowerDNS)
	if err != nil {
		t.Fatalf("DerivePlatformProfile: %s", err)
	}
	if derived.Authority != AuthorityLocusOperatorZone {
		t.Fatalf("authority = %s, want operator-zone", derived.Authority.String())
	}
	if derived.ZoneAllocation != ZoneAllocationSharedParent {
		t.Fatalf("zone allocation = %s, want shared-parent", derived.ZoneAllocation.String())
	}
	if !reflect.DeepEqual(derived.DNSSEC, hnsRoot.DNSSEC) || !reflect.DeepEqual(derived.DANE, hnsRoot.DANE) {
		t.Fatalf("platform profile must inherit the root's DNSSEC and DANE plans")
	}
	if !derived.AllowsTarget(TargetKindIPFS) {
		t.Fatalf("platform profile must inherit the root's allowed targets")
	}
	hasTLSARecord := false
	for _, record := range derived.Records {
		if record.Kind == RecordKindTLSA {
			hasTLSARecord = true
		}
	}
	if !hasTLSARecord {
		t.Fatalf("platform profile of a managed-HNS root must inherit per-name TLSA publication")
	}

	t.Run("rejects non-zone root", func(t *testing.T) {
		owner, ok := DefaultRegistry().Lookup(ProfileIDHNSOwnerNative)
		if !ok {
			t.Fatalf("root profile is not registered")
		}
		if _, err := DerivePlatformProfile(owner, "test.platform.bad", BackendPowerDNS); err == nil {
			t.Fatalf("platform profile derived from a non-zone root was accepted")
		}
	})

	t.Run("rejects empty id and backend", func(t *testing.T) {
		if _, err := DerivePlatformProfile(hnsRoot, EmptyProfileID, BackendPowerDNS); err == nil {
			t.Fatalf("empty platform profile ID accepted")
		}
		if _, err := DerivePlatformProfile(hnsRoot, "test.platform.hns.current-v1", EmptyBackendID); err == nil {
			t.Fatalf("empty platform backend accepted")
		}
	})
}

func containsZoneAllocation(allocations []ZoneAllocation, want ZoneAllocation) bool {
	for _, candidate := range allocations {
		if candidate == want {
			return true
		}
	}
	return false
}

// TestExtraZoneAllocationsPermittedByPortalProfiles checks that the current
// one-zone rule (subdomains reuse the parent's zone) is permitted exactly by
// the profiles whose bindings can be subdomains, and by no others.
func TestExtraZoneAllocationsPermittedByPortalProfiles(t *testing.T) {
	cases := []struct {
		id       ProfileID
		permits  bool
		viaExtra bool
	}{
		{ProfileIDICANNPortal, true, true},
		{ProfileIDHNSPortalNamebaseChild, true, true},
		// Platform profiles carry shared-parent as their canonical
		// allocation (every platform claim adjoins the operator root's
		// zone); they need no extra.
		{ProfileIDPlatformICANN, true, false},
		{ProfileIDPlatformHNSNative, true, false},
		{ProfileIDHNSPortalNative, false, false},
		{ProfileIDICANNOwner, false, false},
		{ProfileIDHNSOwnerNative, false, false},
		{ProfileIDHNSChainEthereum, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.id.String(), func(t *testing.T) {
			profile, ok := DefaultRegistry().Lookup(tc.id)
			if !ok {
				t.Fatalf("profile %q is not registered", tc.id.String())
			}
			sharedParentAllowed := profile.AllowsZoneAllocation(ZoneAllocationSharedParent)
			if sharedParentAllowed != tc.permits {
				t.Fatalf("profile %s permits shared-parent = %v, want %v", tc.id.String(), sharedParentAllowed, tc.permits)
			}
			if tc.viaExtra && !containsZoneAllocation(profile.ExtraZoneAllocations, ZoneAllocationSharedParent) {
				t.Fatalf("profile %s must declare shared-parent as an extra zone allocation", tc.id.String())
			}
			if !tc.viaExtra && len(profile.ExtraZoneAllocations) != 0 {
				t.Fatalf("profile %s must declare no extra zone allocations; got %v", tc.id.String(), profile.ExtraZoneAllocations)
			}
		})
	}
}
