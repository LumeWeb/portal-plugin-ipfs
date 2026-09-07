package domainpolicy

import (
	"testing"
)

// fuzzProfiles returns a deterministic slice of profiles for fuzz targets.
func fuzzProfiles(t *testing.T) []Profile {
	t.Helper()
	profiles, err := CurrentProfiles()
	if err != nil {
		t.Fatalf("CurrentProfiles: %s", err)
	}
	return profiles
}

// clampEnum returns a value in [min, max] from an arbitrary int64 so fuzzed
// numbers cover unknown, valid, and out-of-range values without panicking on
// the fuzz engine's unbounded ints.
func clampEnum(v int64, min, max int64) int64 {
	clamped := v
	if clamped < min {
		clamped = min
	}
	if clamped > max {
		clamped = max
	}
	return clamped
}

// FuzzPlanBinding asserts panic freedom and fail-closed behavior: for any
// inputs, PlanBinding either rejects the pair or returns a plan whose gates
// and records can be evaluated and diffed without panicking.
func FuzzPlanBinding(f *testing.F) {
	f.Add("example.com", int64(1), int64(1), int64(2), int64(1), int64(2), "bafkqtest", false, int64(1))
	f.Add("example.hnsdata.com", int64(2), int64(3), int64(4), int64(2), int64(1), "peerid", true, int64(0))
	f.Add("", int64(1), int64(1), int64(1), int64(1), int64(1), "x", false, int64(3))
	f.Add("example.com", int64(99), int64(99), int64(99), int64(99), int64(99), "", true, int64(-5))
	f.Fuzz(func(t *testing.T, name string, naming, route, authority, hosting, zone int64, targetValue string, zonePresent bool, version int64) {
		profiles := fuzzProfiles(t)
		profile := profiles[0]

		facts := BindingFacts{
			Name:              name,
			Lifecycle:         Lifecycle(clampEnum(1, 0, 8)),
			RequestedHosting:  HostingRequest(clampEnum(hosting, 0, 3)),
			ZonePresent:       zonePresent,
			ZoneAllocation:    ZoneAllocation(clampEnum(zone, 0, 4)),
			DiscoveredRoute:   profile.Route,
			DiscoveredBackend: profile.Backend,
			Target: ContentTarget{
				Kind:  TargetKindIPFS,
				Value: targetValue,
			},
			PolicyVersion: profile.Version,
		}
		plan, err := PlanBinding(profile, facts)
		if err != nil {
			// Rejections are the fail-closed path; nothing to check further.
			return
		}
		// A successful plan must be fully evaluable and diffable.
		obs := goodObservations()
		if _, err := Evaluate(plan, FlowWebsiteValidation, obs); err != nil {
			t.Fatalf("Evaluate on a successful plan failed: %s", err)
		}
		if _, err := Evaluate(plan, FlowDomainVerification, obs); err != nil {
			t.Fatalf("Evaluate on a successful plan failed: %s", err)
		}
		if _, err := Diff(plan, obs); err != nil {
			t.Fatalf("Diff on a successful plan failed: %s", err)
		}
	})
}

// FuzzNewProfile asserts panic freedom and fail-closed behavior of profile
// construction and registration: any spec either constructs and registers or
// is rejected, and a rejected profile never enters a registry.
func FuzzNewProfile(f *testing.F) {
	f.Add(int64(1), int64(3), int64(2), int64(1), "sys", "owner.example", int64(1))
	f.Add(int64(0), int64(1), int64(0), int64(0), "", "", int64(0))
	f.Add(int64(99), int64(99), int64(99), int64(99), "weird", "weird.example", int64(-2))
	f.Fuzz(func(t *testing.T, naming, authority, route, allocation int64, backend, ownershipName string, version int64) {
		spec := ProfileSpec{
			ID:             ProfileID("fuzz." + ownershipName),
			Version:        ProfileVersion(clampEnum(version, 0, 5)),
			NamingSystem:   NamingSystem(clampEnum(naming, 0, 3)),
			Route:          ResolutionRoute(clampEnum(route, 0, 4)),
			Backend:        BackendID(backend),
			Authority:      AuthorityLocus(clampEnum(authority, 0, 5)),
			ZoneAllocation: ZoneAllocation(clampEnum(allocation, 0, 4)),
			Ownership:      gateAllOf(GateDNSLink),
			DNSSEC:         mustNotApplicable(),
			DANE:           mustNotApplicable(),
			AllowedTargets: []TargetKind{TargetKindIPFS},
			WebsiteGates:   []GateSpec{{Kind: GateDNSLink}},
			DomainGates:    []GateSpec{},
		}
		profile, err := NewProfile(spec)
		registry := NewRegistry()
		if err != nil {
			if regErr := registry.Register(profile); regErr == nil {
				t.Fatalf("a rejected profile must never register")
			}
			return
		}
		if err := registry.Register(profile); err != nil {
			t.Fatalf("a valid profile must register: %s", err)
		}
		got, ok := registry.Lookup(profile.ID)
		if !ok || got.ID != profile.ID {
			t.Fatalf("profile not retrievable after registration")
		}
	})
}

func mustNotApplicable() SecurityPlan {
	plan, _ := NewSecurityPlan(RequirementNotApplicable, ActorUnknown, PublicationLocusNone, VerificationModeNone)
	return plan
}

// FuzzEffectAndDiffInputs asserts that Diff never panics on fuzzed
// observation sets: every input either yields an error with no effects, or a
// deterministic effect list.
func FuzzEffectAndDiffInputs(f *testing.F) {
	f.Add(int64(1), int64(2), "value", int64(1), int64(3), "c")
	f.Add(int64(0), int64(0), "", int64(99), int64(99), "")
	f.Fuzz(func(t *testing.T, dnssecState, routeState int64, recordValue string, ownershipState, kindState int64, recordName string) {
		profiles := fuzzProfiles(t)
		for _, profile := range profiles {
			plan, err := planFor(t, profile)
			if err != nil {
				t.Fatalf("PlanBinding on a registered profile failed: %s", err)
			}
			obs := ObservationSet{
				Route: RouteObservation{
					Route: ResolutionRoute(clampEnum(1, 0, 4)),
				},
				DNSLink: &DNSLinkObservation{Value: recordValue},
				ZoneDNSSEC: &ZoneDNSSECObservation{
					State: ZoneDNSSECState(clampEnum(dnssecState, 0, 4)),
				},
				Records: []RecordObservation{{
					Kind:      RecordKind(clampEnum(kindState, 0, 9)),
					Name:      recordName,
					Value:     recordValue,
					Ownership: RecordOwnership(clampEnum(ownershipState, 0, 6)),
				}},
			}

			effects, err := Diff(plan, obs)
			if err != nil {
				if len(effects) != 0 {
					t.Fatalf("Diff returned effects alongside an error")
				}
				continue
			}
			repeatEffects, err := Diff(plan, obs)
			if err != nil {
				t.Fatalf("Diff non-deterministic error: %s", err)
			}
			if len(effects) != len(repeatEffects) {
				t.Fatalf("Diff repeated is not deterministic")
			}
			if _, err := Evaluate(plan, FlowWebsiteValidation, obs); err != nil {
				t.Fatalf("Evaluate on a valid observation set failed: %s", err)
			}
		}
	})
}

// FuzzFactsShape fuzzes binding-fact shapes directly: NewBindingFacts and
// PlanBinding must fail closed without panicking on any name.
func FuzzFactsShape(f *testing.F) {
	f.Add("example.com")
	f.Add("")
	f.Add("日本語.example")
	f.Fuzz(func(t *testing.T, name string) {
		facts, err := NewBindingFacts(name)
		if err != nil {
			if name == "" {
				return
			}
			t.Fatalf("non-empty names are valid: %s", err)
		}
		if facts.Name != name {
			t.Fatalf("facts name round-trip failed")
		}
		profile, ok := DefaultRegistry().Lookup(ProfileIDICANNOwner)
		if !ok {
			t.Fatalf("profile is not registered")
		}
		facts.Lifecycle = LifecycleProvisioning
		facts.RequestedHosting = HostingRequestOwner
		facts.DiscoveredRoute = profile.Route
		facts.DiscoveredBackend = profile.Backend
		facts.Target = ContentTarget{Kind: TargetKindIPFS, Value: "cid"}
		facts.PolicyVersion = profile.Version
		if _, err := PlanBinding(profile, facts); err != nil {
			t.Fatalf("a minimal owner-hosted ICANN binding must plan: %s", err)
		}
	})
}
