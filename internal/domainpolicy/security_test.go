package domainpolicy

import (
	"errors"
	"fmt"
	"testing"
)

// TestSecurityPlanValidPlans covers the plans that must be accepted.
func TestSecurityPlanValidPlans(t *testing.T) {
	tests := []struct {
		name string
		req  Requirement
		prov Actor
		pub  PublicationLocus
		ver  VerificationMode
	}{
		{
			name: "not applicable with all none",
			req:  RequirementNotApplicable, prov: ActorUnknown,
			pub: PublicationLocusNone, ver: VerificationModeNone,
		},
		{
			name: "required portal provisioned dnssec",
			req:  RequirementRequired, prov: ActorPortal,
			pub: PublicationLocusPortalZone, ver: VerificationModeResolveDNS,
		},
		{
			name: "required chain provisioned dane",
			req:  RequirementRequired, prov: ActorOperator,
			pub: PublicationLocusChain, ver: VerificationModeResolveChain,
		},
		{
			name: "required owner published owner dns",
			req:  RequirementRequired, prov: ActorOwner,
			pub: PublicationLocusOwnerDNS, ver: VerificationModeResolveHNS,
		},
		{
			name: "required external backend attestation",
			req:  RequirementRequired, prov: ActorExternalBackend,
			pub: PublicationLocusChain, ver: VerificationModePartnerAttestation,
		},
		{
			name: "optional owner publication without live verification",
			req:  RequirementOptional, prov: ActorOwner,
			pub: PublicationLocusOwnerDNS, ver: VerificationModeNone,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := NewSecurityPlan(tc.req, tc.prov, tc.pub, tc.ver)
			if err != nil {
				t.Fatalf("NewSecurityPlan failed: %v", err)
			}
			if plan.Requirement != tc.req || plan.Provisioner != tc.prov ||
				plan.Publication != tc.pub || plan.Verification != tc.ver {
				t.Fatalf("plan round trip mismatch: %+v", plan)
			}
		})
	}
}

// TestSecurityPlanInvalidPlans covers rejections. Required security must
// have a provisioner, publication locus, and verifier; NotApplicable must
// use none for all three; every unknown member is rejected.
func TestSecurityPlanInvalidPlans(t *testing.T) {
	tests := []struct {
		name string
		req  Requirement
		prov Actor
		pub  PublicationLocus
		ver  VerificationMode
	}{
		// Unknown requirement.
		{"unknown requirement", RequirementUnknown, ActorPortal, PublicationLocusPortalZone, VerificationModeResolveDNS},
		{"out-of-range requirement", Requirement(99), ActorPortal, PublicationLocusPortalZone, VerificationModeResolveDNS},

		// Required with missing pieces.
		{"required with no provisioner", RequirementRequired, ActorUnknown, PublicationLocusPortalZone, VerificationModeResolveDNS},
		{"required with no publication", RequirementRequired, ActorPortal, PublicationLocusNone, VerificationModeResolveDNS},
		{"required with unknown publication", RequirementRequired, ActorPortal, PublicationLocusUnknown, VerificationModeResolveDNS},
		{"required with no verifier", RequirementRequired, ActorPortal, PublicationLocusPortalZone, VerificationModeNone},
		{"required with unknown verifier", RequirementRequired, ActorPortal, PublicationLocusPortalZone, VerificationModeUnknown},
		{"required with unknown provisioner", RequirementRequired, ActorUnknown, PublicationLocusChain, VerificationModeResolveChain},
		{"required with out-of-range provisioner", RequirementRequired, Actor(42), PublicationLocusChain, VerificationModeResolveChain},

		// NotApplicable must use none for all three.
		{"not-applicable with provisioner", RequirementNotApplicable, ActorPortal, PublicationLocusNone, VerificationModeNone},
		{"not-applicable with unknown provisioner", RequirementNotApplicable, Actor(9), PublicationLocusNone, VerificationModeNone},
		{"not-applicable with portal publication", RequirementNotApplicable, ActorUnknown, PublicationLocusPortalZone, VerificationModeNone},
		{"not-applicable with unknown publication", RequirementNotApplicable, ActorUnknown, PublicationLocusUnknown, VerificationModeNone},
		{"not-applicable with dns verifier", RequirementNotApplicable, ActorUnknown, PublicationLocusNone, VerificationModeResolveDNS},
		{"not-applicable with unknown verifier", RequirementNotApplicable, ActorUnknown, PublicationLocusNone, VerificationModeUnknown},

		// Optional still rejects every unknown member.
		{"optional with unknown provisioner", RequirementOptional, ActorUnknown, PublicationLocusOwnerDNS, VerificationModeResolveDNS},
		{"optional with unknown publication", RequirementOptional, ActorOwner, PublicationLocusUnknown, VerificationModeResolveDNS},
		{"optional with unknown verifier", RequirementOptional, ActorOwner, PublicationLocusOwnerDNS, VerificationModeUnknown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := NewSecurityPlan(tc.req, tc.prov, tc.pub, tc.ver)
			if err == nil {
				t.Fatalf("NewSecurityPlan(%v, %v, %v, %v) succeeded with %+v, want rejection",
					tc.req, tc.prov, tc.pub, tc.ver, plan)
			}
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("NewSecurityPlan error = %v, want ErrInvalid", err)
			}
		})
	}
}

// TestNewSecurityPlanFailsClosedOnUnknownEnums double-checks that member
// enums outside their defined range are rejected no matter the requirement.
func TestNewSecurityPlanFailsClosedOnUnknownEnums(t *testing.T) {
	if _, err := NewSecurityPlan(RequirementRequired, Actor(-7), PublicationLocusChain, VerificationModeResolveDNS); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative provisioner accepted: %v", err)
	}
}

// TestNewSecurityPlan_RejectsOutOfRangeEnums is a regression test: sentinel
// equality alone previously let out-of-range enum integers slip through the
// Required and Optional branches. Every unknown publication locus and
// verification mode (negative, far-out-of-range, or the Unknown sentinel)
// must be rejected; None is only rejected for Required security.
func TestNewSecurityPlan_RejectsOutOfRangeEnums(t *testing.T) {
	badPublications := []PublicationLocus{
		PublicationLocus(99),
		PublicationLocus(-1),
		PublicationLocusUnknown,
	}
	badVerifications := []VerificationMode{
		VerificationMode(99),
		VerificationMode(-1),
		VerificationModeUnknown,
	}

	for _, req := range []Requirement{RequirementRequired, RequirementOptional} {
		for _, pub := range append(badPublications, PublicationLocusNone) {
			t.Run(fmt.Sprintf("%s_bad_publication_%d", req, int(pub)), func(t *testing.T) {
				_, err := NewSecurityPlan(req, ActorPortal, pub, VerificationModeResolveDNS)
				if req == RequirementOptional && pub == PublicationLocusNone {
					// None is allowed for Optional; expect no error.
					if err != nil {
						t.Fatalf("NewSecurityPlan with None publication rejected: %v", err)
					}
					return
				}
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("NewSecurityPlan accepted out-of-range publication %d: %v", int(pub), err)
				}
			})
		}
		for _, ver := range append(badVerifications, VerificationModeNone) {
			t.Run(fmt.Sprintf("%s_bad_verification_%d", req, int(ver)), func(t *testing.T) {
				_, err := NewSecurityPlan(req, ActorPortal, PublicationLocusPortalZone, ver)
				if req == RequirementOptional && ver == VerificationModeNone {
					// None is allowed for Optional; expect no error.
					if err != nil {
						t.Fatalf("NewSecurityPlan with None verification rejected: %v", err)
					}
					return
				}
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("NewSecurityPlan accepted out-of-range verification %d: %v", int(ver), err)
				}
			})
		}
	}
}
