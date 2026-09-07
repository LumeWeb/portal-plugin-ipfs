package domainpolicy

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnumValues exercises every enum's String/Valid/constructor round trip
// for all valid values and all unknown values, including out-of-range ones.
func TestEnumValues(t *testing.T) {
	tests := []struct {
		name       string
		valids     map[int]string
		firstValid int
		construct  func(int) error
	}{
		{
			name: "NamingSystem",
			valids: map[int]string{
				int(NamingSystemICANN): "icann",
				int(NamingSystemHNS):   "hns",
			},
			firstValid: int(NamingSystemICANN),
			construct:  func(v int) error { _, err := NewNamingSystem(NamingSystem(v)); return err },
		},
		{
			name: "ResolutionRoute",
			valids: map[int]string{
				int(ResolutionRouteStandardDNS): "standard-dns",
				int(ResolutionRouteHNSRoot):     "hns-root",
				int(ResolutionRouteCrossChain):  "cross-chain",
			},
			firstValid: int(ResolutionRouteStandardDNS),
			construct:  func(v int) error { _, err := NewResolutionRoute(ResolutionRoute(v)); return err },
		},
		{
			name: "AuthorityLocus",
			valids: map[int]string{
				int(AuthorityLocusPortalZone):   "portal-zone",
				int(AuthorityLocusOwnerDNS):     "owner-dns",
				int(AuthorityLocusOperatorZone): "operator-zone",
				int(AuthorityLocusChain):        "chain",
			},
			firstValid: int(AuthorityLocusPortalZone),
			construct:  func(v int) error { _, err := NewAuthorityLocus(AuthorityLocus(v)); return err },
		},
		{
			name: "ZoneAllocation",
			valids: map[int]string{
				int(ZoneAllocationNone):         "none",
				int(ZoneAllocationDedicated):    "dedicated",
				int(ZoneAllocationSharedParent): "shared-parent",
			},
			firstValid: int(ZoneAllocationNone),
			construct:  func(v int) error { _, err := NewZoneAllocation(ZoneAllocation(v)); return err },
		},
		{
			name: "Lifecycle",
			valids: map[int]string{
				int(LifecycleDraft):         "draft",
				int(LifecycleProvisioning):  "provisioning",
				int(LifecycleAwaitingProof): "awaiting-proof",
				int(LifecycleActive):        "active",
				int(LifecycleError):         "error",
				int(LifecycleRetiring):      "retiring",
			},
			firstValid: int(LifecycleDraft),
			construct:  func(v int) error { _, err := NewLifecycle(Lifecycle(v)); return err },
		},
		{
			name: "HostingRequest",
			valids: map[int]string{
				int(HostingRequestPortal): "portal",
				int(HostingRequestOwner):  "owner",
			},
			firstValid: int(HostingRequestPortal),
			construct:  func(v int) error { _, err := NewHostingRequest(HostingRequest(v)); return err },
		},
		{
			name: "TargetKind",
			valids: map[int]string{
				int(TargetKindIPFS): "ipfs",
				int(TargetKindIPNS): "ipns",
			},
			firstValid: int(TargetKindIPFS),
			construct:  func(v int) error { _, err := NewTargetKind(TargetKind(v)); return err },
		},
		{
			name: "Requirement",
			valids: map[int]string{
				int(RequirementNotApplicable): "not-applicable",
				int(RequirementOptional):      "optional",
				int(RequirementRequired):      "required",
			},
			firstValid: int(RequirementNotApplicable),
			construct:  func(v int) error { _, err := NewRequirement(Requirement(v)); return err },
		},
		{
			name: "Actor",
			valids: map[int]string{
				int(ActorPortal):          "portal",
				int(ActorOwner):           "owner",
				int(ActorOperator):        "operator",
				int(ActorExternalBackend): "external-backend",
			},
			firstValid: int(ActorPortal),
			construct:  func(v int) error { _, err := NewActor(Actor(v)); return err },
		},
		{
			name: "PublicationLocus",
			valids: map[int]string{
				int(PublicationLocusNone):       "none",
				int(PublicationLocusPortalZone): "portal-zone",
				int(PublicationLocusOwnerDNS):   "owner-dns",
				int(PublicationLocusChain):      "chain",
			},
			firstValid: int(PublicationLocusNone),
			construct:  func(v int) error { _, err := NewPublicationLocus(PublicationLocus(v)); return err },
		},
		{
			name: "VerificationMode",
			valids: map[int]string{
				int(VerificationModeNone):               "none",
				int(VerificationModeResolveDNS):         "resolve-dns",
				int(VerificationModeResolveHNS):         "resolve-hns",
				int(VerificationModeResolveChain):       "resolve-chain",
				int(VerificationModePartnerAttestation): "partner-attestation",
			},
			firstValid: int(VerificationModeNone),
			construct:  func(v int) error { _, err := NewVerificationMode(VerificationMode(v)); return err },
		},
		{
			name: "GateKind",
			valids: map[int]string{
				int(GateDNSLink):            "dnslink",
				int(GateChallengeTXT):       "challenge-txt",
				int(GateNSDelegation):       "ns-delegation",
				int(GateDSDelegation):       "ds-delegation",
				int(GateTLSA):               "tlsa",
				int(GatePlatformTrust):      "platform-trust",
				int(GatePartnerAttestation): "partner-attestation",
			},
			firstValid: int(GateDNSLink),
			construct:  func(v int) error { _, err := NewGateKind(GateKind(v)); return err },
		},
		{
			name: "RecordKind",
			valids: map[int]string{
				int(RecordKindDNSLink):      "dnslink",
				int(RecordKindChallengeTXT): "challenge-txt",
				int(RecordKindApexA):        "apex-a",
				int(RecordKindApexALIAS):    "apex-alias",
				int(RecordKindTLSA):         "tlsa",
				int(RecordKindNS):           "ns",
				int(RecordKindDS):           "ds",
				int(RecordKindSOA):          "soa",
			},
			firstValid: int(RecordKindDNSLink),
			construct:  func(v int) error { _, err := NewRecordKind(RecordKind(v)); return err },
		},
		{
			name: "RecordOwnership",
			valids: map[int]string{
				int(RecordOwnershipBindingContent):     "binding-content",
				int(RecordOwnershipBindingSecurity):    "binding-security",
				int(RecordOwnershipDelegation):         "delegation",
				int(RecordOwnershipZoneInfrastructure): "zone-infrastructure",
				int(RecordOwnershipOperator):           "operator",
			},
			firstValid: int(RecordOwnershipBindingContent),
			construct:  func(v int) error { _, err := NewRecordOwnership(RecordOwnership(v)); return err },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// The Unknown (zero) value must always be invalid and stringify
			// as "unknown".
			if tc.construct(0) == nil {
				t.Errorf("%s constructor accepted the zero/unknown value", tc.name)
			}
			if tc.valids[0] != "" {
				t.Errorf("%s maps the zero value to a valid name %q", tc.name, tc.valids[0])
			}

			for raw, want := range tc.valids {
				if valid := enumValid(t, tc.name, raw); !valid {
					t.Errorf("%s(%d).Valid() = false, want true", tc.name, raw)
				}
				if got := enumString(t, tc.name, raw); got != want {
					t.Errorf("%s(%d).String() = %q, want %q", tc.name, raw, got, want)
				}
				if err := tc.construct(raw); err != nil {
					t.Errorf("%s constructor rejected valid value %d: %v", tc.name, raw, err)
				}
			}

			// Out-of-range values must be rejected.
			outOfRange := tc.firstValid + len(tc.valids)
			for _, raw := range []int{outOfRange, outOfRange + 100, -1} {
				if err := tc.construct(raw); err == nil {
					t.Errorf("%s constructor accepted out-of-range value %d", tc.name, raw)
				} else if !errors.Is(err, ErrInvalid) {
					t.Errorf("%s constructor error for %d is %T, want *domainpolicy.InvalidError sentinel", tc.name, raw, err)
				}
			}
		})
	}
}

// The two reflection-free trampolines below keep the table above honest
// without a type switch exposing the enums to generic code. compile-time
// assertions appear in TestEnumAccessorsCompile.

func enumValid(t *testing.T, name string, raw int) bool {
	t.Helper()
	switch name {
	case "NamingSystem":
		return NamingSystem(raw).Valid()
	case "ResolutionRoute":
		return ResolutionRoute(raw).Valid()
	case "AuthorityLocus":
		return AuthorityLocus(raw).Valid()
	case "ZoneAllocation":
		return ZoneAllocation(raw).Valid()
	case "Lifecycle":
		return Lifecycle(raw).Valid()
	case "HostingRequest":
		return HostingRequest(raw).Valid()
	case "TargetKind":
		return TargetKind(raw).Valid()
	case "Requirement":
		return Requirement(raw).Valid()
	case "Actor":
		return Actor(raw).Valid()
	case "PublicationLocus":
		return PublicationLocus(raw).Valid()
	case "VerificationMode":
		return VerificationMode(raw).Valid()
	case "GateKind":
		return GateKind(raw).Valid()
	case "RecordKind":
		return RecordKind(raw).Valid()
	case "RecordOwnership":
		return RecordOwnership(raw).Valid()
	default:
		t.Fatalf("unknown enum %s", name)
		return false
	}
}

func enumString(t *testing.T, name string, raw int) string {
	t.Helper()
	switch name {
	case "NamingSystem":
		return NamingSystem(raw).String()
	case "ResolutionRoute":
		return ResolutionRoute(raw).String()
	case "AuthorityLocus":
		return AuthorityLocus(raw).String()
	case "ZoneAllocation":
		return ZoneAllocation(raw).String()
	case "Lifecycle":
		return Lifecycle(raw).String()
	case "HostingRequest":
		return HostingRequest(raw).String()
	case "TargetKind":
		return TargetKind(raw).String()
	case "Requirement":
		return Requirement(raw).String()
	case "Actor":
		return Actor(raw).String()
	case "PublicationLocus":
		return PublicationLocus(raw).String()
	case "VerificationMode":
		return VerificationMode(raw).String()
	case "GateKind":
		return GateKind(raw).String()
	case "RecordKind":
		return RecordKind(raw).String()
	case "RecordOwnership":
		return RecordOwnership(raw).String()
	default:
		t.Fatalf("unknown enum %s", name)
		return ""
	}
}

// TestContentTarget covers constructor validation and DNSLink path output
// for both target kinds, characterizing the byte-exact output against
// internal/db.WebsiteTargetType.ToDNSLinkPath.
func TestContentTarget(t *testing.T) {
	// Characterized outputs: ToDNSLinkPath concatenates "/ipfs/" or
	// "/ipns/" directly in front of the hash with no trimming. Verified
	// against internal/db/website.go ToDNSLinkPath:
	//   IPFS  -> IPFSPrefix + hash = "/ipfs/" + hash
	//   IPNS  -> IPNSPrefix + hash = "/ipns/" + hash
	tests := []struct {
		name     string
		kind     TargetKind
		value    string
		wantPath string
		wantErr  bool
	}{
		{
			name:     "ipfs cid produces gateway path",
			kind:     TargetKindIPFS,
			value:    "bafkreibm2jzxaaphbe3hrz7rjad7u2aff73lvmvfmjzlxayx4s2rd6avzq",
			wantPath: "/ipfs/bafkreibm2jzxaaphbe3hrz7rjad7u2aff73lvmvfmjzlxayx4s2rd6avzq",
		},
		{
			name:     "ipns peer id produces gateway path",
			kind:     TargetKindIPNS,
			value:    "12D3KooWBpwRsSRHNszVdndyRkLuReaVfeuA4jes4zwu6LYGniQX",
			wantPath: "/ipns/12D3KooWBpwRsSRHNszVdndyRkLuReaVfeuA4jes4zwu6LYGniQX",
		},
		{
			name:     "trailing slash in value is preserved byte-exactly",
			kind:     TargetKindIPFS,
			value:    "QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco/wiki/",
			wantPath: "/ipfs/QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco/wiki/",
		},
		{
			name:     "leading slash in value is preserved byte-exactly",
			kind:     TargetKindIPNS,
			value:    "/peer.example.com",
			wantPath: "/ipns//peer.example.com",
		},
		{
			name:    "empty value rejected",
			kind:    TargetKindIPFS,
			value:   "",
			wantErr: true,
		},
		{
			name:    "unknown kind rejected",
			kind:    TargetKindUnknown,
			value:   "QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco",
			wantErr: true,
		},
		{
			name:    "out-of-range kind rejected",
			kind:    TargetKind(99),
			value:   "abc",
			wantErr: true,
		},
		{
			name:    "negative kind rejected",
			kind:    TargetKind(-5),
			value:   "abc",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewContentTarget(tc.kind, tc.value)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewContentTarget(%v, %q) succeeded, want rejection", tc.kind, tc.value)
				}
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("NewContentTarget error = %v, want ErrInvalid", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewContentTarget(%v, %q) failed: %v", tc.kind, tc.value, err)
			}
			if !got.Valid() {
				t.Fatalf("constructed target reports invalid: %+v", got)
			}
			if got.Kind != tc.kind || got.Value != tc.value {
				t.Fatalf("constructed target = %+v, want kind %v value %q", got, tc.kind, tc.value)
			}
			if path := got.DNSLinkPath(); path != tc.wantPath {
				t.Errorf("DNSLinkPath() = %q, want %q", path, tc.wantPath)
			}
		})
	}
}

// TestContentTargetDefaults documents the enum accessors for TargetKind.
func TestParseEnumFunctions(t *testing.T) {
	// The parse functions are the persistence contract for the persisted axis
	// columns: String() output must round-trip through Parse* for every valid
	// value, and any other string must fail closed.
	for _, s := range []string{"draft", "provisioning", "awaiting-proof", "active", "error", "retiring"} {
		v, err := ParseLifecycle(s)
		require.NoError(t, err)
		assert.Equal(t, s, v.String())
	}
	_, err := ParseLifecycle("active ")
	require.Error(t, err)
	_, err = ParseLifecycle("unknown")
	require.Error(t, err)

	for _, s := range []string{"portal-zone", "owner-dns", "operator-zone", "chain"} {
		v, err := ParseAuthorityLocus(s)
		require.NoError(t, err)
		assert.Equal(t, s, v.String())
	}
	_, err = ParseAuthorityLocus("chain2")
	require.Error(t, err)

	for _, s := range []string{"standard-dns", "hns-root", "cross-chain"} {
		v, err := ParseResolutionRoute(s)
		require.NoError(t, err)
		assert.Equal(t, s, v.String())
	}
	_, err = ParseResolutionRoute("bogus")
	require.Error(t, err)

	for _, s := range []string{"portal", "owner"} {
		v, err := ParseHostingRequest(s)
		require.NoError(t, err)
		assert.Equal(t, s, v.String())
	}
	_, err = ParseHostingRequest("")
	require.Error(t, err)
}

func TestTargetKindStrings(t *testing.T) {
	if got := TargetKindUnknown.String(); got != "unknown" {
		t.Errorf("TargetKindUnknown.String() = %q, want %q", got, "unknown")
	}
	if TargetKindUnknown.Valid() {
		t.Error("TargetKindUnknown should not be valid")
	}
}

// TestBackendAndProfileIDs covers the string identifiers.
func TestBackendAndProfileIDs(t *testing.T) {
	if BackendID("").Valid() || ProfileID("").Valid() {
		t.Fatal("empty identifiers must not be valid")
	}
	if _, err := NewBackendID(EmptyBackendID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("NewBackendID(\"\") error = %v, want ErrInvalid", err)
	}
	if _, err := NewProfileID(EmptyProfileID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("NewProfileID(\"\") error = %v, want ErrInvalid", err)
	}

	b, err := NewBackendID("system-dns")
	if err != nil {
		t.Fatalf("NewBackendID failed: %v", err)
	}
	if b.String() != "system-dns" || b != BackendID("system-dns") {
		t.Fatalf("backend round trip failed: %q", b)
	}

	p, err := NewProfileID("icann.portal.current-v1")
	if err != nil {
		t.Fatalf("NewProfileID failed: %v", err)
	}
	if p.String() != "icann.portal.current-v1" || p != ProfileID("icann.portal.current-v1") {
		t.Fatalf("profile round trip failed: %q", p)
	}
}

// TestProfileVersion covers the positive-integer version type.
func TestProfileVersion(t *testing.T) {
	for _, bad := range []int{0, -1, -42} {
		if _, err := NewProfileVersion(bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("NewProfileVersion(%d) error = %v, want ErrInvalid", bad, err)
		}
		if ProfileVersion(bad).Valid() {
			t.Errorf("ProfileVersion(%d).Valid() = true, want false", bad)
		}
	}
	for _, good := range []int{1, 2, 100} {
		v, err := NewProfileVersion(good)
		if err != nil {
			t.Fatalf("NewProfileVersion(%d) failed: %v", good, err)
		}
		if !v.Valid() {
			t.Errorf("ProfileVersion(%d).Valid() = false, want true", good)
		}
		if got, want := v.String(), itoaGood(good); got != want {
			t.Errorf("ProfileVersion(%d).String() = %q, want %q", good, got, want)
		}
	}
}

func itoaGood(v int) string {
	if v == 1 {
		return "1"
	}
	if v == 2 {
		return "2"
	}
	return "100"
}

// TestGateAndProofConstructors flips over gate/proof constructors and
// ProofExpression building.
func TestGateAndProofConstructors(t *testing.T) {
	if _, err := NewGateProof(GateKindUnknown); !errors.Is(err, ErrInvalid) {
		t.Fatalf("NewGateProof(unknown) error = %v, want ErrInvalid", err)
	}
	if _, err := NewGateProof(GateKind(-1)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("NewGateProof(-1) error = %v, want ErrInvalid", err)
	}
	for _, g := range []GateKind{GateDNSLink, GateChallengeTXT, GateNSDelegation, GateDSDelegation, GateTLSA, GatePlatformTrust, GatePartnerAttestation} {
		if _, err := NewGateProof(g); err != nil {
			t.Fatalf("NewGateProof(%v) failed: %v", g, err)
		}
	}

	// Empty expressions are rejected.
	if _, err := NewAllOf(nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("NewAllOf(nil) error = %v, want ErrInvalid", err)
	}
	if _, err := NewAnyOf([]ProofExpression{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("NewAnyOf(empty) error = %v, want ErrInvalid", err)
	}

	// Unknown gates inside children are rejected.
	badChild := ProofExpression{Gate: GateKind(99)}
	if _, err := NewAllOf([]ProofExpression{badChild}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("NewAllOf(unknown-gate child) error = %v, want ErrInvalid", err)
	}

	// Empty composite children are rejected.
	empty := ProofExpression{}
	if _, err := NewAnyOf([]ProofExpression{empty}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("NewAnyOf(empty child) error = %v, want ErrInvalid", err)
	}

	validDNSLink, err := NewGateProof(GateDNSLink)
	if err != nil {
		t.Fatalf("NewGateProof(DNSLink) failed: %v", err)
	}
	validTLSA, err := NewGateProof(GateTLSA)
	if err != nil {
		t.Fatalf("NewGateProof(TLSA) failed: %v", err)
	}
	any, err := NewAnyOf([]ProofExpression{validDNSLink, validTLSA})
	if err != nil {
		t.Fatalf("NewAnyOf failed: %v", err)
	}
	all, err := NewAllOf([]ProofExpression{any, validTLSA})
	if err != nil {
		t.Fatalf("NewAllOf failed: %v", err)
	}
	got := all.Gates()
	if len(got) != 2 || got[0] != GateDNSLink || got[1] != GateTLSA {
		t.Fatalf("all.Gates() = %v, want [dnslink tlsa]", got)
	}
	if all.IsEmpty() {
		t.Error("composite expression should not be empty")
	}

	// A hand-built inconsistent expression must be identifyable as invalid
	// when submitted as a child.
	mixed := ProofExpression{AllOf: []ProofExpression{validDNSLink}, Gate: GateTLSA}
	if verr := validateExpression(mixed, "test"); verr == nil {
		t.Fatal("mixed AllOf+Gate child accepted")
	} else if !errors.Is(verr, ErrInvalid) {
		t.Fatalf("mixed child error = %v, want ErrInvalid", verr)
	}
}

// TestRecordIntentCoversConstructors covers record intents.
func TestRecordIntentConstructors(t *testing.T) {
	good, err := NewRecordIntent(RecordKindDNSLink, "_dnslink", "dnslink=/ipns/abc", 300, RecordOwnershipBindingContent)
	if err != nil {
		t.Fatalf("NewRecordIntent failed: %v", err)
	}
	if !good.Valid() {
		t.Fatal("good record intent reports invalid")
	}

	tests := []struct {
		name string
		kind RecordKind
		n    string
		v    string
		o    RecordOwnership
	}{
		{"unknown kind", RecordKindUnknown, "_dnslink", "x", RecordOwnershipBindingContent},
		{"out-of-range kind", RecordKind(99), "_dnslink", "x", RecordOwnershipBindingContent},
		{"empty name", RecordKindDNSLink, "", "x", RecordOwnershipBindingContent},
		{"empty value", RecordKindDNSLink, "_dnslink", "", RecordOwnershipBindingContent},
		{"unknown ownership", RecordKindDNSLink, "_dnslink", "x", RecordOwnershipUnknown},
		{"out-of-range ownership", RecordKindDNSLink, "_dnslink", "x", RecordOwnership(77)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRecordIntent(tc.kind, tc.n, tc.v, 300, tc.o); !errors.Is(err, ErrInvalid) {
				t.Fatalf("NewRecordIntent error = %v, want ErrInvalid", err)
			}
		})
	}
}
