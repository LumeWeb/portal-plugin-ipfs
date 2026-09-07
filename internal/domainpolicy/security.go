package domainpolicy

// Requirement expresses whether a security duty (DNSSEC or DANE) applies to
// a binding under a profile.
type Requirement int

const (
	RequirementUnknown Requirement = iota
	RequirementNotApplicable
	RequirementOptional
	RequirementRequired
)

// String returns a stable diagnostic name for the requirement.
func (r Requirement) String() string {
	switch r {
	case RequirementNotApplicable:
		return "not-applicable"
	case RequirementOptional:
		return "optional"
	case RequirementRequired:
		return "required"
	default:
		return "unknown"
	}
}

// Valid reports whether the requirement is a known, non-unknown value.
func (r Requirement) Valid() bool {
	switch r {
	case RequirementNotApplicable, RequirementOptional, RequirementRequired:
		return true
	default:
		return false
	}
}

// NewRequirement validates and returns the given requirement, rejecting
// unknown values.
func NewRequirement(r Requirement) (Requirement, error) {
	if !r.Valid() {
		return RequirementUnknown, newInvalid("requirement", "unknown value %d", int(r))
	}
	return r, nil
}

// Actor identifies the party responsible for a security duty.
type Actor int

const (
	ActorUnknown Actor = iota
	ActorPortal
	ActorOwner
	ActorOperator
	ActorExternalBackend
)

// String returns a stable diagnostic name for the actor.
func (a Actor) String() string {
	switch a {
	case ActorPortal:
		return "portal"
	case ActorOwner:
		return "owner"
	case ActorOperator:
		return "operator"
	case ActorExternalBackend:
		return "external-backend"
	default:
		return "unknown"
	}
}

// Valid reports whether the actor is a known, non-unknown value.
func (a Actor) Valid() bool {
	switch a {
	case ActorPortal, ActorOwner, ActorOperator, ActorExternalBackend:
		return true
	default:
		return false
	}
}

// NewActor validates and returns the given actor, rejecting unknown values.
func NewActor(a Actor) (Actor, error) {
	if !a.Valid() {
		return ActorUnknown, newInvalid("actor", "unknown value %d", int(a))
	}
	return a, nil
}

// PublicationLocus names where security evidence is published. None means no
// publication is performed by or for this binding.
type PublicationLocus int

const (
	PublicationLocusUnknown PublicationLocus = iota
	PublicationLocusNone
	PublicationLocusPortalZone
	PublicationLocusOwnerDNS
	PublicationLocusChain
)

// String returns a stable diagnostic name for the publication locus.
func (p PublicationLocus) String() string {
	switch p {
	case PublicationLocusNone:
		return "none"
	case PublicationLocusPortalZone:
		return "portal-zone"
	case PublicationLocusOwnerDNS:
		return "owner-dns"
	case PublicationLocusChain:
		return "chain"
	default:
		return "unknown"
	}
}

// Valid reports whether the publication locus is a known, non-unknown value.
func (p PublicationLocus) Valid() bool {
	switch p {
	case PublicationLocusNone, PublicationLocusPortalZone, PublicationLocusOwnerDNS, PublicationLocusChain:
		return true
	default:
		return false
	}
}

// NewPublicationLocus validates and returns the given publication locus,
// rejecting unknown values.
func NewPublicationLocus(p PublicationLocus) (PublicationLocus, error) {
	if !p.Valid() {
		return PublicationLocusUnknown, newInvalid("publication locus", "unknown value %d", int(p))
	}
	return p, nil
}

// VerificationMode names how a security duty is verified. None means the
// duty is not verified (and therefore not enforced) for this binding.
type VerificationMode int

const (
	VerificationModeUnknown VerificationMode = iota
	VerificationModeNone
	VerificationModeResolveDNS
	VerificationModeResolveHNS
	VerificationModeResolveChain
	VerificationModePartnerAttestation
)

// String returns a stable diagnostic name for the verification mode.
func (v VerificationMode) String() string {
	switch v {
	case VerificationModeNone:
		return "none"
	case VerificationModeResolveDNS:
		return "resolve-dns"
	case VerificationModeResolveHNS:
		return "resolve-hns"
	case VerificationModeResolveChain:
		return "resolve-chain"
	case VerificationModePartnerAttestation:
		return "partner-attestation"
	default:
		return "unknown"
	}
}

// Valid reports whether the verification mode is a known, non-unknown value.
func (v VerificationMode) Valid() bool {
	switch v {
	case VerificationModeNone, VerificationModeResolveDNS, VerificationModeResolveHNS, VerificationModeResolveChain, VerificationModePartnerAttestation:
		return true
	default:
		return false
	}
}

// NewVerificationMode validates and returns the given verification mode,
// rejecting unknown values.
func NewVerificationMode(v VerificationMode) (VerificationMode, error) {
	if !v.Valid() {
		return VerificationModeUnknown, newInvalid("verification mode", "unknown value %d", int(v))
	}
	return v, nil
}

// SecurityPlan captures who provisions security evidence, where it is
// published, and how it is verified for one security axis (DNSSEC or DANE)
// under a profile. It is a value type with no runtime handles.
type SecurityPlan struct {
	// Requirement states whether the duty applies.
	Requirement Requirement
	// Provisioner is the actor responsible for provisioning the evidence.
	Provisioner Actor
	// Publication is where the evidence is published.
	Publication PublicationLocus
	// Verification is how compliance with the duty is checked.
	Verification VerificationMode
}

// NewSecurityPlan validates and returns a SecurityPlan. Required security
// must have a provisioner, a publication locus, and a verifier.
// NotApplicable security must use none for all three: no provisioner
// (ActorUnknown), PublicationLocusNone, and VerificationModeNone. Other
// combinations reject every unknown member.
func NewSecurityPlan(requirement Requirement, provisioner Actor, publication PublicationLocus, verification VerificationMode) (SecurityPlan, error) {
	if !requirement.Valid() {
		return SecurityPlan{}, newInvalid("security plan", "unknown requirement %d", int(requirement))
	}
	switch requirement {
	case RequirementRequired:
		if !provisioner.Valid() {
			return SecurityPlan{}, newInvalid("security plan", "required security must have a provisioner; got provisioner %d", int(provisioner))
		}
		if publication == PublicationLocusUnknown || publication == PublicationLocusNone {
			return SecurityPlan{}, newInvalid("security plan", "required security must have a publication locus; got publication %d", int(publication))
		}
		if verification == VerificationModeUnknown || verification == VerificationModeNone {
			return SecurityPlan{}, newInvalid("security plan", "required security must have a verifier; got verification %d", int(verification))
		}
	case RequirementNotApplicable:
		if provisioner != ActorUnknown {
			return SecurityPlan{}, newInvalid("security plan", "not-applicable security must have no provisioner; got provisioner %d", int(provisioner))
		}
		if publication != PublicationLocusNone {
			return SecurityPlan{}, newInvalid("security plan", "not-applicable security must use no publication locus; got publication %d", int(publication))
		}
		if verification != VerificationModeNone {
			return SecurityPlan{}, newInvalid("security plan", "not-applicable security must use no verifier; got verification %d", int(verification))
		}
	case RequirementOptional:
		if !provisioner.Valid() {
			return SecurityPlan{}, newInvalid("security plan", "optional security must have a provisioner; got provisioner %d", int(provisioner))
		}
		if publication == PublicationLocusUnknown {
			return SecurityPlan{}, newInvalid("security plan", "optional security must have a publication locus; got publication %d", int(publication))
		}
		if verification == VerificationModeUnknown {
			return SecurityPlan{}, newInvalid("security plan", "optional security must have a verification mode; got verification %d", int(verification))
		}
	}
	return SecurityPlan{
		Requirement:  requirement,
		Provisioner:  provisioner,
		Publication:  publication,
		Verification: verification,
	}, nil
}
