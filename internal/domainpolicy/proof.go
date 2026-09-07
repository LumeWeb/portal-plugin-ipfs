package domainpolicy

// GateKind identifies a gate: a piece of evidence a plan may require before
// a binding is considered compliant.
type GateKind int

const (
	GateKindUnknown GateKind = iota
	// GateDNSLink requires the DNSLink TXT record to match the desired
	// content target.
	GateDNSLink
	// GateChallengeTXT requires the portal-account challenge TXT record.
	GateChallengeTXT
	// GateNSDelegation requires NS records delegating to the portal zone.
	GateNSDelegation
	// GateDSDelegation requires a DS record delegating a DNSSEC chain.
	GateDSDelegation
	// GateTLSA requires a TLSA record proving the DANE identity.
	GateTLSA
	// GatePlatformTrust requires the platform trust relationship to hold.
	GatePlatformTrust
	// GatePartnerAttestation requires a partner-backend attestation.
	GatePartnerAttestation
)

// String returns a stable diagnostic name for the gate kind.
func (g GateKind) String() string {
	switch g {
	case GateDNSLink:
		return "dnslink"
	case GateChallengeTXT:
		return "challenge-txt"
	case GateNSDelegation:
		return "ns-delegation"
	case GateDSDelegation:
		return "ds-delegation"
	case GateTLSA:
		return "tlsa"
	case GatePlatformTrust:
		return "platform-trust"
	case GatePartnerAttestation:
		return "partner-attestation"
	default:
		return "unknown"
	}
}

// Valid reports whether the gate kind is a known, non-unknown value.
func (g GateKind) Valid() bool {
	switch g {
	case GateDNSLink, GateChallengeTXT, GateNSDelegation, GateDSDelegation, GateTLSA, GatePlatformTrust, GatePartnerAttestation:
		return true
	default:
		return false
	}
}

// NewGateKind validates and returns the given gate kind, rejecting unknown
// values.
func NewGateKind(g GateKind) (GateKind, error) {
	if !g.Valid() {
		return GateKindUnknown, newInvalid("gate kind", "unknown value %d", int(g))
	}
	return g, nil
}

// ProofExpression is a pure logical expression over gates: either a leaf
// gate, an all-of conjunction, or an any-of disjunction. Exactly one of
// AllOf, AnyOf, or Gate must be populated; empty children are rejected.
type ProofExpression struct {
	// AllOf holds the children that must all hold. Mutually exclusive with
	// AnyOf and Gate.
	AllOf []ProofExpression
	// AnyOf holds the children of which at least one must hold. Mutually
	// exclusive with AllOf and Gate.
	AnyOf []ProofExpression
	// Gate is the leaf gate. Mutually exclusive with AllOf and AnyOf.
	Gate GateKind
}

// NewGateProof validates and returns a leaf proof expression for the given
// gate, rejecting unknown gates.
func NewGateProof(g GateKind) (ProofExpression, error) {
	if !g.Valid() {
		return ProofExpression{}, newInvalid("proof expression", "unknown gate %d", int(g))
	}
	return ProofExpression{Gate: g}, nil
}

// NewAllOf validates and returns an all-of proof expression, rejecting empty
// child lists and invalid children.
func NewAllOf(children []ProofExpression) (ProofExpression, error) {
	return newCompound(allOf, children)
}

// NewAnyOf validates and returns an any-of proof expression, rejecting empty
// child lists and invalid children.
func NewAnyOf(children []ProofExpression) (ProofExpression, error) {
	return newCompound(anyOf, children)
}

type compoundKind int

const (
	allOf compoundKind = iota
	anyOf
)

func (c compoundKind) name() string {
	if c == allOf {
		return "all-of"
	}
	return "any-of"
}

func newCompound(kind compoundKind, children []ProofExpression) (ProofExpression, error) {
	if len(children) == 0 {
		return ProofExpression{}, newInvalid("proof expression", "empty %s expression", kind.name())
	}
	expr := ProofExpression{}
	clone := make([]ProofExpression, 0, len(children))
	for _, child := range children {
		if err := validateExpression(child, kind.name()); err != nil {
			return ProofExpression{}, err
		}
		clone = append(clone, child)
	}
	if kind == allOf {
		expr.AllOf = clone
	} else {
		expr.AnyOf = clone
	}
	return expr, nil
}

// validateExpression checks that an expression is safely constructible
// before it is accepted as a child (or built by a compound constructor).
func validateExpression(e ProofExpression, context string) error {
	hasAll := len(e.AllOf) > 0
	hasAny := len(e.AnyOf) > 0
	hasGate := e.Gate != GateKindUnknown
	switch {
	case hasAll && hasAny, hasAll && hasGate, hasAny && hasGate:
		return newInvalid("proof expression", "%s child sets multiple exclusive fields", context)
	case !hasAll && !hasAny && !hasGate:
		return newInvalid("proof expression", "%s child is empty", context)
	case hasAll && e.Gate != GateKindUnknown:
		return newInvalid("proof expression", "%s child mixes AllOf with a gate", context)
	case hasAny && e.Gate != GateKindUnknown:
		return newInvalid("proof expression", "%s child mixes AnyOf with a gate", context)
	case hasGate && !e.Gate.Valid():
		return newInvalid("proof expression", "%s child has unknown gate %d", context, int(e.Gate))
	}
	return nil
}

// IsEmpty reports whether the expression carries no meaningful content.
func (e ProofExpression) IsEmpty() bool {
	return len(e.AllOf) == 0 && len(e.AnyOf) == 0 && e.Gate == GateKindUnknown
}

// Gates returns every leaf gate that appears in the expression, in first
// appearance order with duplicates removed. It is used for tests and
// diagnostics only.
func (e ProofExpression) Gates() []GateKind {
	seen := make(map[GateKind]bool)
	var out []GateKind
	appendGates := func(gates []GateKind) {
		for _, g := range gates {
			if !seen[g] {
				seen[g] = true
				out = append(out, g)
			}
		}
	}
	appendGates(e.gatesRec())
	return out
}

func (e ProofExpression) gatesRec() []GateKind {
	var out []GateKind
	if !e.Gate.Valid() && len(e.AllOf) == 0 && len(e.AnyOf) == 0 {
		return out
	}
	if e.Gate.Valid() {
		out = append(out, e.Gate)
		return out
	}
	for _, child := range e.AllOf {
		out = append(out, child.gatesRec()...)
	}
	for _, child := range e.AnyOf {
		out = append(out, child.gatesRec()...)
	}
	return out
}
