package domainpolicy

import (
	"fmt"
	"sort"
)

// Flow names the validation flow a gate belongs to. The website validation
// flow mirrors WebsiteService.ValidateDNS (DNSLink, challenge TXT, delegation,
// on-chain TLSA) and the domain verification flow mirrors
// DelegatedDomainService.VerifyDomain (platform trust, DNSSEC, delegation).
// Keeping them explicit prevents Evaluate from mixing gates across flows.
type Flow int

const (
	FlowUnknown Flow = iota
	// FlowWebsiteValidation is the website ValidateDNS flow.
	FlowWebsiteValidation
	// FlowDomainVerification is the domain VerifyDomain flow.
	FlowDomainVerification
)

// String returns a stable diagnostic name for the flow.
func (f Flow) String() string {
	switch f {
	case FlowWebsiteValidation:
		return "website-validation"
	case FlowDomainVerification:
		return "domain-verification"
	default:
		return "unknown"
	}
}

// Valid reports whether the flow is a known, non-unknown value.
func (f Flow) Valid() bool {
	switch f {
	case FlowWebsiteValidation, FlowDomainVerification:
		return true
	default:
		return false
	}
}

// NewFlow validates and returns the given flow, rejecting unknown values.
func NewFlow(f Flow) (Flow, error) {
	if !f.Valid() {
		return FlowUnknown, newInvalid("flow", "unknown value %d", int(f))
	}
	return f, nil
}

// RepairKind identifies a repair the plan declares (as an intent; this package
// never executes one).
type RepairKind int

const (
	RepairKindUnknown RepairKind = iota
	// RepairKindEnsureDNSSEC re-ensures zone DNSSEC (self-healed today by
	// selfHealZone for managed-DNSSEC namespaces).
	RepairKindEnsureDNSSEC
	// RepairKindEnsureSOAMNAME repairs the zone SOA MNAME (self-healed today
	// for every portal-managed PowerDNS zone).
	RepairKindEnsureSOAMNAME
	// RepairKindRotateChallenge rotates an expired validation token
	// (performed today by regenerateExpiredToken during ValidateDNS).
	RepairKindRotateChallenge
)

// String returns a stable diagnostic name for the repair kind.
func (r RepairKind) String() string {
	switch r {
	case RepairKindEnsureDNSSEC:
		return "ensure-dnssec"
	case RepairKindEnsureSOAMNAME:
		return "ensure-soa-mname"
	case RepairKindRotateChallenge:
		return "rotate-challenge"
	default:
		return "unknown"
	}
}

// Valid reports whether the repair kind is a known, non-unknown value.
func (r RepairKind) Valid() bool {
	switch r {
	case RepairKindEnsureDNSSEC, RepairKindEnsureSOAMNAME, RepairKindRotateChallenge:
		return true
	default:
		return false
	}
}

// RepairIntent declares a repair a plan may produce effects for. The Trigger
// is the observation that activates the repair; activation is decided by
// Diff, never by this expression itself.
type RepairIntent struct {
	// Kind names the repair.
	Kind RepairKind
	// Flow names which validation flow's observations may trigger it.
	Flow Flow
}

// NewRepairIntent validates and returns a RepairIntent, rejecting unknown
// kinds and flows.
func NewRepairIntent(kind RepairKind, flow Flow) (RepairIntent, error) {
	if !kind.Valid() {
		return RepairIntent{}, newInvalid("repair intent", "unknown repair kind %d", int(kind))
	}
	if !flow.Valid() {
		return RepairIntent{}, newInvalid("repair intent", "unknown flow %d", int(flow))
	}
	return RepairIntent{Kind: kind, Flow: flow}, nil
}

// GateSpec is one gate as a profile declares it: which evidence is checked and
// whether the gate currently passes without a live check. TriviallyPasses
// encodes gates that today's code performs as a no-op (for example the ICANN
// delegation check, whose provider returns success without an NS lookup); it
// is characterization data, never an instruction to add such a gate.
type GateSpec struct {
	Kind GateKind
	// TriviallyPasses reports that today's code returns success without a
	// live observation for this gate.
	TriviallyPasses bool
}

// NewGateSpec validates and returns a GateSpec, rejecting unknown gates.
func NewGateSpec(kind GateKind, triviallyPasses bool) (GateSpec, error) {
	if !kind.Valid() {
		return GateSpec{}, newInvalid("gate spec", "unknown gate %d", int(kind))
	}
	return GateSpec{Kind: kind, TriviallyPasses: triviallyPasses}, nil
}

// PlannedRecordSpec is the profile-level shape of a record intent: what kind
// of record, where it is published, who owns it, and whether its value comes
// from the binding's content target or is a fixed placeholder.
type PlannedRecordSpec struct {
	// Kind is the record type.
	Kind RecordKind
	// Name is the DNS label relative to the binding's zone apex (e.g.
	// "_dnslink", "@" for the apex, or the challenge label).
	Name string
	// Destination is the publication locus for the record.
	Destination PublicationLocus
	// Ownership names the record's owner.
	Ownership RecordOwnership
	// TTL is the desired TTL in seconds; zero means the adapter default.
	TTL uint32
	// ValueFromTarget reports that the record value is the content target's
	// DNSLink path (mutually exclusive with FixedValue).
	ValueFromTarget bool
	// FixedValue is a fixed record value placeholder (mutually exclusive
	// with ValueFromTarget).
	FixedValue string
}

// NewPlannedRecordSpec validates and returns a PlannedRecordSpec, rejecting
// unknown kinds/ownerships/destinations, empty names, and value shapes that
// are neither exactly one of target-derived or fixed.
func NewPlannedRecordSpec(kind RecordKind, name string, destination PublicationLocus, ownership RecordOwnership, ttl uint32, valueFromTarget bool, fixedValue string) (PlannedRecordSpec, error) {
	if !kind.Valid() {
		return PlannedRecordSpec{}, newInvalid("planned record spec", "unknown record kind %d", int(kind))
	}
	if name == "" {
		return PlannedRecordSpec{}, newInvalid("planned record spec", "empty record name")
	}
	if destination == PublicationLocusUnknown {
		return PlannedRecordSpec{}, newInvalid("planned record spec", "unknown publication locus")
	}
	if !ownership.Valid() {
		return PlannedRecordSpec{}, newInvalid("planned record spec", "unknown record ownership %d", int(ownership))
	}
	if valueFromTarget && fixedValue != "" {
		return PlannedRecordSpec{}, newInvalid("planned record spec", "target-derived record value must have no fixed value")
	}
	if !valueFromTarget && fixedValue == "" {
		return PlannedRecordSpec{}, newInvalid("planned record spec", "record value must be exactly one of target-derived or a fixed value")
	}
	return PlannedRecordSpec{
		Kind:            kind,
		Name:            name,
		Destination:     destination,
		Ownership:       ownership,
		TTL:             ttl,
		ValueFromTarget: valueFromTarget,
		FixedValue:      fixedValue,
	}, nil
}

// Profile describes the full static policy for a class of domain bindings:
// where authority lives, how zones are allocated, what ownership proof the
// binding must satisfy, its DNSSEC and DANE duties, and which content target
// kinds it may serve. Profiles are immutable value data produced only by
// NewProfile; every field fails closed on unknown values.
type Profile struct {
	// ID is the stable profile identity (e.g. "icann.portal.current-v1").
	ID ProfileID
	// Version is the profile's positive version.
	Version ProfileVersion
	// NamingSystem is the naming system the profile applies to.
	NamingSystem NamingSystem
	// Route is the resolution route the profile encodes.
	Route ResolutionRoute
	// Backend is the resolution backend the profile encodes.
	Backend BackendID
	// Authority names who serves the binding's DNS.
	Authority AuthorityLocus
	// ZoneAllocation names how a portal/operator zone is allocated. It is
	// the canonical allocation of a fresh binding for this profile (for
	// portal profiles, the apex zone topology).
	ZoneAllocation ZoneAllocation
	// ExtraZoneAllocations lists additional observed zone topologies the
	// profile permits. A non-platform subdomain of a portal-managed binding
	// reuses the parent's zone (the one-zone rule in resolveManagedZone), so
	// tolerant portal profiles carry the dedicated allocation as their
	// primary and shared-parent as an extra. Owner/chain authority may not
	// declare extras.
	ExtraZoneAllocations []ZoneAllocation
	// Ownership is the proof expression authorizing the binding.
	Ownership ProofExpression
	// DNSSEC is the DNSSEC security plan.
	DNSSEC SecurityPlan
	// DANE is the DANE security plan.
	DANE SecurityPlan
	// AllowedTargets lists the content target kinds the profile may serve.
	AllowedTargets []TargetKind
	// WebsiteGates is the ordered website-validation gate sequence (the
	// ValidateDNS flow order).
	WebsiteGates []GateSpec
	// DomainGates is the ordered domain-verification gate sequence (the
	// VerifyDomain flow order).
	DomainGates []GateSpec
	// Repairs lists the repairs the plan may produce effects for.
	Repairs []RepairIntent
	// Records lists the record shapes the plan materializes.
	Records []PlannedRecordSpec
}

// ProfileSpec is the input to NewProfile. It mirrors Profile field for field
// so that callers cannot construct a Profile directly; NewProfile validates
// every field and returns the validated value.
type ProfileSpec struct {
	ID                   ProfileID
	Version              ProfileVersion
	NamingSystem         NamingSystem
	Route                ResolutionRoute
	Backend              BackendID
	Authority            AuthorityLocus
	ZoneAllocation       ZoneAllocation
	ExtraZoneAllocations []ZoneAllocation
	Ownership            ProofExpression
	DNSSEC               SecurityPlan
	DANE                 SecurityPlan
	AllowedTargets       []TargetKind
	WebsiteGates         []GateSpec
	DomainGates          []GateSpec
	Repairs              []RepairIntent
	Records              []PlannedRecordSpec
}

// NewProfile validates the given spec and returns a Profile. It rejects
// unknown enum values, empty IDs and backends, zero or negative versions,
// empty ownership expressions and allowed-target lists, and any combination
// the current model forbids (for example a chain authority holding a
// PowerDNS zone, a portal authority with no zone intent, a TLSA ownership
// gate outside chain authority, a platform-trust gate outside operator
// authority, or a website sequence without exactly one DNSLink gate).
func NewProfile(spec ProfileSpec) (Profile, error) {
	if _, err := NewProfileID(spec.ID); err != nil {
		return Profile{}, err
	}
	if _, err := NewProfileVersion(int(spec.Version)); err != nil {
		return Profile{}, err
	}
	if _, err := NewNamingSystem(spec.NamingSystem); err != nil {
		return Profile{}, err
	}
	if _, err := NewResolutionRoute(spec.Route); err != nil {
		return Profile{}, err
	}
	if _, err := NewBackendID(spec.Backend); err != nil {
		return Profile{}, err
	}
	if _, err := NewAuthorityLocus(spec.Authority); err != nil {
		return Profile{}, err
	}
	if _, err := NewZoneAllocation(spec.ZoneAllocation); err != nil {
		return Profile{}, err
	}
	if err := validateExtraZoneAllocations(spec); err != nil {
		return Profile{}, err
	}
	if err := validateExpression(spec.Ownership, "profile ownership"); err != nil {
		return Profile{}, err
	}
	if spec.Ownership.IsEmpty() {
		return Profile{}, newInvalid("profile", "empty ownership expression")
	}
	if _, err := NewSecurityPlan(spec.DNSSEC.Requirement, spec.DNSSEC.Provisioner, spec.DNSSEC.Publication, spec.DNSSEC.Verification); err != nil {
		return Profile{}, newInvalid("profile", "invalid DNSSEC plan: %s", err)
	}
	if _, err := NewSecurityPlan(spec.DANE.Requirement, spec.DANE.Provisioner, spec.DANE.Publication, spec.DANE.Verification); err != nil {
		return Profile{}, newInvalid("profile", "invalid DANE plan: %s", err)
	}
	if err := validateAllowedTargets(spec.AllowedTargets); err != nil {
		return Profile{}, err
	}
	if err := validateGateSequences(spec.WebsiteGates, spec.DomainGates); err != nil {
		return Profile{}, err
	}
	for _, repair := range spec.Repairs {
		if _, err := NewRepairIntent(repair.Kind, repair.Flow); err != nil {
			return Profile{}, newInvalid("profile", "invalid repair: %s", err)
		}
	}
	if err := validateRecordSpecs(spec.Records); err != nil {
		return Profile{}, err
	}
	if err := validateProfileCombinations(spec); err != nil {
		return Profile{}, err
	}

	profile := Profile{
		ID:                   spec.ID,
		Version:              spec.Version,
		NamingSystem:         spec.NamingSystem,
		Route:                spec.Route,
		Backend:              spec.Backend,
		Authority:            spec.Authority,
		ZoneAllocation:       spec.ZoneAllocation,
		ExtraZoneAllocations: append([]ZoneAllocation(nil), spec.ExtraZoneAllocations...),
		Ownership:            cloneProofExpression(spec.Ownership),
		DNSSEC:               spec.DNSSEC,
		DANE:                 spec.DANE,
		AllowedTargets:       append([]TargetKind(nil), spec.AllowedTargets...),
		WebsiteGates:         append([]GateSpec(nil), spec.WebsiteGates...),
		DomainGates:          append([]GateSpec(nil), spec.DomainGates...),
		Repairs:              append([]RepairIntent(nil), spec.Repairs...),
		Records:              append([]PlannedRecordSpec(nil), spec.Records...),
	}
	return profile, nil
}

// AllowsTarget reports whether the profile may serve the given target kind.
func (p Profile) AllowsTarget(kind TargetKind) bool {
	for _, allowed := range p.AllowedTargets {
		if allowed == kind {
			return true
		}
	}
	return false
}

// AllowsZoneAllocation reports whether the profile permits a binding carrying
// the given observed zone allocation: either the profile's canonical
// allocation or one of its declared extras.
func (p Profile) AllowsZoneAllocation(allocation ZoneAllocation) bool {
	if allocation == p.ZoneAllocation {
		return true
	}
	for _, extra := range p.ExtraZoneAllocations {
		if extra == allocation {
			return true
		}
	}
	return false
}

// validateExtraZoneAllocations rejects observed-topology extras the current
// model forbids: extras on owner/chain authority (which hold no zone), extra
// values that are unknown, none, redundant with the primary, or duplicated.
func validateExtraZoneAllocations(spec ProfileSpec) error {
	if len(spec.ExtraZoneAllocations) == 0 {
		return nil
	}
	if spec.Authority == AuthorityLocusOwnerDNS || spec.Authority == AuthorityLocusChain {
		return newInvalid("profile", "owner/chain authority cannot declare extra zone allocations; got %d", len(spec.ExtraZoneAllocations))
	}
	seen := map[ZoneAllocation]bool{spec.ZoneAllocation: true}
	for _, extra := range spec.ExtraZoneAllocations {
		if _, err := NewZoneAllocation(extra); err != nil {
			return newInvalid("profile", "invalid extra zone allocation: %s", err)
		}
		if extra == ZoneAllocationNone {
			return newInvalid("profile", "extra zone allocation cannot be none for portal/operator authority")
		}
		if seen[extra] {
			return newInvalid("profile", "duplicate zone allocation %s in primary+extras", extra.String())
		}
		seen[extra] = true
	}
	return nil
}

func validateAllowedTargets(targets []TargetKind) error {
	if len(targets) == 0 {
		return newInvalid("profile", "empty allowed-target list")
	}
	seen := make(map[TargetKind]bool, len(targets))
	for _, t := range targets {
		if !t.Valid() {
			return newInvalid("profile", "unknown allowed target %d", int(t))
		}
		if seen[t] {
			return newInvalid("profile", "duplicate allowed target %s", t.String())
		}
		seen[t] = true
	}
	return nil
}

func validateGateSequences(website, domain []GateSpec) error {
	if err := validateGateSequence(website, "website"); err != nil {
		return err
	}
	if err := validateGateSequence(domain, "domain"); err != nil {
		return err
	}
	dnsLinkCount := 0
	for _, g := range website {
		if g.Kind == GateDNSLink {
			dnsLinkCount++
		}
	}
	if dnsLinkCount != 1 {
		return newInvalid("profile", "website gate sequence must contain exactly one DNSLink gate; got %d", dnsLinkCount)
	}
	return nil
}

func validateGateSequence(gates []GateSpec, flowName string) error {
	seen := make(map[GateKind]bool, len(gates))
	for _, g := range gates {
		if !g.Kind.Valid() {
			return newInvalid("profile", "unknown %s gate %d", flowName, int(g.Kind))
		}
		if seen[g.Kind] {
			return newInvalid("profile", "duplicate %s gate %s", flowName, g.Kind.String())
		}
		seen[g.Kind] = true
	}
	return nil
}

func validateRecordSpecs(records []PlannedRecordSpec) error {
	for _, r := range records {
		if _, err := NewPlannedRecordSpec(r.Kind, r.Name, r.Destination, r.Ownership, r.TTL, r.ValueFromTarget, r.FixedValue); err != nil {
			return newInvalid("profile", "invalid record spec: %s", err)
		}
	}
	return nil
}

// validateProfileCombinations rejects the cross-field combinations the model
// forbids. Each rule below encodes an invariant from the architecture docs.
func validateProfileCombinations(spec ProfileSpec) error {
	portalOrOperator := spec.Authority == AuthorityLocusPortalZone || spec.Authority == AuthorityLocusOperatorZone
	cloudless := spec.Authority == AuthorityLocusOwnerDNS || spec.Authority == AuthorityLocusChain
	switch {
	case portalOrOperator && (spec.ZoneAllocation == ZoneAllocationNone || spec.ZoneAllocation == ZoneAllocationUnknown):
		return newInvalid("profile", "authority %s requires a dedicated or shared-parent zone intent; got allocation %s", spec.Authority.String(), spec.ZoneAllocation.String())
	case cloudless && spec.ZoneAllocation != ZoneAllocationNone:
		return newInvalid("profile", "authority %s cannot hold a zone; got allocation %s", spec.Authority.String(), spec.ZoneAllocation.String())
	case spec.Authority == AuthorityLocusChain && spec.Route != ResolutionRouteCrossChain:
		return newInvalid("profile", "chain authority requires the cross-chain route; got route %s", spec.Route.String())
	}

	ownershipGates := spec.Ownership.Gates()
	ownershipGates = append(ownershipGates, gateKinds(spec.WebsiteGates)...)
	ownershipGates = append(ownershipGates, gateKinds(spec.DomainGates)...)
	hasGate := func(g GateKind) bool {
		for _, candidate := range spec.Ownership.Gates() {
			if candidate == g {
				return true
			}
		}
		return false
	}
	hasGateInAny := func(g GateKind) bool {
		for _, candidate := range ownershipGates {
			if candidate == g {
				return true
			}
		}
		return false
	}
	if hasGateInAny(GateTLSA) {
		if spec.DANE.Requirement == RequirementNotApplicable {
			return newInvalid("profile", "TLSA gate requires a DANE plan that is not not-applicable")
		}
		if spec.Authority != AuthorityLocusChain {
			return newInvalid("profile", "live TLSA validation is only encoded for chain authority today; got authority %s", spec.Authority.String())
		}
	}
	if hasGate(GateChallengeTXT) && spec.NamingSystem != NamingSystemICANN {
		return newInvalid("profile", "challenge TXT gate is kept only for ICANN profiles today; got naming system %s", spec.NamingSystem.String())
	}
	if hasGateInAny(GatePlatformTrust) && spec.Authority != AuthorityLocusOperatorZone {
		return newInvalid("profile", "platform trust gate requires operator-zone authority; got authority %s", spec.Authority.String())
	}
	if hasGateInAny(GatePartnerAttestation) {
		return newInvalid("profile", "partner attestation is not an approved proof for any current profile")
	}
	if spec.DANE.Requirement == RequirementRequired && hasGate(GateDNSLink) && !hasGateInAny(GateTLSA) {
		// Chain profiles must carry the TLSA gate when they require DANE.
		return newInvalid("profile", "required DANE with chain authority must include the TLSA gate")
	}
	return nil
}

func gateKinds(gates []GateSpec) []GateKind {
	out := make([]GateKind, 0, len(gates))
	for _, g := range gates {
		out = append(out, g.Kind)
	}
	return out
}

// cloneProofExpression deep-copies an expression so profiles cannot be
// mutated through their specification slices.
func cloneProofExpression(e ProofExpression) ProofExpression {
	out := ProofExpression{Gate: e.Gate}
	for _, child := range e.AllOf {
		out.AllOf = append(out.AllOf, cloneProofExpression(child))
	}
	for _, child := range e.AnyOf {
		out.AnyOf = append(out.AnyOf, cloneProofExpression(child))
	}
	return out
}

// Registry holds profiles by ID. Registration is a startup activity: it
// rejects duplicate IDs, zero versions, empty backend IDs, and invalid
// profiles, and a registration error must be treated as fatal by startup
// code so an invalid profile can never silently weaken a gate.
type Registry struct {
	profiles map[ProfileID]Profile
}

// NewRegistry returns an empty profile registry.
func NewRegistry() *Registry {
	return &Registry{profiles: make(map[ProfileID]Profile)}
}

// Register validates that the profile is fully populated and stores it,
// rejecting duplicate IDs. Constructed profiles carry validation from
// NewProfile; Register re-checks the identifying fields so a zero-value
// Profile can never enter a registry.
func (r *Registry) Register(profile Profile) error {
	if r == nil || r.profiles == nil {
		return newInvalid("registry", "registry is not initialized")
	}
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
	if _, exists := r.profiles[profile.ID]; exists {
		return newInvalid("registry", "duplicate profile ID %q", profile.ID.String())
	}
	r.profiles[profile.ID] = profile
	return nil
}

// Lookup returns the profile registered under the given ID.
func (r *Registry) Lookup(id ProfileID) (Profile, bool) {
	if r == nil || r.profiles == nil {
		return Profile{}, false
	}
	profile, ok := r.profiles[id]
	return profile, ok
}

// IDs returns every registered profile ID in sorted order so callers observe
// a deterministic registry.
func (r *Registry) IDs() []ProfileID {
	if r == nil || r.profiles == nil {
		return nil
	}
	out := make([]ProfileID, 0, len(r.profiles))
	for id := range r.profiles {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

var defaultRegistry = NewRegistry()

// DefaultRegistry returns the process-wide profile registry holding the
// built-in current-behavior profiles.
func DefaultRegistry() *Registry {
	return defaultRegistry
}

// RegisterDefault registers a profile in the process-wide registry.
func RegisterDefault(profile Profile) error {
	return defaultRegistry.Register(profile)
}

// mustRegister registers built-in profiles and panics on any rejection: an
// invalid built-in profile is a programming error and startup must fail
// closed rather than silently drop a gate.
func mustRegister(profiles ...Profile) {
	for _, profile := range profiles {
		if err := RegisterDefault(profile); err != nil {
			panic(fmt.Sprintf("domainpolicy: builtin profile registration failed: %s", err))
		}
	}
}
