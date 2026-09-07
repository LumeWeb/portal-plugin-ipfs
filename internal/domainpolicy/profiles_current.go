package domainpolicy

import "fmt"

// Backend identity constants for the currently wired resolution backends.
// Backends are opaque adapter-level identities; new chains join by registering
// a new profile with a new BackendID, never by adding a branch.
const (
	BackendSystemDNS BackendID = "system-dns"
	BackendHNSRoot   BackendID = "hns-root"
	BackendEthereum  BackendID = "ethereum"
	// BackendPowerDNS names the portal/operator PowerDNS resolution backend
	// used by portal and platform zones.
	BackendPowerDNS BackendID = "powerdns"
)

// Built-in current-behavior profile IDs. These are the only profile IDs
// registered in the default registry; corrected or experimental profiles must
// be constructed in test fixtures with separate IDs and never registered.
const (
	ProfileIDICANNPortal            ProfileID = "icann.portal.current-v1"
	ProfileIDICANNOwner             ProfileID = "icann.owner.current-v1"
	ProfileIDHNSPortalNative        ProfileID = "hns.portal.native.current-v1"
	ProfileIDHNSPortalNamebaseChild ProfileID = "hns.portal.namebase-child.current-v1"
	ProfileIDHNSOwnerNative         ProfileID = "hns.owner.native.current-v1"
	ProfileIDHNSChainEthereum       ProfileID = "hns.chain.ethereum.current-v1"
	ProfileIDPlatformICANN          ProfileID = "platform.icann.current-v1"
	ProfileIDPlatformHNSNative      ProfileID = "platform.hns.native.current-v1"
)

// Record name and value placeholders used by the current profiles. The
// challenge label defaults to the website service's token key (see
// WebsiteServiceDefault.verificationTokenKey); adapters substitute the
// configured value. Record values that depend on deployment configuration are
// stable placeholders an adapter materializes.
const (
	ChallengeRecordLabel      = "lumeweb-verify"
	ChallengeValuePlaceholder = "<validation-token>"
	ApexALIASPlaceholder      = "<gateway-host>"
	ApexAPlaceholder          = "<portal-gateway-address>"
	TLSAValuePlaceholder      = "<dane-tlsa-rdata>"
	// TLSARecordName is the TLSA label relative to the binding name for the
	// portal's DANE TLSA record (port 443, tcp).
	TLSARecordName = "_443._tcp"
	// DefaultRecordTTL mirrors the 300-second TTL the website DNS writer
	// uses today.
	DefaultRecordTTL uint32 = 300
)

// ProfileVersion1 is the current version of every built-in profile.
const ProfileVersion1 = ProfileVersion(1)

func gate(kind GateKind) GateSpec        { return GateSpec{Kind: kind} }
func trivialGate(kind GateKind) GateSpec { return GateSpec{Kind: kind, TriviallyPasses: true} }

// gateAllOf builds an all-of ownership expression from leaf gates. Shape
// validation still happens in NewProfile; this helper only assembles.
func gateAllOf(gates ...GateKind) ProofExpression {
	expr := ProofExpression{}
	for _, g := range gates {
		expr.AllOf = append(expr.AllOf, ProofExpression{Gate: g})
	}
	return expr
}

// icannPortalProfile encodes the current ICANN Pinner-managed-DNS behavior:
// Pinner writes DNSLink, the apex ALIAS, and the challenge TXT into a
// PowerDNS zone allocated per the one-zone rule — an apex binding owns a
// dedicated zone (the canonical allocation) while a subdomain of a
// portal-managed binding reuses the parent's zone (resolveManagedZone), so
// shared-parent is permitted as an observed topology. Validation is DNSLink
// plus the challenge TXT (the TXT gate is kept — see the overhaul doc before
// removing it); the delegation gate trivially passes because
// ICANNProvider.VerifyDelegation returns true without an NS lookup; no
// DNSSEC is provisioned or required; no DANE.
func icannPortalProfile() (Profile, error) {
	ownership := gateAllOf(GateDNSLink, GateChallengeTXT)
	dnssec, err := NewSecurityPlan(RequirementNotApplicable, ActorUnknown, PublicationLocusNone, VerificationModeNone)
	if err != nil {
		return Profile{}, err
	}
	dane, err := NewSecurityPlan(RequirementNotApplicable, ActorUnknown, PublicationLocusNone, VerificationModeNone)
	if err != nil {
		return Profile{}, err
	}
	repairs, err := newRepairs(
		repairPair{Kind: RepairKindRotateChallenge, Flow: FlowWebsiteValidation},
		repairPair{Kind: RepairKindEnsureSOAMNAME, Flow: FlowDomainVerification},
	)
	if err != nil {
		return Profile{}, err
	}
	dnsLink, err := NewPlannedRecordSpec(RecordKindDNSLink, "_dnslink", PublicationLocusPortalZone, RecordOwnershipBindingContent, DefaultRecordTTL, true, "")
	if err != nil {
		return Profile{}, err
	}
	apex, err := NewPlannedRecordSpec(RecordKindApexALIAS, "@", PublicationLocusPortalZone, RecordOwnershipBindingContent, DefaultRecordTTL, false, ApexALIASPlaceholder)
	if err != nil {
		return Profile{}, err
	}
	challenge, err := NewPlannedRecordSpec(RecordKindChallengeTXT, ChallengeRecordLabel, PublicationLocusPortalZone, RecordOwnershipBindingSecurity, DefaultRecordTTL, false, ChallengeValuePlaceholder)
	if err != nil {
		return Profile{}, err
	}
	return NewProfile(ProfileSpec{
		ID:                   ProfileIDICANNPortal,
		Version:              ProfileVersion1,
		NamingSystem:         NamingSystemICANN,
		Route:                ResolutionRouteStandardDNS,
		Backend:              BackendSystemDNS,
		Authority:            AuthorityLocusPortalZone,
		ZoneAllocation:       ZoneAllocationDedicated,
		ExtraZoneAllocations: []ZoneAllocation{ZoneAllocationSharedParent},
		Ownership:            ownership,
		DNSSEC:               dnssec,
		DANE:                 dane,
		AllowedTargets:       []TargetKind{TargetKindIPFS, TargetKindIPNS},
		WebsiteGates: []GateSpec{
			gate(GateDNSLink),
			gate(GateChallengeTXT),
			// ICANNProvider.VerifyDelegation returns success without a live
			// NS check today; the gate is characterized as trivially passing.
			trivialGate(GateNSDelegation),
		},
		DomainGates: []GateSpec{
			trivialGate(GateNSDelegation),
		},
		Repairs: repairs,
		Records: []PlannedRecordSpec{dnsLink, apex, challenge},
	})
}

// icannOwnerProfile encodes the current ICANN owner-hosted-DNS behavior: no
// portal zone; the owner publishes DNSLink and the challenge TXT themselves;
// the portal requires DNSLink and the TXT gate and leaves DNSSEC to the
// owner; no DANE.
func icannOwnerProfile() (Profile, error) {
	ownership := gateAllOf(GateDNSLink, GateChallengeTXT)
	dnssec, err := NewSecurityPlan(RequirementNotApplicable, ActorUnknown, PublicationLocusNone, VerificationModeNone)
	if err != nil {
		return Profile{}, err
	}
	dane, err := NewSecurityPlan(RequirementNotApplicable, ActorUnknown, PublicationLocusNone, VerificationModeNone)
	if err != nil {
		return Profile{}, err
	}
	repairs, err := newRepairs(repairPair{Kind: RepairKindRotateChallenge, Flow: FlowWebsiteValidation})
	if err != nil {
		return Profile{}, err
	}
	dnsLink, err := NewPlannedRecordSpec(RecordKindDNSLink, "_dnslink", PublicationLocusOwnerDNS, RecordOwnershipBindingContent, DefaultRecordTTL, true, "")
	if err != nil {
		return Profile{}, err
	}
	challenge, err := NewPlannedRecordSpec(RecordKindChallengeTXT, ChallengeRecordLabel, PublicationLocusOwnerDNS, RecordOwnershipBindingSecurity, DefaultRecordTTL, false, ChallengeValuePlaceholder)
	if err != nil {
		return Profile{}, err
	}
	return NewProfile(ProfileSpec{
		ID:             ProfileIDICANNOwner,
		Version:        ProfileVersion1,
		NamingSystem:   NamingSystemICANN,
		Route:          ResolutionRouteStandardDNS,
		Backend:        BackendSystemDNS,
		Authority:      AuthorityLocusOwnerDNS,
		ZoneAllocation: ZoneAllocationNone,
		Ownership:      ownership,
		DNSSEC:         dnssec,
		DANE:           dane,
		AllowedTargets: []TargetKind{TargetKindIPFS, TargetKindIPNS},
		WebsiteGates: []GateSpec{
			gate(GateDNSLink),
			gate(GateChallengeTXT),
		},
		DomainGates: []GateSpec{},
		Repairs:     repairs,
		Records:     []PlannedRecordSpec{dnsLink, challenge},
	})
}

// hnsPortalManagedSecurityPlans returns the DNSSEC and DANE security plans
// shared by the managed native HNS profiles: Pinner signs the zone and the
// parent's live DS is verified (managed DNSSEC), and Pinner publishes TLSA
// into the signed zone while the live TLSA activation gate is an explicit
// product decision that is not made today (so DANE publication is declared
// with no live verifier; the current gap is encoded, not fixed).
func hnsPortalManagedSecurityPlans() (SecurityPlan, SecurityPlan, error) {
	dnssec, err := NewSecurityPlan(RequirementRequired, ActorPortal, PublicationLocusPortalZone, VerificationModeResolveDNS)
	if err != nil {
		return SecurityPlan{}, SecurityPlan{}, err
	}
	dane, err := NewSecurityPlan(RequirementOptional, ActorPortal, PublicationLocusPortalZone, VerificationModeNone)
	if err != nil {
		return SecurityPlan{}, SecurityPlan{}, err
	}
	return dnssec, dane, nil
}

// hnsPortalManagedRecords returns the record shapes shared by the managed
// native HNS profiles: the signed apex carries a real A record so it can hold
// an RRSIG, and the portal publishes TLSA into its own zone.
func hnsPortalManagedRecords() ([]PlannedRecordSpec, error) {
	dnsLink, err := NewPlannedRecordSpec(RecordKindDNSLink, "_dnslink", PublicationLocusPortalZone, RecordOwnershipBindingContent, DefaultRecordTTL, true, "")
	if err != nil {
		return nil, err
	}
	apex, err := NewPlannedRecordSpec(RecordKindApexA, "@", PublicationLocusPortalZone, RecordOwnershipBindingContent, DefaultRecordTTL, false, ApexAPlaceholder)
	if err != nil {
		return nil, err
	}
	tlsa, err := NewPlannedRecordSpec(RecordKindTLSA, TLSARecordName, PublicationLocusPortalZone, RecordOwnershipBindingSecurity, DefaultRecordTTL, false, TLSAValuePlaceholder)
	if err != nil {
		return nil, err
	}
	return []PlannedRecordSpec{dnsLink, apex, tlsa}, nil
}

// repairPair is a (kind, flow) pair for newRepairs.
type repairPair struct {
	Kind RepairKind
	Flow Flow
}

// newRepairs builds a repair-intent list from (kind, flow) pairs.
func newRepairs(pairs ...repairPair) ([]RepairIntent, error) {
	var repairs []RepairIntent
	for _, pair := range pairs {
		repair, err := NewRepairIntent(pair.Kind, pair.Flow)
		if err != nil {
			return nil, err
		}
		repairs = append(repairs, repair)
	}
	return repairs, nil
}

// hnsPortalManagedProfile encodes the current managed HNS behavior: a
// PowerDNS zone signed by Pinner, DNSLink plus live NS and matching DS as the
// activation gates, no TXT challenge, portal-published A apex and TLSA, and
// no live TLSA activation gate (managed-zone DANE publication is declared but
// the activation gate is an open policy decision; the DS/TLSA enforcement
// matrix lives in the invariant tests). extras names observed zone topologies
// beyond the canonical dedicated apex zone: a native HNS binding is a
// single-label TLD and always owns its zone, while a subdomain of a managed
// HNS name reuses the parent's zone (resolveManagedZone), so the Namebase-style
// child profile permits shared-parent.
func hnsPortalManagedProfile(extraZoneAllocations []ZoneAllocation) (Profile, error) {
	ownership := gateAllOf(GateDNSLink, GateNSDelegation, GateDSDelegation)
	dnssec, dane, err := hnsPortalManagedSecurityPlans()
	if err != nil {
		return Profile{}, err
	}
	repairs, err := newRepairs(
		repairPair{Kind: RepairKindEnsureDNSSEC, Flow: FlowDomainVerification},
		repairPair{Kind: RepairKindEnsureSOAMNAME, Flow: FlowDomainVerification},
	)
	if err != nil {
		return Profile{}, err
	}
	records, err := hnsPortalManagedRecords()
	if err != nil {
		return Profile{}, err
	}
	return NewProfile(ProfileSpec{
		ID:                   ProfileIDHNSPortalNative,
		Version:              ProfileVersion1,
		NamingSystem:         NamingSystemHNS,
		Route:                ResolutionRouteHNSRoot,
		Backend:              BackendHNSRoot,
		Authority:            AuthorityLocusPortalZone,
		ZoneAllocation:       ZoneAllocationDedicated,
		ExtraZoneAllocations: extraZoneAllocations,
		Ownership:            ownership,
		DNSSEC:               dnssec,
		DANE:                 dane,
		AllowedTargets:       []TargetKind{TargetKindIPFS, TargetKindIPNS},
		WebsiteGates: []GateSpec{
			gate(GateDNSLink),
			gate(GateNSDelegation),
			gate(GateDSDelegation),
		},
		// VerifyDomain order: DNSSEC (live DS) first, then delegation NS.
		DomainGates: []GateSpec{
			gate(GateDSDelegation),
			gate(GateNSDelegation),
		},
		Repairs: repairs,
		Records: records,
	})
}

// hnsPortalNamebaseChildProfile encodes the current behavior for an HNS
// subdomain delegated through a partner registrar (Namebase-style): the
// delegated name gets its own full PowerDNS zone, so its gates and duties are
// identical to managed native HNS. It is a zone topology of managed HNS, not
// a new namespace. Like every portal-managed subdomain it reuses the parent's
// zone when one exists (the one-zone rule in resolveManagedZone), so
// shared-parent is permitted alongside the canonical dedicated allocation.
func hnsPortalNamebaseChildProfile() (Profile, error) {
	profile, err := hnsPortalManagedProfile([]ZoneAllocation{ZoneAllocationSharedParent})
	if err != nil {
		return Profile{}, err
	}
	profile.ID = ProfileIDHNSPortalNamebaseChild
	return profile, nil
}

// hnsOwnerNativeProfile encodes the current self-hosted HNS behavior, which
// is explicitly a known gap (see the overhaul doc's relevant row and the
// TestCurrentSelfHostedHNS_Gap_* invariants):
//
//   - DNSLink effectively gates validation; the TXT challenge and portal
//     delegation checks are skipped for HNS (UsesDelegationForOwnership).
//   - DNSSEC is not enforced today.
//   - A stable DANE identity is bootstrapped by the portal, but owner TLSA
//     publication and live TLSA validation are not enforced today (DANE is
//     optional with no publication duty enforced and no live verifier).
//
// The profile therefore adds neither a DS nor a TLSA gate. Do not "fix" this
// here: closing the gap is a separate approved policy change. Corrected
// variants exist only as test-only fixtures with separate IDs.
func hnsOwnerNativeProfile() (Profile, error) {
	ownership := gateAllOf(GateDNSLink)
	dnssec, err := NewSecurityPlan(RequirementNotApplicable, ActorUnknown, PublicationLocusNone, VerificationModeNone)
	if err != nil {
		return Profile{}, err
	}
	dane, err := NewSecurityPlan(RequirementOptional, ActorPortal, PublicationLocusNone, VerificationModeNone)
	if err != nil {
		return Profile{}, err
	}
	dnsLink, err := NewPlannedRecordSpec(RecordKindDNSLink, "_dnslink", PublicationLocusOwnerDNS, RecordOwnershipBindingContent, DefaultRecordTTL, true, "")
	if err != nil {
		return Profile{}, err
	}
	tlsa, err := NewPlannedRecordSpec(RecordKindTLSA, TLSARecordName, PublicationLocusOwnerDNS, RecordOwnershipBindingSecurity, DefaultRecordTTL, false, TLSAValuePlaceholder)
	if err != nil {
		return Profile{}, err
	}
	return NewProfile(ProfileSpec{
		ID:             ProfileIDHNSOwnerNative,
		Version:        ProfileVersion1,
		NamingSystem:   NamingSystemHNS,
		Route:          ResolutionRouteHNSRoot,
		Backend:        BackendHNSRoot,
		Authority:      AuthorityLocusOwnerDNS,
		ZoneAllocation: ZoneAllocationNone,
		Ownership:      ownership,
		DNSSEC:         dnssec,
		DANE:           dane,
		AllowedTargets: []TargetKind{TargetKindIPFS, TargetKindIPNS},
		WebsiteGates: []GateSpec{
			gate(GateDNSLink),
		},
		DomainGates: []GateSpec{},
		Repairs:     []RepairIntent{},
		Records:     []PlannedRecordSpec{dnsLink, tlsa},
	})
}

// hnsChainEthereumProfile encodes the current HIP-5 Ethereum-backed HNS
// website behavior: no portal zone (chain authority; a portal zone is
// refused), ownership proven at bind time with no TXT token, DNSLink and a
// live on-chain TLSA as the website gates, no DNSSEC (the Ethereum route has
// none), and DANE via a portal-stored identity the owner publishes on-chain
// and the portal verifies live.
func hnsChainEthereumProfile() (Profile, error) {
	ownership := gateAllOf(GateDNSLink, GateTLSA)
	dnssec, err := NewSecurityPlan(RequirementNotApplicable, ActorUnknown, PublicationLocusNone, VerificationModeNone)
	if err != nil {
		return Profile{}, err
	}
	dane, err := NewSecurityPlan(RequirementRequired, ActorOwner, PublicationLocusChain, VerificationModeResolveHNS)
	if err != nil {
		return Profile{}, err
	}
	dnsLink, err := NewPlannedRecordSpec(RecordKindDNSLink, "_dnslink", PublicationLocusChain, RecordOwnershipBindingContent, DefaultRecordTTL, true, "")
	if err != nil {
		return Profile{}, err
	}
	tlsa, err := NewPlannedRecordSpec(RecordKindTLSA, TLSARecordName, PublicationLocusChain, RecordOwnershipBindingSecurity, DefaultRecordTTL, false, TLSAValuePlaceholder)
	if err != nil {
		return Profile{}, err
	}
	return NewProfile(ProfileSpec{
		ID:             ProfileIDHNSChainEthereum,
		Version:        ProfileVersion1,
		NamingSystem:   NamingSystemHNS,
		Route:          ResolutionRouteCrossChain,
		Backend:        BackendEthereum,
		Authority:      AuthorityLocusChain,
		ZoneAllocation: ZoneAllocationNone,
		Ownership:      ownership,
		DNSSEC:         dnssec,
		DANE:           dane,
		AllowedTargets: []TargetKind{TargetKindIPFS, TargetKindIPNS},
		WebsiteGates: []GateSpec{
			gate(GateDNSLink),
			gate(GateTLSA),
		},
		DomainGates: []GateSpec{},
		Repairs:     []RepairIntent{},
		Records:     []PlannedRecordSpec{dnsLink, tlsa},
	})
}

// DerivePlatformProfile derives a platform-subdomain profile from a portal
// root profile's policy. Platform subdomains are minted under an
// operator-owned shared zone: platform trust plus DNSLink gate them, there is
// no user TXT, and DNSSEC/DANE duties are inherited from the root policy.
func DerivePlatformProfile(root Profile, id ProfileID, backend BackendID) (Profile, error) {
	if _, err := NewProfileID(id); err != nil {
		return Profile{}, err
	}
	if _, err := NewBackendID(backend); err != nil {
		return Profile{}, err
	}
	if root.Authority != AuthorityLocusPortalZone && root.Authority != AuthorityLocusOperatorZone {
		return Profile{}, newInvalid("platform profile", "root profile %q must be a portal- or operator-zone authority; got %s", root.ID.String(), root.Authority.String())
	}
	ownership := gateAllOf(GatePlatformTrust, GateDNSLink)
	websiteGates := []GateSpec{
		gate(GateDNSLink),
		// checkDelegation routes platform bindings through VerifyDomain,
		// which verifies the operator-trust relationship (shared validator).
		gate(GatePlatformTrust),
	}
	domainGates := []GateSpec{
		gate(GatePlatformTrust),
	}
	soa, err := NewRepairIntent(RepairKindEnsureSOAMNAME, FlowDomainVerification)
	if err != nil {
		return Profile{}, err
	}
	repairs := []RepairIntent{soa}
	if root.DNSSEC.Requirement == RequirementRequired {
		ensureDNSSEC, err := NewRepairIntent(RepairKindEnsureDNSSEC, FlowDomainVerification)
		if err != nil {
			return Profile{}, err
		}
		repairs = append(repairs, ensureDNSSEC)
	}
	dnsLink, err := NewPlannedRecordSpec(RecordKindDNSLink, "_dnslink", PublicationLocusPortalZone, RecordOwnershipBindingContent, DefaultRecordTTL, true, "")
	if err != nil {
		return Profile{}, err
	}
	records := []PlannedRecordSpec{dnsLink}
	for _, r := range root.Records {
		if r.Kind == RecordKindApexA || r.Kind == RecordKindApexALIAS || r.Kind == RecordKindTLSA {
			records = append(records, r)
		}
	}
	return NewProfile(ProfileSpec{
		ID:             id,
		Version:        root.Version,
		NamingSystem:   root.NamingSystem,
		Route:          root.Route,
		Backend:        backend,
		Authority:      AuthorityLocusOperatorZone,
		ZoneAllocation: ZoneAllocationSharedParent,
		Ownership:      ownership,
		DNSSEC:         root.DNSSEC,
		DANE:           root.DANE,
		AllowedTargets: append([]TargetKind(nil), root.AllowedTargets...),
		WebsiteGates:   websiteGates,
		DomainGates:    domainGates,
		Repairs:        repairs,
		Records:        records,
	})
}

// CurrentProfiles builds the full set of current-behavior profiles without
// touching the default registry. It is the single authoritative encoding of
// the observed behavior matrix; it encodes what the code
// does today, including the self-hosted HNS enforcement gap, which is
// deliberately not fixed here.
func CurrentProfiles() ([]Profile, error) {
	icannPortal, err := icannPortalProfile()
	if err != nil {
		return nil, err
	}
	icannOwner, err := icannOwnerProfile()
	if err != nil {
		return nil, err
	}
	hnsNative, err := hnsPortalManagedProfile(nil)
	if err != nil {
		return nil, err
	}
	hnsChild, err := hnsPortalNamebaseChildProfile()
	if err != nil {
		return nil, err
	}
	hnsOwner, err := hnsOwnerNativeProfile()
	if err != nil {
		return nil, err
	}
	hnsChain, err := hnsChainEthereumProfile()
	if err != nil {
		return nil, err
	}
	platformICANN, err := DerivePlatformProfile(icannPortal, ProfileIDPlatformICANN, BackendPowerDNS)
	if err != nil {
		return nil, err
	}
	platformHNS, err := DerivePlatformProfile(hnsNative, ProfileIDPlatformHNSNative, BackendPowerDNS)
	if err != nil {
		return nil, err
	}
	return []Profile{
		icannPortal,
		icannOwner,
		hnsNative,
		hnsChild,
		hnsOwner,
		hnsChain,
		platformICANN,
		platformHNS,
	}, nil
}

// registerBuiltins registers the built-in current-behavior profiles in the
// default registry. Any rejection is a programming error.
func registerBuiltins() error {
	profiles, err := CurrentProfiles()
	if err != nil {
		return err
	}
	for _, profile := range profiles {
		if err := RegisterDefault(profile); err != nil {
			return err
		}
	}
	return nil
}

func init() {
	if err := registerBuiltins(); err != nil {
		panic(fmt.Sprintf("domainpolicy: builtin profile registration failed: %s", err))
	}
}
