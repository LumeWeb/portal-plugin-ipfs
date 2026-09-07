package domain

// policy_compat.go carries the legacy compatibility mapper between the
// persisted binding rows (internal/db) and the pure domainpolicy vocabulary
// (internal/domainpolicy). While the pure path takes over runtime decisions,
// this file maps legacy persisted state to policy facts;
// DelegatedDomainService.CurrentBindingPlan is the single runtime entry
// point for that mapping, and gate selection at runtime consults the
// resulting plan's gates only. Any plan/unavailability divergence falls back
// to the legacy predicates with a loud log, never to plan-only behavior.
//
// The row→facts/profile mapping (legacyFacts, legacyProfileFor,
// legacyZoneAllocation and their helpers) is also used as a test-only
// comparison against the legacy implementation. The shadow machinery
// (LegacyDecision, ShadowCompare) remains test-only and must never be called
// from runtime code.
//
// Review check: grep for legacyFacts/legacyProfileFor outside _test.go files;
// their only permitted non-test caller is CurrentBindingPlan in
// delegated_domain_service.go. ShadowCompare/LegacyDecision must stay
// test-only.
//
// Precedence encoded here mirrors db.WebsiteDomain.Class exactly:
//
//  1. on-chain managed status (ClassOnChainManaged) — wins over any zone
//     reference (a stray zone on an on-chain binding is data incoherence);
//  2. a non-zero ZoneID (ClassPortalManaged);
//  3. an explicit self_hosted status with no zone (ClassSelfHosted);
//  4. everything else is unresolved and fails closed.
//
// dns_hosting_enabled is not a hosting-locus input (matching
// Class); it maps to BindingFacts.RequestedHosting as the requested axis
// only, never as proof of actual authority. The legacy HIP-5 mapping to the
// Ethereum backend lives here in compatibility code (github BackendEthereum)
// because Ethereum is the only cross-chain backend today; a second chain
// requires new profiles in domainpolicy, never edits to this file.

import (
	"context"
	"fmt"
	"strings"

	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	"go.lumeweb.com/portal/db"
	"gorm.io/gorm"
)

// CompatErrorKind enumerates the typed rejections the legacy mapper emits.
// Every rejection fails closed: an incoherent persisted row never maps to a
// plan that authorizes portal DNS writes.
type CompatErrorKind int

const (
	CompatErrorUnknown CompatErrorKind = iota
	// CompatErrorOnChainWithZone: an on-chain managed binding carries a
	// non-zero ZoneID (stray portal zone). Never a portal authorization.
	CompatErrorOnChainWithZone
	// CompatErrorOnChainICANN: an ICANN binding marked on-chain managed.
	// ICANNProvider.Inspect always reports false, so no current flow can
	// produce this state.
	CompatErrorOnChainICANN
	// CompatErrorPortalWithoutZone: the binding's lifecycle (past
	// provisioning) and requested hosting say portal DNS, but no usable zone
	// reference exists (an enable-orphan). No portal DNS writes may
	// authorize.
	CompatErrorPortalWithoutZone
	// CompatErrorUnknownNamespace: the binding namespace is neither ICANN
	// nor HNS; there is no profile for it.
	CompatErrorUnknownNamespace
	// CompatErrorImpossiblePlatformRelation: the binding claims platform
	// membership but the persisted platform trust relationship does not hold
	// (missing root, namespace mismatch, non-descendant name, wrong zone).
	CompatErrorImpossiblePlatformRelation
	// CompatErrorUnknownTarget: the owning website's target type is not a
	// recognized IPFS/IPNS target.
	CompatErrorUnknownTarget
	// CompatErrorUnresolved: the hosting locus is not determinable from
	// persisted state (draft/error, no zone, explicit self-hosted decision
	// absent). No profile exists for an unresolved binding; fail closed.
	CompatErrorUnresolved
	// CompatErrorRouteMismatch: a probed route observation disagrees with
	// the route derived from persisted state.
	CompatErrorRouteMismatch
	// CompatErrorPlatformTrustUnavailable: the mapper needs database access
	// to check the platform trust relationship, and the service has none.
	CompatErrorPlatformTrustUnavailable
	// CompatErrorZoneStateUnavailable: the mapper needs database access to
	// determine whether a subdomain's zone is shared, and the service has
	// none.
	CompatErrorZoneStateUnavailable
	// CompatErrorProfileUnregistered: the mapper selected a profile ID that is
	// absent from the default registry. A current-behavior profile missing at
	// runtime is a programming error, not a data condition.
	CompatErrorProfileUnregistered
)

// String returns a stable diagnostic name for the compat error kind.
func (k CompatErrorKind) String() string {
	switch k {
	case CompatErrorOnChainWithZone:
		return "on-chain-with-zone"
	case CompatErrorOnChainICANN:
		return "on-chain-icann"
	case CompatErrorPortalWithoutZone:
		return "portal-without-zone"
	case CompatErrorUnknownNamespace:
		return "unknown-namespace"
	case CompatErrorImpossiblePlatformRelation:
		return "impossible-platform-relation"
	case CompatErrorUnknownTarget:
		return "unknown-target"
	case CompatErrorUnresolved:
		return "unresolved-binding"
	case CompatErrorRouteMismatch:
		return "route-mismatch"
	case CompatErrorPlatformTrustUnavailable:
		return "platform-trust-unavailable"
	case CompatErrorZoneStateUnavailable:
		return "zone-state-unavailable"
	case CompatErrorProfileUnregistered:
		return "profile-unregistered"
	default:
		return "unknown"
	}
}

// CompatError is the typed error returned by the legacy mapper. Match
// sentinels with errors.Is; each sentinel below carves out one rejection
// class so tests (and later, migration tooling) can distinguish them.
type CompatError struct {
	Kind   CompatErrorKind
	Domain string
	Reason string
}

// Error returns the mapper rejection message.
func (e *CompatError) Error() string {
	if e == nil {
		return "policy-compat: <nil>"
	}
	if e.Domain != "" {
		return fmt.Sprintf("policy-compat: %s (%s): %s", e.Kind, e.Domain, e.Reason)
	}
	return fmt.Sprintf("policy-compat: %s: %s", e.Kind, e.Reason)
}

// Is reports whether target is a CompatError of the same kind, so
// errors.Is(err, ErrCompatUnknownNamespace) and friends behave as sentinels.
func (e *CompatError) Is(target error) bool {
	t, ok := target.(*CompatError)
	return ok && t != nil && t.Kind == e.Kind
}

// Typed mapper sentinels. These are sentinels only: they are never returned
// unwrapped (construct *CompatError via newCompatError so rows carry
// context).
var (
	ErrCompatOnChainWithZone            = &CompatError{Kind: CompatErrorOnChainWithZone}
	ErrCompatOnChainICANN               = &CompatError{Kind: CompatErrorOnChainICANN}
	ErrCompatPortalWithoutZone          = &CompatError{Kind: CompatErrorPortalWithoutZone}
	ErrCompatUnknownNamespace           = &CompatError{Kind: CompatErrorUnknownNamespace}
	ErrCompatImpossiblePlatformRelation = &CompatError{Kind: CompatErrorImpossiblePlatformRelation}
	ErrCompatUnknownTarget              = &CompatError{Kind: CompatErrorUnknownTarget}
	ErrCompatUnresolved                 = &CompatError{Kind: CompatErrorUnresolved}
	ErrCompatRouteMismatch              = &CompatError{Kind: CompatErrorRouteMismatch}
	ErrCompatPlatformTrustUnavailable   = &CompatError{Kind: CompatErrorPlatformTrustUnavailable}
	ErrCompatZoneStateUnavailable       = &CompatError{Kind: CompatErrorZoneStateUnavailable}
	ErrCompatProfileUnregistered        = &CompatError{Kind: CompatErrorProfileUnregistered}
)

func newCompatError(kind CompatErrorKind, wd *pluginDb.WebsiteDomain, reasonFormat string, args ...any) error {
	domain := ""
	if wd != nil {
		domain = wd.Domain
	}
	return &CompatError{
		Kind:   kind,
		Domain: domain,
		Reason: fmt.Sprintf(reasonFormat, args...),
	}
}

// legacyFacts maps one persisted binding row (plus its owning website) into
// the BindingFacts the pure policy consumes, encoding current precedence:
// on-chain status first, then a non-zero ZoneID, then an explicit self-hosted
// status, otherwise unresolved (typed rejection). The dns_hosting_enabled
// flag maps to RequestedHosting only — never to actual authority —
// mirroring db.DomainClass, which excludes the flag.
//
// The only runtime caller is DelegatedDomainService.CurrentBindingPlan.
// Platform bindings are mapped only when the existing platform trust
// relationship is available and holds (ValidatePlatformBinding); a broken
// relationship is a typed rejection, never a guess.
func (s *DelegatedDomainService) legacyFacts(wd *pluginDb.WebsiteDomain, website *pluginDb.Website) (domainpolicy.BindingFacts, error) {
	if wd == nil {
		return domainpolicy.BindingFacts{}, newCompatError(CompatErrorUnknownNamespace, nil, "nil binding row")
	}
	facts, err := domainpolicy.NewBindingFacts(wd.Domain)
	if err != nil {
		return domainpolicy.BindingFacts{}, err
	}

	namespace, err := legacyNamespace(wd.Namespace)
	if err != nil {
		return domainpolicy.BindingFacts{}, err
	}

	target, err := legacyTarget(website)
	if err != nil {
		return domainpolicy.BindingFacts{}, err
	}

	platform := wd.PlatformDomainID != nil

	// Hosting-locus mapping with the exact Class() precedence.
	var route domainpolicy.ResolutionRoute
	var backend domainpolicy.BackendID
	zonePresent := false
	zoneAllocation := domainpolicy.ZoneAllocationNone
	requested := domainpolicy.HostingRequestOwner
	switch wd.Class() {
	case pluginDb.ClassOnChainManaged:
		// On-chain status wins over any zone reference: a stray zone on an
		// on-chain binding is data incoherence, never portal authorization.
		if wd.ZoneID != 0 {
			return domainpolicy.BindingFacts{}, newCompatError(CompatErrorOnChainWithZone, wd, "zone_id=%d (stray portal zone on an on-chain managed binding)", wd.ZoneID)
		}
		// Only cross-chain backend today: map legacy HIP-5 to Ethereum.
		// This mapping is compat-local; a second chain gets new profiles.
		if namespace != domainpolicy.NamingSystemHNS {
			return domainpolicy.BindingFacts{}, newCompatError(CompatErrorOnChainICANN, wd, "ICANN names are never on-chain managed (Inspect always reports false)")
		}
		route = domainpolicy.ResolutionRouteCrossChain
		backend = domainpolicy.BackendEthereum
		// Requested hosting is owner-only on the chain route: the bind and
		// conversion paths coerce dns_hosting_enabled to false, and chain
		// authority never accepts a portal request. A stray true flag is
		// ignored here (the flag is never a hosting-locus input).
		requested = domainpolicy.HostingRequestOwner
	case pluginDb.ClassPortalManaged:
		if platform {
			// Platform bindings map only when the existing platform trust
			// relationship is available and holds.
			if s.BaseComponent == nil || s.DB() == nil {
				return domainpolicy.BindingFacts{}, newCompatError(CompatErrorPlatformTrustUnavailable, wd, "platform binding %q requires database access to check platform trust", wd.Domain)
			}
			if terr := s.ValidatePlatformBinding(context.Background(), wd); terr != nil {
				return domainpolicy.BindingFacts{}, newCompatError(CompatErrorImpossiblePlatformRelation, wd, "%s", terr)
			}
			// Platform bindings share the operator root's zone by design.
			route = domainpolicy.ResolutionRouteStandardDNS
			backend = domainpolicy.BackendPowerDNS
			zonePresent = true
			zoneAllocation = domainpolicy.ZoneAllocationSharedParent
		} else {
			route, backend, err = legacyRouteForNamespace(namespace)
			if err != nil {
				return domainpolicy.BindingFacts{}, err
			}
			zonePresent = true
			zoneAllocation, err = s.legacyZoneAllocation(wd)
			if err != nil {
				return domainpolicy.BindingFacts{}, err
			}
		}
		requested = legacyRequestedHosting(wd)
	case pluginDb.ClassSelfHosted:
		// Explicit self-hosted decision with no portal zone (a stray ZoneID
		// would have been classified portal-managed above — the persisted
		// zone reference is authoritative).
		route, backend, err = legacyRouteForNamespace(namespace)
		if err != nil {
			return domainpolicy.BindingFacts{}, err
		}
		requested = domainpolicy.HostingRequestOwner
	case pluginDb.ClassUnresolved:
		// Not treated as self-hosted: provisioning neither established a
		// portal zone nor recorded an explicit self-hosted decision. A
		// hosted flag combined with a past-provisioning lifecycle is an
		// enable-orphan (portal intent without a usable zone); any other
		// unresolved row simply has no profile and fails closed.
		if wd.DNSHostingEnabled && portalIntentStatus(wd.Status) {
			return domainpolicy.BindingFacts{}, newCompatError(CompatErrorPortalWithoutZone, wd, "status %q with dns_hosting_enabled=true but zone_id=0 (enable-orphan)", string(wd.Status))
		}
		return domainpolicy.BindingFacts{}, newCompatError(CompatErrorUnresolved, wd, "no hosting locus determinable (status %q, zone_id=%d)", string(wd.Status), wd.ZoneID)
	default:
		return domainpolicy.BindingFacts{}, newCompatError(CompatErrorUnresolved, wd, "unknown hosting class %d", int(wd.Class()))
	}

	facts.Lifecycle = legacyLifecycle(wd.Status)
	facts.RequestedHosting = requested
	facts.ZonePresent = zonePresent
	facts.ZoneAllocation = zoneAllocation
	facts.DiscoveredRoute = route
	facts.DiscoveredBackend = backend
	facts.Target = target
	facts.PolicyVersion = domainpolicy.ProfileVersion1
	if platform {
		rootID := *wd.PlatformDomainID
		facts.PlatformRootID = &rootID
	}
	return facts, nil
}

// legacyProfileFor selects the current-behavior profile for a persisted
// binding given a route observation. A zero/unknown observation means
// "not probed" and the route is assumed from persisted state. A probed
// observation that disagrees with the derived route is a typed rejection,
// never a silent reclassification. CurrentBindingPlan passes the zero
// observation: plan-derived gate selection must never probe.
func (s *DelegatedDomainService) legacyProfileFor(wd *pluginDb.WebsiteDomain, routeObservation domainpolicy.RouteObservation) (domainpolicy.ProfileID, error) {
	if wd == nil {
		return domainpolicy.EmptyProfileID, newCompatError(CompatErrorUnknownNamespace, nil, "nil binding row")
	}
	namespace, err := legacyNamespace(wd.Namespace)
	if err != nil {
		return domainpolicy.EmptyProfileID, err
	}

	var (
		id           domainpolicy.ProfileID
		expected     domainpolicy.ResolutionRoute
		expectedBack domainpolicy.BackendID
	)
	switch class := wd.Class(); class {
	case pluginDb.ClassOnChainManaged:
		if namespace != domainpolicy.NamingSystemHNS {
			return domainpolicy.EmptyProfileID, newCompatError(CompatErrorOnChainICANN, wd, "ICANN names are never on-chain managed (Inspect always reports false)")
		}
		// Legacy HIP-5 maps to the Ethereum backend, the only cross-chain
		// backend today; the mapping stays in compat code.
		id = domainpolicy.ProfileIDHNSChainEthereum
		expected = domainpolicy.ResolutionRouteCrossChain
		expectedBack = domainpolicy.BackendEthereum
	case pluginDb.ClassPortalManaged:
		expected, expectedBack, err = legacyRouteForNamespace(namespace)
		if err != nil {
			return domainpolicy.EmptyProfileID, err
		}
		if wd.PlatformDomainID != nil {
			switch namespace {
			case domainpolicy.NamingSystemICANN:
				id = domainpolicy.ProfileIDPlatformICANN
			case domainpolicy.NamingSystemHNS:
				id = domainpolicy.ProfileIDPlatformHNSNative
			}
		} else if namespace == domainpolicy.NamingSystemICANN {
			id = domainpolicy.ProfileIDICANNPortal
		} else {
			// Managed HNS: a single-label name is a Handshake-root TLD
			// delegated directly (native); any multi-label name such as
			// `a.hns` sits under an externally delegated TLD and gets its
			// own dedicated zone (Namebase-style child). Both profiles
			// encode identical behavior; this label-count heuristic only
			// picks the profile identity.
			if strings.Count(strings.TrimSuffix(wd.Domain, "."), ".") >= 1 {
				id = domainpolicy.ProfileIDHNSPortalNamebaseChild
			} else {
				id = domainpolicy.ProfileIDHNSPortalNative
			}
		}
	case pluginDb.ClassSelfHosted:
		id = legacySelfHostedProfile(namespace)
		expected, expectedBack, err = legacyRouteForNamespace(namespace)
		if err != nil {
			return domainpolicy.EmptyProfileID, err
		}
	default:
		return domainpolicy.EmptyProfileID, newCompatError(CompatErrorUnresolved, wd, "no profile for an unresolved binding (status %q, zone_id=%d)", string(wd.Status), wd.ZoneID)
	}

	if err := checkRouteObservation(expected, expectedBack, routeObservation); err != nil {
		return domainpolicy.EmptyProfileID, newCompatError(CompatErrorRouteMismatch, wd, "%s (expected %s/%s, observed %s/%s)", err, expected, expectedBack, routeObservation.Route, routeObservation.Backend)
	}
	return id, nil
}

// legacyZoneAllocation derives the observed zone allocation for a
// non-platform portal-managed binding: an apex binding owns its entire zone
// (dedicated); a subdomain sharing its zone with other live bindings shares
// the parent's zone (shared parent). Needs database access; a service
// without one fails closed with a typed error.
func (s *DelegatedDomainService) legacyZoneAllocation(wd *pluginDb.WebsiteDomain) (domainpolicy.ZoneAllocation, error) {
	if parentDomain(wd.Domain) == "" {
		// Apex (single- or two-label for HNS, apex for ICANN): the binding
		// owns its zone. Subdomain bindings under it live inside this zone,
		// but the apex is still the zone's owner.
		return domainpolicy.ZoneAllocationDedicated, nil
	}
	if s.BaseComponent == nil || s.DB() == nil {
		return domainpolicy.ZoneAllocationUnknown, newCompatError(CompatErrorZoneStateUnavailable, wd, "subdomain allocation requires database access to check zone sharing")
	}
	var sharers int64
	if err := db.RetryableComponentTransaction(s, context.Background(), func(tx *gorm.DB) *gorm.DB {
		if err := tx.Model(&pluginDb.WebsiteDomain{}).
			Where("zone_id = ? AND id != ? AND deleted_at IS NULL", wd.ZoneID, wd.ID).
			Count(&sharers).Error; err != nil {
			_ = tx.AddError(err)
		}
		return tx
	}); err != nil {
		return domainpolicy.ZoneAllocationUnknown, fmt.Errorf("count zone sharers for %q: %w", wd.Domain, err)
	}
	if sharers > 0 {
		// Subdomain sharing the parent's zone with sibling bindings. The
		// tolerant portal profiles (those whose bindings can be subdomains)
		// permit shared-parent as an observed zone topology (see
		// domains.CurrentProfiles), so this state binds against the pure
		// plan's zone intent instead of diverging.
		return domainpolicy.ZoneAllocationSharedParent, nil
	}
	// A subdomain whose parent zone did not exist got its own zone.
	return domainpolicy.ZoneAllocationDedicated, nil
}

// Shadow-compare support -------------------------------------------------
//
// LegacyDecision is the characterized set of current decisions for one
// binding, expressed over the same axes the pure plan declares. Shadow tests
// populate it (from the current-behavior matrix) and ShadowCompare checks the
// generated plan against it.
//
// Test-only: runtime services must not use these types or functions.

// LegacyDecision captures the currently implemented decisions for a binding
// class so the tests can compare them with a pure plan.
type LegacyDecision struct {
	// PortalDNSWrites: the portal may write the binding's managed-zone
	// records (every runtime write is gated on Class() == ClassPortalManaged,
	// plus platform bindings, which route through VerifyDomain's shared
	// operator-zone path).
	PortalDNSWrites bool
	// TokenTXTGate: the user-side verification-token TXT gate applies
	// (shouldPerformTokenCheck: skipped for platform bindings, on-chain
	// managed HIP-5, and namespaces proving ownership by delegation).
	TokenTXTGate bool
	// WebsiteDelegationGate: the website validation flow runs a delegation
	// check (checkDelegation: portal-managed bindings only).
	WebsiteDelegationGate bool
	// DelegationGateTrivial: the delegation gate passes without a live
	// observation (ICANNProvider.VerifyDelegation returns true
	// unconditionally today).
	DelegationGateTrivial bool
	// DNSSECRequired / DNSSECProvisionedByPortal: the DNSSEC duty axes
	// (provider policy: HNS requires and the portal signs; ICANN neither).
	DNSSECRequired            bool
	DNSSECProvisionedByPortal bool
	// DANEPublicationLocus: "portal-zone" (managed-zone TLSA), "chain"
	// (owner publishes in chain-backed data), or "" (no DANE publication).
	DANEPublicationLocus string
	// DANEVerifiedLive: a live TLSA verification runs (on-chain TLSA gate
	// for chain-managed bindings).
	DANEVerifiedLive bool
	// ApexRecordType: "A" (HNS signed apex), "ALIAS" (ICANN), or "" (no
	// apex record).
	ApexRecordType string
	// PortalPublishesDNSLink: the portal writes the DNSLink record; false
	// means the owner publishes it (owner DNS or chain-backed data).
	PortalPublishesDNSLink bool
}

// ShadowResult reports the outcome of one shadow comparison.
type ShadowResult struct {
	// Case names the compared case.
	Case string
	// Match reports whether every compared plan attribute agrees with the
	// legacy decision.
	Match bool
	// Divergences lists each disagreement with attribute-level detail.
	Divergences []string
}

// ShadowCompare compares a generated pure plan against the characterized
// legacy decisions for the same binding, returning every disagreement. It is
// pure, performs no I/O, and never mutates the plan.
//
// Test-only: runtime services must not call this.
func ShadowCompare(name string, plan domainpolicy.Plan, legacy LegacyDecision) ShadowResult {
	result := ShadowResult{Case: name, Match: true}
	diverge := func(detail string) {
		result.Match = false
		result.Divergences = append(result.Divergences, detail)
	}

	portalWrites := plan.Authority == domainpolicy.AuthorityLocusPortalZone ||
		plan.Authority == domainpolicy.AuthorityLocusOperatorZone
	if portalWrites != legacy.PortalDNSWrites {
		diverge(fmt.Sprintf("portal DNS writes: legacy=%v plan=%v (authority %s)", legacy.PortalDNSWrites, portalWrites, plan.Authority))
	}

	hasWebsiteGate := func(kind domainpolicy.GateKind) bool {
		for _, g := range plan.Gates {
			if g.Flow == domainpolicy.FlowWebsiteValidation && g.Kind == kind {
				return true
			}
		}
		return false
	}
	websiteGateTrivial := func(kind domainpolicy.GateKind) bool {
		for _, g := range plan.Gates {
			if g.Flow == domainpolicy.FlowWebsiteValidation && g.Kind == kind {
				return g.TriviallyPasses
			}
		}
		return false
	}

	if hasWebsiteGate(domainpolicy.GateChallengeTXT) != legacy.TokenTXTGate {
		diverge(fmt.Sprintf("token TXT gate: legacy=%v plan=%v", legacy.TokenTXTGate, hasWebsiteGate(domainpolicy.GateChallengeTXT)))
	}

	delegationGate := hasWebsiteGate(domainpolicy.GateNSDelegation) || hasWebsiteGate(domainpolicy.GatePlatformTrust)
	if delegationGate != legacy.WebsiteDelegationGate {
		diverge(fmt.Sprintf("website delegation gate: legacy=%v plan=%v", legacy.WebsiteDelegationGate, delegationGate))
	}
	if legacy.WebsiteDelegationGate && legacy.DelegationGateTrivial {
		// The legacy delegation check is a no-op only on the ICANN portal
		// path; a trivially-passing plan NS gate must carry that flag, and a
		// platform-trust or live-NS gate diverges.
		if !hasWebsiteGate(domainpolicy.GateNSDelegation) || !websiteGateTrivial(domainpolicy.GateNSDelegation) {
			diverge("delegation gate triviality: legacy checks delegation without a live NS lookup, plan gate is live")
		}
	}

	if (plan.DNSSEC.Requirement == domainpolicy.RequirementRequired) != legacy.DNSSECRequired {
		diverge(fmt.Sprintf("DNSSEC required: legacy=%v plan=%v", legacy.DNSSECRequired, plan.DNSSEC.Requirement))
	}
	portalProvisioned := plan.DNSSEC.Provisioner == domainpolicy.ActorPortal
	if portalProvisioned != legacy.DNSSECProvisionedByPortal {
		diverge(fmt.Sprintf("DNSSEC provisioned by portal: legacy=%v plan=%v", legacy.DNSSECProvisionedByPortal, portalProvisioned))
	}

	daneLocus := ""
	switch plan.DANE.Publication {
	case domainpolicy.PublicationLocusPortalZone:
		daneLocus = "portal-zone"
	case domainpolicy.PublicationLocusChain:
		daneLocus = "chain"
	}
	if daneLocus != legacy.DANEPublicationLocus {
		diverge(fmt.Sprintf("DANE publication locus: legacy=%q plan=%q", legacy.DANEPublicationLocus, daneLocus))
	}
	verifiedLive := plan.DANE.Verification == domainpolicy.VerificationModeResolveHNS
	if verifiedLive != legacy.DANEVerifiedLive {
		diverge(fmt.Sprintf("live DANE verification: legacy=%v plan=%v", legacy.DANEVerifiedLive, verifiedLive))
	}

	apex := ""
	portalPublishesDNSLink := false
	for _, record := range plan.Records {
		switch record.Intent.Kind {
		case domainpolicy.RecordKindApexA:
			apex = "A"
		case domainpolicy.RecordKindApexALIAS:
			apex = "ALIAS"
		case domainpolicy.RecordKindDNSLink:
			portalPublishesDNSLink = record.Destination == domainpolicy.PublicationLocusPortalZone
		}
	}
	if apex != legacy.ApexRecordType {
		diverge(fmt.Sprintf("apex record: legacy=%q plan=%q", legacy.ApexRecordType, apex))
	}
	if portalPublishesDNSLink != legacy.PortalPublishesDNSLink {
		diverge(fmt.Sprintf("portal publishes DNSLink: legacy=%v plan=%v", legacy.PortalPublishesDNSLink, portalPublishesDNSLink))
	}
	return result
}

// Mapping helpers ---------------------------------------------------------

// legacyNamespace maps a persisted namespace to the policy vocabulary.
func legacyNamespace(namespace pluginDb.DomainNamespace) (domainpolicy.NamingSystem, error) {
	switch namespace {
	case pluginDb.DomainNamespaceICANN:
		return domainpolicy.NamingSystemICANN, nil
	case pluginDb.DomainNamespaceHNS:
		return domainpolicy.NamingSystemHNS, nil
	default:
		return domainpolicy.NamingSystemUnknown, newCompatError(CompatErrorUnknownNamespace, nil, "unknown namespace %q", string(namespace))
	}
}

// legacyRouteForNamespace derives the assumed route/backend for a non-chain
// binding from its namespace. The handled namespaces are the portal stem
// and the owner-hosted stem.
func legacyRouteForNamespace(namespace domainpolicy.NamingSystem) (domainpolicy.ResolutionRoute, domainpolicy.BackendID, error) {
	switch namespace {
	case domainpolicy.NamingSystemICANN:
		return domainpolicy.ResolutionRouteStandardDNS, domainpolicy.BackendSystemDNS, nil
	case domainpolicy.NamingSystemHNS:
		return domainpolicy.ResolutionRouteHNSRoot, domainpolicy.BackendHNSRoot, nil
	default:
		return domainpolicy.ResolutionRouteUnknown, domainpolicy.EmptyBackendID, newCompatError(CompatErrorUnknownNamespace, nil, "unknown namespace %q", namespace.String())
	}
}

// legacySelfHostedProfile maps a self-hosted binding's namespace to its
// current-behavior profile.
func legacySelfHostedProfile(namespace domainpolicy.NamingSystem) domainpolicy.ProfileID {
	if namespace == domainpolicy.NamingSystemHNS {
		return domainpolicy.ProfileIDHNSOwnerNative
	}
	return domainpolicy.ProfileIDICANNOwner
}

// legacyRequestedHosting maps dns_hosting_enabled to the requested hosting
// axis. It is intent only: the flag never proves authority (that is
// db.DomainClass's job and why Class excludes the flag).
func legacyRequestedHosting(wd *pluginDb.WebsiteDomain) domainpolicy.HostingRequest {
	if wd.DNSHostingEnabled {
		return domainpolicy.HostingRequestPortal
	}
	return domainpolicy.HostingRequestOwner
}

// legacyTarget maps the owning website's persisted target into a
// ContentTarget, rejecting unknown target kinds.
func legacyTarget(website *pluginDb.Website) (domainpolicy.ContentTarget, error) {
	if website == nil {
		return domainpolicy.ContentTarget{}, newCompatError(CompatErrorUnknownTarget, nil, "no website row for binding")
	}
	switch website.TargetType {
	case string(pluginDb.WebsiteTargetTypeIPFS):
		return domainpolicy.NewContentTarget(domainpolicy.TargetKindIPFS, website.TargetHash())
	case string(pluginDb.WebsiteTargetTypeIPNS):
		return domainpolicy.NewContentTarget(domainpolicy.TargetKindIPNS, website.TargetHash())
	default:
		return domainpolicy.ContentTarget{}, newCompatError(CompatErrorUnknownTarget, nil, "unknown website target type %q", website.TargetType)
	}
}

// legacyLifecycle maps a persisted lifecycle status to the policy lifecycle.
// Self-hosted and on-chain statuses are awaiting-proof phases of their
// hosting locus (both await explicit DNS evidence); unknown statuses map to
// the unknown lifecycle, which PlanBinding rejects (fail closed).
func legacyLifecycle(status pluginDb.DomainStatus) domainpolicy.Lifecycle {
	switch status {
	case pluginDb.DomainStatusDraft:
		return domainpolicy.LifecycleDraft
	case pluginDb.DomainStatusRecordsGenerated:
		return domainpolicy.LifecycleProvisioning
	case pluginDb.DomainStatusWaitingDelegation:
		return domainpolicy.LifecycleAwaitingProof
	case pluginDb.DomainStatusActive:
		return domainpolicy.LifecycleActive
	case pluginDb.DomainStatusError:
		return domainpolicy.LifecycleError
	case pluginDb.DomainStatusSelfHosted:
		return domainpolicy.LifecycleAwaitingProof
	case pluginDb.DomainStatusOnchainManaged:
		return domainpolicy.LifecycleAwaitingProof
	default:
		return domainpolicy.LifecycleUnknown
	}
}

// portalIntentStatus reports whether a lifecycle status implies the binding's
// provisioning already progressed toward portal DNS (so a hosted flag with no
// zone is an enable-orphan, i.e. portal intent without a usable zone).
func portalIntentStatus(status pluginDb.DomainStatus) bool {
	switch status {
	case pluginDb.DomainStatusRecordsGenerated,
		pluginDb.DomainStatusWaitingDelegation,
		pluginDb.DomainStatusActive:
		return true
	default:
		return false
	}
}

// checkRouteObservation verifies a probed route observation against the
// assumed route. An unknown (not-probed) observation passes; a probed one
// must agree on both route and backend.
func checkRouteObservation(expected domainpolicy.ResolutionRoute, expectedBackend domainpolicy.BackendID, observed domainpolicy.RouteObservation) error {
	if observed.Route == domainpolicy.ResolutionRouteUnknown && observed.Backend == domainpolicy.EmptyBackendID {
		return nil // not probed: route assumed from persisted state
	}
	if observed.Route != expected || observed.Backend != expectedBackend {
		return fmt.Errorf("route observation disagrees with persisted state")
	}
	return nil
}
