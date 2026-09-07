package domainpolicy

import "strconv"

// NamingSystem identifies the naming system a domain belongs to.
type NamingSystem int

const (
	// NamingSystemUnknown is the zero value; it fails closed.
	NamingSystemUnknown NamingSystem = iota
	NamingSystemICANN
	NamingSystemHNS
)

// String returns a stable diagnostic name for the naming system.
func (n NamingSystem) String() string {
	switch n {
	case NamingSystemICANN:
		return "icann"
	case NamingSystemHNS:
		return "hns"
	default:
		return "unknown"
	}
}

// Valid reports whether the naming system is a known, non-unknown value.
func (n NamingSystem) Valid() bool {
	switch n {
	case NamingSystemICANN, NamingSystemHNS:
		return true
	default:
		return false
	}
}

// NewNamingSystem validates and returns the given naming system, rejecting
// unknown values.
func NewNamingSystem(n NamingSystem) (NamingSystem, error) {
	if !n.Valid() {
		return NamingSystemUnknown, newInvalid("naming system", "unknown value %d", int(n))
	}
	return n, nil
}

// ResolutionRoute describes how a domain name resolves to its content.
type ResolutionRoute int

const (
	ResolutionRouteUnknown ResolutionRoute = iota
	ResolutionRouteStandardDNS
	ResolutionRouteHNSRoot
	ResolutionRouteCrossChain
)

// String returns a stable diagnostic name for the resolution route.
func (r ResolutionRoute) String() string {
	switch r {
	case ResolutionRouteStandardDNS:
		return "standard-dns"
	case ResolutionRouteHNSRoot:
		return "hns-root"
	case ResolutionRouteCrossChain:
		return "cross-chain"
	default:
		return "unknown"
	}
}

// Valid reports whether the resolution route is a known, non-unknown value.
func (r ResolutionRoute) Valid() bool {
	switch r {
	case ResolutionRouteStandardDNS, ResolutionRouteHNSRoot, ResolutionRouteCrossChain:
		return true
	default:
		return false
	}
}

// NewResolutionRoute validates and returns the given route, rejecting unknown
// values.
func NewResolutionRoute(r ResolutionRoute) (ResolutionRoute, error) {
	if !r.Valid() {
		return ResolutionRouteUnknown, newInvalid("resolution route", "unknown value %d", int(r))
	}
	return r, nil
}

// AuthorityLocus names where authority over a binding is held: which party
// (or system) ultimately controls the DNS that serves the binding.
type AuthorityLocus int

const (
	AuthorityLocusUnknown AuthorityLocus = iota
	AuthorityLocusPortalZone
	AuthorityLocusOwnerDNS
	AuthorityLocusOperatorZone
	AuthorityLocusChain
)

// String returns a stable diagnostic name for the authority locus.
func (a AuthorityLocus) String() string {
	switch a {
	case AuthorityLocusPortalZone:
		return "portal-zone"
	case AuthorityLocusOwnerDNS:
		return "owner-dns"
	case AuthorityLocusOperatorZone:
		return "operator-zone"
	case AuthorityLocusChain:
		return "chain"
	default:
		return "unknown"
	}
}

// Valid reports whether the authority locus is a known, non-unknown value.
func (a AuthorityLocus) Valid() bool {
	switch a {
	case AuthorityLocusPortalZone, AuthorityLocusOwnerDNS, AuthorityLocusOperatorZone, AuthorityLocusChain:
		return true
	default:
		return false
	}
}

// NewAuthorityLocus validates and returns the given authority locus,
// rejecting unknown values.
func NewAuthorityLocus(a AuthorityLocus) (AuthorityLocus, error) {
	if !a.Valid() {
		return AuthorityLocusUnknown, newInvalid("authority locus", "unknown value %d", int(a))
	}
	return a, nil
}

// ZoneAllocation describes how a portal-managed zone is allocated to a
// binding.
type ZoneAllocation int

const (
	// ZoneAllocationUnknown is the zero value; it fails closed.
	ZoneAllocationUnknown ZoneAllocation = iota
	// ZoneAllocationNone means no portal zone is allocated.
	ZoneAllocationNone
	// ZoneAllocationDedicated means the binding owns an entire zone.
	ZoneAllocationDedicated
	// ZoneAllocationSharedParent means the binding is a subdomain sharing a
	// parent zone with other bindings.
	ZoneAllocationSharedParent
)

// String returns a stable diagnostic name for the zone allocation.
func (z ZoneAllocation) String() string {
	switch z {
	case ZoneAllocationNone:
		return "none"
	case ZoneAllocationDedicated:
		return "dedicated"
	case ZoneAllocationSharedParent:
		return "shared-parent"
	default:
		return "unknown"
	}
}

// Valid reports whether the zone allocation is a known, non-unknown value.
func (z ZoneAllocation) Valid() bool {
	switch z {
	case ZoneAllocationNone, ZoneAllocationDedicated, ZoneAllocationSharedParent:
		return true
	default:
		return false
	}
}

// NewZoneAllocation validates and returns the given zone allocation,
// rejecting unknown values.
func NewZoneAllocation(z ZoneAllocation) (ZoneAllocation, error) {
	if !z.Valid() {
		return ZoneAllocationUnknown, newInvalid("zone allocation", "unknown value %d", int(z))
	}
	return z, nil
}

// Lifecycle is the provisioning lifecycle state of a domain binding.
type Lifecycle int

const (
	LifecycleUnknown Lifecycle = iota
	LifecycleDraft
	LifecycleProvisioning
	LifecycleAwaitingProof
	LifecycleActive
	LifecycleError
	LifecycleRetiring
)

// String returns a stable diagnostic name for the lifecycle state.
func (l Lifecycle) String() string {
	switch l {
	case LifecycleDraft:
		return "draft"
	case LifecycleProvisioning:
		return "provisioning"
	case LifecycleAwaitingProof:
		return "awaiting-proof"
	case LifecycleActive:
		return "active"
	case LifecycleError:
		return "error"
	case LifecycleRetiring:
		return "retiring"
	default:
		return "unknown"
	}
}

// Valid reports whether the lifecycle state is a known, non-unknown value.
func (l Lifecycle) Valid() bool {
	switch l {
	case LifecycleDraft, LifecycleProvisioning, LifecycleAwaitingProof, LifecycleActive, LifecycleError, LifecycleRetiring:
		return true
	default:
		return false
	}
}

// NewLifecycle validates and returns the given lifecycle state, rejecting
// unknown values.
func NewLifecycle(l Lifecycle) (Lifecycle, error) {
	if !l.Valid() {
		return LifecycleUnknown, newInvalid("lifecycle", "unknown value %d", int(l))
	}
	return l, nil
}

// ParseLifecycle parses the String() form of a lifecycle back into the enum.
// The string forms are the persistence contract for the lifecycle_status
// column; an unknown value fails closed.
func ParseLifecycle(s string) (Lifecycle, error) {
	for _, l := range []Lifecycle{LifecycleDraft, LifecycleProvisioning, LifecycleAwaitingProof, LifecycleActive, LifecycleError, LifecycleRetiring} {
		if l.String() == s {
			return l, nil
		}
	}
	return LifecycleUnknown, newInvalid("lifecycle", "unknown value %q", s)
}

// ParseAuthorityLocus parses the String() form of an authority locus.
func ParseAuthorityLocus(s string) (AuthorityLocus, error) {
	for _, a := range []AuthorityLocus{AuthorityLocusPortalZone, AuthorityLocusOwnerDNS, AuthorityLocusOperatorZone, AuthorityLocusChain} {
		if a.String() == s {
			return a, nil
		}
	}
	return AuthorityLocusUnknown, newInvalid("authority locus", "unknown value %q", s)
}

// ParseResolutionRoute parses the String() form of a resolution route.
func ParseResolutionRoute(s string) (ResolutionRoute, error) {
	for _, r := range []ResolutionRoute{ResolutionRouteStandardDNS, ResolutionRouteHNSRoot, ResolutionRouteCrossChain} {
		if r.String() == s {
			return r, nil
		}
	}
	return ResolutionRouteUnknown, newInvalid("resolution route", "unknown value %q", s)
}

// ParseHostingRequest parses the String() form of a hosting request.
func ParseHostingRequest(s string) (HostingRequest, error) {
	for _, h := range []HostingRequest{HostingRequestPortal, HostingRequestOwner} {
		if h.String() == s {
			return h, nil
		}
	}
	return HostingRequestUnknown, newInvalid("hosting request", "unknown value %q", s)
}

// HostingRequest records who requested the hosting relationship. It is the
// requested axis, never proof of actual authority: a request to host does not
// by itself authorize DNS writes.
type HostingRequest int

const (
	HostingRequestUnknown HostingRequest = iota
	HostingRequestPortal
	HostingRequestOwner
)

// String returns a stable diagnostic name for the hosting request.
func (h HostingRequest) String() string {
	switch h {
	case HostingRequestPortal:
		return "portal"
	case HostingRequestOwner:
		return "owner"
	default:
		return "unknown"
	}
}

// Valid reports whether the hosting request is a known, non-unknown value.
func (h HostingRequest) Valid() bool {
	switch h {
	case HostingRequestPortal, HostingRequestOwner:
		return true
	default:
		return false
	}
}

// NewHostingRequest validates and returns the given hosting request,
// rejecting unknown values.
func NewHostingRequest(h HostingRequest) (HostingRequest, error) {
	if !h.Valid() {
		return HostingRequestUnknown, newInvalid("hosting request", "unknown value %d", int(h))
	}
	return h, nil
}

// BackendID identifies a resolution backend (e.g. "system-dns",
// "hns-root", "ethereum"). Backends are adapter-level identities; this
// package treats them as opaque strings.
type BackendID string

// ProfileID identifies a domainpolicy profile, e.g.
// "icann.portal.current-v1". Profile identities are stable contracts; they
// never encode mutable state.
type ProfileID string

// EmptyBackendID and EmptyProfileID are the empty forms of the string
// identifiers; they are rejected by constructors.
const (
	EmptyBackendID BackendID = ""
	EmptyProfileID ProfileID = ""
)

// String returns the backend ID itself for stable diagnostics.
func (b BackendID) String() string { return string(b) }

// String returns the profile ID itself for stable diagnostics.
func (p ProfileID) String() string { return string(p) }

// Valid reports whether the backend ID is non-empty.
func (b BackendID) Valid() bool { return b != EmptyBackendID }

// Valid reports whether the profile ID is non-empty.
func (p ProfileID) Valid() bool { return p != EmptyProfileID }

// NewBackendID validates and returns the given backend ID, rejecting empty
// values.
func NewBackendID(b BackendID) (BackendID, error) {
	if !b.Valid() {
		return EmptyBackendID, newInvalid("backend ID", "empty value")
	}
	return b, nil
}

// NewProfileID validates and returns the given profile ID, rejecting empty
// values.
func NewProfileID(p ProfileID) (ProfileID, error) {
	if !p.Valid() {
		return EmptyProfileID, newInvalid("profile ID", "empty value")
	}
	return p, nil
}

// ProfileVersion is the version of a profile. It must be a positive integer;
// zero and negatives are invalid so that an unset version is always
// distinguishable from a real version.
type ProfileVersion int

// String returns the decimal representation of the version.
func (v ProfileVersion) String() string {
	return strconv.Itoa(int(v))
}

// Valid reports whether the version is a positive integer.
func (v ProfileVersion) Valid() bool { return v > 0 }

// NewProfileVersion validates and returns the given profile version,
// rejecting zero and negative values.
func NewProfileVersion(v int) (ProfileVersion, error) {
	pv := ProfileVersion(v)
	if !pv.Valid() {
		return ProfileVersion(0), newInvalid("profile version", "must be positive, got %d", v)
	}
	return pv, nil
}
