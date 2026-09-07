package domainpolicy

// BindingFacts are the resolved, per-binding inputs PlanBinding consumes.
// Facts describe the actual state of one binding (lifecycle, requested
// hosting, zone infrastructure, discovered route, content target); they are
// separate from policy (the Profile) and neither field replaces authority —
// a disagreement between facts and profile is a rejection, never a guess.
// All enum fields fail closed: unknown values reject the plan.
type BindingFacts struct {
	// Name is the binding's FQDN (e.g. "example.hnsdata.com").
	Name string
	// Lifecycle is the binding's provisioning lifecycle state.
	Lifecycle Lifecycle
	// RequestedHosting records who requested the hosting relationship. It is
	// intent, never proof of authority.
	RequestedHosting HostingRequest
	// PlatformRootID is set when the binding is a platform subdomain minted
	// under an operator-owned root; nil otherwise.
	PlatformRootID *uint
	// ZonePresent reports whether a portal/operator zone actually exists for
	// the binding right now.
	ZonePresent bool
	// ZoneAllocation is the observed zone allocation. When ZonePresent is
	// false it must be ZoneAllocationNone or unknown-free callers can leave
	// it unknown; PlanBinding rejects a present-zone/none-allocation pair.
	ZoneAllocation ZoneAllocation
	// DiscoveredRoute is the observed resolution route.
	DiscoveredRoute ResolutionRoute
	// DiscoveredBackend is the observed resolution backend.
	DiscoveredBackend BackendID
	// Target is the content the binding serves.
	Target ContentTarget
	// PolicyVersion is the profile version selected for the binding; it must
	// match the profile's version or PlanBinding rejects the pair.
	PolicyVersion ProfileVersion
}

// NewBindingFacts validates the identifying facts and returns them, rejecting
// empty names. PlanBinding performs the full cross-field validation (it needs
// the profile to judge combinations).
func NewBindingFacts(name string) (BindingFacts, error) {
	if name == "" {
		return BindingFacts{}, newInvalid("binding facts", "empty name")
	}
	return BindingFacts{Name: name}, nil
}
