package domain

// policy_compat_test.go unit-tests the legacy mapper's typed rejections and
// its coherent persisted-row → BindingFacts mappings. The shadow tests'
// end-to-end case matrix lives in policy_shadow_test.go.

import (
	"errors"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// compatTestCID is a fixed valid CIDv1 (raw) content identifier used by the
// compat fixtures.
const compatTestCID = "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s2zyi"

// compatTestPeerID and compatTestPeerID2 are fixed valid libp2p peer IDs
// (ed25519-derived) used by the compat fixtures.
const (
	compatTestPeerID  = "12D3KooWCoaotGbGeErYCS4PAwZWxsWd8pspLEy5uA5p724F4CdN"
	compatTestPeerID2 = "12D3KooWGzxzKZYveHS9zW8jZAnCPHfvUZqDpx1yJuJSAVfjVngq"
)

// compatIPFSWebsite builds an in-memory IPFS website (no BeforeSave hooks
// run; the mapper consumes rows as values).
func compatIPFSWebsite(t *testing.T) *pluginDb.Website {
	t.Helper()
	c := cid.MustParse(compatTestCID)
	version := uint8(c.Version())
	codec := uint8(c.Type())
	return &pluginDb.Website{
		UserID:          1,
		TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
		TargetMultihash: c.Hash(),
		CIDVersion:      &version,
		CIDType:         &codec,
	}
}

// compatIPNSWebsite builds an in-memory IPNS website.
func compatIPNSWebsite(t *testing.T) *pluginDb.Website {
	t.Helper()
	target, err := pluginDb.NewIPNSTargetFromString(compatTestPeerID)
	require.NoError(t, err)
	return &pluginDb.Website{
		UserID:          1,
		TargetType:      string(pluginDb.WebsiteTargetTypeIPNS),
		TargetMultihash: target.ToMultihash(),
	}
}

func TestLegacyFacts_CoherentStateMappings(t *testing.T) {
	svc := &DelegatedDomainService{} // no DB wired: only non-platform paths run

	t.Run("icann_portal_managed", func(t *testing.T) {
		wd := &pluginDb.WebsiteDomain{
			Domain: "example.com", Namespace: pluginDb.DomainNamespaceICANN,
			ZoneID: 7, Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
		}
		facts, err := svc.legacyFacts(wd, compatIPFSWebsite(t))
		require.NoError(t, err)
		assert.Equal(t, "example.com", facts.Name)
		assert.Equal(t, domainpolicy.LifecycleActive, facts.Lifecycle)
		assert.Equal(t, domainpolicy.HostingRequestPortal, facts.RequestedHosting)
		assert.True(t, facts.ZonePresent)
		assert.Equal(t, domainpolicy.ZoneAllocationDedicated, facts.ZoneAllocation)
		assert.Equal(t, domainpolicy.ResolutionRouteStandardDNS, facts.DiscoveredRoute)
		assert.Equal(t, domainpolicy.BackendSystemDNS, facts.DiscoveredBackend)
		assert.Nil(t, facts.PlatformRootID)
		assert.Equal(t, domainpolicy.ProfileVersion1, facts.PolicyVersion)
		assert.Equal(t, domainpolicy.TargetKindIPFS, facts.Target.Kind)
		assert.Equal(t, "/ipfs/"+compatTestCID, facts.Target.DNSLinkPath())

		profileID, err := svc.legacyProfileFor(wd, domainpolicy.RouteObservation{})
		require.NoError(t, err)
		assert.Equal(t, domainpolicy.ProfileIDICANNPortal, profileID)
	})

	t.Run("icann_owner_hosted", func(t *testing.T) {
		wd := &pluginDb.WebsiteDomain{
			Domain: "example.com", Namespace: pluginDb.DomainNamespaceICANN,
			Status: pluginDb.DomainStatusSelfHosted, DNSHostingEnabled: false,
		}
		facts, err := svc.legacyFacts(wd, compatIPFSWebsite(t))
		require.NoError(t, err)
		assert.False(t, facts.ZonePresent)
		assert.Equal(t, domainpolicy.ZoneAllocationNone, facts.ZoneAllocation)
		assert.Equal(t, domainpolicy.HostingRequestOwner, facts.RequestedHosting)
		assert.Equal(t, domainpolicy.ResolutionRouteStandardDNS, facts.DiscoveredRoute)

		profileID, err := svc.legacyProfileFor(wd, domainpolicy.RouteObservation{})
		require.NoError(t, err)
		assert.Equal(t, domainpolicy.ProfileIDICANNOwner, profileID)
	})

	t.Run("native_hns_managed_single_label", func(t *testing.T) {
		wd := &pluginDb.WebsiteDomain{
			Domain: "altroot", Namespace: pluginDb.DomainNamespaceHNS,
			ZoneID: 5, Status: pluginDb.DomainStatusWaitingDelegation, DNSHostingEnabled: true,
		}
		facts, err := svc.legacyFacts(wd, compatIPFSWebsite(t))
		require.NoError(t, err)
		assert.Equal(t, domainpolicy.LifecycleAwaitingProof, facts.Lifecycle)
		assert.True(t, facts.ZonePresent)
		assert.Equal(t, domainpolicy.ResolutionRouteHNSRoot, facts.DiscoveredRoute)
		assert.Equal(t, domainpolicy.BackendHNSRoot, facts.DiscoveredBackend)

		profileID, err := svc.legacyProfileFor(wd, domainpolicy.RouteObservation{})
		require.NoError(t, err)
		assert.Equal(t, domainpolicy.ProfileIDHNSPortalNative, profileID)
	})

	t.Run("hns_two_label_is_namebase_child", func(t *testing.T) {
		wd := &pluginDb.WebsiteDomain{
			Domain: "a.hns", Namespace: pluginDb.DomainNamespaceHNS,
			ZoneID: 6, Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
		}
		profileID, err := svc.legacyProfileFor(wd, domainpolicy.RouteObservation{})
		require.NoError(t, err)
		assert.Equal(t, domainpolicy.ProfileIDHNSPortalNamebaseChild, profileID)
		// The Namebase-child profile encodes identical behavior to managed
		// native HNS; the facts are identical too.
		facts, err := svc.legacyFacts(wd, compatIPFSWebsite(t))
		require.NoError(t, err)
		assert.Equal(t, domainpolicy.ZoneAllocationDedicated, facts.ZoneAllocation)
	})

	t.Run("hns_owner_hosted_gap", func(t *testing.T) {
		wd := &pluginDb.WebsiteDomain{
			Domain: "altroot", Namespace: pluginDb.DomainNamespaceHNS,
			Status: pluginDb.DomainStatusSelfHosted, DNSHostingEnabled: false,
		}
		facts, err := svc.legacyFacts(wd, compatIPFSWebsite(t))
		require.NoError(t, err)
		assert.False(t, facts.ZonePresent)
		assert.Equal(t, domainpolicy.HostingRequestOwner, facts.RequestedHosting)
		assert.Equal(t, domainpolicy.ResolutionRouteHNSRoot, facts.DiscoveredRoute)

		profileID, err := svc.legacyProfileFor(wd, domainpolicy.RouteObservation{})
		require.NoError(t, err)
		assert.Equal(t, domainpolicy.ProfileIDHNSOwnerNative, profileID)

		// The plan must encode the current enforcement GAP, not a corrected
		// policy: the website gate sequence is DNSLink only, and no live TLSA
		// verification is encoded.
		profile, ok := domainpolicy.DefaultRegistry().Lookup(profileID)
		require.True(t, ok)
		plan, err := domainpolicy.PlanBinding(profile, facts)
		require.NoError(t, err)
		var websiteGates []domainpolicy.GateKind
		for _, g := range plan.Gates {
			if g.Flow == domainpolicy.FlowWebsiteValidation {
				websiteGates = append(websiteGates, g.Kind)
			}
		}
		assert.Equal(t, []domainpolicy.GateKind{domainpolicy.GateDNSLink}, websiteGates)
		assert.Equal(t, domainpolicy.VerificationModeNone, plan.DANE.Verification)
	})

	t.Run("hns_hip5_onchain", func(t *testing.T) {
		wd := &pluginDb.WebsiteDomain{
			Domain: "altroot", Namespace: pluginDb.DomainNamespaceHNS,
			Status: pluginDb.DomainStatusOnchainManaged, DNSHostingEnabled: false,
		}
		facts, err := svc.legacyFacts(wd, compatIPFSWebsite(t))
		require.NoError(t, err)
		assert.False(t, facts.ZonePresent)
		assert.Equal(t, domainpolicy.ZoneAllocationNone, facts.ZoneAllocation)
		assert.Equal(t, domainpolicy.HostingRequestOwner, facts.RequestedHosting)
		// Legacy HIP-5 maps to the Ethereum backend, the only cross-chain
		// backend today; the mapping stays inside this compat code.
		assert.Equal(t, domainpolicy.ResolutionRouteCrossChain, facts.DiscoveredRoute)
		assert.Equal(t, domainpolicy.BackendEthereum, facts.DiscoveredBackend)

		profileID, err := svc.legacyProfileFor(wd, domainpolicy.RouteObservation{})
		require.NoError(t, err)
		assert.Equal(t, domainpolicy.ProfileIDHNSChainEthereum, profileID)

		// A probed observation agreeing with persisted state passes; one that
		// disagrees is a typed mismatch.
		_, err = svc.legacyProfileFor(wd, domainpolicy.RouteObservation{
			Route: domainpolicy.ResolutionRouteCrossChain, Backend: domainpolicy.BackendEthereum,
		})
		assert.NoError(t, err)
		_, err = svc.legacyProfileFor(wd, domainpolicy.RouteObservation{
			Route: domainpolicy.ResolutionRouteHNSRoot, Backend: domainpolicy.BackendHNSRoot,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompatRouteMismatch)
	})
}

func TestLegacyFacts_TypedRejections(t *testing.T) {
	svc := &DelegatedDomainService{}

	ipfs := compatIPFSWebsite(t)

	t.Run("unknown_namespace", func(t *testing.T) {
		wd := &pluginDb.WebsiteDomain{
			Domain: "vitalik.eth", Namespace: pluginDb.DomainNamespace("ens"),
			Status: pluginDb.DomainStatusActive,
		}
		_, err := svc.legacyFacts(wd, ipfs)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompatUnknownNamespace)
		_, err = svc.legacyProfileFor(wd, domainpolicy.RouteObservation{})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompatUnknownNamespace)
	})

	t.Run("onchain_with_stray_zone", func(t *testing.T) {
		wd := &pluginDb.WebsiteDomain{
			Domain: "altroot", Namespace: pluginDb.DomainNamespaceHNS,
			Status: pluginDb.DomainStatusOnchainManaged, ZoneID: 9, DNSHostingEnabled: false,
		}
		_, err := svc.legacyFacts(wd, ipfs)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompatOnChainWithZone)
		var compatErr *CompatError
		require.True(t, errors.As(err, &compatErr))
		assert.Equal(t, CompatErrorOnChainWithZone, compatErr.Kind)
	})

	t.Run("onchain_icann_is_impossible", func(t *testing.T) {
		wd := &pluginDb.WebsiteDomain{
			Domain: "example.com", Namespace: pluginDb.DomainNamespaceICANN,
			Status: pluginDb.DomainStatusOnchainManaged,
		}
		_, err := svc.legacyFacts(wd, ipfs)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompatOnChainICANN)
		_, err = svc.legacyProfileFor(wd, domainpolicy.RouteObservation{})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompatOnChainICANN)
	})

	t.Run("portal_intent_without_usable_zone", func(t *testing.T) {
		// Enable-orphan: the lifecycle is past draft and the hosted flag is
		// true, but no zone reference survived.
		wd := &pluginDb.WebsiteDomain{
			Domain: "example.com", Namespace: pluginDb.DomainNamespaceICANN,
			Status: pluginDb.DomainStatusWaitingDelegation, ZoneID: 0, DNSHostingEnabled: true,
		}
		_, err := svc.legacyFacts(wd, ipfs)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompatPortalWithoutZone)
	})

	t.Run("hosted_flag_with_no_zone_unresolved", func(t *testing.T) {
		// Provisioning never progressed: unresolved, no portal DNS writes.
		wd := &pluginDb.WebsiteDomain{
			Domain: "example.com", Namespace: pluginDb.DomainNamespaceICANN,
			Status: pluginDb.DomainStatusDraft, ZoneID: 0, DNSHostingEnabled: true,
		}
		_, err := svc.legacyFacts(wd, ipfs)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompatUnresolved)

		wd.Status = pluginDb.DomainStatusError
		_, err = svc.legacyFacts(wd, ipfs)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompatUnresolved)
	})

	t.Run("unknown_target", func(t *testing.T) {
		wd := &pluginDb.WebsiteDomain{
			Domain: "example.com", Namespace: pluginDb.DomainNamespaceICANN,
			ZoneID: 3, Status: pluginDb.DomainStatusActive,
		}
		_, err := svc.legacyFacts(wd, &pluginDb.Website{TargetType: "torrent"})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompatUnknownTarget)

		_, err = svc.legacyFacts(wd, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompatUnknownTarget)
	})

	t.Run("platform_trust_unavailable", func(t *testing.T) {
		id := uint(4)
		wd := &pluginDb.WebsiteDomain{
			Domain: "docs.platform.test", Namespace: pluginDb.DomainNamespaceICANN,
			ZoneID: 8, Status: pluginDb.DomainStatusActive,
			PlatformDomainID: &id,
		}
		_, err := svc.legacyFacts(wd, ipfs)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompatPlatformTrustUnavailable)
	})
}

func TestLegacyTarget_Kinds(t *testing.T) {
	t.Run("ipfs", func(t *testing.T) {
		target, err := legacyTarget(compatIPFSWebsite(t))
		require.NoError(t, err)
		assert.Equal(t, domainpolicy.TargetKindIPFS, target.Kind)
		assert.Equal(t, "/ipfs/"+compatTestCID, target.DNSLinkPath())
	})
	t.Run("ipns", func(t *testing.T) {
		target, err := legacyTarget(compatIPNSWebsite(t))
		require.NoError(t, err)
		assert.Equal(t, domainpolicy.TargetKindIPNS, target.Kind)
		assert.Contains(t, target.DNSLinkPath(), "/ipns/")
	})
}

func TestLegacyFacts_PlatformAndSharedZones(t *testing.T) {
	// Platform bindings and zone-sharing states need database access
	// (platform trust rows, sibling zone sharers).
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)
		database := ctx.DB()
		ipfs := compatIPFSWebsite(t)

		t.Run("platform_icann_subdomain_sharing_operator_zone", func(t *testing.T) {
			pd := createPlatformRoot(tb, ctx, "platform.test", pluginDb.DomainNamespaceICANN, 700, true)
			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 2, Domain: "docs.platform.test",
				Namespace: pluginDb.DomainNamespaceICANN, ZoneID: pd.ZoneID,
				Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
				PlatformDomainID: &pd.ID,
			}
			require.NoError(t, database.Create(wd).Error)

			facts, err := svc.legacyFacts(wd, ipfs)
			require.NoError(t, err)
			require.NotNil(t, facts.PlatformRootID)
			assert.Equal(t, pd.ID, *facts.PlatformRootID)
			assert.True(t, facts.ZonePresent)
			assert.Equal(t, domainpolicy.ZoneAllocationSharedParent, facts.ZoneAllocation)
			assert.Equal(t, domainpolicy.ResolutionRouteStandardDNS, facts.DiscoveredRoute)
			assert.Equal(t, domainpolicy.BackendPowerDNS, facts.DiscoveredBackend)
			assert.Equal(t, domainpolicy.HostingRequestPortal, facts.RequestedHosting)

			profileID, err := svc.legacyProfileFor(wd, domainpolicy.RouteObservation{})
			require.NoError(t, err)
			assert.Equal(t, domainpolicy.ProfileIDPlatformICANN, profileID)

			// The generated plan must validate against the platform profile
			// (shared operator zone, platform trust).
			profile, ok := domainpolicy.DefaultRegistry().Lookup(profileID)
			require.True(t, ok)
			_, err = domainpolicy.PlanBinding(profile, facts)
			require.NoError(t, err)
		})

		t.Run("nonplatform_subdomain_shares_parent_zone_binds", func(t *testing.T) {
			// Current runtime lets a subdomain reuse the parent's zone; the
			// portal profiles encode that observed one-zone topology by
			// permitting shared-parent alongside the canonical dedicated
			// allocation. This mapper maps the SHARED state honestly, and the
			// generated plan must carry a shared-parent zone intent (no
			// create-zone effect when reconciling).
			apex := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "example.xyz",
				Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 701,
				Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
			}
			require.NoError(t, database.Create(apex).Error)
			sub := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "docs.example.xyz",
				Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 701,
				Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
			}
			require.NoError(t, database.Create(sub).Error)

			facts, err := svc.legacyFacts(sub, ipfs)
			require.NoError(t, err)
			assert.Equal(t, domainpolicy.ZoneAllocationSharedParent, facts.ZoneAllocation)

			profileID, err := svc.legacyProfileFor(sub, domainpolicy.RouteObservation{})
			require.NoError(t, err)
			assert.Equal(t, domainpolicy.ProfileIDICANNPortal, profileID)

			profile, ok := domainpolicy.DefaultRegistry().Lookup(profileID)
			require.True(t, ok)
			plan, err := domainpolicy.PlanBinding(profile, facts)
			require.NoError(t, err, "shared-parent zone facts must bind against the portal profile")
			assert.Equal(t, domainpolicy.ZoneAllocationSharedParent, plan.Zone.Allocation,
				"the plan's zone intent must carry the observed shared-parent allocation")

			// A subdomain reusing the parent's zone adopts it — it never
			// provisions a new one. Diff against the live parent zone yields
			// reuse-zone, never create-zone.
			obs := domainpolicy.ObservationSet{Zone: &domainpolicy.ZonePresenceObservation{
				Present:    true,
				Allocation: domainpolicy.ZoneAllocationSharedParent,
			}}
			effects, err := domainpolicy.Diff(plan, obs)
			require.NoError(t, err)
			require.NotEmpty(t, effects)
			assert.Equal(t, domainpolicy.EffectKindReuseZone, effects[0].Kind,
				"a shared-parent binding adopts the parent zone, never creates one")
			for _, effect := range effects {
				assert.NotEqual(t, domainpolicy.EffectKindCreateZone, effect.Kind,
					"shared-parent topology must never produce a create-zone effect")
			}
		})

		t.Run("native_hns_cannot_share_parent_zone", func(t *testing.T) {
			// Native HNS names are single-label TLDs: there is no parent zone
			// for a native binding to share, so the native portal profile
			// keeps the dedicated allocation only. Shared-parent facts on the
			// native profile remain a typed rejection (fail closed), never a
			// guess.
			apex := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "diverge-altroot",
				Namespace: pluginDb.DomainNamespaceHNS, ZoneID: 711,
				Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
			}
			require.NoError(t, database.Create(apex).Error)

			facts, err := svc.legacyFacts(apex, ipfs)
			require.NoError(t, err)
			assert.Equal(t, domainpolicy.ZoneAllocationDedicated, facts.ZoneAllocation,
				"an apex (single-label) HNS binding always owns its zone")

			profileID, err := svc.legacyProfileFor(apex, domainpolicy.RouteObservation{})
			require.NoError(t, err)
			assert.Equal(t, domainpolicy.ProfileIDHNSPortalNative, profileID)

			// The profile rejects a fabricated shared-parent allocation: the
			// native HNS topology cannot produce one, and PlanBinding must
			// not authorize a zone intent the runtime cannot reach.
			profile, ok := domainpolicy.DefaultRegistry().Lookup(profileID)
			require.True(t, ok)
			facts.ZoneAllocation = domainpolicy.ZoneAllocationSharedParent
			facts.ZonePresent = true
			_, err = domainpolicy.PlanBinding(profile, facts)
			require.Error(t, err, "shared-parent zone facts must be rejected by the native HNS portal profile")
		})
	}, TestOptions)
}
