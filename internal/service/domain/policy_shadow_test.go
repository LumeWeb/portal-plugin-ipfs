package domain

// policy_shadow_test.go pins the legacy mapper's agreement with the
// pure plan: it maps the documented current-behavior cases
// (docs/architecture/domain-hosting-overhaul.md,
// current-behavior matrix) through the legacy mapper plus pure PlanBinding and
// compares the resulting plan against the characterized legacy decisions.
// Cases whose legacy persisted state diverges from the encoded profiles
// are reported as divergences (never silently reconciled) and asserted
// against an explicit expected-divergence set.

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/datatypes"
)

// runShadowCase maps one fixture through legacyFacts / legacyProfileFor /
// PlanBinding and compares the plan against the legacy decisions. Cases with
// a non-nil wantFactsErr must be rejected at facts level (fail closed, no
// divergence); cases with a non-nil wantPlanErr are expected divergences
// (facts map, but the pure profile set cannot authorize the state).
func runShadowCase(tb coreTesting.TB, svc *DelegatedDomainService, name string, wd *pluginDb.WebsiteDomain, website *pluginDb.Website, legacy LegacyDecision, observed domainpolicy.RouteObservation, wantFactsErr, wantPlanErr error) ShadowResult {
	tb.Helper()
	facts, factsErr := svc.legacyFacts(wd, website)
	if wantFactsErr != nil {
		require.Error(tb, factsErr, "case %q must be rejected at facts level (%v)", name, wantFactsErr)
		assert.ErrorIs(tb, factsErr, wantFactsErr)
		return ShadowResult{Case: name, Match: true, Divergences: []string{"rejected at facts: " + factsErr.Error()}}
	}
	require.NoError(tb, factsErr, "case %q failed at facts level", name)

	profileID, err := svc.legacyProfileFor(wd, observed)
	require.NoError(tb, err, "case %q profile selection failed", name)
	profile, ok := domainpolicy.DefaultRegistry().Lookup(profileID)
	require.True(tb, ok, "profile %q not registered", profileID)

	plan, planErr := domainpolicy.PlanBinding(profile, facts)
	if wantPlanErr != nil {
		require.Error(tb, planErr, "case %q should be rejected at plan level (expected divergence)", name)
		return ShadowResult{Case: name, Match: false, Divergences: []string{"rejected at plan (legacy/plan divergence): " + planErr.Error()}}
	}
	require.NoError(tb, planErr, "case %q plan binding failed", name)
	return ShadowCompare(name, plan, legacy)
}

// TestLegacyShadow_CurrentBehaviorCases runs the full documented shadow-case
// matrix. A summary table is logged so review output carries the
// match/mismatch status per case.
func TestLegacyShadow_CurrentBehaviorCases(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		var results []ShadowResult
		record := func(r ShadowResult) {
			results = append(results, r)
		}
		ipfs := compatIPFSWebsite(t)

		// ICANN managed plus owner-hosted (both halves of the first matrix
		// row).
		icannManagedWd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "example.com",
			Namespace: pluginDb.DomainNamespaceICANN,
			ZoneID:    702, Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
		}
		record(runShadowCase(tb, svc, "icann managed", icannManagedWd, ipfs, LegacyDecision{
			PortalDNSWrites:        true,
			TokenTXTGate:           true,
			WebsiteDelegationGate:  true,
			DelegationGateTrivial:  true,
			ApexRecordType:         "ALIAS",
			PortalPublishesDNSLink: true,
		}, domainpolicy.RouteObservation{}, nil, nil))

		icannOwnerWd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "example.com",
			Namespace: pluginDb.DomainNamespaceICANN,
			Status:    pluginDb.DomainStatusSelfHosted, DNSHostingEnabled: false,
		}
		record(runShadowCase(tb, svc, "icann owner-hosted", icannOwnerWd, ipfs, LegacyDecision{
			PortalDNSWrites: false,
			TokenTXTGate:    true,
		}, domainpolicy.RouteObservation{}, nil, nil))

		// Native HNS managed (single-label Handshake-root TLD, delegated
		// directly).
		hnsNativeWd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "altroot",
			Namespace: pluginDb.DomainNamespaceHNS,
			ZoneID:    703, Status: pluginDb.DomainStatusWaitingDelegation, DNSHostingEnabled: true,
			DelegationData: datatypes.JSONMap{"authoritative_records": map[string]any{"type": "ns"}},
		}
		record(runShadowCase(tb, svc, "native HNS managed", hnsNativeWd, ipfs, LegacyDecision{
			PortalDNSWrites:           true,
			TokenTXTGate:              false,
			WebsiteDelegationGate:     true,
			DNSSECRequired:            true,
			DNSSECProvisionedByPortal: true,
			DANEPublicationLocus:      "portal-zone",
			ApexRecordType:            "A",
			PortalPublishesDNSLink:    true,
		}, domainpolicy.RouteObservation{}, nil, nil))

		// a.hns managed as a dedicated HNS zone (Namebase-style child).
		hnsChildWd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "a.hns",
			Namespace: pluginDb.DomainNamespaceHNS,
			ZoneID:    704, Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
		}
		record(runShadowCase(tb, svc, "a.hns dedicated HNS zone (namebase child)", hnsChildWd, ipfs, LegacyDecision{
			PortalDNSWrites:           true,
			TokenTXTGate:              false,
			WebsiteDelegationGate:     true,
			DNSSECRequired:            true,
			DNSSECProvisionedByPortal: true,
			DANEPublicationLocus:      "portal-zone",
			ApexRecordType:            "A",
			PortalPublishesDNSLink:    true,
		}, domainpolicy.RouteObservation{}, nil, nil))

		// HNS owner-hosted: the known current enforcement gap (DNSLink
		// effectively gates; TXT, delegation, DNSSEC, and live TLSA gates
		// are absent).
		hnsOwnerWd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "altroot",
			Namespace: pluginDb.DomainNamespaceHNS,
			Status:    pluginDb.DomainStatusSelfHosted, DNSHostingEnabled: false,
		}
		record(runShadowCase(tb, svc, "HNS owner-hosted current gap", hnsOwnerWd, ipfs, LegacyDecision{
			PortalDNSWrites:        false,
			TokenTXTGate:           false,
			WebsiteDelegationGate:  false,
			ApexRecordType:         "",
			PortalPublishesDNSLink: false,
		}, domainpolicy.RouteObservation{}, nil, nil))

		// HNS HIP-5 chain route with no zone.
		hnsChainWd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "altroot",
			Namespace: pluginDb.DomainNamespaceHNS,
			Status:    pluginDb.DomainStatusOnchainManaged, DNSHostingEnabled: false,
		}
		record(runShadowCase(tb, svc, "HNS HIP-5 with no zone", hnsChainWd, ipfs, LegacyDecision{
			PortalDNSWrites:        false,
			TokenTXTGate:           false,
			WebsiteDelegationGate:  false,
			DANEPublicationLocus:   "chain",
			DANEVerifiedLive:       true,
			ApexRecordType:         "",
			PortalPublishesDNSLink: false,
		}, domainpolicy.RouteObservation{
			Route: domainpolicy.ResolutionRouteCrossChain, Backend: domainpolicy.BackendEthereum,
		}, nil, nil))

		// Platform root + subdomain sharing the operator's zone.
		pd := createPlatformRoot(tb, ctx, "platform.test", pluginDb.DomainNamespaceICANN, 705, true)
		platformSubWd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 2, Domain: "docs.platform.test",
			Namespace: pluginDb.DomainNamespaceICANN, ZoneID: pd.ZoneID,
			Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
			PlatformDomainID: &pd.ID,
		}
		require.NoError(tb, ctx.DB().Create(platformSubWd).Error)
		record(runShadowCase(tb, svc, "platform root + subdomain sharing", platformSubWd, ipfs, LegacyDecision{
			PortalDNSWrites:        true,
			TokenTXTGate:           false,
			WebsiteDelegationGate:  true,
			ApexRecordType:         "ALIAS",
			PortalPublishesDNSLink: true,
		}, domainpolicy.RouteObservation{}, nil, nil))

		// On-chain status with a stray zone: fail closed, no plan.
		strayZoneWd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "altroot",
			Namespace: pluginDb.DomainNamespaceHNS,
			ZoneID:    706, Status: pluginDb.DomainStatusOnchainManaged, DNSHostingEnabled: false,
		}
		record(runShadowCase(tb, svc, "on-chain status with stray zone", strayZoneWd, ipfs, LegacyDecision{},
			domainpolicy.RouteObservation{}, ErrCompatOnChainWithZone, nil))

		// Hosted flag with no zone: both enable-orphan shapes fail closed.
		hostNoZoneDraft := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "example.com",
			Namespace: pluginDb.DomainNamespaceICANN,
			Status:    pluginDb.DomainStatusDraft, DNSHostingEnabled: true,
		}
		record(runShadowCase(tb, svc, "hosted flag with no zone (draft)", hostNoZoneDraft, ipfs, LegacyDecision{},
			domainpolicy.RouteObservation{}, ErrCompatUnresolved, nil))
		hostNoZoneOrphan := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "example.com",
			Namespace: pluginDb.DomainNamespaceICANN,
			Status:    pluginDb.DomainStatusWaitingDelegation, DNSHostingEnabled: true,
		}
		record(runShadowCase(tb, svc, "hosted flag with no zone (enable-orphan)", hostNoZoneOrphan, ipfs, LegacyDecision{},
			domainpolicy.RouteObservation{}, ErrCompatPortalWithoutZone, nil))

		// Disabled flag with a delegation-owned zone: the legacy class is
		// portal-managed (the zone reference wins over the flag), but the
		// requested-hosting axis reads owner from the flag and the
		// portal profile requires a portal request — an EXPECTED divergence,
		// rejected at plan level and flagged loudly, never reconciled.
		disabledDelegationWd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "altroot",
			Namespace: pluginDb.DomainNamespaceHNS,
			ZoneID:    707, Status: pluginDb.DomainStatusWaitingDelegation, DNSHostingEnabled: false,
			DelegationData: datatypes.JSONMap{"authoritative_records": map[string]any{"type": "ns"}},
		}
		require.NoError(tb, ctx.DB().Create(disabledDelegationWd).Error)
		record(runShadowCase(tb, svc, "disabled flag with delegation-owned zone", disabledDelegationWd, ipfs, LegacyDecision{},
			domainpolicy.RouteObservation{}, nil, errors.New("portal profile requires a portal hosting request")))

		// Stale delegation data: delegation data present while the lifecycle
		// sits in draft — the zone reference still classifies the binding as
		// portal-managed and the plan matches.
		staleDelegationWd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "stale.hns",
			Namespace: pluginDb.DomainNamespaceHNS,
			ZoneID:    708, Status: pluginDb.DomainStatusDraft, DNSHostingEnabled: true,
			DelegationData: datatypes.JSONMap{"authoritative_records": map[string]any{"type": "ns"}},
		}
		record(runShadowCase(tb, svc, "stale delegation data", staleDelegationWd, ipfs, LegacyDecision{
			PortalDNSWrites:           true,
			TokenTXTGate:              false,
			WebsiteDelegationGate:     true,
			DNSSECRequired:            true,
			DNSSECProvisionedByPortal: true,
			DANEPublicationLocus:      "portal-zone",
			ApexRecordType:            "A",
			PortalPublishesDNSLink:    true,
		}, domainpolicy.RouteObservation{}, nil, nil))

		// Non-platform subdomain sharing the parent's zone: the mapper maps
		// the shared state honestly and the ICANN portal profile permits
		// shared-parent as an observed zone topology (the one-zone rule in
		// resolveManagedZone). The plan therefore binds and must adopt the
		// parent's zone — the legacy decisions are unchanged from the apex
		// case (only the zone allocation differs).
		sharedApex := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "shared.xyz",
			Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 709,
			Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
		}
		require.NoError(tb, ctx.DB().Create(sharedApex).Error)
		sharedSub := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "docs.shared.xyz",
			Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 709,
			Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
		}
		require.NoError(tb, ctx.DB().Create(sharedSub).Error)
		record(runShadowCase(tb, svc, "non-platform subdomain shares parent zone", sharedSub, ipfs, LegacyDecision{
			PortalDNSWrites:        true,
			TokenTXTGate:           true,
			WebsiteDelegationGate:  true,
			DelegationGateTrivial:  true,
			ApexRecordType:         "ALIAS",
			PortalPublishesDNSLink: true,
		}, domainpolicy.RouteObservation{}, nil, nil))

		// Target changes: IPFS, IPNS, and IPNS-to-IPNS value changes.
		targetsWd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "targets.example.com",
			Namespace: pluginDb.DomainNamespaceICANN, ZoneID: 710,
			Status: pluginDb.DomainStatusActive, DNSHostingEnabled: true,
		}
		ipfsPlan := shadowPlanFor(tb, svc, targetsWd, ipfs)
		assert.Equal(t, domainpolicy.TargetKindIPFS, ipfsPlan.Target.Kind)
		assert.Equal(t, "/ipfs/"+compatTestCID, ipfsPlan.Target.DNSLinkPath())

		ipnsPlan := shadowPlanFor(tb, svc, targetsWd, compatIPNSWebsite(t))
		assert.Equal(t, domainpolicy.TargetKindIPNS, ipnsPlan.Target.Kind)

		// IPNS-to-IPNS change: same kind, different value (stale pointers,
		// legacy content-target drift).
		changedIPNS := compatIPNSWebsite(t)
		otherPeer, err := pluginDb.NewIPNSTargetFromString(compatTestPeerID2)
		require.NoError(t, err)
		changedIPNS.TargetMultihash = otherPeer.ToMultihash()
		changedPlan := shadowPlanFor(tb, svc, targetsWd, changedIPNS)
		assert.Equal(t, domainpolicy.TargetKindIPNS, changedPlan.Target.Kind)
		assert.NotEqual(t, ipnsPlan.Target.Value, changedPlan.Target.Value, "IPNS-to-IPNS retarget must change the target value")
		record(ShadowResult{Case: "IPFS, IPNS, and IPNS-to-IPNS target changes", Match: true,
			Divergences: []string{"plans carry the exact target of each fixture"}})

		// Report table ----------------------------------------------------
		t.Logf("shadow case results (%d cases):", len(results))
		for _, r := range results {
			status := "match"
			if !r.Match {
				status = "MISMATCH"
			}
			t.Logf("  %-9s %-48s %v", status, r.Case, r.Divergences)
		}

		// The known legacy-mapper divergence is asserted EXPLICITLY (flagged,
		// never silently reconciled): a delegation-owned zone with the
		// hosting flag disabled (requested-hosting axis reads owner while the
		// legacy class is portal-managed). The former shared-parent mismatch
		// was closed by permitting the observed one-zone topology on the
		// tolerant portal profiles. Any OTHER divergence is a failure; any
		// divergence is a report item.
		expectedDivergent := map[string]string{
			"disabled flag with delegation-owned zone": "portal hosting request",
		}
		actualDivergent := map[string]string{}
		for _, r := range results {
			if r.Match {
				continue
			}
			require.Len(t, r.Divergences, 1, "case %q must produce exactly one divergence", r.Case)
			actualDivergent[r.Case] = r.Divergences[0]
			require.ErrorContains(t, errors.New(r.Divergences[0]), expectedDivergent[r.Case],
				"divergence for case %q does not carry the expected reason", r.Case)
			delete(expectedDivergent, r.Case)
		}
		assert.Empty(t, expectedDivergent, "expected divergences missing from the shadow run: %v", expectedDivergent)
	}, TestOptions)
}

// shadowPlanFor maps wd+website through the mapper and returns the bound
// plan, failing the test on any rejection.
func shadowPlanFor(tb coreTesting.TB, svc *DelegatedDomainService, wd *pluginDb.WebsiteDomain, website *pluginDb.Website) domainpolicy.Plan {
	tb.Helper()
	facts, err := svc.legacyFacts(wd, website)
	require.NoError(tb, err)
	profileID, err := svc.legacyProfileFor(wd, domainpolicy.RouteObservation{})
	require.NoError(tb, err)
	profile, ok := domainpolicy.DefaultRegistry().Lookup(profileID)
	require.True(tb, ok)
	plan, err := domainpolicy.PlanBinding(profile, facts)
	require.NoError(tb, err)
	return plan
}
