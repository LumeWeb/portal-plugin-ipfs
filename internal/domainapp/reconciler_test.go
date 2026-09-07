package domainapp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	"go.uber.org/zap"
)

// --- fakes -------------------------------------------------------------------

type recordingDNSLinkExecutor struct {
	writes    []string // "zoneID|domain|target"
	deletes   []string // "zoneID|domain"
	writeErr  error
	deleteErr error
}

func (f *recordingDNSLinkExecutor) WriteDNSLinkRecord(_ context.Context, zoneID uint, domain, target string) error {
	if f.writeErr != nil {
		return f.writeErr
	}
	f.writes = append(f.writes, uintToString(zoneID)+"|"+domain+"|"+target)
	return nil
}

func (f *recordingDNSLinkExecutor) DeleteDNSLinkRecord(_ context.Context, zoneID uint, domain string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deletes = append(f.deletes, uintToString(zoneID)+"|"+domain)
	return nil
}

func uintToString(v uint) string {
	const digits = "0123456789"
	if v == 0 {
		return "0"
	}
	out := ""
	for v > 0 {
		out = string(digits[v%10]) + out
		v /= 10
	}
	return out
}

// --- plan builders -----------------------------------------------------------

// portalPlan binds the registered ICANN portal-current-behavior profile to
// dedicated-portal-zone facts for name/target, giving a plan whose DNSLink
// record intent publishes into the portal zone.
func portalPlan(t *testing.T, name string, target domainpolicy.ContentTarget) domainpolicy.Plan {
	t.Helper()
	return boundPlan(t, domainpolicy.ProfileIDICANNPortal, name, target,
		domainpolicy.HostingRequestPortal, true, domainpolicy.ZoneAllocationDedicated)
}

// ownerPlan binds the registered ICANN owner-hosted profile: DNSLink is an
// owner-side publication duty, never a portal write.
func ownerPlan(t *testing.T, name string, target domainpolicy.ContentTarget) domainpolicy.Plan {
	t.Helper()
	return boundPlan(t, domainpolicy.ProfileIDICANNOwner, name, target,
		domainpolicy.HostingRequestOwner, false, domainpolicy.ZoneAllocationNone)
}

func boundPlan(t *testing.T, id domainpolicy.ProfileID, name string, target domainpolicy.ContentTarget,
	requested domainpolicy.HostingRequest, zonePresent bool, allocation domainpolicy.ZoneAllocation) domainpolicy.Plan {
	t.Helper()
	profile, ok := domainpolicy.DefaultRegistry().Lookup(id)
	require.True(t, ok, "profile %s must be registered", id)
	facts, err := domainpolicy.NewBindingFacts(name)
	require.NoError(t, err)
	facts.Lifecycle = domainpolicy.LifecycleActive
	facts.RequestedHosting = requested
	facts.ZonePresent = zonePresent
	facts.ZoneAllocation = allocation
	facts.DiscoveredRoute = profile.Route
	facts.DiscoveredBackend = profile.Backend
	facts.Target = target
	facts.PolicyVersion = profile.Version
	plan, err := domainpolicy.PlanBinding(profile, facts)
	require.NoError(t, err)
	return plan
}

func ipfsTarget(t *testing.T, cid string) domainpolicy.ContentTarget {
	t.Helper()
	target, err := domainpolicy.NewContentTarget(domainpolicy.TargetKindIPFS, cid)
	require.NoError(t, err)
	return target
}

func ipnsTarget(t *testing.T, peerID string) domainpolicy.ContentTarget {
	t.Helper()
	target, err := domainpolicy.NewContentTarget(domainpolicy.TargetKindIPNS, peerID)
	require.NoError(t, err)
	return target
}

const (
	testZone  = uint(42)
	testCid   = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
	testPeerA = "12D3KooWCqvCZqaG6LmG4mtoWZZwrvYB911DK8qqwE9gc25s4Hft"
	testPeerB = "k51qzi5uqu5dlts3p5vfpw8kneqp5ye1ttb2jlt8qkt5mq9f2gvgmet6sec29r"
)

// --- reconciler ---------------------------------------------------------------

// TestReconcileDNSLink_PortalWriteCases covers the common cases the flagged
// reconciler owns: IPFS and IPNS targets, IPNS-to-IPNS switches, missing and
// mismatched live records, and the matching-record no-op.
func TestReconcileDNSLink_PortalWriteCases(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name        string
		plan        func(t *testing.T) domainpolicy.Plan
		observed    *fakeDNSLinkCollector
		wantApplied int
		wantNoOp    bool
	}{
		{
			name: "ipfs target with missing record writes",
			plan: func(t *testing.T) domainpolicy.Plan {
				return portalPlan(t, "example.com", ipfsTarget(t, testCid))
			},
			observed:    &fakeDNSLinkCollector{nxdomain: true},
			wantApplied: 1,
		},
		{
			name: "ipfs target with mismatched record writes",
			plan: func(t *testing.T) domainpolicy.Plan {
				return portalPlan(t, "example.com", ipfsTarget(t, testCid))
			},
			observed:    &fakeDNSLinkCollector{value: "/ipfs/stale-cid"},
			wantApplied: 1,
		},
		{
			name: "ipns target with missing record writes",
			plan: func(t *testing.T) domainpolicy.Plan {
				return portalPlan(t, "example.com", ipnsTarget(t, testPeerA))
			},
			observed:    &fakeDNSLinkCollector{},
			wantApplied: 1,
		},
		{
			name: "ipns-to-ipns key switch rewrites",
			plan: func(t *testing.T) domainpolicy.Plan {
				return portalPlan(t, "example.com", ipnsTarget(t, testPeerA))
			},
			observed:    &fakeDNSLinkCollector{value: "/ipns/" + testPeerB},
			wantApplied: 1,
		},
		{
			name: "matching record is a no-op",
			plan: func(t *testing.T) domainpolicy.Plan {
				return portalPlan(t, "example.com", ipfsTarget(t, testCid))
			},
			observed: &fakeDNSLinkCollector{
				value: "/ipfs/" + testCid,
			},
			wantNoOp: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := tc.plan(t)
			desired := plan.Target.DNSLinkPath()
			executor := &recordingDNSLinkExecutor{}
			result, err := ReconcileDNSLink(ctx, DNSLinkReconcileInput{
				Plan:          &plan,
				Domain:        "example.com",
				ZoneID:        testZone,
				DesiredTarget: desired,
			}, tc.observed, executor, zap.NewNop())
			require.NoError(t, err)

			assert.Equal(t, tc.wantNoOp, result.NoOp())
			assert.Len(t, result.Applied, tc.wantApplied)
			if tc.wantApplied == 1 {
				require.Len(t, executor.writes, 1)
				assert.Equal(t, uintToString(testZone)+"|example.com|"+desired, executor.writes[0])
				// The applied effect is the plan's DNSLink write: content
				// comes from the plan intent, not the call site.
				assert.Equal(t, desired, result.Applied[0].Record.Intent.Value)
				assert.Empty(t, executor.deletes)
			} else {
				assert.Empty(t, executor.writes)
				assert.Empty(t, executor.deletes)
			}
		})
	}
}

// TestReconcileDNSLink_DeferredPortalEffects verifies the executor's family
// scope: a portal plan produces zone-family effects that are REPORTED
// (deferred) and never executed; the unobserved-record write is still applied.
func TestReconcileDNSLink_DeferredPortalEffects(t *testing.T) {
	ctx := context.Background()
	plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
	executor := &recordingDNSLinkExecutor{}
	result, err := ReconcileDNSLink(ctx, DNSLinkReconcileInput{
		Plan:          &plan,
		Domain:        "example.com",
		ZoneID:        testZone,
		DesiredTarget: plan.Target.DNSLinkPath(),
	}, nil, executor, zap.NewNop())
	require.NoError(t, err)

	// The unobserved record yields the DNSLink write...
	require.Len(t, result.Applied, 1)
	require.Len(t, executor.writes, 1)
	// ...while zone provisioning (and anything else outside the DNSLink
	// family) is deferred, not executed.
	require.NotEmpty(t, result.Deferred)
	assert.Contains(t, result.Deferred, "create-zone zone dedicated")
	for _, deferred := range result.Deferred {
		assert.NotContains(t, deferred, "dnslink ")
	}
}

// TestReconcileDNSLink_OwnerInstruction verifies owner-dns and chain
// publication loci return an owner instruction and perform no write.
func TestReconcileDNSLink_OwnerInstruction(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name  string
		build func(t *testing.T) domainpolicy.Plan
	}{
		{name: "owner dns", build: func(t *testing.T) domainpolicy.Plan {
			return ownerPlan(t, "example.com", ipfsTarget(t, testCid))
		}},
		{name: "chain", build: func(t *testing.T) domainpolicy.Plan {
			// The chain-locus profile for the current behavior is the HNS
			// (HIP-5) chain profile; bind it with matching facts.
			return boundPlan(t, domainpolicy.ProfileIDHNSChainEthereum, "example.hns",
				ipfsTarget(t, testCid), domainpolicy.HostingRequestOwner, false, domainpolicy.ZoneAllocationNone)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := tc.build(t)
			executor := &recordingDNSLinkExecutor{}
			result, err := ReconcileDNSLink(ctx, DNSLinkReconcileInput{
				Plan:          &plan,
				Domain:        plan.Name,
				ZoneID:        0,
				DesiredTarget: plan.Target.DNSLinkPath(),
			}, &fakeDNSLinkCollector{nxdomain: true}, executor, zap.NewNop())
			require.NoError(t, err)
			assert.NotEmpty(t, result.OwnerInstruction)
			assert.True(t, result.NoOp())
			assert.Empty(t, executor.writes)
			assert.Empty(t, executor.deletes)
		})
	}
}

// TestReconcileDNSLink_FailClosed verifies the unknown/unrepresentable inputs
// fail closed: no effects, no writes, ErrDNSLinkNotReconciled.
func TestReconcileDNSLink_FailClosed(t *testing.T) {
	ctx := context.Background()

	goodPlan := portalPlan(t, "example.com", ipfsTarget(t, testCid))

	cases := []struct {
		name  string
		input DNSLinkReconcileInput
	}{
		{name: "nil plan", input: DNSLinkReconcileInput{
			Plan: nil, Domain: "example.com", ZoneID: testZone,
			DesiredTarget: "/ipfs/" + testCid,
		}},
		{name: "empty domain", input: DNSLinkReconcileInput{
			Plan: &goodPlan, Domain: "", ZoneID: testZone,
			DesiredTarget: "/ipfs/" + testCid,
		}},
		{name: "empty desired target", input: DNSLinkReconcileInput{
			Plan: &goodPlan, Domain: "example.com", ZoneID: testZone,
			DesiredTarget: "",
		}},
		{name: "plan/service target divergence", input: DNSLinkReconcileInput{
			Plan: &goodPlan, Domain: "example.com", ZoneID: testZone,
			DesiredTarget: "/ipns/" + testPeerA,
		}},
		{name: "portal intent without a zone", input: DNSLinkReconcileInput{
			Plan: &goodPlan, Domain: "example.com", ZoneID: 0,
			DesiredTarget: "/ipfs/" + testCid,
		}},
		{name: "plan without a DNSLink intent", input: DNSLinkReconcileInput{
			Plan: func() *domainpolicy.Plan {
				// Strip the plan's records to exercise the missing-intent
				// rejection directly.
				plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
				plan.Records = nil
				return &plan
			}(), Domain: "example.com", ZoneID: testZone,
			DesiredTarget: "/ipfs/" + testCid,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executor := &recordingDNSLinkExecutor{}
			result, err := ReconcileDNSLink(ctx, tc.input, &fakeDNSLinkCollector{nxdomain: true}, executor, zap.NewNop())
			require.ErrorIs(t, err, ErrDNSLinkNotReconciled)
			assert.Empty(t, result.Applied)
			assert.Empty(t, executor.writes)
			assert.Empty(t, executor.deletes)
		})
	}
}

// TestReconcileDNSLink_ObservationFailureWritesUnconditionally reproduces the
// legacy reconcile semantics: an unreadable live record must not block the
// idempotent converge write.
func TestReconcileDNSLink_ObservationFailureWritesUnconditionally(t *testing.T) {
	ctx := context.Background()
	plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
	executor := &recordingDNSLinkExecutor{}
	result, err := ReconcileDNSLink(ctx, DNSLinkReconcileInput{
		Plan:          &plan,
		Domain:        "example.com",
		ZoneID:        testZone,
		DesiredTarget: plan.Target.DNSLinkPath(),
	}, &fakeDNSLinkCollector{err: assert.AnError}, executor, zap.NewNop())
	require.NoError(t, err)
	require.Len(t, result.Applied, 1)
	require.Len(t, executor.writes, 1)
}

// TestReconcileDNSLink_EffectWriteErrorPropagates makes sure a failing write
// surfaces (the caller falls back or logs) instead of being swallowed.
func TestReconcileDNSLink_EffectWriteErrorPropagates(t *testing.T) {
	ctx := context.Background()
	plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
	executor := &recordingDNSLinkExecutor{writeErr: assert.AnError}
	_, err := ReconcileDNSLink(ctx, DNSLinkReconcileInput{
		Plan:          &plan,
		Domain:        "example.com",
		ZoneID:        testZone,
		DesiredTarget: plan.Target.DNSLinkPath(),
	}, nil, executor, zap.NewNop())
	require.ErrorIs(t, err, assert.AnError)
}

// TestReconcileDNSLink_DiffIdempotency pins predictability: identical inputs
// produce an identical, stable effect list (same order, same idempotency
// keys), twice in a row.
func TestReconcileDNSLink_DiffIdempotency(t *testing.T) {
	ctx := context.Background()
	plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
	input := DNSLinkReconcileInput{
		Plan:          &plan,
		Domain:        "example.com",
		ZoneID:        testZone,
		DesiredTarget: plan.Target.DNSLinkPath(),
	}

	keys := func(effects []domainpolicy.Effect) []string {
		out := make([]string, 0, len(effects))
		for _, e := range effects {
			out = append(out, e.IdempotencyKey)
		}
		return out
	}

	first, err := ReconcileDNSLink(ctx, input, nil, &recordingDNSLinkExecutor{}, zap.NewNop())
	require.NoError(t, err)
	second, err := ReconcileDNSLink(ctx, input, nil, &recordingDNSLinkExecutor{}, zap.NewNop())
	require.NoError(t, err)

	assert.Equal(t, keys(first.Effects), keys(second.Effects))
	assert.Equal(t, keys(first.Applied), keys(second.Applied))
	assert.Equal(t, first.Deferred, second.Deferred)
	// And the derived effect key material is stable, not random.
	for _, effect := range first.Effects {
		assert.NotEmpty(t, effect.IdempotencyKey)
		assert.NotEmpty(t, effect.KeyMaterial)
	}
}

// --- executor family scope ----------------------------------------------------

// TestApplyDNSLinkEffects_FamilyScope verifies that only the DNSLink
// record-write family executes and everything else is reported as deferred;
// an unknown effect kind fails closed.
func TestApplyDNSLinkEffects_FamilyScope(t *testing.T) {
	ctx := context.Background()
	plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))

	// Take the real effects Diff produces and add out-of-family ones.
	effects, err := domainpolicy.Diff(plan, domainpolicy.ObservationSet{})
	require.NoError(t, err)
	require.NotEmpty(t, effects)

	effects = append(effects,
		domainpolicy.Effect{Kind: domainpolicy.EffectKindEnsureDNSSEC, Reason: "test"},
		domainpolicy.Effect{Kind: domainpolicy.EffectKindRotateChallenge, Reason: "test"},
		domainpolicy.Effect{Kind: domainpolicy.EffectKind(99)}, // unknown
	)

	executor := &recordingDNSLinkExecutor{}
	applied, deferred, err := ApplyDNSLinkEffects(ctx, "example.com", testZone, effects, executor, zap.NewNop())
	require.ErrorIs(t, err, ErrDNSLinkNotReconciled)
	// Everything applied before the unknown effect stays applied...
	assert.NotEmpty(t, applied)
	assert.NotEmpty(t, executor.writes)
	// ...and the out-of-family effects are reported as deferred.
	assert.True(t, containsPrefix(deferred, "create-zone zone dedicated"))
	assert.True(t, containsPrefix(deferred, "ensure-dnssec"))
	assert.True(t, containsPrefix(deferred, "rotate-challenge"))
}

func containsPrefix(list []string, prefix string) bool {
	for _, item := range list {
		if len(item) >= len(prefix) && item[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// TestApplyDNSLinkEffects_DeleteFamily verifies the DNSLink delete effect is
// routed to the executor's delete port.
func TestApplyDNSLinkEffects_DeleteFamily(t *testing.T) {
	ctx := context.Background()
	executor := &recordingDNSLinkExecutor{}
	effects := []domainpolicy.Effect{{
		Kind: domainpolicy.EffectKindDeleteRecord,
		Record: &domainpolicy.PlannedRecord{
			Intent: domainpolicy.RecordIntent{
				Kind:      domainpolicy.RecordKindDNSLink,
				Name:      "_dnslink",
				Value:     "/ipfs/stale",
				Ownership: domainpolicy.RecordOwnershipBindingContent,
			},
			Destination: domainpolicy.PublicationLocusPortalZone,
		},
	}}
	_, deferred, err := ApplyDNSLinkEffects(ctx, "example.com", testZone, effects, executor, zap.NewNop())
	require.NoError(t, err)
	assert.Empty(t, deferred)
	require.Len(t, executor.deletes, 1)
	assert.Equal(t, uintToString(testZone)+"|example.com", executor.deletes[0])
}

// TestApplyDNSLinkEffects_NonDNSLinkWritesDeferred verifies that record
// writes belonging to other families (apex, challenge) are never executed.
func TestApplyDNSLinkEffects_NonDNSLinkWritesDeferred(t *testing.T) {
	ctx := context.Background()
	executor := &recordingDNSLinkExecutor{}
	effects := []domainpolicy.Effect{
		{
			Kind: domainpolicy.EffectKindWriteRecord,
			Record: &domainpolicy.PlannedRecord{
				Intent: domainpolicy.RecordIntent{
					Kind:      domainpolicy.RecordKindChallengeTXT,
					Name:      "lumeweb-verify",
					Value:     "lumeweb-verify=test-token",
					Ownership: domainpolicy.RecordOwnershipBindingSecurity,
				},
				Destination: domainpolicy.PublicationLocusPortalZone,
			},
		},
		{
			Kind: domainpolicy.EffectKindDeleteRecord,
			Record: &domainpolicy.PlannedRecord{
				Intent: domainpolicy.RecordIntent{
					Kind:      domainpolicy.RecordKindTLSA,
					Name:      "_443._tcp",
					Value:     "3 1 1 abc",
					Ownership: domainpolicy.RecordOwnershipBindingSecurity,
				},
				Destination: domainpolicy.PublicationLocusPortalZone,
			},
		},
	}
	_, deferred, err := ApplyDNSLinkEffects(ctx, "example.com", testZone, effects, executor, zap.NewNop())
	require.NoError(t, err)
	assert.Empty(t, executor.writes)
	assert.Empty(t, executor.deletes)
	assert.Len(t, deferred, 2)
}

// TestApplyDNSLinkEffects_NoExecutorFailsClosed verifies that a missing
// executor never silently drops eligible DNSLink effects.
func TestApplyDNSLinkEffects_NoExecutorFailsClosed(t *testing.T) {
	ctx := context.Background()
	plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
	effects, err := domainpolicy.Diff(plan, domainpolicy.ObservationSet{})
	require.NoError(t, err)

	_, deferred, err := ApplyDNSLinkEffects(ctx, "example.com", testZone, effects, nil, zap.NewNop())
	require.Error(t, err)
	// Deferred effects are still reported alongside the failure.
	assert.NotEmpty(t, deferred)
}
