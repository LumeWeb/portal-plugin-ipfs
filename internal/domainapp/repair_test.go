package domainapp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	"go.uber.org/zap"
)

// --- fakes -------------------------------------------------------------------

type recordingRepairExecutor struct {
	linkWrites     []string // "zoneID|domain|target"
	challengeCalls []string // "zoneID|domain|tokenRecord"
	dnssecEnsures  []uint
	soaEnsures     []string // "zoneID|domain|ns0"
	linkErr        error
	challengeErr   error
	dnssecErr      error
	soaErr         error
}

func (f *recordingRepairExecutor) WriteDNSLinkRecord(_ context.Context, zoneID uint, domain, target string) error {
	if f.linkErr != nil {
		return f.linkErr
	}
	f.linkWrites = append(f.linkWrites, uintToString(zoneID)+"|"+domain+"|"+target)
	return nil
}

func (f *recordingRepairExecutor) WriteChallengeRecord(_ context.Context, zoneID uint, domain, tokenRecord string) error {
	if f.challengeErr != nil {
		return f.challengeErr
	}
	f.challengeCalls = append(f.challengeCalls, uintToString(zoneID)+"|"+domain+"|"+tokenRecord)
	return nil
}

func (f *recordingRepairExecutor) EnableZoneDNSSEC(_ context.Context, zoneID uint) error {
	if f.dnssecErr != nil {
		return f.dnssecErr
	}
	f.dnssecEnsures = append(f.dnssecEnsures, zoneID)
	return nil
}

func (f *recordingRepairExecutor) EnsureZoneSOAMNAME(_ context.Context, zoneID uint, domain string, nameservers []string) error {
	if f.soaErr != nil {
		return f.soaErr
	}
	ns0 := ""
	if len(nameservers) > 0 {
		ns0 = nameservers[0]
	}
	f.soaEnsures = append(f.soaEnsures, uintToString(zoneID)+"|"+domain+"|"+ns0)
	return nil
}

func effectKinds(effects []domainpolicy.Effect) []domainpolicy.EffectKind {
	kinds := make([]domainpolicy.EffectKind, 0, len(effects))
	for _, e := range effects {
		kinds = append(kinds, e.Kind)
	}
	return kinds
}

func containsKind(effects []domainpolicy.Effect, kind domainpolicy.EffectKind) bool {
	for _, k := range effectKinds(effects) {
		if k == kind {
			return true
		}
	}
	return false
}

// --- challenge rotation ----------------------------------------------

func testRotationInput(t *testing.T, plan domainpolicy.Plan, mutate func(*ChallengeRotationInput)) ChallengeRotationInput {
	t.Helper()
	input := ChallengeRotationInput{
		Plan:               &plan,
		Domain:             "example.com",
		ZoneID:             testZone,
		ChallengeExpired:   true,
		DesiredDNSLinkPath: plan.Target.DNSLinkPath(),
		TokenRecord:        domainpolicy.ChallengeRecordLabel + "=fresh-token",
	}
	if mutate != nil {
		mutate(&input)
	}
	return input
}

func TestAuthorizeChallengeRotation(t *testing.T) {
	t.Run("portal-managed challenge with a zone authorizes", func(t *testing.T) {
		plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
		locus, err := AuthorizeChallengeRotation(testRotationInput(t, plan, nil))
		require.NoError(t, err)
		assert.Equal(t, domainpolicy.PublicationLocusPortalZone, locus)
	})

	t.Run("owner-published challenge authorizes without a zone", func(t *testing.T) {
		plan := ownerPlan(t, "example.com", ipfsTarget(t, testCid))
		locus, err := AuthorizeChallengeRotation(testRotationInput(t, plan, func(i *ChallengeRotationInput) {
			i.ZoneID = 0
		}))
		require.NoError(t, err)
		assert.Equal(t, domainpolicy.PublicationLocusOwnerDNS, locus)
	})

	t.Run("nil plan fails closed", func(t *testing.T) {
		_, err := AuthorizeChallengeRotation(testRotationInput(t, portalPlan(t, "example.com", ipfsTarget(t, testCid)), func(i *ChallengeRotationInput) {
			i.Plan = nil
		}))
		require.ErrorIs(t, err, ErrRepairNotReconciled)
	})

	t.Run("unexpired token fails closed", func(t *testing.T) {
		plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
		_, err := AuthorizeChallengeRotation(testRotationInput(t, plan, func(i *ChallengeRotationInput) {
			i.ChallengeExpired = false
		}))
		require.ErrorIs(t, err, ErrRepairNotReconciled)
	})

	t.Run("plan without a rotate repair fails closed", func(t *testing.T) {
		// HNS portal-native encodes ensure-dnssec/ensure-soa-mname heals but
		// no challenge rotation.
		plan := boundPlan(t, domainpolicy.ProfileIDHNSPortalNative, "example.com", ipfsTarget(t, testCid),
			domainpolicy.HostingRequestPortal, true, domainpolicy.ZoneAllocationDedicated)
		_, err := AuthorizeChallengeRotation(testRotationInput(t, plan, nil))
		require.ErrorIs(t, err, ErrRepairNotReconciled)
	})

	t.Run("portal locus without a zone reference fails closed", func(t *testing.T) {
		plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
		_, err := AuthorizeChallengeRotation(testRotationInput(t, plan, func(i *ChallengeRotationInput) {
			i.ZoneID = 0
		}))
		require.ErrorIs(t, err, ErrRepairNotReconciled)
	})

	t.Run("token key divergence from the plan label fails closed", func(t *testing.T) {
		plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
		_, err := AuthorizeChallengeRotation(testRotationInput(t, plan, func(i *ChallengeRotationInput) {
			i.TokenRecord = "other-key=token"
		}))
		require.ErrorIs(t, err, ErrRepairNotReconciled)
	})

	t.Run("plan/service DNSLink divergence fails closed", func(t *testing.T) {
		plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
		_, err := AuthorizeChallengeRotation(testRotationInput(t, plan, func(i *ChallengeRotationInput) {
			i.DesiredDNSLinkPath = "/ipns/divergent"
		}))
		require.ErrorIs(t, err, ErrRepairNotReconciled)
	})
}

func TestReconcileChallengeRotation_PortalWrite(t *testing.T) {
	ctx := context.Background()
	plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
	executor := &recordingRepairExecutor{}
	result, err := ReconcileChallengeRotation(ctx, testRotationInput(t, plan, nil), executor, zap.NewNop())
	require.NoError(t, err)

	// The rotation command wrote the fresh token's challenge record and
	// re-asserted the DNSLink (legacy rotation wrote it unconditionally).
	require.Len(t, executor.challengeCalls, 1)
	assert.Equal(t, uintToString(testZone)+"|example.com|"+domainpolicy.ChallengeRecordLabel+"=fresh-token", executor.challengeCalls[0])
	require.Len(t, executor.linkWrites, 1)
	assert.Equal(t, uintToString(testZone)+"|example.com|/ipfs/"+testCid, executor.linkWrites[0])

	// The zone-heal and zone-lifecycle families are never touched here.
	assert.Empty(t, executor.dnssecEnsures)
	assert.Empty(t, executor.soaEnsures)

	assert.Equal(t, domainpolicy.PublicationLocusPortalZone, result.PublicationLocus)
	assert.True(t, containsKind(result.Applied, domainpolicy.EffectKindRotateChallenge))
	assert.NotEmpty(t, result.Deferred, "unrepresentable effects are reported, never silently dropped")
}

func TestReconcileChallengeRotation_OwnerPublished(t *testing.T) {
	ctx := context.Background()
	plan := ownerPlan(t, "example.com", ipfsTarget(t, testCid))
	executor := &recordingRepairExecutor{}
	result, err := ReconcileChallengeRotation(ctx, testRotationInput(t, plan, func(i *ChallengeRotationInput) {
		i.ZoneID = 0
	}), executor, zap.NewNop())
	require.NoError(t, err)

	// Owner-published challenges are never a portal write: the portal only
	// rotated the persisted DB token (the caller's concern).
	assert.Empty(t, executor.challengeCalls)
	assert.Empty(t, executor.linkWrites)
	assert.Equal(t, domainpolicy.PublicationLocusOwnerDNS, result.PublicationLocus)
	assert.NotEmpty(t, result.OwnerInstruction)
	assert.Empty(t, result.Applied)
}

func TestReconcileChallengeRotation_BestEffortRecordFailure(t *testing.T) {
	ctx := context.Background()
	plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
	executor := &recordingRepairExecutor{challengeErr: errors.New("pdns down"), linkErr: errors.New("pdns down")}
	result, err := ReconcileChallengeRotation(ctx, testRotationInput(t, plan, nil), executor, zap.NewNop())

	// Record-write failures are best-effort (the token was already
	// persisted): logged, not returned, never double-written.
	require.NoError(t, err)
	assert.Empty(t, result.Applied)
	assert.NotEmpty(t, result.Deferred)
}

func TestReconcileChallengeRotation_MissingTokenRecordFailsClosed(t *testing.T) {
	ctx := context.Background()
	plan := portalPlan(t, "example.com", ipfsTarget(t, testCid))
	_, err := ReconcileChallengeRotation(ctx, testRotationInput(t, plan, func(i *ChallengeRotationInput) {
		i.TokenRecord = ""
	}), &recordingRepairExecutor{}, zap.NewNop())
	require.ErrorIs(t, err, ErrRepairNotReconciled)
}

func TestReconcileChallengeRotation_NoExecutorFailsClosed(t *testing.T) {
	ctx := context.Background()
	_, _, err := ApplyRepairEffects(ctx, RepairApplyInput{
		Domain:          "example.com",
		ZoneID:          testZone,
		ChallengeRecord: domainpolicy.ChallengeRecordLabel + "=fresh-token",
	}, []domainpolicy.Effect{{
		Kind:           domainpolicy.EffectKindRotateChallenge,
		IdempotencyKey: "k",
		Record:         &domainpolicy.PlannedRecord{},
	}}, nil, zap.NewNop())
	require.Error(t, err, "no repair executor wired must fail closed, never silently drop effects")
}

// --- zone heal ---------------------------------------------------------

func TestReconcileZoneHeal(t *testing.T) {
	ctx := context.Background()

	t.Run("disabled DNSSEC and drifted SOA are healed", func(t *testing.T) {
		plan := boundPlan(t, domainpolicy.ProfileIDHNSPortalNative, "example.com", ipfsTarget(t, testCid),
			domainpolicy.HostingRequestPortal, true, domainpolicy.ZoneAllocationDedicated)
		executor := &recordingRepairExecutor{}
		result, err := ReconcileZoneHeal(ctx, ZoneHealInput{
			Plan:       &plan,
			Domain:     "example.com",
			ZoneID:     testZone,
			ZoneDNSSEC: &domainpolicy.ZoneDNSSECObservation{State: domainpolicy.ZoneDNSSECStateDisabled},
			SOAMNAME: &domainpolicy.SOAMNAMEObservation{
				Found: true, Current: "ns1.legacy.example", MatchesPortalMNAME: false,
			},
			Nameservers: []string{"ns1.portal.example"},
		}, executor, zap.NewNop())
		require.NoError(t, err)

		assert.Equal(t, []uint{testZone}, executor.dnssecEnsures, "ensure-dnssec applied once")
		require.Len(t, executor.soaEnsures, 1)
		assert.Equal(t, uintToString(testZone)+"|example.com|ns1.portal.example", executor.soaEnsures[0])
		assert.True(t, result.DNSSECEnsured)
		// Unrepresentable placeholder record writes (apex ALIAS and the
		// challenge token) are reported, never executed.
		assert.NotEmpty(t, result.Deferred)
		for _, effect := range result.Effects {
			if effect.Kind == domainpolicy.EffectKindWriteRecord {
				assert.NotEqual(t, domainpolicy.RecordKindDNSLink, effect.Record.Intent.Kind,
					"the heal must not rewrite the DNSLink record (observed as matching)")
			}
		}
		// The DNSLink intent was observed as matching: no portal write for it.
		for _, d := range executor.linkWrites {
			assert.NotEqual(t, uintToString(testZone)+"|example.com", d[:len(d)-len("/ipfs/"+testCid)-1]+"|/ipfs/"+testCid)
		}
	})

	t.Run("healthy zone is a no-op", func(t *testing.T) {
		plan := boundPlan(t, domainpolicy.ProfileIDHNSPortalNative, "example.com", ipfsTarget(t, testCid),
			domainpolicy.HostingRequestPortal, true, domainpolicy.ZoneAllocationDedicated)
		executor := &recordingRepairExecutor{}
		result, err := ReconcileZoneHeal(ctx, ZoneHealInput{
			Plan:       &plan,
			Domain:     "example.com",
			ZoneID:     testZone,
			ZoneDNSSEC: &domainpolicy.ZoneDNSSECObservation{State: domainpolicy.ZoneDNSSECStateEnabled},
			SOAMNAME: &domainpolicy.SOAMNAMEObservation{
				Found: true, Current: "ns1.portal.example.", MatchesPortalMNAME: true,
			},
			Nameservers: []string{"ns1.portal.example"},
		}, executor, zap.NewNop())
		require.NoError(t, err)
		assert.Empty(t, executor.dnssecEnsures)
		assert.Empty(t, executor.soaEnsures)
		assert.False(t, result.DNSSECEnsured)
		assert.Empty(t, result.Applied)
	})

	t.Run("DNSSEC ensure failure is fatal", func(t *testing.T) {
		plan := boundPlan(t, domainpolicy.ProfileIDHNSPortalNative, "example.com", ipfsTarget(t, testCid),
			domainpolicy.HostingRequestPortal, true, domainpolicy.ZoneAllocationDedicated)
		executor := &recordingRepairExecutor{dnssecErr: errors.New("pdns refusing")}
		_, err := ReconcileZoneHeal(ctx, ZoneHealInput{
			Plan:       &plan,
			Domain:     "example.com",
			ZoneID:     testZone,
			ZoneDNSSEC: &domainpolicy.ZoneDNSSECObservation{State: domainpolicy.ZoneDNSSECStateDisabled},
			SOAMNAME: &domainpolicy.SOAMNAMEObservation{
				Found: true, Current: "ns1.legacy.example", MatchesPortalMNAME: false,
			},
			Nameservers: []string{"ns1.portal.example"},
		}, executor, zap.NewNop())
		require.Error(t, err, "a managed zone that cannot be signed must fail the verification")
		assert.Empty(t, executor.soaEnsures, "no effects run after a fatal family failure")
	})

	t.Run("SOA ensure failure is best-effort", func(t *testing.T) {
		plan := boundPlan(t, domainpolicy.ProfileIDHNSPortalNative, "example.com", ipfsTarget(t, testCid),
			domainpolicy.HostingRequestPortal, true, domainpolicy.ZoneAllocationDedicated)
		executor := &recordingRepairExecutor{soaErr: errors.New("pdns refusing")}
		result, err := ReconcileZoneHeal(ctx, ZoneHealInput{
			Plan:       &plan,
			Domain:     "example.com",
			ZoneID:     testZone,
			ZoneDNSSEC: &domainpolicy.ZoneDNSSECObservation{State: domainpolicy.ZoneDNSSECStateEnabled},
			SOAMNAME: &domainpolicy.SOAMNAMEObservation{
				Found: true, Current: "ns1.legacy.example", MatchesPortalMNAME: false,
			},
			Nameservers: []string{"ns1.portal.example"},
		}, executor, zap.NewNop())
		require.NoError(t, err, "the SOA MNAME heal is best-effort, never fatal")
		assert.Empty(t, result.Applied, "a failed reconciled write is not reported as applied (no double-write)")
	})

	t.Run("nil plan fails closed without writes", func(t *testing.T) {
		executor := &recordingRepairExecutor{}
		_, err := ReconcileZoneHeal(ctx, ZoneHealInput{
			Domain: "example.com", ZoneID: testZone,
		}, executor, zap.NewNop())
		require.ErrorIs(t, err, ErrRepairNotReconciled)
		assert.Empty(t, executor.dnssecEnsures)
		assert.Empty(t, executor.soaEnsures)
	})

	t.Run("zero zone reference fails closed", func(t *testing.T) {
		plan := boundPlan(t, domainpolicy.ProfileIDHNSPortalNative, "example.com", ipfsTarget(t, testCid),
			domainpolicy.HostingRequestPortal, true, domainpolicy.ZoneAllocationDedicated)
		_, err := ReconcileZoneHeal(ctx, ZoneHealInput{
			Plan: &plan, Domain: "example.com", ZoneID: 0,
		}, &recordingRepairExecutor{}, zap.NewNop())
		require.ErrorIs(t, err, ErrRepairNotReconciled)
	})
}

// --- shared doctrine -------------------------------------------------------------

func TestApplyRepairEffects_UnknownKindFailsClosed(t *testing.T) {
	ctx := context.Background()
	_, _, err := ApplyRepairEffects(ctx, RepairApplyInput{Domain: "example.com", ZoneID: testZone},
		[]domainpolicy.Effect{{Kind: domainpolicy.EffectKind(99), IdempotencyKey: "k"}},
		&recordingRepairExecutor{}, zap.NewNop())
	require.ErrorIs(t, err, ErrRepairNotReconciled)
}
