package domain

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// syntheticTestProvider is a configurable DomainProvider test double for the
// capability matrix, kept local to this package so the domain test binary does
// not pull in the heavier test-support dependency graph (which would conflict
// on protobuf registration). See internal/testing/util.SyntheticDomainProvider
// for the equivalent shared double used by the API tests.
type syntheticTestProvider struct {
	protocol  string
	policy    pluginCore.ProviderPolicy
	called    bool   // set when VerifyDelegation runs
	verifyDS  string // expectedDS delivered to VerifyDelegation
	verifyOK  bool
	verifyErr error
}

func (p *syntheticTestProvider) Protocol() string { return p.protocol }
func (p *syntheticTestProvider) Validate(string) error {
	return nil
}
func (p *syntheticTestProvider) Inspect(context.Context, string) (bool, error) {
	return false, nil
}
func (p *syntheticTestProvider) BuildDelegation(context.Context, uint, string, *pluginDb.Website, json.RawMessage) (json.RawMessage, error) {
	return json.Marshal(map[string]any{"protocol": p.protocol})
}
func (p *syntheticTestProvider) VerifyDelegation(_ context.Context, _ string, expectedDS string) (bool, error) {
	p.called = true
	p.verifyDS = expectedDS
	return p.verifyOK, p.verifyErr
}
func (p *syntheticTestProvider) Policy() pluginCore.ProviderPolicy { return p.policy }
func (p *syntheticTestProvider) Nameservers() []string             { return nil }
func (p *syntheticTestProvider) LiveNameservers(context.Context, string) ([]string, error) {
	return nil, nil
}
func (p *syntheticTestProvider) UsesManagedZoneTLSA() bool {
	return p.policy.TLSA == pluginCore.TLSAManaged
}
func (p *syntheticTestProvider) RequiresDNSSEC() bool {
	return p.policy.DNSSEC == pluginCore.DNSSECRequired
}
func (p *syntheticTestProvider) ApexRecordType() pluginCore.RecordType {
	return p.policy.ApexRecordType
}

// TestVerifyDomain_DNSSECCapabilityMatrix exercises VerifyDomain across every
// relevant DNSSEC/TLSA policy combination using a synthetic provider, rather
// than relying only on the concrete ICANN/HNS implementations. It verifies:
//
//   - DNSSEC-required providers resolve the live DS (gated on the DNSSEC
//     policy, not the TLSA policy) and hand it to delegation verification.
//   - A DNSSEC-required zone with no live DS self-heals via EnableDNSSEC and
//     re-reads the DS; failing to heal fails closed (error, no provider
//     verification, no Active) instead of silently degrading to NS-only.
//   - DS read errors are fatal for DNSSEC-required providers and best-effort
//     non-fatal for providers that do not require DNSSEC.
func TestVerifyDomain_DNSSECCapabilityMatrix(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, mockDNS)

		cases := []struct {
			name             string
			protocol         string
			policy           pluginCore.ProviderPolicy
			initialDS        string
			initialDSErr     error
			enableHeal       bool
			healedDS         string
			wantState        DelegationVerificationState
			wantErr          bool
			expectDSNonEmpty bool // the DS handed to the provider must be live
		}{
			{
				name:             "dnssec_required_tlsa_enabled_existing_ds",
				protocol:         "synth-a",
				policy:           pluginCore.ProviderPolicy{DNSSEC: pluginCore.DNSSECRequired, TLSA: pluginCore.TLSAManaged, ApexRecordType: pluginCore.RecordTypeA},
				initialDS:        "live-ds-a",
				wantState:        DelegationVerified,
				expectDSNonEmpty: true,
			},
			{
				// The key capability-matrix case: DNSSEC required but DANE/TLSA
				// disabled — the live DS must still be resolved and passed to
				// the provider; the TLSA capability is NOT a proxy for DNSSEC.
				name:             "dnssec_required_tlsa_disabled_existing_ds",
				protocol:         "synth-b",
				policy:           pluginCore.ProviderPolicy{DNSSEC: pluginCore.DNSSECRequired, TLSA: pluginCore.TLSANotManaged, ApexRecordType: pluginCore.RecordTypeA},
				initialDS:        "live-ds-b",
				wantState:        DelegationVerified,
				expectDSNonEmpty: true,
			},
			{
				name:      "dnssec_not_required_tlsa_disabled_no_ds",
				protocol:  "synth-c",
				policy:    pluginCore.ProviderPolicy{DNSSEC: pluginCore.DNSSECNotRequired, TLSA: pluginCore.TLSANotManaged, ApexRecordType: pluginCore.RecordTypeALIAS},
				initialDS: "",
				wantState: DelegationVerified,
			},
			{
				name:             "dnssec_not_required_ds_read_error_nonfatal",
				protocol:         "synth-d",
				policy:           pluginCore.ProviderPolicy{DNSSEC: pluginCore.DNSSECNotRequired, TLSA: pluginCore.TLSANotManaged, ApexRecordType: pluginCore.RecordTypeALIAS},
				initialDSErr:     errors.New("powerdns down"),
				wantState:        DelegationVerified,
				expectDSNonEmpty: false,
			},
			{
				name:         "dnssec_required_ds_read_error_fatal",
				protocol:     "synth-e",
				policy:       pluginCore.ProviderPolicy{DNSSEC: pluginCore.DNSSECRequired, TLSA: pluginCore.TLSANotManaged, ApexRecordType: pluginCore.RecordTypeA},
				initialDSErr: errors.New("powerdns down"),
				wantErr:      true,
			},
			{
				name:             "dnssec_required_empty_then_enable_then_live_ds",
				protocol:         "synth-f",
				policy:           pluginCore.ProviderPolicy{DNSSEC: pluginCore.DNSSECRequired, TLSA: pluginCore.TLSANotManaged, ApexRecordType: pluginCore.RecordTypeA},
				initialDS:        "",
				enableHeal:       true,
				healedDS:         "minted-ds",
				wantState:        DelegationVerified,
				expectDSNonEmpty: true,
			},
			{
				// Fail-closed: EnableDNSSEC succeeded but the post-heal DS read
				// is still empty. VerifyDomain must error, never call the
				// provider's VerifyDelegation, and never mark the binding Active.
				name:       "dnssec_required_empty_after_heal_fails_closed",
				protocol:   "synth-g",
				policy:     pluginCore.ProviderPolicy{DNSSEC: pluginCore.DNSSECRequired, TLSA: pluginCore.TLSAManaged, ApexRecordType: pluginCore.RecordTypeA},
				initialDS:  "",
				enableHeal: true,
				healedDS:   "",
				wantErr:    true,
			},
		}

		zoneSeq := 0
		for _, tc := range cases {
			zoneSeq++
			zone := uint(2000 + zoneSeq)
			domain := tc.protocol + ".example"

			prov := &syntheticTestProvider{protocol: tc.protocol, policy: tc.policy, verifyOK: true}
			svc.registry.Register(prov)

			t.Run(tc.name, func(t *testing.T) {
				wd := &pluginDb.WebsiteDomain{
					WebsiteID: 1, UserID: 1, Domain: domain, Namespace: pluginDb.DomainNamespace(tc.protocol),
					ZoneID: zone, Status: pluginDb.DomainStatusRecordsGenerated,
				}
				require.NoError(t, db.Create(wd).Error)

				if tc.initialDSErr != nil {
					mockDNS.EXPECT().GetActiveDNSSECDS(mock.Anything, zone).Return("", tc.initialDSErr).Once()
				} else {
					mockDNS.EXPECT().GetActiveDNSSECDS(mock.Anything, zone).Return(tc.initialDS, nil).Once()
				}
				if tc.enableHeal {
					mockDNS.EXPECT().EnableDNSSEC(mock.Anything, zone).Return("dnskey", nil).Once()
					// Post-heal re-read; registered after the initial read so
					// testimony matches the first call to the second expectation.
					mockDNS.EXPECT().GetActiveDNSSECDS(mock.Anything, zone).Return(tc.healedDS, nil).Once()
				}
				mockDNS.EXPECT().EnsureSOAMNAME(mock.Anything, zone, domain, mock.Anything).Return(nil).Maybe()

				res, err := svc.VerifyDomain(context.Background(), wd)
				if tc.wantErr {
					require.Error(t, err)
					assert.False(t, prov.called, "provider VerifyDelegation must not run when the DNSSEC invariant failed")
					var persisted pluginDb.WebsiteDomain
					require.NoError(t, db.Unscoped().First(&persisted, wd.ID).Error)
					assert.NotEqual(t, pluginDb.DomainStatusActive, persisted.Status,
						"a binding whose DNSSEC invariant could not be established must not become Active")
					return
				}

				require.NoError(t, err)
				assert.Equal(t, tc.wantState, res.State)
				assert.True(t, prov.called, "delegation verification must run for a healthy portal-managed binding")
				if tc.expectDSNonEmpty {
					assert.NotEmpty(t, prov.verifyDS, "a DNSSEC-required provider must receive the live DS")
				}
				var persisted pluginDb.WebsiteDomain
				require.NoError(t, db.Unscoped().First(&persisted, wd.ID).Error)
				if tc.wantState == DelegationVerified {
					assert.Equal(t, pluginDb.DomainStatusActive, persisted.Status)
				}
			})
		}
	}, TestOptions)
}

// TestVerifyDomain_OnchainStrayZone_NotApplicableNoPortalDNS proves the
// cross-service safety rule: an on-chain managed binding carrying a stray
// (incoherent) zone ID is classified OnChainManaged — VerifyDomain returns
// NotApplicable and performs no portal DNS reads (GetActiveDNSSECDS is never
// consulted), no EnableDNSSEC, and no provider delegation verification.
func TestVerifyDomain_OnchainStrayZone_NotApplicableNoPortalDNS(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		// Any namespace provider here; the on-chain status short-circuits
		// before any provider capability is consulted.
		prov := &syntheticTestProvider{
			protocol: "onchain-synth",
			policy:   pluginCore.ProviderPolicy{DNSSEC: pluginCore.DNSSECRequired, TLSA: pluginCore.TLSAManaged, ApexRecordType: pluginCore.RecordTypeA},
			verifyOK: true,
		}
		svc.registry.Register(prov)

		wd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "onchain.example",
			Namespace: pluginDb.DomainNamespace("onchain-synth"),
			ZoneID:    77, // stray, incoherent zone reference
			Status:    pluginDb.DomainStatusOnchainManaged,
		}
		require.NoError(t, db.Create(wd).Error)

		res, err := svc.VerifyDomain(context.Background(), wd)
		require.NoError(t, err)
		assert.Equal(t, DelegationNotApplicable, res.State)
		assert.False(t, prov.called, "no provider verification for a non-portal binding")

		mockDNS.AssertNotCalled(t, "GetActiveDNSSECDS")
		mockDNS.AssertNotCalled(t, "EnableDNSSEC")
		mockDNS.AssertNotCalled(t, "EnsureSOAMNAME")

		// Status must be left untouched (not promoted to Active).
		var persisted pluginDb.WebsiteDomain
		require.NoError(t, db.First(&persisted, wd.ID).Error)
		assert.Equal(t, pluginDb.DomainStatusOnchainManaged, persisted.Status)
	}, TestOptions)
}
