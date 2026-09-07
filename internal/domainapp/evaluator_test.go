package domainapp

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
)

// --- collector fakes ----------------------------------------------------------

type fakeDNSLinkCollector struct {
	value    string
	nxdomain bool
	err      error
	calls    int
}

func (f *fakeDNSLinkCollector) CollectDNSLink(context.Context, string) (DNSLinkObserved, error) {
	f.calls++
	if f.err != nil {
		return DNSLinkObserved{}, f.err
	}
	return DNSLinkObserved{
		Observation: domainpolicy.DNSLinkObservation{Value: f.value},
		NXDOMAIN:    f.nxdomain,
	}, nil
}

type fakeTokenTXTCollector struct {
	records []string
	err     error
	calls   int
}

func (f *fakeTokenTXTCollector) CollectTokenTXT(context.Context, string) ([]string, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.records, nil
}

type fakeChainTLSACollector struct {
	observed ChainTLSAObserved
	err      error
	calls    int
}

func (f *fakeChainTLSACollector) CollectChainTLSA(context.Context, string) (ChainTLSAObserved, error) {
	f.calls++
	if f.err != nil {
		return ChainTLSAObserved{}, f.err
	}
	return f.observed, nil
}

// parityPlan builds a minimal plan usable by domainpolicy.Evaluate: a byte
// fixture, not a behavior statement — it exists so the plan path (Evaluate)
// and the legacy-mirroring path (decideWebsiteGate) receive identical gates.
func parityPlan(t *testing.T, dnsLinkExpected string) domainpolicy.Plan {
	t.Helper()
	dnssec, err := domainpolicy.NewSecurityPlan(domainpolicy.RequirementNotApplicable,
		domainpolicy.ActorUnknown, domainpolicy.PublicationLocusNone, domainpolicy.VerificationModeNone)
	require.NoError(t, err)
	dane, err := domainpolicy.NewSecurityPlan(domainpolicy.RequirementNotApplicable,
		domainpolicy.ActorUnknown, domainpolicy.PublicationLocusNone, domainpolicy.VerificationModeNone)
	require.NoError(t, err)
	target, err := domainpolicy.NewContentTarget(domainpolicy.TargetKindIPFS, "bafk-test")
	require.NoError(t, err)
	return domainpolicy.Plan{
		ProfileID:      domainpolicy.ProfileID("test.website.parity"),
		ProfileVersion: domainpolicy.ProfileVersion(1),
		Name:           "example.com",
		Target:         target,
		Route:          domainpolicy.ResolutionRouteStandardDNS,
		Authority:      domainpolicy.AuthorityLocusOwnerDNS,
		Zone: domainpolicy.ZoneIntent{
			Authority:  domainpolicy.AuthorityLocusOwnerDNS,
			Allocation: domainpolicy.ZoneAllocationNone,
		},
		Gates: []domainpolicy.Gate{
			{Kind: domainpolicy.GateDNSLink, Flow: domainpolicy.FlowWebsiteValidation, Expected: dnsLinkExpected},
			{Kind: domainpolicy.GateChallengeTXT, Flow: domainpolicy.FlowWebsiteValidation},
			{Kind: domainpolicy.GateTLSA, Flow: domainpolicy.FlowWebsiteValidation},
		},
		DNSSEC: dnssec,
		DANE:   dane,
	}
}

// expectedDNSLink is the fixture target path; the parity plan encodes it as
// its DNSLink gate expectation.
const expectedDNSLink = "/ipfs/bafk-test"

func stageInput(plan *domainpolicy.Plan) WebsiteValidationInput {
	return WebsiteValidationInput{
		Domain:          "example.com",
		Plan:            plan,
		ExpectedDNSLink: expectedDNSLink,
	}
}

// --- DNSLink gate -------------------------------------------------------------

func TestEvaluateWebsiteStage_DNSLinkGate(t *testing.T) {
	t.Run("pass_adds_matching_check_with_expected_and_found", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.DNSLink = &fakeDNSLinkCollector{value: expectedDNSLink}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{DNSLink: &DNSLinkGate{}})
		require.NoError(t, err)
		assert.Nil(t, res.Failure)
		require.Len(t, res.Checks, 1)
		assert.Equal(t, pluginCore.ValidationCheckDNSLink, res.Checks[0].Name)
		assert.True(t, res.Checks[0].OK)
		assert.Equal(t, MsgWebsiteDNSLinkMatched, res.Checks[0].Message)
		assert.Equal(t, expectedDNSLink, res.Checks[0].Expected)
		assert.Equal(t, expectedDNSLink, res.Checks[0].Found)
	})

	t.Run("nxdomain_reports_missing_with_dnsmisssing_reason", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.DNSLink = &fakeDNSLinkCollector{nxdomain: true}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{DNSLink: &DNSLinkGate{}})
		require.NoError(t, err)
		require.NotNil(t, res.Failure)
		assert.Equal(t, pluginCore.ValidationReasonDNSMissing, res.Failure.Reason)
		assert.Equal(t, fmt.Sprintf(MsgWebsiteDNSMissing, "example.com"), res.Failure.Message)
		require.Len(t, res.Checks, 1)
		assert.False(t, res.Checks[0].OK)
		assert.Equal(t, fmt.Sprintf(MsgWebsiteDNSLinkNotFoundAt, "example.com"), res.Checks[0].Message)
		assert.Empty(t, res.Checks[0].Expected)
		assert.Empty(t, res.Checks[0].Found)
	})

	t.Run("mismatch_reports_dns_mismatch_with_expected_and_found", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.DNSLink = &fakeDNSLinkCollector{value: "/ipfs/served"}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{DNSLink: &DNSLinkGate{}})
		require.NoError(t, err)
		require.NotNil(t, res.Failure)
		assert.Equal(t, pluginCore.ValidationReasonDNSMismatch, res.Failure.Reason)
		detail := fmt.Sprintf(MsgWebsiteDNSMismatch, expectedDNSLink, "/ipfs/served")
		assert.Equal(t, detail, res.Failure.Message)
		require.Len(t, res.Checks, 1)
		assert.False(t, res.Checks[0].OK)
		assert.Equal(t, detail, res.Checks[0].Message)
		assert.Equal(t, expectedDNSLink, res.Checks[0].Expected)
		assert.Equal(t, "/ipfs/served", res.Checks[0].Found)
	})

	t.Run("mismatch_reports_collector_supplied_legacy_found_verbatim", func(t *testing.T) {
		// The collector collapses the resolver result into the legacy-found
		// candidate (first ipns link on failure when the ipfs link missed —
		// see the website service's legacyDNSLinkMatched). The check must
		// carry that value verbatim so mismatch diagnostics stay
		// legacy-identical for TXT with both an ipfs and an ipns link.
		input := stageInput(nil)
		ipnsCandidate := "/ipns/stale-peer"
		input.Collectors.DNSLink = &fakeDNSLinkCollector{value: ipnsCandidate}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{DNSLink: &DNSLinkGate{}})
		require.NoError(t, err)
		require.NotNil(t, res.Failure)
		assert.Equal(t, pluginCore.ValidationReasonDNSMismatch, res.Failure.Reason)
		assert.Contains(t, res.Failure.Message, ipnsCandidate)
		require.Len(t, res.Checks, 1)
		assert.Equal(t, ipnsCandidate, res.Checks[0].Found)
	})

	t.Run("present_record_without_links_reports_mismatch_with_empty_found", func(t *testing.T) {
		// Today: a record that exists but carries no ipfs/ipns link is a
		// mismatch with an empty found value — NOT record-class absence.
		input := stageInput(nil)
		input.Collectors.DNSLink = &fakeDNSLinkCollector{value: ""}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{DNSLink: &DNSLinkGate{}})
		require.NoError(t, err)
		require.NotNil(t, res.Failure)
		assert.Equal(t, pluginCore.ValidationReasonDNSMismatch, res.Failure.Reason)
		require.Len(t, res.Checks, 1)
		assert.Empty(t, res.Checks[0].Found)
		assert.NotEqual(t, fmt.Sprintf(MsgWebsiteDNSLinkNotFoundAt, "example.com"), res.Checks[0].Message)
	})

	t.Run("transport_failure_aborts_with_wrapped_error", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.DNSLink = &fakeDNSLinkCollector{err: errors.New("network error")}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{DNSLink: &DNSLinkGate{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "network error")
		assert.NotNil(t, res) // zero result: caller discards on error
		assert.Nil(t, res.Failure)
	})

	t.Run("unwired_gate_collector_fails_closed", func(t *testing.T) {
		res, err := EvaluateWebsiteStage(context.Background(), stageInput(nil), WebsiteStages{DNSLink: &DNSLinkGate{}})
		require.Error(t, err)
		assert.Nil(t, res.Failure)
	})
}

// --- Challenge TXT gate ---------------------------------------------------------

func TestEvaluateWebsiteStage_TokenTXTGate(t *testing.T) {
	tokenGate := &TokenTXTGate{TokenKey: "lumeweb-verify", Token: "tok123"}
	tokenRecord := "lumeweb-verify=tok123"

	t.Run("pass_adds_check_without_found_value", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.TokenTXT = &fakeTokenTXTCollector{records: []string{"unrelated=1", tokenRecord}}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{TokenTXT: tokenGate})
		require.NoError(t, err)
		assert.Nil(t, res.Failure)
		require.Len(t, res.Checks, 1)
		assert.True(t, res.Checks[0].OK)
		assert.Equal(t, MsgWebsiteTokenPresent, res.Checks[0].Message)
		assert.Empty(t, res.Checks[0].Expected)
		assert.Empty(t, res.Checks[0].Found)
	})

	t.Run("contains_match_passes_like_the_legacy_token_check", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.TokenTXT = &fakeTokenTXTCollector{records: []string{"extra " + tokenRecord}}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{TokenTXT: tokenGate})
		require.NoError(t, err)
		assert.Nil(t, res.Failure)
	})

	t.Run("missing_token_reports_token_missing", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.TokenTXT = &fakeTokenTXTCollector{records: []string{"some-other=foo"}}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{TokenTXT: tokenGate})
		require.NoError(t, err)
		require.NotNil(t, res.Failure)
		assert.Equal(t, pluginCore.ValidationReasonTokenMissing, res.Failure.Reason)
		msg := fmt.Sprintf(MsgWebsiteTokenMissing, "lumeweb-verify", "example.com", "example.com")
		assert.Equal(t, msg, res.Failure.Message)
		require.Len(t, res.Checks, 1)
		assert.False(t, res.Checks[0].OK)
		assert.Equal(t, msg, res.Checks[0].Message)
		assert.Equal(t, tokenRecord, res.Checks[0].Expected)
		assert.Empty(t, res.Checks[0].Found)
	})

	t.Run("empty_records_report_token_missing", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.TokenTXT = &fakeTokenTXTCollector{}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{TokenTXT: tokenGate})
		require.NoError(t, err)
		require.NotNil(t, res.Failure)
		assert.Equal(t, pluginCore.ValidationReasonTokenMissing, res.Failure.Reason)
	})

	t.Run("lookup_failure_aborts_with_wrapped_error", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.TokenTXT = &fakeTokenTXTCollector{err: errors.New("TXT lookup timeout")}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{TokenTXT: tokenGate})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "TXT lookup timeout")
		assert.Nil(t, res.Failure)
	})
}

// --- On-chain TLSA gate ----------------------------------------------------------

func TestEvaluateWebsiteStage_ChainTLSAGate(t *testing.T) {
	t.Run("pass_with_detail_adds_check", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.ChainTLSA = &fakeChainTLSACollector{observed: ChainTLSAObserved{
			Observation: domainpolicy.TLSAObservation{Found: true, Value: "3 1 1 abc"},
			Detail:      "match detail", Expected: "3 1 1 abc",
		}}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{ChainTLSA: &ChainTLSAGate{}})
		require.NoError(t, err)
		assert.Nil(t, res.Failure)
		require.Len(t, res.Checks, 1)
		assert.True(t, res.Checks[0].OK)
		assert.Equal(t, "match detail", res.Checks[0].Message)
		assert.Equal(t, "3 1 1 abc", res.Checks[0].Expected)
		assert.Equal(t, "3 1 1 abc", res.Checks[0].Found)
	})

	t.Run("pass_without_detail_adds_no_check", func(t *testing.T) {
		// Today's self-guard case: the legacy call returns OK with no detail
		// and no check must be added.
		input := stageInput(nil)
		input.Collectors.ChainTLSA = &fakeChainTLSACollector{observed: ChainTLSAObserved{
			Observation: domainpolicy.TLSAObservation{Found: true},
		}}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{ChainTLSA: &ChainTLSAGate{}})
		require.NoError(t, err)
		assert.Nil(t, res.Failure)
		assert.Empty(t, res.Checks)
	})

	t.Run("missing_record_reports_tlsa_missing", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.ChainTLSA = &fakeChainTLSACollector{observed: ChainTLSAObserved{
			Detail:   "TLSA record _443._tcp.example.com is not published in the name's on-chain zone data",
			Expected: "3 1 1 abc",
		}}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{ChainTLSA: &ChainTLSAGate{}})
		require.NoError(t, err)
		require.NotNil(t, res.Failure)
		assert.Equal(t, pluginCore.ValidationReasonTLSAMissing, res.Failure.Reason)
		require.Len(t, res.Checks, 1)
		assert.False(t, res.Checks[0].OK)
		assert.Equal(t, "3 1 1 abc", res.Checks[0].Expected)
		assert.Empty(t, res.Checks[0].Found)
	})

	t.Run("mismatched_record_reports_tlsa_mismatch", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.ChainTLSA = &fakeChainTLSACollector{observed: ChainTLSAObserved{
			Observation: domainpolicy.TLSAObservation{Found: false, Value: "3 1 1 dead"},
			Detail:      "TLSA mismatch", Expected: "3 1 1 abc",
		}}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{ChainTLSA: &ChainTLSAGate{}})
		require.NoError(t, err)
		require.NotNil(t, res.Failure)
		assert.Equal(t, pluginCore.ValidationReasonTLSAMismatch, res.Failure.Reason)
		assert.Equal(t, "TLSA mismatch", res.Failure.Message)
		require.Len(t, res.Checks, 1)
		assert.False(t, res.Checks[0].OK)
		assert.Equal(t, "3 1 1 dead", res.Checks[0].Found)
		assert.Equal(t, "3 1 1 abc", res.Checks[0].Expected)
	})

	t.Run("unavailable_degrades_to_sanitized_outcome_not_error", func(t *testing.T) {
		input := stageInput(nil)
		input.Collectors.ChainTLSA = &fakeChainTLSACollector{
			err: errors.New("HNS resolver not configured (DnsConfig.HNSResolver)"),
		}
		res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{ChainTLSA: &ChainTLSAGate{}})
		require.NoError(t, err, "resolver failure must not produce a hard error")
		require.NotNil(t, res.Failure)
		assert.Equal(t, pluginCore.ValidationReasonTLSAUnavailable, res.Failure.Reason)
		assert.Equal(t, MsgWebsiteTLSAUnavailable, res.Failure.Message)
		assert.NotContains(t, res.Failure.Message, "DnsConfig.HNSResolver")
		require.Len(t, res.Checks, 1)
		assert.False(t, res.Checks[0].OK)
		assert.Equal(t, MsgWebsiteTLSAUnavailable, res.Checks[0].Message)
		assert.Empty(t, res.Checks[0].Expected)
		assert.Empty(t, res.Checks[0].Found)
	})
}

// --- Short-circuiting -------------------------------------------------------------

func TestEvaluateWebsiteStage_ShortCircuitsOnFirstFailedGate(t *testing.T) {
	// The DNSLink gate fails: the challenge TXT must NOT be looked up —
	// today's early return prevents the unnecessary lookup, and positive
	// proof of ownership later in the flow must not run on an unverified
	// content pointer.
	input := stageInput(nil)
	dns := &fakeDNSLinkCollector{value: "/ipfs/other"}
	txt := &fakeTokenTXTCollector{records: []string{"lumeweb-verify=tok123"}}
	input.Collectors.DNSLink = dns
	input.Collectors.TokenTXT = txt

	res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{
		DNSLink:  &DNSLinkGate{},
		TokenTXT: &TokenTXTGate{TokenKey: "lumeweb-verify", Token: "tok123"},
	})
	require.NoError(t, err)
	require.NotNil(t, res.Failure)
	assert.Equal(t, 1, dns.calls)
	assert.Zero(t, txt.calls, "collecting stops at the first failed gate")
	require.Len(t, res.Checks, 1, "only the failed gate's check is returned")
}

func TestEvaluateWebsiteStage_UnwiredStagesAreIndependent(t *testing.T) {
	// Stage calls select their gates explicitly: a TLSA-only stage does not
	// need (or touch) the DNSLink collector.
	input := stageInput(nil)
	input.Collectors.ChainTLSA = &fakeChainTLSACollector{observed: ChainTLSAObserved{
		Observation: domainpolicy.TLSAObservation{Found: true, Value: "rdata"},
		Detail:      "ok detail", Expected: "rdata",
	}}
	res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{ChainTLSA: &ChainTLSAGate{}})
	require.NoError(t, err)
	assert.Nil(t, res.Failure)
	require.Len(t, res.Checks, 1)
}

// --- Plan/legacy parity ------------------------------------------------------------

// TestEvaluateWebsiteStage_PlanParity pins the plan path
// (domainpolicy.Evaluate) against the legacy-mirroring decisions used when no
// plan is available. Both paths must produce identical gate outcomes for
// every missing / mismatched / unavailable / passing case — the byte-equivalence
// guarantee for fixtures whose plan mapping fails.
func TestEvaluateWebsiteStage_PlanParity(t *testing.T) {
	plan := parityPlan(t, expectedDNSLink)

	cases := []struct {
		name    string
		dnsLink DNSLinkObserved
		txt     []string
		tlsa    ChainTLSAObserved
	}{
		{name: "dnslink_match", dnsLink: DNSLinkObserved{Observation: domainpolicy.DNSLinkObservation{Value: expectedDNSLink}}},
		{name: "dnslink_mismatch", dnsLink: DNSLinkObserved{Observation: domainpolicy.DNSLinkObservation{Value: "/ipfs/other"}}},
		{name: "dnslink_absent", dnsLink: DNSLinkObserved{Observation: domainpolicy.DNSLinkObservation{Value: ""}, NXDOMAIN: true}},
		{name: "token_match", txt: []string{"noise lumeweb-verify=tok1"}},
		{name: "token_missing", txt: []string{"unrelated=1"}},
		{name: "tlsa_match", txt: []string{"lumeweb-verify=tok1"}, tlsa: ChainTLSAObserved{
			Observation: domainpolicy.TLSAObservation{Found: true, Value: "2 1 1 ff"}, Detail: "d", Expected: "2 1 1 ff",
		}},
		{name: "tlsa_missing", txt: []string{"lumeweb-verify=tok1"}, tlsa: ChainTLSAObserved{
			Detail: "missing", Expected: "2 1 1 ff",
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withPlan := WebsiteStages{DNSLink: &DNSLinkGate{}, TokenTXT: &TokenTXTGate{TokenKey: "lumeweb-verify", Token: "tok1"}, ChainTLSA: &ChainTLSAGate{}}
			withoutPlan := withPlan

			inputPlan := stageInput(&plan)
			inputPlan.Collectors.DNSLink = &fakeDNSLinkCollector{value: tc.dnsLink.Observation.Value, nxdomain: tc.dnsLink.NXDOMAIN}
			inputPlan.Collectors.TokenTXT = &fakeTokenTXTCollector{records: tc.txt}
			inputPlan.Collectors.ChainTLSA = &fakeChainTLSACollector{observed: tc.tlsa}

			inputLegacy := stageInput(nil)
			inputLegacy.Collectors.DNSLink = &fakeDNSLinkCollector{value: tc.dnsLink.Observation.Value, nxdomain: tc.dnsLink.NXDOMAIN}
			inputLegacy.Collectors.TokenTXT = &fakeTokenTXTCollector{records: tc.txt}
			inputLegacy.Collectors.ChainTLSA = &fakeChainTLSACollector{observed: tc.tlsa}

			planRes, err := EvaluateWebsiteStage(context.Background(), inputPlan, withPlan)
			require.NoError(t, err)
			legacyRes, err := EvaluateWebsiteStage(context.Background(), inputLegacy, withoutPlan)
			require.NoError(t, err)

			require.Equal(t, len(legacyRes.Checks), len(planRes.Checks))
			for i := range legacyRes.Checks {
				assert.Equal(t, legacyRes.Checks[i], planRes.Checks[i])
			}
			if (legacyRes.Failure == nil) != (planRes.Failure == nil) {
				t.Fatalf("failure presence mismatch: legacy=%v plan=%v",
					legacyRes.Failure, planRes.Failure)
			}
			if legacyRes.Failure != nil {
				assert.Equal(t, *legacyRes.Failure, *planRes.Failure)
			}
			// The pure gate outcomes must match too, not just the conversion.
			require.Equal(t, len(legacyRes.Evaluated), len(planRes.Evaluated))
			for i := range legacyRes.Evaluated {
				assert.Equal(t, legacyRes.Evaluated[i].Kind, planRes.Evaluated[i].Kind)
				assert.Equal(t, legacyRes.Evaluated[i].OK, planRes.Evaluated[i].OK)
				assert.Equal(t, legacyRes.Evaluated[i].Found, planRes.Evaluated[i].Found)
			}
		})
	}
}

// TestEvaluateWithPlan_UsesServiceDerivedExpectationOnDivergence proves the
// divergence guard: when the plan's DNSLink expectation disagrees with the
// service-derived target path, the service-derived value wins (byte-identical
// diagnostics), mirroring the plan/legacy fallback doctrine: on divergence
// the legacy answer wins at runtime.
func TestEvaluateWithPlan_UsesServiceDerivedExpectationOnDivergence(t *testing.T) {
	plan := parityPlan(t, "/ipfs/from-plan")
	input := WebsiteValidationInput{
		Domain:          "example.com",
		Plan:            &plan,
		ExpectedDNSLink: "/ipfs/from-service",
	}
	input.Collectors.DNSLink = &fakeDNSLinkCollector{value: "/ipfs/from-service"}
	input.Collectors.TokenTXT = &fakeTokenTXTCollector{}

	res, err := EvaluateWebsiteStage(context.Background(), input, WebsiteStages{DNSLink: &DNSLinkGate{}})
	require.NoError(t, err)
	assert.Nil(t, res.Failure)
	require.Len(t, res.Checks, 1)
	assert.True(t, res.Checks[0].OK)
	assert.Equal(t, "/ipfs/from-service", res.Checks[0].Expected)
}
