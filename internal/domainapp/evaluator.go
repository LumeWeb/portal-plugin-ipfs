package domainapp

import (
	"context"
	"fmt"
	"strings"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	"go.uber.org/zap"
)

// Client-facing gate diagnostics, single-sourced here since gate evaluation
// was centralized in this package (the website service keeps aliases for its
// historical unexported names; tests and API payloads are unchanged). All
// strings match the prior imperative validation flow byte-for-byte.
const (
	// MsgWebsiteDNSLinkMatched is the passing DNSLink gate message.
	MsgWebsiteDNSLinkMatched = "DNSLink matches the configured target"
	// MsgWebsiteDNSLinkNotFoundAt reports a narrowly missing DNSLink name.
	MsgWebsiteDNSLinkNotFoundAt = "no DNSLink record found at _dnslink.%s"
	// MsgWebsiteDNSMissing is the flow message for record-class absence.
	MsgWebsiteDNSMissing = "No DNS records found for %s. Please add the required TXT records to your DNS configuration"
	// MsgWebsiteDNSMismatch is the mismatch message and gate detail.
	MsgWebsiteDNSMismatch = "DNS validation failed: missing or incorrect dnslink record (expected: %s, found: %s)"
	// MsgWebsiteTokenMissing is the token failure message and gate detail.
	MsgWebsiteTokenMissing = "DNS validation failed: missing validation token at %s.%s for %s"
	// MsgWebsiteTokenPresent is the passing token gate message.
	MsgWebsiteTokenPresent = "validation token present"
	// MsgWebsiteTLSAUnavailable is the DEGRADED (sanitized) client message
	// shown when the on-chain TLSA cannot be confirmed; the raw resolver
	// error never reaches the HTTP response body.
	MsgWebsiteTLSAUnavailable = "on-chain TLSA cannot be confirmed: the DNS resolver is currently unavailable"
)

// WebsiteValidationInput carries the fixed inputs shared by every stage of
// one ValidateDNS run.
type WebsiteValidationInput struct {
	// Domain is the binding's primary domain being validated.
	Domain string
	// Plan is the binding's current-behavior plan from persisted facts, or
	// nil when no plan is available (the mapping failed, or no domain
	// service is wired): the evaluator then mirrors the legacy decisions —
	// byte-identical and parity-tested against domainpolicy.Evaluate in the
	// package tests. Legacy-fallback semantics follow the established
	// plan/legacy fallback doctrine: plan-bearing decisions follow
	// domainpolicy.Evaluate exactly.
	Plan *domainpolicy.Plan
	// ExpectedDNSLink is the target path the DNSLink gate compares against,
	// derived from the website's persisted target at the service boundary
	// (pluginDb.ToDNSLinkPath). The plan's gate expectation is derived from
	// the same target and equal by characterization; a divergence keeps the
	// service-derived value (so diagnostics stay byte-identical) and logs
	// loudly — the "legacy wins" fallback doctrine.
	ExpectedDNSLink string
	// Collectors are the observation-collection adapters. Requesting a gate
	// whose collector is nil fails closed.
	Collectors Collectors
	// Logger receives the same server-side diagnostics the imperative flow
	// logged before centralization. A nil logger disables them.
	Logger *zap.Logger
}

// WebsiteStages selects which passive gates a single evaluation call covers.
// ValidateDNS runs it in (up to) two invocations that reproduce today's flow
// order: DNSLink then challenge TXT (only when the token gate is selected)
// as the first stage, and the on-chain TLSA gate after the delegation step —
// the step whose effects (status writes via VerifyDomain) remain on the
// legacy service paths.
type WebsiteStages struct {
	// DNSLink selects the DNSLink gate.
	DNSLink *DNSLinkGate
	// TokenTXT selects the challenge-TXT gate.
	TokenTXT *TokenTXTGate
	// ChainTLSA selects the on-chain TLSA gate.
	ChainTLSA *ChainTLSAGate
}

// DNSLinkGate selects the DNSLink gate.
type DNSLinkGate struct{}

// TokenTXTGate selects the challenge-TXT gate and carries the record
// ingredients the check compares against.
type TokenTXTGate struct {
	// TokenKey is the verification label (e.g. "lumeweb-verify").
	TokenKey string
	// Token is the website's validation token.
	Token string
}

// ChainTLSAGate selects the on-chain TLSA gate.
type ChainTLSAGate struct{}

// GateFailure is the flow-level outcome the service serializes unchanged
// when a gate fails: today's top-level Message and Reason.
type GateFailure struct {
	Reason  pluginCore.ValidationReason
	Message string
}

// WebsiteStageResult is a stage's outcome: the converted per-gate checks in
// evaluation order and, when a gate failed the flow, the failure the service
// attaches to the response.
type WebsiteStageResult struct {
	// Checks are the per-gate results converted to core.ValidationCheck —
	// name, ok, message, expected, and found exactly as today's imperative
	// addCheck calls produced them.
	Checks []pluginCore.ValidationCheck
	// Failure is non-nil when a gate failed the flow; the caller stops
	// immediately (the short-circuit is preserved inside the stage too).
	Failure *GateFailure
	// Evaluated are the underlying pure gate outcomes, in evaluation order,
	// for tracing and parity assertions.
	Evaluated []domainpolicy.GateResult
}

// EvaluateWebsiteStage evaluates one website-validation stage. Gates run in
// the flow's order (DNSLink, challenge TXT, on-chain TLSA) and the stage
// stops at the first failed gate — preserving today's short-circuiting that
// prevents unnecessary (or unsafe) network calls and downstream gates.
// Collector errors reproduce today's abort semantics: DNSLink/TXT transport
// failures abort the flow via the returned error; a TLSA unavailability
// degrades to the sanitized unavailable outcome instead.
func EvaluateWebsiteStage(ctx context.Context, input WebsiteValidationInput, stages WebsiteStages) (WebsiteStageResult, error) {
	result := WebsiteStageResult{}

	if stages.DNSLink != nil {
		if input.Collectors.DNSLink == nil {
			return WebsiteStageResult{}, fmt.Errorf("domainapp: no DNSLink collector wired for %s", input.Domain)
		}
		collected, err := input.Collectors.DNSLink.CollectDNSLink(ctx, input.Domain)
		if err != nil {
			return WebsiteStageResult{}, err
		}
		obs := domainpolicy.ObservationSet{DNSLink: &collected.Observation}
		outcome, err := gateOutcome(input, domainpolicy.GateDNSLink, obs)
		if err != nil {
			return WebsiteStageResult{}, err
		}
		result.Evaluated = append(result.Evaluated, outcome)
		check, failure := convertDNSLinkGate(input, collected.NXDOMAIN, outcome)
		result.Checks = append(result.Checks, check)
		if failure != nil {
			result.Failure = failure
			return result, nil
		}
	}

	if stages.TokenTXT != nil {
		if input.Collectors.TokenTXT == nil {
			return WebsiteStageResult{}, fmt.Errorf("domainapp: no challenge-TXT collector wired for %s", input.Domain)
		}
		fqdn := stages.TokenTXT.TokenKey + "." + input.Domain
		records, err := input.Collectors.TokenTXT.CollectTokenTXT(ctx, fqdn)
		if err != nil {
			return WebsiteStageResult{}, err
		}
		expectedRecord := fmt.Sprintf("%s=%s", stages.TokenTXT.TokenKey, stages.TokenTXT.Token)
		matched := matchTokenRecord(records, expectedRecord)
		obs := domainpolicy.ObservationSet{
			ChallengeTXT: &domainpolicy.TXTObservation{Value: matched},
		}
		outcome, err := gateOutcome(input, domainpolicy.GateChallengeTXT, obs)
		if err != nil {
			return WebsiteStageResult{}, err
		}
		result.Evaluated = append(result.Evaluated, outcome)
		check, failure := convertTokenGate(input, stages.TokenTXT, expectedRecord, outcome)
		result.Checks = append(result.Checks, check)
		if failure != nil {
			result.Failure = failure
			return result, nil
		}
	}

	if stages.ChainTLSA != nil {
		if input.Collectors.ChainTLSA == nil {
			return WebsiteStageResult{}, fmt.Errorf("domainapp: no on-chain TLSA collector wired for %s", input.Domain)
		}
		collected, err := input.Collectors.ChainTLSA.CollectChainTLSA(ctx, input.Domain)
		if err != nil {
			// Resolver not configured / unreachable: the on-chain TLSA cannot
			// be confirmed. Degrade the TLSA gate to a distinct non-OK outcome
			// (not a 500) so validation fails closed with a clear message
			// rather than taking down the whole check. The client-facing
			// message must be sanitized — the underlying error embeds
			// internal resolver config — so echo the friendly message and
			// log the raw error server-side.
			if input.Logger != nil {
				input.Logger.Warn("on-chain TLSA resolver unavailable during validation",
					zap.Error(err), zap.String("domain", input.Domain))
			}
			result.Checks = append(result.Checks, pluginCore.ValidationCheck{
				Name: pluginCore.ValidationCheckTLSA, OK: false,
				Message: MsgWebsiteTLSAUnavailable, Expected: "", Found: "",
			})
			result.Failure = &GateFailure{
				Reason:  pluginCore.ValidationReasonTLSAUnavailable,
				Message: MsgWebsiteTLSAUnavailable,
			}
			return result, nil
		}
		obs := domainpolicy.ObservationSet{TLSA: &collected.Observation}
		outcome, err := gateOutcome(input, domainpolicy.GateTLSA, obs)
		if err != nil {
			return WebsiteStageResult{}, err
		}
		result.Evaluated = append(result.Evaluated, outcome)
		check, failure := convertChainTLSAGate(input, collected, outcome)
		if check != nil {
			result.Checks = append(result.Checks, *check)
		}
		if failure != nil {
			result.Failure = failure
			return result, nil
		}
	}

	return result, nil
}

// gateOutcome resolves one gate's outcome. With a plan the decision is
// delegated to domainpolicy.Evaluate — the owner of gate outcomes since
// evaluation was centralized — extracting the requested gate's result from
// the per-flow evaluation
// (other plan gates are intentionally unobserved and ignored here). Without
// a plan the legacy-mirroring decideWebsiteGate runs. Both paths are proven
// equivalent by the parity tests.
func gateOutcome(input WebsiteValidationInput, kind domainpolicy.GateKind, obs domainpolicy.ObservationSet) (domainpolicy.GateResult, error) {
	if input.Plan != nil {
		result, found, err := evaluateWithPlan(*input.Plan, kind, obs, input.ExpectedDNSLink, input.Domain, input.Logger)
		if err != nil {
			return domainpolicy.GateResult{}, err
		}
		if found {
			return result, nil
		}
		// The plan should carry every website gate it selected; an absent
		// gate result falls through to the mirrored decision rather than
		// failing the evaluation — the legacy behavior for this gate.
	}
	return decideWebsiteGate(kind, input.ExpectedDNSLink, obs), nil
}

// evaluateWithPlan evaluates obs through domainpolicy.Evaluate for the plan's
// website flow and returns the requested gate's result.
func evaluateWithPlan(plan domainpolicy.Plan, kind domainpolicy.GateKind, obs domainpolicy.ObservationSet, expectedDNSLink, domain string, logger *zap.Logger) (domainpolicy.GateResult, bool, error) {
	if kind == domainpolicy.GateDNSLink {
		if g, i, ok := websiteGate(plan, kind); ok && g.Expected != expectedDNSLink {
			if logger != nil {
				logger.Warn("plan/legacy divergence (DNSLink expectation): service-derived target wins at runtime",
					zap.String("domain", domain),
					zap.String("profile", plan.ProfileID.String()),
					zap.String("expected", g.Expected),
					zap.String("legacy", expectedDNSLink))
			}
			// Keep the service-derived expectation so the diagnostics (and
			// the compared values) stay byte-identical to the legacy flow.
			g.Expected = expectedDNSLink
			plan.Gates = append([]domainpolicy.Gate(nil), plan.Gates...)
			plan.Gates[i] = g
		}
	}
	evaluation, err := domainpolicy.Evaluate(plan, domainpolicy.FlowWebsiteValidation, obs)
	if err != nil {
		return domainpolicy.GateResult{}, false, err
	}
	for _, r := range evaluation.Results {
		if r.Kind == kind {
			return r, true, nil
		}
	}
	return domainpolicy.GateResult{}, false, nil
}

// websiteGate locates a gate of the given kind in the plan's website flow.
func websiteGate(plan domainpolicy.Plan, kind domainpolicy.GateKind) (domainpolicy.Gate, int, bool) {
	for i, g := range plan.Gates {
		if g.Flow == domainpolicy.FlowWebsiteValidation && g.Kind == kind {
			return g, i, true
		}
	}
	return domainpolicy.Gate{}, 0, false
}

// decideWebsiteGate mirrors domainpolicy.evaluateGate for the website flow's
// three passive gates. It runs when no plan is available (the legacy
// fallback). The parity tests pin it to Evaluate's decisions so the two can
// never drift apart silently.
func decideWebsiteGate(kind domainpolicy.GateKind, expectedDNSLink string, obs domainpolicy.ObservationSet) domainpolicy.GateResult {
	result := domainpolicy.GateResult{
		Kind:     kind,
		Flow:     domainpolicy.FlowWebsiteValidation,
		Expected: expectedDNSLink,
		OK:       false,
		Found:    "unobserved",
	}
	switch kind {
	case domainpolicy.GateDNSLink:
		if obs.DNSLink == nil {
			return result
		}
		result.Found = obs.DNSLink.Value
		if !obs.DNSLink.Found() {
			return result
		}
		result.OK = obs.DNSLink.Value == expectedDNSLink
	case domainpolicy.GateChallengeTXT:
		if obs.ChallengeTXT == nil {
			return result
		}
		result.Found = obs.ChallengeTXT.Value
		// Token freshness is the rotate repair's business, not the gate's:
		// the gate requires the record's presence, exactly like today.
		result.OK = obs.ChallengeTXT.Value != ""
	case domainpolicy.GateTLSA:
		if obs.TLSA == nil {
			return result
		}
		result.Found = obs.TLSA.Value
		result.OK = obs.TLSA.Found
	default:
		result.Found = "unknown gate kind"
	}
	return result
}

// matchTokenRecord reproduces today's token check: the gate passes when any
// TXT record contains the expected "<key>=<token>" content; the matched
// record becomes the observation value.
func matchTokenRecord(records []string, expectedRecord string) string {
	for _, record := range records {
		if strings.Contains(record, expectedRecord) {
			return record
		}
	}
	return ""
}

// convertDNSLinkGate converts the DNSLink gate outcome into the check the
// service appends and — on failure — the flow-level failure with today's
// reason codes: NXDOMAIN is "missing", a present-but-different record is a
// "mismatch".
func convertDNSLinkGate(input WebsiteValidationInput, nxdomain bool, outcome domainpolicy.GateResult) (pluginCore.ValidationCheck, *GateFailure) {
	if outcome.OK {
		if input.Logger != nil {
			input.Logger.Debug("Found valid DNSlink record",
				zap.String("domain", input.Domain),
				zap.String("dnslink", outcome.Found))
		}
		return pluginCore.ValidationCheck{
			Name: pluginCore.ValidationCheckDNSLink, OK: true,
			Message:  MsgWebsiteDNSLinkMatched,
			Expected: input.ExpectedDNSLink, Found: input.ExpectedDNSLink,
		}, nil
	}
	if nxdomain {
		return pluginCore.ValidationCheck{
				Name: pluginCore.ValidationCheckDNSLink, OK: false,
				Message:  fmt.Sprintf(MsgWebsiteDNSLinkNotFoundAt, input.Domain),
				Expected: "", Found: "",
			}, &GateFailure{
				Reason:  pluginCore.ValidationReasonDNSMissing,
				Message: fmt.Sprintf(MsgWebsiteDNSMissing, input.Domain),
			}
	}
	detail := fmt.Sprintf(MsgWebsiteDNSMismatch, input.ExpectedDNSLink, outcome.Found)
	if input.Logger != nil {
		input.Logger.Warn("DNS validation failed: missing or incorrect dnslink record",
			zap.String("domain", input.Domain),
			zap.String("expected", input.ExpectedDNSLink),
			zap.String("found", outcome.Found))
	}
	return pluginCore.ValidationCheck{
			Name: pluginCore.ValidationCheckDNSLink, OK: false,
			Message:  detail,
			Expected: input.ExpectedDNSLink, Found: outcome.Found,
		}, &GateFailure{
			Reason:  pluginCore.ValidationReasonDNSMismatch,
			Message: detail,
		}
}

// convertTokenGate converts the challenge-TXT gate outcome. Today's
// diagnostics never expose the found record value (expected/found carry the
// expected record content and nothing), so the conversion reproduces that.
func convertTokenGate(input WebsiteValidationInput, gate *TokenTXTGate, expectedRecord string, outcome domainpolicy.GateResult) (pluginCore.ValidationCheck, *GateFailure) {
	if outcome.OK {
		if input.Logger != nil {
			input.Logger.Debug("Found valid validation token",
				zap.String("domain", input.Domain),
				zap.String("token", gate.Token))
		}
		return pluginCore.ValidationCheck{
			Name: pluginCore.ValidationCheckToken, OK: true,
			Message:  MsgWebsiteTokenPresent,
			Expected: "", Found: "",
		}, nil
	}
	msg := fmt.Sprintf(MsgWebsiteTokenMissing, gate.TokenKey, input.Domain, input.Domain)
	if input.Logger != nil {
		input.Logger.Warn("DNS validation failed: missing validation token",
			zap.String("domain", input.Domain),
			zap.String("expected_token", gate.Token))
	}
	return pluginCore.ValidationCheck{
			Name: pluginCore.ValidationCheckToken, OK: false,
			Message:  msg,
			Expected: expectedRecord, Found: "",
		}, &GateFailure{
			Reason:  pluginCore.ValidationReasonTokenMissing,
			Message: msg,
		}
}

// convertChainTLSAGate converts the on-chain TLSA gate outcome. A passing
// gate adds a check only when the legacy detail is non-empty (today's
// "gated call passes through" case); failing gates keep the collector's
// legacy detail, expected, and found values verbatim, with missing vs
// mismatch distinguished by whether the record was actually served.
func convertChainTLSAGate(input WebsiteValidationInput, collected ChainTLSAObserved, outcome domainpolicy.GateResult) (*pluginCore.ValidationCheck, *GateFailure) {
	if outcome.OK {
		if collected.Detail == "" {
			// The gated legacy call would have passed through without
			// adding a check; do the same.
			return nil, nil
		}
		return &pluginCore.ValidationCheck{
			Name: pluginCore.ValidationCheckTLSA, OK: true,
			Message:  collected.Detail,
			Expected: collected.Expected, Found: collected.Observation.Value,
		}, nil
	}
	reason := pluginCore.ValidationReasonTLSAMissing
	if collected.Observation.Value != "" {
		reason = pluginCore.ValidationReasonTLSAMismatch
	}
	return &pluginCore.ValidationCheck{
			Name: pluginCore.ValidationCheckTLSA, OK: false,
			Message:  collected.Detail,
			Expected: collected.Expected, Found: collected.Observation.Value,
		}, &GateFailure{
			Reason:  reason,
			Message: collected.Detail,
		}
}
