package domainpolicy

// GateResult is the evaluated outcome of one gate: its identity, whether it
// passed, and the expected versus found values for diagnostics.
type GateResult struct {
	// Kind is the gate's kind.
	Kind GateKind
	// Flow is the flow the gate was evaluated in.
	Flow Flow
	// OK reports whether the gate passed.
	OK bool
	// Expected is the stable expected value.
	Expected string
	// Found is the observed value, or a stable placeholder when the gate was
	// trivially passed ("trivial pass (not checked today)") or not observed
	// ("unobserved"). Failures never invent an observed value.
	Found string
}

// Evaluation is the ordered per-flow output of Evaluate: the gates of one
// flow, in that flow's order, with nothing from the other flow.
type Evaluation struct {
	// Flow is the flow that was evaluated.
	Flow Flow
	// ProfileID names the plan's profile.
	ProfileID ProfileID
	// Results are the per-gate outcomes in the plan's per-flow order.
	Results []GateResult
	// Passed reports whether every evaluated gate passed.
	Passed bool
}

// Evaluate evaluates the given plan's gates for the given flow against the
// observation set. It is pure, deterministic, and fail closed:
//
//   - it never returns a gate from the other flow (the flow is explicit);
//   - a missing or absent observation fails its gate (Found "unobserved");
//   - an observation set carrying unknown enum values is rejected, never
//     evaluated;
//   - trivially-passing gates are reported as passing with the finding
//     "not checked (trivial pass today)", encoding the current no-op
//     behavior instead of inventing a live result.
//
// Evaluate produces no effects and mutates nothing.
func Evaluate(plan Plan, flow Flow, obs ObservationSet) (Evaluation, error) {
	if !flow.Valid() {
		return Evaluation{}, newInvalid("evaluation", "unknown flow %d", int(flow))
	}
	if err := validatePlanUsable(plan); err != nil {
		return Evaluation{}, err
	}
	if err := obs.Validate(); err != nil {
		return Evaluation{}, err
	}

	evaluation := Evaluation{
		Flow:      flow,
		ProfileID: plan.ProfileID,
		Passed:    true,
	}
	for _, gate := range plan.Gates {
		if gate.Flow != flow {
			continue
		}
		result := evaluateGate(gate, plan.Name, obs)
		evaluation.Results = append(evaluation.Results, result)
		if !result.OK {
			evaluation.Passed = false
		}
	}
	return evaluation, nil
}

// evaluateGate determines one gate's outcome from the observation set.
func evaluateGate(gate Gate, name string, obs ObservationSet) GateResult {
	result := GateResult{
		Kind:     gate.Kind,
		Flow:     gate.Flow,
		Expected: gate.Expected,
		OK:       false,
		Found:    "unobserved",
	}
	if gate.TriviallyPasses {
		result.OK = true
		result.Found = "not checked (trivial pass today)"
		return result
	}
	switch gate.Kind {
	case GateDNSLink:
		if obs.DNSLink == nil {
			return result
		}
		result.Found = obs.DNSLink.Value
		if !obs.DNSLink.Found() {
			return result
		}
		result.OK = obs.DNSLink.Value == gate.Expected
	case GateChallengeTXT:
		if obs.ChallengeTXT == nil {
			return result
		}
		result.Found = obs.ChallengeTXT.Value
		// The current token check requires the record to be present; the
		// token's freshness is handled by the rotate repair, not the gate.
		result.OK = obs.ChallengeTXT.Value != ""
	case GateNSDelegation:
		if obs.NS == nil {
			return result
		}
		if obs.NS.Found {
			result.OK = true
			result.Found = joinNameservers(obs.NS.Nameservers)
		}
	case GateDSDelegation:
		if obs.DS == nil {
			return result
		}
		result.Found = obs.DS.Value
		result.OK = obs.DS.Found
	case GateTLSA:
		if obs.TLSA == nil {
			return result
		}
		result.Found = obs.TLSA.Value
		result.OK = obs.TLSA.Found
	case GatePlatformTrust:
		if obs.PlatformTrust == nil {
			return result
		}
		result.OK = obs.PlatformTrust.Trusted
		if obs.PlatformTrust.Trusted {
			result.Found = "trusted"
		} else {
			result.Found = "untrusted: " + obs.PlatformTrust.Reason
		}
	default:
		// Unknown gate kinds fail closed with the unobserved finding.
		result.Found = "unknown gate kind"
	}
	return result
}

// joinNameservers renders the observed NS targets as a stable diagnostic.
func joinNameservers(nameservers []string) string {
	if len(nameservers) == 0 {
		return "present"
	}
	out := ""
	for i, ns := range nameservers {
		if i > 0 {
			out += " "
		}
		out += ns
	}
	return out
}
