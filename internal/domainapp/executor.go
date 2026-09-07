package domainapp

import (
	"context"
	"fmt"

	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	"go.uber.org/zap"
)

// ApplyDNSLinkEffects filters plan-derived effect descriptors to the DNSLink
// record-write family and applies them through the EffectExecutor. It is the
// executor half of the DNSLink desired-state reconciler.
//
// Family scope:
//
//   - EffectKindWriteRecord whose record intent is a DNSLink record is
//     applied as an idempotent create/update (WriteDNSLinkRecord).
//   - EffectKindDeleteRecord whose record intent is a DNSLink record is
//     applied as a record deletion (DeleteDNSLinkRecord).
//   - EffectKindReportRouteDrift is a report: it never writes.
//   - every other effect kind — zone create/reuse, apex/challenge record
//     writes and deletions, DNSSEC, SOA, challenge rotation — is out of the
//     family: it is returned as deferred (reported, not executed) and never
//     executed here. The reconciler never invents side effects.
//   - an unknown effect kind fails closed with an error: the caller falls
//     back to the legacy path rather than guessing.
//
// The effect list is applied in Diff's order; Diff is deterministic, so the
// applied subset is stable for identical inputs.
func ApplyDNSLinkEffects(ctx context.Context, domain string, zoneID uint, effects []domainpolicy.Effect, executor EffectExecutor, logger *zap.Logger) (applied []domainpolicy.Effect, deferred []string, err error) {
	applied = []domainpolicy.Effect{}
	deferred = []string{}

	if executor == nil {
		// Fail closed instead of silently dropping effects: the first
		// eligible DNSLink effect aborts the application; out-of-family
		// effects seen so far are still reported as deferred.
		for _, effect := range effects {
			if isDNSLinkWriteEffect(effect) {
				return applied, deferred, fmt.Errorf("domainapp: DNSLink effect %q for %q cannot be applied: no effect executor wired", effect.IdempotencyKey, domain)
			}
			deferred = append(deferred, effect.Kind.String()+" "+effect.KeyMaterial)
		}
		return applied, deferred, nil
	}

	for _, effect := range effects {
		switch effect.Kind {
		case domainpolicy.EffectKindWriteRecord, domainpolicy.EffectKindDeleteRecord:
			if !isDNSLinkWriteEffect(effect) {
				// Apex, challenge, TLSA, NS/DS, SOA writes/deletions stay on
				// the legacy writers.
				deferred = append(deferred, effect.Kind.String()+" "+effect.KeyMaterial)
				continue
			}
			if effect.Kind == domainpolicy.EffectKindWriteRecord {
				if err := executor.WriteDNSLinkRecord(ctx, zoneID, domain, effect.Record.Intent.Value); err != nil {
					return applied, deferred, fmt.Errorf("failed to write dnslink record for %s (effect %s): %w", domain, effect.IdempotencyKey, err)
				}
			} else {
				if err := executor.DeleteDNSLinkRecord(ctx, zoneID, domain); err != nil {
					return applied, deferred, fmt.Errorf("failed to delete dnslink record for %s (effect %s): %w", domain, effect.IdempotencyKey, err)
				}
			}
			applied = append(applied, effect)
			if logger != nil {
				logger.Info("DNSLink reconcile effect applied",
					zap.String("domain", domain),
					zap.Uint("zone_id", zoneID),
					zap.String("effect", effect.Kind.String()),
					zap.String("idempotency_key", effect.IdempotencyKey),
					zap.String("reason", effect.Reason))
			}
		case domainpolicy.EffectKindReportRouteDrift:
			// A finding, not a write; the caller surfaces it on the result.
			deferred = append(deferred, effect.Kind.String()+": "+effect.Reason)
		case domainpolicy.EffectKindCreateZone, domainpolicy.EffectKindReuseZone,
			domainpolicy.EffectKindEnsureDNSSEC, domainpolicy.EffectKindEnsureSOAMNAME,
			domainpolicy.EffectKindRotateChallenge:
			// Out of the DNSLink effect family; legacy paths own these
			// effects. Report, never execute.
			deferred = append(deferred, effect.Kind.String()+" "+effect.KeyMaterial)
		default:
			return applied, deferred, fmt.Errorf("%w: unknown effect kind %d (%s) for %s", ErrDNSLinkNotReconciled, int(effect.Kind), effect.Kind.String(), domain)
		}
	}
	return applied, deferred, nil
}

// isDNSLinkWriteEffect reports whether the effect addresses the DNSLink
// record (kind and record intent populated correctly).
func isDNSLinkWriteEffect(effect domainpolicy.Effect) bool {
	return effect.Record != nil &&
		effect.Record.Intent.Kind == domainpolicy.RecordKindDNSLink &&
		effect.Record.Intent.Value != ""
}
