package domainapp

import "context"

// EffectExecutor is the write-side port the DNSLink desired-state reconciler
// drives. It covers ONLY the
// DNSLink record-write family: create/update (the write is an idempotent
// PowerDNS REPLACE) and delete of the `_dnslink.<domain>` TXT record through
// the existing DNS zone service. Challenge rotation, apex records, DNSSEC,
// SOA, TLSA, and zone lifecycle effects remain on the legacy service paths —
// this port must never gain them.
//
// Implementations adapt concrete DNS infrastructure; they must be idempotent
// and must reproduce today's record contents byte-for-byte: the DNSLink TXT
// record carries content `dnslink=<target>` at owner `_dnslink.<domain>` with
// TTL 300, where target is the website's DNSLink path ("/ipfs/<cid>" or
// "/ipns/<peer-id>").
type EffectExecutor interface {
	// WriteDNSLinkRecord creates or replaces the DNSLink TXT record for
	// domain (the record owner's FQDN: the zone apex for an apex binding, or
	// a subdomain living inside a reused parent zone) in zone zoneID with the
	// desired target path (REPLACE semantics, TTL 300).
	WriteDNSLinkRecord(ctx context.Context, zoneID uint, domain string, target string) error
	// DeleteDNSLinkRecord deletes the DNSLink TXT record (the entire
	// `_dnslink.<domain>` TXT RRSet) from zone zoneID. Deletion only ever
	// arises from a plan-explicit delete effect; a nil-effect no-op must not
	// delete.
	DeleteDNSLinkRecord(ctx context.Context, zoneID uint, domain string) error
}
