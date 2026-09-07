package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDnsConfig_Defaults(t *testing.T) {
	d := DnsConfig{}
	defs := d.Defaults()

	// HNS nameservers default to empty and are distinct from ICANN nameservers.
	_, hasHNSNS := defs["HNSNameservers"]
	assert.True(t, hasHNSNS, "DnsConfig defaults must include HNSNameservers")
	assert.Equal(t, []string{}, defs["HNSNameservers"])
	assert.Equal(t, []string{}, defs["Nameservers"])

	// The plan-driven DNSLink reconciler defaults to OFF: legacy
	// writers remain the runtime behavior until the flag is enabled.
	assert.Equal(t, false, defs["DomainPolicyDNSLinkReconcilerEnabled"],
		"DnsConfig defaults must include DomainPolicyDNSLinkReconcilerEnabled=false")
	assert.False(t, d.DomainPolicyDNSLinkReconcilerEnabled)

	// The plan-driven repair reconciler (challenge rotation, DNSSEC
	// ensure, SOA MNAME) defaults to OFF: the legacy repair paths remain the
	// runtime behavior until the flag is enabled.
	assert.Equal(t, false, defs["DomainPolicyRepairReconcilerEnabled"],
		"DnsConfig defaults must include DomainPolicyRepairReconcilerEnabled=false")
	assert.False(t, d.DomainPolicyRepairReconcilerEnabled)

	// The bounded application backfill of the persisted policy axes is
	// REGISTERED but NOT auto-enabled: it no-ops while this flag is false.
	assert.Equal(t, false, defs["DomainPolicyAxesBackfillEnabled"],
		"DnsConfig defaults must include DomainPolicyAxesBackfillEnabled=false")
	assert.False(t, d.DomainPolicyAxesBackfillEnabled)
}
