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
}
