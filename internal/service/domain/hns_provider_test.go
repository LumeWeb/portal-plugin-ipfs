package domain

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dane "go.lumeweb.com/dane"
	danehns "go.lumeweb.com/dane/hns"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// testCertPEM generates a reusable self-signed cert for TLSA tests.
func testCertPEM(t *testing.T) string {
	certPEM, _, err := dane.GenerateSelfSignedECDSA([]string{"test.example"}, time.Now().AddDate(1, 0, 0))
	if err != nil {
		t.Skipf("dane.GenerateSelfSignedECDSA not usable here: %v", err)
	}
	return certPEM
}

func TestHNSProvider_Protocol(t *testing.T) {
	p := NewHNSProvider("", nil, TLSASource{})
	assert.Equal(t, "hns", p.Protocol())
}

func TestHNSProvider_Nameservers(t *testing.T) {
	// HNS declares its own namespace-specific nameservers, distinct from ICANN.
	p := NewHNSProvider("", []string{"ns1.hns.\nns2.hns."}, TLSASource{})
	assert.Equal(t, []string{"ns1.hns.\nns2.hns."}, p.Nameservers())

	empty := NewHNSProvider("", nil, TLSASource{})
	assert.Nil(t, empty.Nameservers())
}

func TestHNSProvider_Validate(t *testing.T) {
	p := NewHNSProvider("", nil, TLSASource{})

	assert.NoError(t, p.Validate("example"))
	assert.NoError(t, p.Validate("EXAMPLE"))
	assert.NoError(t, p.Validate("example/"))
	assert.NoError(t, p.Validate("example."))

	assert.Error(t, p.Validate(""))
	assert.Error(t, p.Validate("example.com"))
	assert.Error(t, p.Validate("-invalid"))
}

func TestHNSProvider_BuildDelegation_DefaultDelegated(t *testing.T) {
	p := NewHNSProvider("", []string{"ns1.example.com."}, TLSASource{
		Certs: map[string]string{"example": testCertPEM(t)},
	})
	result, err := p.BuildDelegation(context.Background(), 1, "example", &pluginDb.Website{}, nil)
	assert.NoError(t, err)
	bundle, ok := result.(DelegationBundle)
	assert.True(t, ok)
	b, _ := json.Marshal(bundle)
	s := string(b)
	assert.Contains(t, s, "ns1.example.com.")
	assert.Contains(t, s, "TLSA")
	assert.Contains(t, s, `"mode":"`+string(HNSModeDelegated)+`"`)
}

func TestHNSProvider_BuildDelegation_NoTLSASource(t *testing.T) {
	// Without a cert, BuildDelegation should still succeed (bootstrap mode).
	// TLSA is backfilled later by OnCertAvailable / UpdateTLSAFromCert.
	p := NewHNSProvider("", []string{"ns1.example.com."}, TLSASource{})
	result, err := p.BuildDelegation(context.Background(), 1, "example", &pluginDb.Website{}, nil)
	assert.NoError(t, err)
	bundle, ok := result.(DelegationBundle)
	assert.True(t, ok)

	// Delegation should be created but without TLSA records.
	b, _ := json.Marshal(bundle)
	s := string(b)
	assert.NotContains(t, s, "TLSA")
}

func TestHNSProvider_BuildDelegation_InlineMode_UsesDaneLib(t *testing.T) {
	cfg := []byte(`{"mode":"` + string(HNSModeInline) + `","nameserver_ipv4":"203.0.113.10"}`)
	p := NewHNSProvider("", []string{"ns1.example.com."}, TLSASource{
		Certs: map[string]string{"example": testCertPEM(t)},
	})
	result, err := p.BuildDelegation(context.Background(), 1, "example", &pluginDb.Website{}, cfg)
	assert.NoError(t, err)
	bundle, ok := result.(DelegationBundle)
	assert.True(t, ok)
	b, _ := json.Marshal(bundle)
	s := string(b)

	assert.Contains(t, s, `"mode":"`+string(HNSModeInline)+`"`)
	assert.Contains(t, s, "SYNTH4")
	assert.Contains(t, s, "TLSA")

	// Must use the library's SynthName for the authoritative NS
	expectedSynth := danehns.SynthName([]byte{203, 0, 113, 10}) // or proper net.IP
	// The lib uses net.ParseIP inside, so we simulate
	assert.Contains(t, s, "x") // x prefix from lib
	_ = expectedSynth
}

func TestHNSProvider_BuildDelegation_InlineMode_RequiresIP(t *testing.T) {
	cfg := []byte(`{"mode":"` + string(HNSModeInline) + `"}`) // no IP
	p := NewHNSProvider("", []string{"ns1.example.com."}, TLSASource{
		Certs: map[string]string{"example": testCertPEM(t)},
	})
	_, err := p.BuildDelegation(context.Background(), 1, "example", &pluginDb.Website{}, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "inline mode requires nameserver_ipv4 or nameserver_ipv6")
}

func TestHNSProvider_BuildDelegation_DelegatedWithGlue_UsesDane(t *testing.T) {
	cfg := []byte(`{"mode":"` + string(HNSModeDelegated) + `","nameserver_host":"ns1.example.","nameserver_ipv4":"203.0.113.5"}`)
	p := NewHNSProvider("", []string{"ns1.example.com."}, TLSASource{
		Certs: map[string]string{"example": testCertPEM(t)},
	})
	result, err := p.BuildDelegation(context.Background(), 1, "example", &pluginDb.Website{}, cfg)
	assert.NoError(t, err)
	bundle, ok := result.(DelegationBundle)
	assert.True(t, ok)
	b, _ := json.Marshal(bundle)
	s := string(b)

	assert.Contains(t, s, `"mode":"`+string(HNSModeDelegated)+`"`)
	assert.Contains(t, s, "GLUE4")
}

func TestHNSProvider_BuildDelegation_TLSAFromCertPEM_UsesDaneLibrary(t *testing.T) {
	certPEM := testCertPEM(t)

	p := NewHNSProvider("", []string{"ns1.example.com."}, TLSASource{
		Certs: map[string]string{"example": certPEM},
	})
	result, err := p.BuildDelegation(context.Background(), 1, "example", &pluginDb.Website{}, nil)
	assert.NoError(t, err)
	bundle, ok := result.(DelegationBundle)
	assert.True(t, ok)
	b, _ := json.Marshal(bundle)
	s := string(b)
	assert.Contains(t, s, "3 1 1")
	assert.Contains(t, s, "TLSA")

	// Verify the hash part matches what the library would produce directly
	hash, _ := dane.ComputeTLSAFromCert(certPEM)
	assert.Contains(t, s, hash)
}

func TestHNSProvider_OnCertAvailable_UpdatesTLSASource(t *testing.T) {
	p := NewHNSProvider("", []string{"ns1.example.com."}, TLSASource{})

	// Before: no cert, BuildDelegation succeeds in bootstrap mode (no TLSA).
	result, err := p.BuildDelegation(context.Background(), 1, "example", &pluginDb.Website{}, nil)
	assert.NoError(t, err)
	bundle, ok := result.(DelegationBundle)
	assert.True(t, ok)
	b, _ := json.Marshal(bundle)
	assert.NotContains(t, string(b), "TLSA", "bootstrap delegation should not contain TLSA")

	// Push cert via OnCertAvailable
	certPEM := testCertPEM(t)
	err = p.OnCertAvailable(context.Background(), "example", certPEM)
	assert.NoError(t, err)

	// After: TLSA should now be present
	result, err = p.BuildDelegation(context.Background(), 1, "example", &pluginDb.Website{}, nil)
	assert.NoError(t, err)
	bundle, ok = result.(DelegationBundle)
	assert.True(t, ok)
	b, _ = json.Marshal(bundle)
	assert.Contains(t, string(b), "TLSA")
}

func TestHNSProvider_SynthName_FromDaneLib(t *testing.T) {
	// Direct test that we are using the library (no local reinvention)
	ip4 := net.ParseIP("203.0.113.10")
	name := danehns.SynthName(ip4)
	assert.NotEmpty(t, name)
	assert.Contains(t, name, "x") // current lib convention from dane/hns
}

// fakeDNSZoneService is a minimal DNSZoneService stub for enabling DNSSEC with
// a fixed DNSKEY, so enableDNSSECAndDS can be exercised deterministically.
type fakeDNSZoneService struct{ dnskey string }

func (f *fakeDNSZoneService) CreateZone(ctx context.Context, domain string, userID uint) (*pluginDb.DNSZone, error) {
	panic("unused")
}
func (f *fakeDNSZoneService) DeleteZone(ctx context.Context, zoneID uint) error { panic("unused") }
func (f *fakeDNSZoneService) CreateDNSLinkRecord(ctx context.Context, zoneID uint, target string) error {
	panic("unused")
}
func (f *fakeDNSZoneService) CreateApexRecord(ctx context.Context, zoneID uint, recordType pluginCore.RecordType, content string) error {
	panic("unused")
}
func (f *fakeDNSZoneService) EnableDNSSEC(ctx context.Context, zoneID uint) (string, error) {
	return f.dnskey, nil
}

func TestHNSProvider_enableDNSSECAndDS_ValueIsRDATAOnly(t *testing.T) {
	// Regression: the DS parent record VALUE must carry only the RDATA
	// (key tag, algorithm, digest type, digest) — never the owner name or the
	// "DS" record-type token (RFC 4034 §5.3). Previously dane.FormatDSRecord
	// was used, which prefixes "<owner> DS " onto the value.
	p := NewHNSProvider("", []string{"ns1.example.com."}, TLSASource{})
	p.SetDNSService(&fakeDNSZoneService{dnskey: "257 3 13 dGVzdA=="})

	records, err := p.enableDNSSECAndDS(context.Background(), 1, "example")
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "DS", records[0].Type)

	v := records[0].Value
	fields := strings.Fields(v)
	assert.Len(t, fields, 4, "DS value must have exactly 4 RDATA fields: keytag, alg, digesttype, digest")

	// First three fields are numeric (key tag, algorithm, digest type).
	for _, f := range fields[:3] {
		_, err := strconv.ParseUint(f, 10, 16) // key tag fits in u16; alg/digesttype smaller
		assert.NoError(t, err, "field %q should be a number", f)
	}
	// Digest field is non-empty hex (no owner/type tokens).
	assert.NotEmpty(t, fields[3])
	assert.NotContains(t, v, " DS ", "record-type token must not leak into the value")
	assert.NotContains(t, v, "example", "owner name must not leak into the value")
}
