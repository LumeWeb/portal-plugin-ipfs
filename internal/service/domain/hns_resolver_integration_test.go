package domain

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This integration test verifies that HNSProvider.VerifyDelegation genuinely
// uses the configured resolver address, including a CUSTOM PORT (host:port).
//
// Regression context: users configuring an HNS resolver on a non-default port
// (e.g. `hns_resolver: handshake:5349` -> the HSD DNS listener) saw errors like
//
//	HNS resolver query failed: lookup lumeweb. on 127.0.0.11:53: no such host
//
// Despite the error text listing the system resolver (127.0.0.11:53 = Docker's
// embedded DNS from /etc/resolv.conf), the actual dial is still sent to the
// configured `host:port`. Go's net.Resolver stamps error messages with the
// /etc/resolv.conf server even when a custom Dial targets a different address.
//
// This test proves the custom resolver/port IS used end-to-end by standing up a
// real local DNS server on an ephemeral port and asserting VerifyDelegation
// resolves against it (not the system resolver).

// startCustomPortDNSServer runs a real miekg/dns UDP+TCP server bound to an
// ephemeral port on loopback. The handler answers NS queries for `name` with
// the given nameservers and increments a counter so the test can assert the
// query actually reached this server.
func startCustomPortDNSServer(t *testing.T, name string, nsRecords []string) (addr string, served *atomicCounter) {
	t.Helper()

	served = &atomicCounter{}
	handler := dns.HandlerFunc(func(w dns.ResponseWriter, req *dns.Msg) {
		served.add(1)
		m := new(dns.Msg)
		m.SetReply(req)
		m.Authoritative = true
		if len(req.Question) > 0 && req.Question[0].Qtype == dns.TypeNS {
			for _, ns := range nsRecords {
				rr := &dns.NS{
					Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeNS, Class: dns.ClassINET, Ttl: 300},
					Ns:  ns,
				}
				m.Answer = append(m.Answer, rr)
			}
		}
		_ = w.WriteMsg(m)
	})

	// Bind a real UDP socket first so the port is guaranteed free and the
	// server is immediately servable. Passing PacketConn avoids the race where
	// Addr-based dns.Server hasn't bound yet when the resolver first dials.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	addr = pc.LocalAddr().String()

	udpSrv := &dns.Server{PacketConn: pc, Handler: handler}
	go func() { _ = udpSrv.ActivateAndServe() }()

	// Optionally mirror on TCP for resolver fallback (net.Resolver tries both).
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		tcpSrv := &dns.Server{Listener: ln, Handler: handler}
		go func() { _ = tcpSrv.ActivateAndServe() }()
		t.Cleanup(func() {
			_ = tcpSrv.Shutdown()
			_ = udpSrv.Shutdown()
		})
	} else {
		t.Cleanup(func() { _ = udpSrv.Shutdown() })
	}

	// Wait until the UDP server is accepting queries. Use an A query (not NS) so
	// this readiness probe does NOT increment the NS `served` counter — otherwise
	// the counter would be >= 1 before VerifyDelegation ever runs, making the
	// "custom resolver was queried" assertions below vacuous (they must prove
	// VerifyDelegation itself reached this server, not just the probe).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c := &dns.Client{Net: "udp", Timeout: 200 * time.Millisecond}
		m := new(dns.Msg)
		m.SetQuestion(name, dns.TypeA)
		if _, _, err := c.Exchange(m, addr); err == nil {
			return addr, served
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("custom-port DNS server did not become ready at %s", addr)
	return "", nil
}

type atomicCounter struct {
	mu sync.Mutex
	n  int
}

func (c *atomicCounter) add(d int) {
	c.mu.Lock()
	c.n += d
	c.mu.Unlock()
}

func (c *atomicCounter) value() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func TestHNSProvider_VerifyDelegation_UsesCustomPortResolver(t *testing.T) {
	const domain = "lumeweb."
	const ns1 = "ns1.lumeweb."
	const ns2 = "ns2.lumeweb."

	addr, served := startCustomPortDNSServer(t, domain, []string{ns1, ns2})

	// The resolver address points at our local DNS server on a CUSTOM port.
	p := NewHNSProvider(addr, []string{"ns1.lumeweb.", "ns2.lumeweb."}, TLSASource{})

	before := served.value()
	verified, err := p.VerifyDelegation(context.Background(), "lumeweb", json.RawMessage(`{}`))
	require.NoError(t, err)
	assert.True(t, verified, "delegation should verify when NS records match")

	// The critical assertion: our custom-port server actually received the NS
	// query DURING VerifyDelegation. If the code had silently used the system
	// resolver instead of the configured host:port, the counter would not move.
	assert.Greater(t, served.value(), before,
		"custom-port resolver must have received the NS query during VerifyDelegation")
}

func TestHNSProvider_VerifyDelegation_CustomPort_NSMismatch(t *testing.T) {
	const domain = "lumeweb."

	// Server that answers with nameservers that do NOT match the expected ones.
	addr, served := startCustomPortDNSServer(t, domain, []string{"ns.wrong.hns."})

	p := NewHNSProvider(addr, []string{"ns1.lumeweb.", "ns2.lumeweb."}, TLSASource{})

	before := served.value()
	verified, err := p.VerifyDelegation(context.Background(), "lumeweb", json.RawMessage(`{}`))
	require.NoError(t, err)
	assert.False(t, verified, "delegation must not verify when returned NS do not match")
	assert.Greater(t, served.value(), before, "custom-port resolver must have been queried even on mismatch")
}

func TestHNSProvider_VerifyDelegation_NoResolverConfigured(t *testing.T) {
	p := NewHNSProvider("", []string{"ns1.lumeweb.", "ns2.lumeweb."}, TLSASource{})

	_, err := p.VerifyDelegation(context.Background(), "lumeweb", json.RawMessage(`{}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HNS resolver not configured")
}

// Demonstrates the misleading-error scenario that triggered this work: Go's
// net.Resolver stamps the error string with the /etc/resolv.conf server even
// when the custom Dial targets a different host:port. The data still reaches
// the configured custom port. This test proves the custom port was dialed.
func TestHNSProvider_VerifyDelegation_CustomPort_ErrorStillProvesDial(t *testing.T) {
	const domain = "lumeweb."
	addr, served := startCustomPortDNSServer(t, domain, []string{"ns1.lumeweb."})

	p := NewHNSProvider(addr, []string{"ns1.lumeweb.", "ns2.lumeweb."}, TLSASource{})

	before := served.value()
	valid, err := p.VerifyDelegation(context.Background(), "lumeweb", json.RawMessage(`{}`))
	t.Logf("VerifyDelegation valid=%v err=%v customPort=%s served=%d", valid, err, addr, served.value())

	// The custom-port resolver was queried during VerifyDelegation regardless
	// of the match outcome.
	assert.Greater(t, served.value(), before, "custom-port resolver must have been queried")

	// Prove the resolver address is a custom (non-53) port -> port support is
	// exercised end-to-end, not just the default DNS port.
	_, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	assert.NotEqual(t, 53, port, "test resolver must run on a custom (non-default) port")
}

// The error returned on lookup failure must surface the ACTUAL resolver address
// we dial, not Go's misleading /etc/resolv.conf server label ("on 127.0.0.11:53").
// Go hardcodes the DNSError.Server field from the system DNS config and never
// observes that our custom Dial bypassed it, so we wrap the error explicitly.
func TestHNSProvider_VerifyDelegation_ErrorSurfacesActualResolverAddr(t *testing.T) {
	// Bind a UDP socket to a free custom port, then close it so connections
	// refuse (forcing a lookup error) while we know the exact host:port we'd dial.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr := pc.LocalAddr().String()
	_ = pc.Close()

	p := NewHNSProvider(deadAddr, []string{"ns1.lumeweb.", "ns2.lumeweb."}, TLSASource{})

	_, err = p.VerifyDelegation(context.Background(), "lumeweb", json.RawMessage(`{}`))
	require.Error(t, err)

	// The wrapped error must name the custom resolver (host:port) so operators
	// can see which resolver was actually dialed.
	assert.Contains(t, err.Error(), deadAddr,
		"error must surface the actual resolver address %q, got: %v", deadAddr, err)
	assert.Contains(t, err.Error(), "HNS resolver query failed",
		"error must retain the operation context")
	t.Logf("resolver error surfaces actual dial target: %v", err)
}
