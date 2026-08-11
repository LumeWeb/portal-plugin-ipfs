package ipfs

import (
	"net"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/multiformats/go-multiaddr"
)

func mustDecodePeerFromMultiaddr(t *testing.T, raw string) peer.ID {
	t.Helper()
	// Extract the peer ID from a multiaddr string like
	// /ip4/10.1.2.3/p2p/12D3Koo...
	const p2pPrefix = "/p2p/"
	idx := strings.LastIndex(raw, p2pPrefix)
	if idx < 0 {
		t.Fatalf("multiaddr %q has no /p2p/ peer ID", raw)
	}
	id, err := peer.Decode(raw[idx+len(p2pPrefix):])
	if err != nil {
		t.Fatalf("failed to decode peer ID from %q: %v", raw, err)
	}
	return id
}

func ipNetContains(t *testing.T, nets []*net.IPNet, contains string) {
	t.Helper()
	ip := net.ParseIP(contains)
	if ip == nil {
		t.Fatalf("bad test IP %q", contains)
	}
	for _, n := range nets {
		if n.Contains(ip) {
			return
		}
	}
	t.Fatalf("no parsed network contains %q", contains)
}

func TestParseGatewayMultiaddrs_HonorsIPCIDR(t *testing.T) {
	// Regression guard: /ipcidr/24 must yield the /24 subnet, not collapse to
	// a /32 host route (the old parser ignored the ipcidr component).
	ipNets, peerIDs, allowlisted, err := parseGatewayMultiaddrs([]string{
		"/ip4/10.10.0.0/ipcidr/24",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(ipNets) != 1 {
		t.Fatalf("expected 1 network, got %d", len(ipNets))
	}
	ones, _ := ipNets[0].Mask.Size()
	if ones != 24 {
		t.Fatalf("expected /24 prefix, got /%d", ones)
	}
	ipNetContains(t, ipNets, "10.10.0.200") // inside subnet
	// Ensure the single host itself is covered and a host outside the /24 is not.
	if ipNets[0].Contains(net.ParseIP("10.10.1.1")) {
		t.Fatalf("network %s unexpectedly contains 10.10.1.1 (should be /24 not /16)", ipNets[0])
	}

	if want := 1; len(allowlisted) != want {
		t.Fatalf("allowlisted: got %d, want %d", len(allowlisted), want)
	}
	if len(peerIDs) != 0 {
		t.Fatalf("expected no peer IDs, got %d", len(peerIDs))
	}
}

func TestParseGatewayMultiaddrs_HostDefaultsToHostBits(t *testing.T) {
	// A bare IPv4/IPv6 without /ipcidr defaults to a /32 or /128 host route.
	ipNets, _, _, err := parseGatewayMultiaddrs([]string{
		"/ip4/10.1.2.3",
		"/ip6/::1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ipNets) != 2 {
		t.Fatalf("expected 2 networks, got %d", len(ipNets))
	}

	mask4 := ipNets[0].Mask
	ones4, _ := mask4.Size()
	if ones4 != 32 {
		t.Fatalf("bare ipv4 must be /32, got /%d", ones4)
	}

	ones6, _ := ipNets[1].Mask.Size()
	if ones6 != 128 {
		t.Fatalf("bare ipv6 must be /128, got /%d", ones6)
	}
}

func TestParseGatewayMultiaddrs_PeerConstrainedForm(t *testing.T) {
	rawA := "/ip4/10.1.2.3/p2p/12D3KooWB5sNsAocc5F37JnGjYE1t6WKkJwGeahDgg99cN1QL3dM"
	rawB := "/ip6/::1/p2p/12D3KooWB5sNsAocc5F37JnGjYE1t6WKkJwGeahDgg99cN1QL3dM"

	ipNets, peerIDs, allowlisted, err := parseGatewayMultiaddrs([]string{rawA, rawB})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(peerIDs) != 2 {
		t.Fatalf("expected 2 peer IDs, got %d", len(peerIDs))
	}
	if wantA := mustDecodePeerFromMultiaddr(t, rawA); peerIDs[0] != wantA {
		t.Errorf("peerIDs[0] mismatch, got %s want %s", peerIDs[0], wantA)
	}

	// The allowlist keeps the full original multiaddr (including /p2p), so the
	// rcmgr can do the post-handshake peer re-check (rcmgr.go:799
	// AllowedPeerAndMultiaddr) after the IP-based admission.
	if len(allowlisted) != 2 {
		t.Fatalf("expected 2 allowlisted multiaddrs, got %d", len(allowlisted))
	}
	for i, am := range allowlisted {
		if _, hasP2P := am.ValueForProtocol(multiaddr.P_P2P); hasP2P != nil {
			t.Errorf("allowlisted[%d] lost its /p2p component: %s", i, am)
		}
	}

	// IP networks are still derived from the peer-constrained addrs.
	if len(ipNets) != 2 {
		t.Fatalf("expected 2 networks, got %d", len(ipNets))
	}
	ipNetContains(t, ipNets, "10.1.2.3")
}

func TestParseGatewayMultiaddrs_BareP2PNoStartupAbort(t *testing.T) {
	// A bare /p2p gateway multiaddr (peer-only, no IP component) must not
	// reach rcmgr.WithAllowlistedMultiaddrs, which rejects IP-less multiaddrs
	// with "missing ip address" and would fail node startup. The peer must
	// still be extracted for cmgr.Protect / Bitswap bypass.
	ipNets, peerIDs, allowlisted, err := parseGatewayMultiaddrs([]string{
		"/p2p/12D3KooWB5sNsAocc5F37JnGjYE1t6WKkJwGeahDgg99cN1QL3dM",
	})
	if err != nil {
		t.Fatalf("bare /p2p must not fail startup: %v", err)
	}
	if len(ipNets) != 0 {
		t.Fatalf("bare /p2p has no IP, expected 0 networks, got %d", len(ipNets))
	}
	// The peer is still protected even though it is not allowlisted.
	if len(peerIDs) != 1 {
		t.Fatalf("expected 1 peer ID, got %d", len(peerIDs))
	}
	// No IP means nothing to allowlist — passing this to the rcmgr would abort.
	if len(allowlisted) != 0 {
		t.Fatalf("bare /p2p must not be allowlisted, got %d", len(allowlisted))
	}
}

func TestParseGatewayMultiaddrs_InvalidReturnsError(t *testing.T) {
	// A malformed gateway multiaddr is a config error (the old parser silently
	// skipped it), so a typo'd gateway fails fast at startup instead of
	// silently losing gateway trust policy.
	_, _, _, err := parseGatewayMultiaddrs([]string{"not-a-multiaddr"})
	if err == nil {
		t.Fatal("expected error for invalid multiaddr, got nil")
	}
}

func TestParseGatewayMultiaddrs_NonNumericIPCIDRIsError(t *testing.T) {
	// A non-numeric /ipcidr value must fail fast (config error), not silently
	// fall back to a /32 or /128 host route. That fallback would collapse an
	// intended subnet into a single-host trust policy with the wrong security
	// semantics, contradicting the fail-fast behavior of the parser.
	for _, raw := range []string{
		"/ip4/10.10.0.0/ipcidr/foo",
		"/ip6/2001:db8::/ipcidr/bar",
	} {
		_, _, _, err := parseGatewayMultiaddrs([]string{raw})
		if err == nil {
			t.Fatalf("expected error for non-numeric /ipcidr in %q, got nil", raw)
		}
	}
}

func TestParseGatewayMultiaddrs_InvalidP2PPeerIsError(t *testing.T) {
	// A /p2p value that fails peer.Decode must fail fast (config error), not be
	// silently dropped. Silently dropping it would leave the gateway allowlisted
	// by IP while silently disabling connmgr.Protect and the Bitswap want-rate
	// bypass — granting IP trust without the peer-level protections, which
	// changes trust semantics for a config error.
	for _, raw := range []string{
		"/ip4/10.1.2.3/p2p/not-a-valid-peer-id",
		"/ip6/::1/p2p/!!!invalid!!!",
	} {
		_, _, _, err := parseGatewayMultiaddrs([]string{raw})
		if err == nil {
			t.Fatalf("expected error for invalid /p2p in %q, got nil", raw)
		}
	}
}

func TestParseGatewayMultiaddrs_HostBitsSetNotAnError(t *testing.T) {
	// A gateway address carrying host bits below its /ipcidr prefix (e.g.
	// /10.10.0.5/24) must not be rejected. net.ParseCIDR masks the network
	// (10.10.0.0/24) rather than erroring, so this must parse cleanly and cover
	// the whole advertised subnet. Regression guard against any future switch
	// to netip.ParsePrefix, which does reject set host bits.
	ipNets, _, _, err := parseGatewayMultiaddrs([]string{
		"/ip4/10.10.0.5/ipcidr/24",
		"/ip6/2001:db8::5/ipcidr/64",
	})
	if err != nil {
		t.Fatalf("unexpected error for host-bits-set cidr: %v", err)
	}
	if len(ipNets) != 2 {
		t.Fatalf("expected 2 networks, got %d", len(ipNets))
	}

	// ipv4 network must be the masked /24.
	if !ipNets[0].Contains(net.ParseIP("10.10.0.200")) {
		t.Fatalf("v4 network %s must be 10.10.0.0/24", ipNets[0])
	}
	if ipNets[0].Contains(net.ParseIP("10.10.1.1")) {
		t.Fatalf("v4 network %s escaped the /24", ipNets[0])
	}

	// ipv6 network must be the masked /64.
	if !ipNets[1].Contains(net.ParseIP("2001:db8::1234")) {
		t.Fatalf("v6 network %s must be 2001:db8::/64", ipNets[1])
	}
	ones6, _ := ipNets[1].Mask.Size()
	if ones6 != 64 {
		t.Fatalf("v6 network prefix bits = %d, want 64", ones6)
	}
}

func TestPrivateAndLoopbackNets_ContainsLoopbackAndPrivate(t *testing.T) {
	nets := privateAndLoopbackNets()

	// All the classic private/loopback/ULA ranges must be present.
	for _, ip := range []string{"127.0.0.1", "::1", "10.0.0.1", "172.16.0.1", "192.168.1.1", "fd00::1"} {
		ipNetContains(t, nets, ip)
	}
	// A public address must not be covered.
	if containsPublic(nets, "8.8.8.8") {
		t.Fatal("privateAndLoopbackNets must not contain public address 8.8.8.8")
	}
}

func TestParseGatewayMultiaddrs_RejectsOversizedIPCIDR(t *testing.T) {
	// A /ipcidr prefix larger than the address family's bit length is a config
	// error and must fail fast, not silently broaden or mis-scope the trust
	// subnet.
	for _, raw := range []string{
		"/ip4/10.10.0.0/ipcidr/64",   // > 32 for IPv4
		"/ip6/2001:db8::/ipcidr/129", // > 128 for IPv6
		"/ip4/10.10.0.0/ipcidr/33",   // > 32 for IPv4
	} {
		_, _, _, err := parseGatewayMultiaddrs([]string{raw})
		if err == nil {
			t.Fatalf("expected error for oversized /ipcidr in %q, got nil", raw)
		}
	}
}

func TestParseGatewayMultiaddrs_EmitsNetworkPerIP(t *testing.T) {
	// Regression guard: a p2p-circuit relayed gateway multiaddr has more than
	// one IP component. The relay IP that actually originates the connection
	// must stay in ipNets so it remains rate/conn-exempt and allowlisted. The
	// old parser kept only the last IP (dropping the relay).
	ipNets, _, _, err := parseGatewayMultiaddrs([]string{
		"/ip4/10.9.9.9/tcp/4001/p2p-circuit/ip4/10.8.8.8/tcp/4001/ipfs/12D3KooWB5sNsAocc5F37JnGjYE1t6WKkJwGeahDgg99cN1QL3dM",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ipNetContains(t, ipNets, "10.9.9.9") // relay
	ipNetContains(t, ipNets, "10.8.8.8") // target
	if len(ipNets) != 2 {
		t.Fatalf("expected a network per IP (relay + target), got %d", len(ipNets))
	}
}

func TestGatewayAllowlist_ExpandsIPCIDRSubnet(t *testing.T) {
	// Regression guard: the full gateway multiaddr (including /ipcidr/N) must
	// be passed through to the rcmgr allowlist unchanged. libp2p's allowlist
	// expands /ipcidr into a *net.IPNet and matches addresses with
	// network.Contains, so a /24 gateway entry must cover the whole subnet —
	// not only the base host. Stripping the /ipcidr component here would
	// silently reduce subnet gateways to a single /32 reserve slot.
	_, _, allowlisted, err := parseGatewayMultiaddrs([]string{
		"/ip4/10.10.0.0/ipcidr/24",
	})
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(allowlisted) != 1 {
		t.Fatalf("expected 1 allowlisted multiaddr, got %d", len(allowlisted))
	}

	rm, err := rcmgr.NewResourceManager(
		rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.AutoScale()),
		rcmgr.WithAllowlistedMultiaddrs(allowlisted),
	)
	if err != nil {
		t.Fatalf("rcmgr rejected /ipcidr gateway multiaddr: %v", err)
	}
	al := rm.(interface{ GetAllowlist() *rcmgr.Allowlist }).GetAllowlist()

	for _, host := range []string{"10.10.0.0", "10.10.0.200"} {
		ma, _ := multiaddr.NewMultiaddr("/ip4/" + host)
		if !al.Allowed(ma) {
			t.Fatalf("gateway host %s not allowed — /24 did not expand", host)
		}
	}
	outside, _ := multiaddr.NewMultiaddr("/ip4/10.10.1.1")
	if al.Allowed(outside) {
		t.Fatal("host outside /24 unexpectedly allowed — subnet over-expanded")
	}
}

func containsPublic(nets []*net.IPNet, ipStr string) bool {
	ip := net.ParseIP(ipStr)
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}
