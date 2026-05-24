package ipfs

import (
	"net"

	"github.com/pires/go-proxyproto"
)

// newProxyProtocolListener wraps a net.Listener with PROXY protocol v1/v2
// parsing using the pires/go-proxyproto library. When a valid PROXY header
// is found, the returned connection's RemoteAddr() returns the real client
// IP. All connections must come through a PROXY-aware source.
//
// trustedProxies controls which source IPs are allowed to send PROXY headers.
// If empty, all sources are trusted (suitable when the listener is bound to
// a private network interface only reachable by the proxy).
func newProxyProtocolListener(ln net.Listener, trustedProxies []*net.IPNet) net.Listener {
	policy := makeProxyPolicy(trustedProxies)
	return &proxyproto.Listener{
		Listener: ln,
		Policy:   policy,
	}
}

// makeProxyPolicy builds a proxyproto.PolicyFunc that only accepts PROXY
// headers from trusted proxy IPs. If the trusted list is empty, all
// connections are trusted (USE requirement).
func makeProxyPolicy(trustedProxies []*net.IPNet) proxyproto.PolicyFunc {
	if len(trustedProxies) == 0 {
		return func(upstream net.Addr) (proxyproto.Policy, error) {
			return proxyproto.USE, nil
		}
	}

	allowedIPs := make([]string, 0, len(trustedProxies))
	for _, cidr := range trustedProxies {
		allowedIPs = append(allowedIPs, cidr.String())
	}

	policy, err := proxyproto.LaxWhiteListPolicy(allowedIPs)
	if err != nil {
		return func(upstream net.Addr) (proxyproto.Policy, error) {
			return proxyproto.REJECT, nil
		}
	}
	return policy
}
