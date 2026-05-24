package ipfs

import (
	"net"

	"github.com/pires/go-proxyproto"
)

func newProxyProtocolListener(ln net.Listener, trustedProxies []string) net.Listener {
	policy := makeProxyPolicy(trustedProxies)
	return &proxyproto.Listener{
		Listener: ln,
		Policy:   policy,
	}
}

func makeProxyPolicy(trustedProxies []string) proxyproto.PolicyFunc {
	if len(trustedProxies) == 0 {
		return func(upstream net.Addr) (proxyproto.Policy, error) {
			return proxyproto.USE, nil
		}
	}

	policy, err := proxyproto.LaxWhiteListPolicy(trustedProxies)
	if err != nil {
		return func(upstream net.Addr) (proxyproto.Policy, error) {
			return proxyproto.REJECT, nil
		}
	}
	return policy
}
