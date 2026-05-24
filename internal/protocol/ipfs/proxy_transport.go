package ipfs

import (
	"context"
	"net"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/transport"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

type proxyTCPTransport struct {
	inner          *tcp.TcpTransport
	upgrader       transport.Upgrader
	rcmgr          network.ResourceManager
	trustedProxies []string
}

func newProxyTCPTransport(trustedProxies []string) interface{} {
	return func(upgrader transport.Upgrader, rcmgr network.ResourceManager) (*proxyTCPTransport, error) {
		if rcmgr == nil {
			rcmgr = &network.NullResourceManager{}
		}
		inner, err := tcp.NewTCPTransport(upgrader, rcmgr, nil)
		if err != nil {
			return nil, err
		}
		return &proxyTCPTransport{
			inner:          inner,
			upgrader:       upgrader,
			rcmgr:          rcmgr,
			trustedProxies: trustedProxies,
		}, nil
	}
}

func (t *proxyTCPTransport) Listen(laddr ma.Multiaddr) (transport.Listener, error) {
	lnet, lnaddr, err := manet.DialArgs(laddr)
	if err != nil {
		return nil, err
	}

	nl, err := net.Listen(lnet, lnaddr)
	if err != nil {
		return nil, err
	}

	proxyLn := newProxyProtocolListener(nl, t.trustedProxies)
	mal, err := manet.WrapNetListener(proxyLn)
	if err != nil {
		proxyLn.Close()
		return nil, err
	}

	gatedListener := t.upgrader.GateMaListener(mal)
	return t.upgrader.UpgradeGatedMaListener(t, gatedListener), nil
}

func (t *proxyTCPTransport) Dial(ctx context.Context, raddr ma.Multiaddr, p peer.ID) (transport.CapableConn, error) {
	return t.inner.Dial(ctx, raddr, p)
}

func (t *proxyTCPTransport) CanDial(addr ma.Multiaddr) bool {
	return t.inner.CanDial(addr)
}

func (t *proxyTCPTransport) Protocols() []int {
	return t.inner.Protocols()
}

func (t *proxyTCPTransport) Proxy() bool {
	return t.inner.Proxy()
}

func (t *proxyTCPTransport) String() string {
	return t.inner.String()
}
