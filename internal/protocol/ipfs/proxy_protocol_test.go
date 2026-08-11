package ipfs

import (
	"net"
	"testing"
	"time"

	"github.com/pires/go-proxyproto"
)

func TestProxyProtocolV1TCP4(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	proxyLn := newProxyProtocolListener(ln, nil)

	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		conn.Write([]byte("PROXY TCP4 10.0.0.1 172.18.0.11 12345 4001\r\n"))
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		conn.Read(buf)
	}()

	conn, err := proxyLn.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	addr := conn.RemoteAddr().(*net.TCPAddr)
	if addr.IP.String() != "10.0.0.1" {
		t.Errorf("expected IP 10.0.0.1, got %s", addr.IP)
	}
	if addr.Port != 12345 {
		t.Errorf("expected port 12345, got %d", addr.Port)
	}
}

func TestProxyProtocolV1TCP6(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	proxyLn := newProxyProtocolListener(ln, nil)

	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		conn.Write([]byte("PROXY TCP6 ::1 ::1 12345 4001\r\n"))
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		conn.Read(buf)
	}()

	conn, err := proxyLn.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	addr := conn.RemoteAddr().(*net.TCPAddr)
	if addr.IP.String() != "::1" {
		t.Errorf("expected IP ::1, got %s", addr.IP)
	}
	if addr.Port != 12345 {
		t.Errorf("expected port 12345, got %d", addr.Port)
	}
}

func TestProxyProtocolV1Unknown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	proxyLn := newProxyProtocolListener(ln, nil)

	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		conn.Write([]byte("PROXY UNKNOWN\r\n"))
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		conn.Read(buf)
	}()

	conn, err := proxyLn.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if conn.RemoteAddr().String() == "" {
		t.Error("expected non-empty remote addr for UNKNOWN")
	}
}

func TestProxyProtocolV2IPv4(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	proxyLn := newProxyProtocolListener(ln, nil)

	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()

		header := &proxyproto.Header{
			Version:           2,
			Command:           proxyproto.PROXY,
			TransportProtocol: proxyproto.TCPv4,
			SourceAddr:        &net.TCPAddr{IP: net.ParseIP("192.168.1.100"), Port: 54321},
			DestinationAddr:   &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 4001},
		}
		if _, err := header.WriteTo(conn); err != nil {
			t.Error(err)
			return
		}
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		conn.Read(buf)
	}()

	conn, err := proxyLn.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	addr := conn.RemoteAddr().(*net.TCPAddr)
	if addr.IP.String() != "192.168.1.100" {
		t.Errorf("expected IP 192.168.1.100, got %s", addr.IP)
	}
	if addr.Port != 54321 {
		t.Errorf("expected port 54321, got %d", addr.Port)
	}
}

func TestProxyProtocolV2IPv6(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	proxyLn := newProxyProtocolListener(ln, nil)

	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()

		header := &proxyproto.Header{
			Version:           2,
			Command:           proxyproto.PROXY,
			TransportProtocol: proxyproto.TCPv6,
			SourceAddr:        &net.TCPAddr{IP: net.ParseIP("::1"), Port: 54321},
			DestinationAddr:   &net.TCPAddr{IP: net.ParseIP("::1"), Port: 4001},
		}
		if _, err := header.WriteTo(conn); err != nil {
			t.Error(err)
			return
		}
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		conn.Read(buf)
	}()

	conn, err := proxyLn.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	addr := conn.RemoteAddr().(*net.TCPAddr)
	if addr.IP.String() != "::1" {
		t.Errorf("expected IP ::1, got %s", addr.IP)
	}
	if addr.Port != 54321 {
		t.Errorf("expected port 54321, got %d", addr.Port)
	}
}

func TestProxyProtocolV2Local(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	proxyLn := newProxyProtocolListener(ln, nil)

	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()

		header := &proxyproto.Header{
			Version:           2,
			Command:           proxyproto.LOCAL,
			TransportProtocol: proxyproto.UNSPEC,
		}
		if _, err := header.WriteTo(conn); err != nil {
			t.Error(err)
			return
		}
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		conn.Read(buf)
	}()

	conn, err := proxyLn.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if conn.RemoteAddr().String() == "" {
		t.Error("expected non-empty remote addr for LOCAL")
	}
}

func TestProxyProtocolTrustedProxies(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	proxyLn := newProxyProtocolListener(ln, []string{"127.0.0.0/8"})

	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		conn.Write([]byte("PROXY TCP4 10.0.0.1 127.0.0.1 12345 4001\r\n"))
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		conn.Read(buf)
	}()

	conn, err := proxyLn.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	addr := conn.RemoteAddr().(*net.TCPAddr)
	if addr.IP.String() != "10.0.0.1" {
		t.Errorf("expected IP 10.0.0.1, got %s", addr.IP)
	}
}

func TestProxyProtocolUntrustedProxy(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	proxyLn := newProxyProtocolListener(ln, []string{"10.0.0.0/8"})

	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		conn.Write([]byte("PROXY TCP4 10.0.0.1 127.0.0.1 12345 4001\r\n"))
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		conn.Read(buf)
	}()

	conn, err := proxyLn.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	addr := conn.RemoteAddr().(*net.TCPAddr)
	if addr.IP.String() == "10.0.0.1" {
		t.Error("untrusted proxy should not have PROXY header parsed")
	}
}

// TestProxyProtocolUseRequiresHeader demonstrates why the node must NOT install
// the proxy transport when no proxy is configured. The empty-whitelist policy
// is proxyproto.USE, which requires an inbound connection to open with a PROXY
// protocol header before its data is relayed. A direct libp2p peer sends raw
// multistream+noise bytes (no PROXY header), so its first bytes are consumed by
// header detection and its dial does not proceed — the failure mode behind the
// reported downtime. The config gate in node.go (proxy_protocol=false → plain
// TCP transport) avoids this path entirely.
func TestProxyProtocolUseRequiresHeader(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Default (no trusted proxies) policy is USE.
	proxyLn := newProxyProtocolListener(ln, nil)

	accepted := make(chan net.Conn, 1)
	go func() {
		c, aerr := proxyLn.Accept()
		if aerr == nil {
			accepted <- c
		} else {
			accepted <- nil
		}
	}()

	// A raw client sends libp2p-style bytes with no PROXY header. Under the USE
	// policy these bytes are swallowed by proxy-protocol detection, so the
	// server-side handshake cannot observe them.
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Send non-PROXY bytes (e.g. a libp2p multistream header).
	conn.Write([]byte("/multistream/1.0.0\n"))
	conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	buf := make([]byte, 64)
	n, _ := conn.Read(buf)

	select {
	case acceptedConn := <-accepted:
		acceptedConn.Close()
		// The raw dial did not receive a response within the deadline: the
		// server consumed the bytes as a (malformed) proxy header and did not
		// drive a libp2p handshake. n == 0 asserts no reply was echoed back.
		if n > 0 {
			t.Fatalf("under USE policy a raw client was still served: %q", buf[:n])
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("raw client connection was never accepted as a proxied connection")
	}
}

// TestMakeProxyPolicyDefaultIsUse pins the current empty-whitelist default so
// the config gate that selects the plain TCP transport when proxy_protocol is
// disabled stays meaningful: with no trusted proxies configured the wrapper
// would otherwise require the PROXY header.
func TestMakeProxyPolicyDefaultIsUse(t *testing.T) {
	p, _ := makeProxyPolicy(nil)(&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4001})
	if p != proxyproto.USE {
		t.Errorf("makeProxyPolicy(nil) = %v, want proxyproto.USE", p)
	}
}
