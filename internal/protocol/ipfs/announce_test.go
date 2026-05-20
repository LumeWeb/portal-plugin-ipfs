package ipfs

import (
	"testing"

	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnnouncementAddresses_AnnounceWeb(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4001"),
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4001/ws"),
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4001/quic-v1"),
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4001/quic-v1/webtransport"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	expected := []string{
		"/dns/ipfs.example.com/tcp/4001",
		"/dns/web.ipfs.example.com/tcp/443/wss",
		"/dns/ipfs.example.com/udp/4001/quic-v1",
		"/dns/ipfs.example.com/udp/4001/quic-v1/webtransport",
	}
	require.Len(t, result, len(expected))
	for i, addr := range result {
		assert.Equal(t, expected[i], addr.String())
	}
}

func TestAnnouncementAddresses_AnnounceWebFalse(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4001"),
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4001/ws"),
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4001/quic-v1"),
		multiaddr.StringCast("/ip4/192.168.1.1/tcp/4001/ws"),
		multiaddr.StringCast("/ip4/127.0.0.1/tcp/4001/ws"),
	}

	result, err := AnnouncementAddresses(false, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 3)
	assert.Equal(t, "/ip4/1.2.3.4/tcp/4001", result[0].String())
	assert.Equal(t, "/ip4/1.2.3.4/tcp/4001/ws", result[1].String())
	assert.Equal(t, "/ip4/1.2.3.4/udp/4001/quic-v1", result[2].String())
}

func TestAnnouncementAddresses_PlainTCP(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4001"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/ipfs.example.com/tcp/4001", result[0].String())
}

func TestAnnouncementAddresses_AnnounceWebWSSToPort443(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4001/ws"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/web.ipfs.example.com/tcp/443/wss", result[0].String())
}

func TestAnnouncementAddresses_AnnounceWebWSSPassthrough(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/443/wss"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/web.ipfs.example.com/tcp/443/wss", result[0].String())
}

func TestAnnouncementAddresses_DeduplicatesWSS(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4001/ws"),
		multiaddr.StringCast("/ip6/::/tcp/4001/ws"),
		multiaddr.StringCast("/ip4/10.0.0.1/tcp/4001/ws"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/web.ipfs.example.com/tcp/443/wss", result[0].String())
}

func TestAnnouncementAddresses_QUICUsesApexDomain(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4001/quic-v1"),
		multiaddr.StringCast("/ip4/10.0.0.1/udp/4001/quic-v1"),
		multiaddr.StringCast("/ip4/127.0.0.1/udp/4001/quic-v1"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/ipfs.example.com/udp/4001/quic-v1", result[0].String())
}

func TestAnnouncementAddresses_EphemeralUDPPortFiltered(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4001/quic-v1"),
		multiaddr.StringCast("/ip4/1.2.3.4/udp/42966/quic-v1"),
		multiaddr.StringCast("/ip4/1.2.3.4/udp/42966/quic-v1/webtransport/certhash/uEiBzadLZbQCvscarMZg74tDg1l0trRpbTcWQOipBLFmSGg"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/ipfs.example.com/udp/4001/quic-v1", result[0].String())
}

func TestAnnouncementAddresses_WEBTRANSPORTWithCertHash(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4001/quic-v1/webtransport/certhash/uEiBzadLZbQCvscarMZg74tDg1l0trRpbTcWQOipBLFmSGg"),
		multiaddr.StringCast("/ip4/10.0.0.1/udp/4001/quic-v1/webtransport/certhash/uEiBzadLZbQCvscarMZg74tDg1l0trRpbTcWQOipBLFmSGg"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Contains(t, result[0].String(), "/dns/ipfs.example.com/udp/4001/quic-v1/webtransport/certhash/")
}

func TestAnnouncementAddresses_EmptyHostAddrs(t *testing.T) {
	result, err := AnnouncementAddresses(true, "ipfs.example.com", nil, 4001)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestAnnouncementAddresses_EmptyHostAddrsNoWeb(t *testing.T) {
	result, err := AnnouncementAddresses(false, "", nil, 4001)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestAnnouncementAddresses_AnnounceWebNoDomain(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4001/ws"),
	}

	result, err := AnnouncementAddresses(true, "", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/ip4/1.2.3.4/tcp/4001/ws", result[0].String())
}

func TestFilterPublicAddrs(t *testing.T) {
	addrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4001/ws"),
		multiaddr.StringCast("/ip4/127.0.0.1/tcp/4001/ws"),
		multiaddr.StringCast("/ip4/10.0.0.1/tcp/4001/ws"),
		multiaddr.StringCast("/ip4/0.0.0.0/tcp/4001/ws"),
		multiaddr.StringCast("/ip4/192.168.1.1/tcp/4001/ws"),
		multiaddr.StringCast("/ip4/172.16.0.1/tcp/4001/ws"),
	}

	result := filterPublicAddrs(addrs)
	require.Len(t, result, 1)
	assert.Equal(t, "/ip4/1.2.3.4/tcp/4001/ws", result[0].String())
}

func TestAnnouncementAddresses_IPv6WSReplaced(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip6/2607:f8b0:4004:800::200e/tcp/4001/ws"),
		multiaddr.StringCast("/ip6/2607:f8b0:4004:800::200e/udp/4001/quic-v1"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 2)
	assert.Equal(t, "/dns/web.ipfs.example.com/tcp/443/wss", result[0].String())
	assert.Equal(t, "/dns/ipfs.example.com/udp/4001/quic-v1", result[1].String())
}

func TestAnnouncementAddresses_DNSHostAddrReplaced(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/dns4/old.example.com/tcp/4001/ws"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/web.ipfs.example.com/tcp/443/wss", result[0].String())
}

func TestAnnouncementAddresses_MixedTransports(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4001"),
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4001/ws"),
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4001/quic-v1"),
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4001/quic-v1/webtransport/certhash/uEiBzadLZbQCvscarMZg74tDg1l0trRpbTcWQOipBLFmSGg"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	expected := []string{
		"/dns/ipfs.example.com/tcp/4001",
		"/dns/web.ipfs.example.com/tcp/443/wss",
		"/dns/ipfs.example.com/udp/4001/quic-v1",
		"/dns/ipfs.example.com/udp/4001/quic-v1/webtransport/certhash/uEiBzadLZbQCvscarMZg74tDg1l0trRpbTcWQOipBLFmSGg",
	}
	require.Len(t, result, len(expected))
	for i, addr := range result {
		assert.Equal(t, expected[i], addr.String())
	}
}

func TestAnnouncementAddresses_NoWSFallback(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4001/quic-v1"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/ipfs.example.com/udp/4001/quic-v1", result[0].String())
}

func TestAnnouncementAddresses_WebRTCDirect(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4001/webrtc-direct"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs, 4001)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/ipfs.example.com/udp/4001/webrtc-direct", result[0].String())
}
