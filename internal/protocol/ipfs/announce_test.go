package ipfs

import (
	"testing"

	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnnouncementAddresses_AnnounceWeb(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4002/ws"),
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4002/quic-v1"),
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4002/quic-v1/webtransport"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs)
	require.NoError(t, err)

	expected := []string{
		"/dns/web.ipfs.example.com/tcp/4002/wss",
		"/dns/web.ipfs.example.com/udp/4002/quic-v1",
		"/dns/web.ipfs.example.com/udp/4002/quic-v1/webtransport",
	}
	require.Len(t, result, len(expected))
	for i, addr := range result {
		assert.Equal(t, expected[i], addr.String())
	}
}

func TestAnnouncementAddresses_AnnounceWebFalse(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4002/ws"),
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4002/quic-v1"),
		multiaddr.StringCast("/ip4/192.168.1.1/tcp/4002/ws"),
		multiaddr.StringCast("/ip4/127.0.0.1/tcp/4002/ws"),
	}

	result, err := AnnouncementAddresses(false, "ipfs.example.com", hostAddrs)
	require.NoError(t, err)

	require.Len(t, result, 2)
	assert.Equal(t, "/ip4/1.2.3.4/tcp/4002/ws", result[0].String())
	assert.Equal(t, "/ip4/1.2.3.4/udp/4002/quic-v1", result[1].String())
}

func TestAnnouncementAddresses_AnnounceWebConvertsWSToWSS(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4002/ws"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/web.ipfs.example.com/tcp/4002/wss", result[0].String())
}

func TestAnnouncementAddresses_AnnounceWebWSSPassthrough(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/443/wss"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/web.ipfs.example.com/tcp/443/wss", result[0].String())
}

func TestAnnouncementAddresses_DeduplicatesByProtoAndPort(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4002/ws"),
		multiaddr.StringCast("/ip6/::/tcp/4002/ws"),
		multiaddr.StringCast("/ip4/10.0.0.1/tcp/4002/ws"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/web.ipfs.example.com/tcp/4002/wss", result[0].String())
}

func TestAnnouncementAddresses_CertHashPreserved(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4002/quic-v1/webtransport/certhash/uEiBzadLZbQCvscarMZg74tDg1l0trRpbTcWQOipBLFmSGg"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Contains(t, result[0].String(), "/dns/web.ipfs.example.com/udp/4002/quic-v1/webtransport/certhash/")
}

func TestAnnouncementAddresses_EmptyHostAddrs(t *testing.T) {
	result, err := AnnouncementAddresses(true, "ipfs.example.com", nil)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestAnnouncementAddresses_EmptyHostAddrsNoWeb(t *testing.T) {
	result, err := AnnouncementAddresses(false, "", nil)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestAnnouncementAddresses_AnnounceWebNoDomain(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4002/ws"),
	}

	result, err := AnnouncementAddresses(true, "", hostAddrs)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/ip4/1.2.3.4/tcp/4002/ws", result[0].String())
}

func TestFilterPublicAddrs(t *testing.T) {
	addrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4002/ws"),
		multiaddr.StringCast("/ip4/127.0.0.1/tcp/4002/ws"),
		multiaddr.StringCast("/ip4/10.0.0.1/tcp/4002/ws"),
		multiaddr.StringCast("/ip4/0.0.0.0/tcp/4002/ws"),
		multiaddr.StringCast("/ip4/192.168.1.1/tcp/4002/ws"),
		multiaddr.StringCast("/ip4/172.16.0.1/tcp/4002/ws"),
	}

	result := filterPublicAddrs(addrs)
	require.Len(t, result, 1)
	assert.Equal(t, "/ip4/1.2.3.4/tcp/4002/ws", result[0].String())
}

func TestAnnouncementAddresses_IPv6Replaced(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip6/::1/tcp/4002/ws"),
		multiaddr.StringCast("/ip6/2001:db8::1/udp/4002/quic-v1"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs)
	require.NoError(t, err)

	require.Len(t, result, 2)
	assert.Equal(t, "/dns/web.ipfs.example.com/tcp/4002/wss", result[0].String())
	assert.Equal(t, "/dns/web.ipfs.example.com/udp/4002/quic-v1", result[1].String())
}

func TestAnnouncementAddresses_DNSHostAddrReplaced(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/dns4/old.example.com/tcp/4002/ws"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs)
	require.NoError(t, err)

	require.Len(t, result, 1)
	assert.Equal(t, "/dns/web.ipfs.example.com/tcp/4002/wss", result[0].String())
}

func TestAnnouncementAddresses_MultiplePorts(t *testing.T) {
	hostAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/4002/ws"),
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/443/wss"),
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4002/quic-v1"),
		multiaddr.StringCast("/ip4/1.2.3.4/udp/4002/quic-v1/webtransport"),
	}

	result, err := AnnouncementAddresses(true, "ipfs.example.com", hostAddrs)
	require.NoError(t, err)

	expected := []string{
		"/dns/web.ipfs.example.com/tcp/4002/wss",
		"/dns/web.ipfs.example.com/tcp/443/wss",
		"/dns/web.ipfs.example.com/udp/4002/quic-v1",
		"/dns/web.ipfs.example.com/udp/4002/quic-v1/webtransport",
	}
	require.Len(t, result, len(expected))
	for i, addr := range result {
		assert.Equal(t, expected[i], addr.String())
	}
}
