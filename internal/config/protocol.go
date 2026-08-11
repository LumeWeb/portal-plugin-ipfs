package config

import (
	"fmt"

	z "github.com/Oudwins/zog"
	"go.lumeweb.com/portal/config"
)

const (
	DefaultPort   = 4001
	DefaultWSPort = 4002

	DefaultBitswapGlobalWantRateLimit  = 2500
	DefaultBitswapGlobalWantBurst      = 5000
	DefaultBitswapPerPeerWantRateLimit = 200
	DefaultBitswapPerPeerWantBurst     = 512

	DHTModeBasic  = "basic"
	DHTModeFullRT = "fullrt"
)

var _ config.Defaults = (*ProtocolConfig)(nil)
var _ config.Defaults = (*BitswapConfig)(nil)

type ProtocolConfig struct {
	Port                  int           `config:"port"`
	WSPort                int           `config:"ws_port"`
	AnnounceWeb           bool          `config:"announce_web"`
	ListenAddresses       []string      `config:"listen_addresses"`
	Peers                 []IPFSPeer    `config:"peers"`
	BootstrapPeers        []IPFSPeer    `config:"bootstrap_peers"`
	Provider              IPFSProvider  `config:"provider"`
	BlockStore            BlockStore    `config:"blockstore"`
	IPNS                  IPNS          `config:"ipns"`
	LogLevel              string        `config:"log_level"`
	DisableResourceLimits bool          `config:"disable_resource_limits"`
	DHTMode               string        `config:"dht_mode"`
	TrustedProxies        []string      `config:"trusted_proxies"`
	ProxyProtocol         bool          `config:"proxy_protocol"`
	Gateways              []string      `config:"gateways"`
	Bitswap               BitswapConfig `config:"bitswap"`
}

type BitswapConfig struct {
	MaxQueuedWantlistEntriesPerPeer uint    `config:"max_queued_wantlist_entries_per_peer"`
	GlobalWantRateLimit             float64 `config:"global_want_rate_limit"`
	GlobalWantBurst                 int     `config:"global_want_burst"`
	PerPeerWantRateLimit            float64 `config:"per_peer_want_rate_limit"`
	PerPeerWantBurst                int     `config:"per_peer_want_burst"`
}

func (b BitswapConfig) Defaults() map[string]any {
	return map[string]any{
		"GlobalWantRateLimit":  DefaultBitswapGlobalWantRateLimit,
		"GlobalWantBurst":      DefaultBitswapGlobalWantBurst,
		"PerPeerWantRateLimit": DefaultBitswapPerPeerWantRateLimit,
		"PerPeerWantBurst":     DefaultBitswapPerPeerWantBurst,
	}
}

func (c ProtocolConfig) Defaults() map[string]any {
	return map[string]any{
		"Port":           DefaultPort,
		"WSPort":         DefaultWSPort,
		"BootstrapPeers": BootstrapPeers,
		"DHTMode":        "fullrt",
	}
}

func (l ProtocolConfig) Schema() z.ZogSchema {
	return z.Struct(z.Shape{
		"Port":   z.Int().Default(DefaultPort).GT(0, z.Message("port must be positive")),
		"WSPort": z.Int().Default(DefaultWSPort).GT(0, z.Message("ws port must be positive")),
		"LogLevel": z.String().
			Default("info").
			OneOf([]string{"debug", "info", "warn", "error", "fatal"}, z.Message("log level must be one of: debug, info, warn, error, fatal")),
		"DHTMode": z.String().
			Default(DHTModeFullRT).
			OneOf([]string{DHTModeBasic, DHTModeFullRT}, z.Message("dht mode must be one of: basic, fullrt")),
	})
}

func (c ProtocolConfig) ListenAddrs() []string {
	port := c.Port
	wsPort := c.WSPort

	base := []string{
		fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port),
		fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", port),
		fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1/webtransport", port),
		fmt.Sprintf("/ip4/0.0.0.0/udp/%d/webrtc-direct", port),
		fmt.Sprintf("/ip6/::/tcp/%d", port),
		fmt.Sprintf("/ip6/::/udp/%d/quic-v1", port),
		fmt.Sprintf("/ip6/::/udp/%d/quic-v1/webtransport", port),
		fmt.Sprintf("/ip6/::/udp/%d/webrtc-direct", port),
		fmt.Sprintf("/ip4/0.0.0.0/tcp/%d/ws", wsPort),
		fmt.Sprintf("/ip6/::/tcp/%d/ws", wsPort),
	}

	return append(base, c.ListenAddresses...)
}
