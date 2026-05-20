package config

import (
	"fmt"

	z "github.com/Oudwins/zog"
	"go.lumeweb.com/portal/config"
)

const (
	DefaultPort = 4002

	DHTModeBasic  = "basic"
	DHTModeFullRT = "fullrt"
)

var _ config.Defaults = (*ProtocolConfig)(nil)

type ProtocolConfig struct {
	Port                    int          `config:"port"`
	AnnounceWeb             bool         `config:"announce_web"`
	ListenAddresses         []string     `config:"listen_addresses"`
	Peers                   []IPFSPeer   `config:"peers"`
	BootstrapPeers          []IPFSPeer   `config:"bootstrap_peers"`
	Provider                IPFSProvider `config:"provider"`
	BlockStore              BlockStore   `config:"blockstore"`
	LogLevel                string       `config:"log_level"`
	AutoScaleResourceLimits bool         `config:"auto_scale_resource_limits"`
	DHTMode                 string       `config:"dht_mode"`
}

func (c ProtocolConfig) Defaults() map[string]any {
	return map[string]any{
		"Port":           DefaultPort,
		"BootstrapPeers": BootstrapPeers,
		"DHTMode":        "fullrt",
	}
}

func (l ProtocolConfig) Schema() z.ZogSchema {
	return z.Struct(z.Shape{
		"Port": z.Int().Default(DefaultPort).GT(0, z.Message("port must be positive")),
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
	if port == 0 {
		port = DefaultPort
	}

	base := []string{
		fmt.Sprintf("/ip4/0.0.0.0/tcp/%d/ws", port),
		fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", port),
		fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1/webtransport", port),
		fmt.Sprintf("/ip6/::/tcp/%d/ws", port),
		fmt.Sprintf("/ip6/::/udp/%d/quic-v1", port),
		fmt.Sprintf("/ip6/::/udp/%d/quic-v1/webtransport", port),
	}

	return append(base, c.ListenAddresses...)
}
