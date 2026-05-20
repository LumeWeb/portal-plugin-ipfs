package config

import (
	z "github.com/Oudwins/zog"
	"go.lumeweb.com/portal/config"
)

const (
	// DHTModeBasic represents a standard libp2p Kademlia DHT
	DHTModeBasic = "basic"
	// DHTModeFullRT represents the FullRT DHT with full network visibility
	DHTModeFullRT = "fullrt"
)

var _ config.Defaults = (*ProtocolConfig)(nil)

type ProtocolConfig struct {
	ListenAddresses         []string        `config:"listen_addresses"`
	AnnounceAddresses       []string        `config:"announce_addresses"`
	Peers                   []IPFSPeer      `config:"peers"`
	BootstrapPeers          []IPFSPeer      `config:"bootstrap_peers"`
	Provider                IPFSProvider    `config:"provider"`
	BlockStore              BlockStore      `config:"blockstore"`
	LogLevel                string          `config:"log_level"`
	AutoScaleResourceLimits bool            `config:"auto_scale_resource_limits"`
	DHTMode                 string          `config:"dht_mode"`
}

func (c ProtocolConfig) Defaults() map[string]any {
	return map[string]any{
		"ListenAddresses": []string{
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/tcp/4002/ws",
			"/ip4/0.0.0.0/udp/443/quic-v1",
			"/ip4/0.0.0.0/udp/443/quic-v1/webtransport",
			"/ip6/::/tcp/4001",
			"/ip6/::/tcp/4002/ws",
			"/ip6/::/udp/443/quic-v1",
			"/ip6/::/udp/443/quic-v1/webtransport",
		},
		"BootstrapPeers":  BootstrapPeers,
		"DHTMode":         "fullrt",
	}
}
func (l ProtocolConfig) Schema() z.ZogSchema {
	return z.Struct(z.Shape{
		"LogLevel": z.String().
			Default("info").
			OneOf([]string{"debug", "info", "warn", "error", "fatal"}, z.Message("log level must be one of: debug, info, warn, error, fatal")),
		"DHTMode": z.String().
			Default(DHTModeFullRT).
			OneOf([]string{DHTModeBasic, DHTModeFullRT}, z.Message("dht mode must be one of: basic, fullrt")),
	})
}
