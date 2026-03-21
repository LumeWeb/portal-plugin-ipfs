package config

import (
	z "github.com/Oudwins/zog"
	"go.lumeweb.com/portal/config"
)

var _ config.Defaults = (*ProtocolConfig)(nil)

type ProtocolConfig struct {
	ListenAddresses         []string        `config:"listen_addresses"`
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
		"ListenAddresses": []string{"/ip4/0.0.0.0/tcp/4001"},
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
			Default("fullrt").
			OneOf([]string{"basic", "fullrt"}, z.Message("dht mode must be one of: basic, fullrt")),
	})
}
