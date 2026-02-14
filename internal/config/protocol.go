package config

import (
	z "github.com/Oudwins/zog"
	"go.lumeweb.com/portal/config"
)

var _ config.Defaults = (*ProtocolConfig)(nil)

type ProtocolConfig struct {
	ListenAddresses         []string     `config:"listen_addresses"`
	Peers                   []IPFSPeer   `config:"peers"`
	BootstrapPeers          []IPFSPeer   `config:"bootstrap_peers"`
	Provider                IPFSProvider `config:"provider"`
	BlockStore              BlockStore   `config:"blockstore"`
	Website                 WebsiteConfig `config:"website"`
	LogLevel                string       `config:"log_level"`
	AutoScaleResourceLimits bool         `config:"auto_scale_resource_limits"`
}

func (c ProtocolConfig) Defaults() map[string]any {
	return map[string]any{
		"ListenAddresses": []string{"/ip4/0.0.0.0/tcp/4001"},
		"BootstrapPeers":  BootstrapPeers,
	}
}
func (l ProtocolConfig) Schema() z.ZogSchema {
	return z.Struct(z.Shape{
		"LogLevel": z.String().
			Default("info").
			OneOf([]string{"debug", "info", "warn", "error", "fatal"}, z.Message("log level must be one of: debug, info, warn, error, fatal")),
	})
}
