package config

import (
	"errors"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.lumeweb.com/portal/config"
	"time"
)

var _ config.Validator = (*Config)(nil)
var _ config.Defaults = (*Config)(nil)
var _ config.Defaults = (*BlockStore)(nil)
var _ config.Defaults = (*IPFSProvider)(nil)

func mustParsePeer(s string) IPFSPeer {
	info, err := peer.AddrInfoFromString(s)
	if err != nil {
		panic(err)
	}
	return NewIPFSPeer(*info)
}

var bootstrapPeers = []IPFSPeer{
	mustParsePeer("/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"),
	mustParsePeer("/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa"),
	mustParsePeer("/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb"),
	mustParsePeer("/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt"),
	mustParsePeer("/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"),
	mustParsePeer("/ip4/104.131.131.82/udp/4001/quic/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"),
}

type Config struct {
	ListenAddresses []string     `mapstructure:"listen_addresses"`
	Peers           []IPFSPeer   `mapstructure:"peers"`
	BootstrapPeers  []IPFSPeer   `mapstructure:"bootstrap_peers"`
	Provider        IPFSProvider `mapstructure:"provider"`
	BlockStore      BlockStore   `mapstructure:"blockstore"`
	LogLevel        string       `mapstructure:"log_level"`
}

func (c Config) Defaults() map[string]any {
	return map[string]any{
		"listen_addresses": []string{"/ip4/0.0.0.0/tcp/4001"},
		"bootstrap_peers":  bootstrapPeers,
	}
}

func (c Config) Validate() error {
	if len(c.ListenAddresses) == 0 {
		return errors.New("listen_addresses is required")
	}

	if len(c.BootstrapPeers) == 0 {
		return errors.New("bootstrap_peers is required")
	}

	if c.LogLevel != "" {
		switch c.LogLevel {
		case "debug", "info", "warn", "error":
		default:
			return errors.New("log_level must be one of debug, info, warn, error")
		}
	}

	return nil
}

type (
	// BlockStore configures the blockstore.
	BlockStore struct {
		// MaxConcurrent is the maximum number of concurrent block fetches.
		MaxConcurrentFetches  int `mapstructure:"max_concurrent_fetches"`
		MaxConcurrentRequests int `mapstructure:"max_concurrent_requests"`
		// CacheSize is the maximum number of blocks to cache in memory.
		CacheSize int           `mapstructure:"cache_size"`
		Timeout   time.Duration `mapstructure:"timeout"`
	}

	// IPFSProvider contains the configuration for the IPFS provider
	IPFSProvider struct {
		BatchSize int           `mapstructure:"batch_size"`
		Interval  time.Duration `mapstructure:"interval"`
		Timeout   time.Duration `mapstructure:"timeout"`
	}
)

func (b BlockStore) Defaults() map[string]any {
	return map[string]any{
		"max_concurrent_fetches":  10,
		"max_concurrent_requests": 50,
		"cache_size":              65536,
		"timeout":                 120 * time.Second,
	}
}

func (I IPFSProvider) Defaults() map[string]any {
	return map[string]any{
		"batch_size": 5000,
		"interval":   18 * time.Hour,
		"timeout":    30 * time.Minute,
	}
}
