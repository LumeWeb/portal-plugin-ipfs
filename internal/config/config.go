package config

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"go.lumeweb.com/portal/config"
)

// var _ config.Validator = (*ProtocolConfig)(nil)
var _ config.Defaults = (*BlockStore)(nil)
var _ config.Defaults = (*IPFSProvider)(nil)

func mustParsePeer(s string) IPFSPeer {
	info, err := peer.AddrInfoFromString(s)
	if err != nil {
		panic(err)
	}
	return NewIPFSPeer(*info)
}

var BootstrapPeers = []IPFSPeer{
	mustParsePeer("/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"),
	mustParsePeer("/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa"),
	mustParsePeer("/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb"),
	mustParsePeer("/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt"),
	mustParsePeer("/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"),
	mustParsePeer("/ip4/104.131.131.82/udp/4001/quic/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"),
}

type (
	// BlockStore configures the blockstore.
	BlockStore struct {
		// MaxConcurrent is the maximum number of concurrent block fetches.
		MaxConcurrentFetches  int `config:"max_concurrent_fetches"`
		MaxConcurrentRequests int `config:"max_concurrent_requests"`
		// ProcessingWorkers is the number of concurrent workers for block processing during uploads.
		// 0 = auto (uses MaxConcurrentRequests). Block processing is I/O-bound (S3 upload + DB write),
		// so the limit is downstream I/O parallelism, not CPU count.
		ProcessingWorkers int `config:"processing_workers"`
		// CacheSize is the maximum number of blocks to cache in memory.
		CacheSize int           `config:"cache_size"`
		Timeout   time.Duration `config:"timeout"`
	}

	// IPFSProvider contains the configuration for the IPFS provider
	IPFSProvider struct {
		BatchSize int           `config:"batch_size"`
		Interval  time.Duration `config:"interval"`
		Timeout   time.Duration `config:"timeout"`
	}
)

func (b BlockStore) Defaults() map[string]any {
	return map[string]any{
		"MaxConcurrentFetches":  10,
		"MaxConcurrentRequests": 50,
		"ProcessingWorkers":     0,
		"CacheSize":             65536,
		"Timeout":               120 * time.Second,
	}
}

func (I IPFSProvider) Defaults() map[string]any {
	return map[string]any{
		"BatchSize": 5000,
		"Interval":  18 * time.Hour,
		"Timeout":   30 * time.Minute,
	}
}
