package config

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"go.lumeweb.com/portal/config"
)

// var _ config.Validator = (*ProtocolConfig)(nil)
var _ config.Defaults = (*BlockStore)(nil)
var _ config.Defaults = (*IPFSProvider)(nil)
var _ config.Defaults = (*IPNS)(nil)

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
		BatchSize      int           `config:"batch_size"`
		Interval       time.Duration `config:"interval"`
		PerCIDTimeout  time.Duration `config:"per_cid_timeout"`
		ProvideWorkers int           `config:"provide_workers"`
	}

	// IPNS configures IPNS record publishing and republishing
	IPNS struct {
		// RepublishInterval is how frequently IPNS records are republished to the DHT.
		// Default is 1h (boxo default is 4h).
		RepublishInterval time.Duration `config:"republish_interval"`
		// RecordLifetime is how long republished IPNS records remain valid.
		// Default is 48h which is the DHT hard cap — records are dropped after 48h regardless.
		RecordLifetime time.Duration `config:"record_lifetime"`
		// PubSubRebroadcastInterval is how frequently IPNS records are rebroadcast over PubSub.
		// Default is 10m.
		PubSubRebroadcastInterval time.Duration `config:"pubsub_rebroadcast_interval"`
		// PubSubRebroadcastInitialDelay is the delay before the first PubSub rebroadcast.
		// Default is 1m.
		PubSubRebroadcastInitialDelay time.Duration `config:"pubsub_rebroadcast_initial_delay"`
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
		"BatchSize":      500,
		"Interval":       4 * time.Hour,
		"PerCIDTimeout":  15 * time.Second,
		"ProvideWorkers": 32,
	}
}

func (i IPNS) Defaults() map[string]any {
	return map[string]any{
		"RepublishInterval":             time.Hour,
		"RecordLifetime":                48 * time.Hour,
		"PubSubRebroadcastInterval":     10 * time.Minute,
		"PubSubRebroadcastInitialDelay": 1 * time.Minute,
	}
}
