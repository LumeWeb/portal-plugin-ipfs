package ipfs

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// Reprovider metrics
	MetricReprovideAttempts      = "reprovide_attempts_total"
	MetricReprovideSuccesses     = "reprovide_successes_total"
	MetricReprovideFailures      = "reprovide_failures_total"
	MetricReprovideCIDsTotal     = "reprovide_cids_total"
	MetricReprovideCIDFailures   = "reprovide_cid_failures_total"
	MetricReprovideDuration      = "reprovide_duration_seconds"
	MetricReprovideCIDDuration   = "reprovide_cid_duration_seconds"
	MetricReprovideBatchSize     = "reprovide_batch_size"
	MetricReprovideProviderReady = "reprovide_provider_ready"
	MetricReprovideCIDLeaks      = "reprovide_cid_leaks_total"
	MetricReprovideThrottled     = "reprovide_provide_throttled_total"

	// Global pinned-state gauges
	MetricReprovidePinnedTotal    = "reprovide_pinned_total"
	MetricReprovideAnnouncedTotal = "reprovide_announced_total"
	MetricReprovidePendingTotal   = "reprovide_pending_total"

	// DHT metrics
	MetricCompanionDHTHealthy           = "companion_dht_healthy"
	MetricCompanionDHTRoutingTable      = "companion_dht_routing_table_size"
	MetricCompanionDHTBootstrapAttempts = "companion_dht_bootstrap_attempts_total"
	MetricCompanionDHTBootstrapFailures = "companion_dht_bootstrap_failures_total"
	MetricCompanionDHTConnectedPeers    = "companion_dht_connected_peers"
	MetricFullRTReady                   = "fullrt_ready"
	MetricFullRTRoutingTableSize        = "fullrt_routing_table_size"

	// WantBlockFilter metrics
	MetricWantBlockRequests       = "want_block_requests_total"
	MetricWantBlockGatewayPeers   = "want_block_gateway_peers"
	MetricWantBlockPeerLimiters   = "want_block_peer_limiters"
	MetricWantBlockTopDeniedPeers = "want_block_top_denied_peers"
)

const (
	subSystemReprovider = "ipfs.reprovider"
	subSystemDHT        = "ipfs.dht"
	subSystemBitswap    = "ipfs.bitswap"
)

const (
	LabelResultSuccess    = "success"
	LabelResultFailure    = "failure"
	LabelTriggerScheduled = "scheduled"
	LabelTriggerManual    = "manual"

	LabelWantAllowed           = "allowed"
	LabelWantDeniedGlobalRate  = "denied_global_rate"
	LabelWantDeniedPerPeerRate = "denied_per_peer_rate"
	LabelWantAllowedGateway    = "allowed_gateway"
	LabelWantAllowedSelf       = "allowed_self"

	LabelCIDResultSuccess = "success"
	LabelCIDResultTimeout = "timeout"
	LabelCIDResultOther   = "other"
)

var (
	// Reprovider counters
	ReprovideAttemptsTotal  *prometheus.CounterVec
	ReprovideSuccessesTotal prometheus.Counter
	ReprovideFailuresTotal  prometheus.Counter
	ReprovideCIDsTotal      *prometheus.CounterVec
	ReprovideCIDFailures    *prometheus.CounterVec

	// Reprovider histograms
	ReprovideDuration    *prometheus.HistogramVec
	ReprovideCIDDuration *prometheus.HistogramVec
	ReprovideBatchSize *prometheus.HistogramVec

	// Reprovider leak counter
	ReprovideCIDLeaks prometheus.Counter

	// Reprovider throttle counter — incremented when too many leaked goroutines prevent starting a new provide
	ReprovideThrottled prometheus.Counter

	// Reprovider gauges
	ReprovideProviderReady prometheus.Gauge

	// Global pinned-state gauges
	ReprovidePinnedTotal    prometheus.Gauge
	ReprovideAnnouncedTotal prometheus.Gauge
	ReprovidePendingTotal   prometheus.Gauge

	// DHT gauges
	CompanionDHTHealthy                prometheus.Gauge
	CompanionDHTRoutingTableSize       prometheus.Gauge
	CompanionDHTBootstrapAttemptsTotal prometheus.Counter
	CompanionDHTBootstrapFailuresTotal prometheus.Counter
	CompanionDHTConnectedPeers         prometheus.Gauge

	// FullRT gauges
	FullRTReady            prometheus.Gauge
	FullRTRoutingTableSize prometheus.Gauge

	// WantBlockFilter metrics
	WantBlockRequestsTotal *prometheus.CounterVec
	WantBlockGatewayPeers  prometheus.Gauge
	WantBlockPeerLimiters  prometheus.Gauge
)

func init() {
	// Counters
	ReprovideAttemptsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovideAttempts,
			Help:      "Total number of performProvide invocations",
		},
		[]string{"trigger"},
	)

	ReprovideSuccessesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovideSuccesses,
			Help:      "Total number of successful reprovide cycles",
		},
	)

	ReprovideFailuresTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovideFailures,
			Help:      "Total number of failed reprovide cycles",
		},
	)

	ReprovideCIDsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovideCIDsTotal,
			Help:      "Total CIDs processed by the reprovider",
		},
		[]string{"result"},
	)

	ReprovideCIDFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovideCIDFailures,
			Help:      "Total per-CID provide failures with error classification",
		},
		[]string{"error_type"},
	)

	// Histograms
	provideDurationBuckets := []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300}
	batchSizeBuckets := []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000}

	ReprovideDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovideDuration,
			Help:      "Wall-clock duration of performProvide in seconds",
			Buckets:   provideDurationBuckets,
		},
		[]string{},
	)

	ReprovideCIDDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovideCIDDuration,
			Help:      "Per-CID DHT Provide() call duration in seconds, by result",
			Buckets:   provideDurationBuckets,
		},
		[]string{"result"},
	)

	ReprovideBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovideBatchSize,
			Help:      "Number of eligible CIDs in each reprovide batch",
			Buckets:   batchSizeBuckets,
		},
		[]string{},
	)

	ReprovideCIDLeaks = prometheus.NewCounter(
		prometheus.CounterOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovideCIDLeaks,
			Help:      "CIDs where Provide() was abandoned due to context expiry (goroutine may still be running)",
		},
	)

	ReprovideThrottled = prometheus.NewCounter(
		prometheus.CounterOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovideThrottled,
			Help:      "Provides refused because the leaked-goroutine semaphore is full",
		},
	)

	ReprovideProviderReady = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovideProviderReady,
			Help:      "1 when the provider reports Ready, 0 otherwise",
		},
	)

	// Global pinned-state gauges
	ReprovidePinnedTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovidePinnedTotal,
			Help:      "Total ready pinned CIDs in the database",
		},
	)

	ReprovideAnnouncedTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovideAnnouncedTotal,
			Help:      "Pinned CIDs announced within the current reprovide interval",
		},
	)

	ReprovidePendingTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: subSystemReprovider,
			Name:      MetricReprovidePendingTotal,
			Help:      "Pinned CIDs not yet announced in the current interval (pending or failed)",
		},
	)

	// DHT gauges
	CompanionDHTHealthy = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: subSystemDHT,
			Name:      MetricCompanionDHTHealthy,
			Help:      "1 when companion DHT is healthy (bootstrapped), 0 otherwise",
		},
	)

	CompanionDHTRoutingTableSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: subSystemDHT,
			Name:      MetricCompanionDHTRoutingTable,
			Help:      "Number of entries in the companion DHT routing table",
		},
	)

	CompanionDHTBootstrapAttemptsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Subsystem: subSystemDHT,
			Name:      MetricCompanionDHTBootstrapAttempts,
			Help:      "Total companion DHT bootstrap attempts (initial + recovery retries)",
		},
	)

	CompanionDHTBootstrapFailuresTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Subsystem: subSystemDHT,
			Name:      MetricCompanionDHTBootstrapFailures,
			Help:      "Total companion DHT bootstrap failures",
		},
	)

	CompanionDHTConnectedPeers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: subSystemDHT,
			Name:      MetricCompanionDHTConnectedPeers,
			Help:      "Number of peers connected to the companion DHT host",
		},
	)

	// FullRT gauges
	FullRTReady = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: subSystemDHT,
			Name:      MetricFullRTReady,
			Help:      "1 when FullRT reports Ready (initial crawl complete), 0 otherwise",
		},
	)

	FullRTRoutingTableSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: subSystemDHT,
			Name:      MetricFullRTRoutingTableSize,
			Help:      "Number of peers in FullRT's cached routing table",
		},
	)

	// WantBlockFilter metrics
	WantBlockRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subSystemBitswap,
			Name:      MetricWantBlockRequests,
			Help:      "Total want-block requests by outcome (allowed, allowed_gateway, allowed_self, denied_global_rate, denied_per_peer_rate)",
		},
		[]string{"result"},
	)

	WantBlockGatewayPeers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: subSystemBitswap,
			Name:      MetricWantBlockGatewayPeers,
			Help:      "Number of gateway peers whitelisted in the want-block filter",
		},
	)

	WantBlockPeerLimiters = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: subSystemBitswap,
			Name:      MetricWantBlockPeerLimiters,
			Help:      "Number of active per-peer rate limiters in the want-block filter",
		},
	)
}

func GetMetricsCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		ReprovideAttemptsTotal,
		ReprovideSuccessesTotal,
		ReprovideFailuresTotal,
		ReprovideCIDsTotal,
		ReprovideCIDFailures,
		ReprovideDuration,
		ReprovideCIDDuration,
		ReprovideBatchSize,
		ReprovideCIDLeaks,
		ReprovideThrottled,
		ReprovideProviderReady,
		ReprovidePinnedTotal,
		ReprovideAnnouncedTotal,
		ReprovidePendingTotal,
		CompanionDHTHealthy,
		CompanionDHTRoutingTableSize,
		CompanionDHTBootstrapAttemptsTotal,
		CompanionDHTBootstrapFailuresTotal,
		CompanionDHTConnectedPeers,
		FullRTReady,
		FullRTRoutingTableSize,
		WantBlockRequestsTotal,
		WantBlockGatewayPeers,
		WantBlockPeerLimiters,
	}
}
