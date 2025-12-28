package block

import (
	"github.com/prometheus/client_golang/prometheus"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
)

const (
	MetricGetBlockMeta      = "get_block_meta_total"
	MetricGetBlockMetaBatch = "get_block_meta_batch_total"
	MetricDuration          = "duration_seconds"
)

const (
	LabelStatusError   = "error"
	LabelStatusSuccess = "success"
)

var (
	GetBlockMetaTotal         prometheus.CounterVec
	GetBlockMetaBatchTotal    prometheus.CounterVec
	GetBlockMetaDuration      prometheus.HistogramVec
	GetBlockMetaBatchDuration prometheus.HistogramVec
)

func init() {
	GetBlockMetaTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricGetBlockMeta,
			Subsystem: pluginCore.BLOCK_SERVICE,
			Help:      "Total number of GetBlockMeta operations",
		},
		[]string{"status"},
	)

	GetBlockMetaBatchTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricGetBlockMetaBatch,
			Subsystem: pluginCore.BLOCK_SERVICE,
			Help:      "Total number of GetBlockMetaBatch operations",
		},
		[]string{"status"},
	)

	GetBlockMetaDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.BLOCK_SERVICE,
			Help:      "Duration of GetBlockMeta operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	GetBlockMetaBatchDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.BLOCK_SERVICE,
			Help:      "Duration of GetBlockMetaBatch operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)
}

func GetCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		GetBlockMetaTotal,
		GetBlockMetaBatchTotal,
		GetBlockMetaDuration,
		GetBlockMetaBatchDuration,
	}
}
