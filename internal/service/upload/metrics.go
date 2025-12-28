package upload

import (
	"github.com/prometheus/client_golang/prometheus"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
)

const (
	MetricHandleUpload         = "handle_upload_total"
	MetricHandleUploadWithMode = "handle_upload_with_mode_total"
	MetricProcessUpload        = "process_upload_total"
	MetricCreateRootPin        = "create_root_pin_total"
	MetricDuration             = "duration_seconds"
)

const (
	LabelStatusError   = "error"
	LabelStatusSuccess = "success"
	LabelModeConvert   = "convert"
	LabelModePreserve  = "preserve"
)

var (
	HandleUploadTotal         prometheus.CounterVec
	HandleUploadWithModeTotal prometheus.CounterVec
	ProcessUploadTotal        prometheus.CounterVec
	CreateRootPinTotal        prometheus.CounterVec

	HandleUploadDuration         prometheus.HistogramVec
	HandleUploadWithModeDuration prometheus.HistogramVec
	ProcessUploadDuration        prometheus.HistogramVec
	CreateRootPinDuration        prometheus.HistogramVec
)

func init() {
	// Counters
	HandleUploadTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricHandleUpload,
			Subsystem: pluginCore.UPLOAD_SERVICE,
			Help:      "Total number of HandleUpload operations",
		},
		[]string{"status"},
	)

	HandleUploadWithModeTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricHandleUploadWithMode,
			Subsystem: pluginCore.UPLOAD_SERVICE,
			Help:      "Total number of HandleUploadWithMode operations",
		},
		[]string{"status", "mode"},
	)

	ProcessUploadTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricProcessUpload,
			Subsystem: pluginCore.UPLOAD_SERVICE,
			Help:      "Total number of ProcessUpload operations",
		},
		[]string{"status"},
	)

	CreateRootPinTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricCreateRootPin,
			Subsystem: pluginCore.UPLOAD_SERVICE,
			Help:      "Total number of CreateRootPin operations",
		},
		[]string{"status"},
	)

	// Histograms
	HandleUploadDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.UPLOAD_SERVICE,
			Help:      "Duration of HandleUpload operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	HandleUploadWithModeDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.UPLOAD_SERVICE,
			Help:      "Duration of HandleUploadWithMode operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	ProcessUploadDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.UPLOAD_SERVICE,
			Help:      "Duration of ProcessUpload operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	CreateRootPinDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.UPLOAD_SERVICE,
			Help:      "Duration of CreateRootPin operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)
}

func GetCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		HandleUploadTotal,
		HandleUploadWithModeTotal,
		ProcessUploadTotal,
		CreateRootPinTotal,

		HandleUploadDuration,
		HandleUploadWithModeDuration,
		ProcessUploadDuration,
		CreateRootPinDuration,
	}
}
