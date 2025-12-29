package pin

import (
	"github.com/prometheus/client_golang/prometheus"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
)

const (
	MetricAddPin            = "add_pin_total"
	MetricGetPinByRequestID = "get_pin_by_request_id_total"
	MetricListPins          = "list_pins_total"
	MetricReplacePin        = "replace_pin_total"
	MetricDeletePin         = "delete_pin_total"
	MetricUpdatePinStatus   = "update_pin_status_total"
	MetricValidateDAG       = "validate_dag_completion_total"
	MetricGetPinByCID       = "get_pin_by_cid_total"
	MetricDuration          = "duration_seconds"
)

const (
	LabelStatusError   = "error"
	LabelStatusSuccess = "success"
)

var (
	AddPinTotal            prometheus.CounterVec
	GetPinByRequestIDTotal prometheus.CounterVec
	ListPinsTotal          prometheus.CounterVec
	ReplacePinTotal        prometheus.CounterVec
	DeletePinTotal         prometheus.CounterVec
	UpdatePinStatusTotal   prometheus.CounterVec
	ValidateDAGTotal       prometheus.CounterVec
	GetPinByCIDTotal       prometheus.CounterVec

	AddPinDuration            prometheus.HistogramVec
	GetPinByRequestIDDuration prometheus.HistogramVec
	ListPinsDuration          prometheus.HistogramVec
	ReplacePinDuration        prometheus.HistogramVec
	DeletePinDuration         prometheus.HistogramVec
	UpdatePinStatusDuration   prometheus.HistogramVec
	ValidateDAGDuration       prometheus.HistogramVec
	GetPinByCIDDuration       prometheus.HistogramVec
)

func init() {
	// Counters
	AddPinTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricAddPin,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Total number of AddPin operations",
		},
		[]string{"status"},
	)

	GetPinByRequestIDTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricGetPinByRequestID,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Total number of GetPinByRequestID operations",
		},
		[]string{"status"},
	)

	ListPinsTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricListPins,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Total number of ListPins operations",
		},
		[]string{"status"},
	)

	ReplacePinTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricReplacePin,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Total number of ReplacePin operations",
		},
		[]string{"status"},
	)

	DeletePinTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricDeletePin,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Total number of DeletePin operations",
		},
		[]string{"status"},
	)

	UpdatePinStatusTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricUpdatePinStatus,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Total number of UpdatePinStatus operations",
		},
		[]string{"status"},
	)

	ValidateDAGTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricValidateDAG,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Total number of ValidateDAGCompletion operations",
		},
		[]string{"status"},
	)

	GetPinByCIDTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricGetPinByCID,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Total number of GetPinByCID operations",
		},
		[]string{"status"},
	)

	// Histograms
	AddPinDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Duration of AddPin operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	GetPinByRequestIDDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Duration of GetPinByRequestID operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	ListPinsDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Duration of ListPins operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	ReplacePinDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Duration of ReplacePin operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	DeletePinDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Duration of DeletePin operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	UpdatePinStatusDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Duration of UpdatePinStatus operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	ValidateDAGDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Duration of ValidateDAGCompletion operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	GetPinByCIDDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDuration,
			Subsystem: pluginCore.PIN_SERVICE,
			Help:      "Duration of GetPinByCID operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)
}

func GetCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		AddPinTotal,
		GetPinByRequestIDTotal,
		ListPinsTotal,
		ReplacePinTotal,
		DeletePinTotal,
		UpdatePinStatusTotal,
		ValidateDAGTotal,
		GetPinByCIDTotal,

		AddPinDuration,
		GetPinByRequestIDDuration,
		ListPinsDuration,
		ReplacePinDuration,
		DeletePinDuration,
		UpdatePinStatusDuration,
		ValidateDAGDuration,
		GetPinByCIDDuration,
	}
}
