package website

import (
	"github.com/prometheus/client_golang/prometheus"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
)

const (
	MetricCreateWebsite      = "create_website_total"
	MetricGetWebsite         = "get_website_total"
	MetricGetWebsiteByDomain = "get_website_by_domain_total"
	MetricListWebsites       = "list_websites_total"
	MetricUpdateWebsite      = "update_website_total"
	MetricDeleteWebsite      = "delete_website_total"
	MetricValidateDNS               = "validate_dns_total"
	MetricCheckStatus               = "check_status_total"
	MetricCreateWebsiteDuration     = "create_website_duration_seconds"
	MetricGetWebsiteDuration        = "get_website_duration_seconds"
	MetricGetWebsiteByDomainDuration = "get_website_by_domain_duration_seconds"
	MetricListWebsitesDuration       = "list_websites_duration_seconds"
	MetricUpdateWebsiteDuration     = "update_website_duration_seconds"
	MetricDeleteWebsiteDuration     = "delete_website_duration_seconds"
	MetricValidateDNSDuration       = "validate_dns_duration_seconds"
	MetricCheckStatusDuration       = "check_status_duration_seconds"
)

const (
	LabelStatusError   = "error"
	LabelStatusSuccess = "success"
)

var (
	CreateWebsiteTotal      prometheus.CounterVec
	GetWebsiteTotal         prometheus.CounterVec
	GetWebsiteByDomainTotal prometheus.CounterVec
	ListWebsitesTotal       prometheus.CounterVec
	UpdateWebsiteTotal      prometheus.CounterVec
	DeleteWebsiteTotal      prometheus.CounterVec
	ValidateDNSTotal        prometheus.CounterVec
	CheckStatusTotal        prometheus.CounterVec

	CreateWebsiteDuration      prometheus.HistogramVec
	GetWebsiteDuration         prometheus.HistogramVec
	GetWebsiteByDomainDuration prometheus.HistogramVec
	ListWebsitesDuration       prometheus.HistogramVec
	UpdateWebsiteDuration      prometheus.HistogramVec
	DeleteWebsiteDuration      prometheus.HistogramVec
	ValidateDNSDuration        prometheus.HistogramVec
	CheckStatusDuration        prometheus.HistogramVec
)

func init() {
	// Counters
	CreateWebsiteTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricCreateWebsite,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Total number of CreateWebsite operations",
		},
		[]string{"status"},
	)

	GetWebsiteTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricGetWebsite,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Total number of GetWebsite operations",
		},
		[]string{"status"},
	)

	GetWebsiteByDomainTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricGetWebsiteByDomain,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Total number of GetWebsiteByDomain operations",
		},
		[]string{"status"},
	)

	ListWebsitesTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricListWebsites,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Total number of ListWebsites operations",
		},
		[]string{"status"},
	)

	UpdateWebsiteTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricUpdateWebsite,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Total number of UpdateWebsite operations",
		},
		[]string{"status"},
	)

	DeleteWebsiteTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricDeleteWebsite,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Total number of DeleteWebsite operations",
		},
		[]string{"status"},
	)

	ValidateDNSTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricValidateDNS,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Total number of ValidateDNS operations",
		},
		[]string{"status"},
	)

	CheckStatusTotal = *prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricCheckStatus,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Total number of CheckStatus operations",
		},
		[]string{"status"},
	)

	// Histograms
	CreateWebsiteDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricCreateWebsiteDuration,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Duration of CreateWebsite operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	GetWebsiteDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricGetWebsiteDuration,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Duration of GetWebsite operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	GetWebsiteByDomainDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricGetWebsiteByDomainDuration,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Duration of GetWebsiteByDomain operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	ListWebsitesDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricListWebsitesDuration,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Duration of ListWebsites operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	UpdateWebsiteDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricUpdateWebsiteDuration,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Duration of UpdateWebsite operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	DeleteWebsiteDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDeleteWebsiteDuration,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Duration of DeleteWebsite operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	ValidateDNSDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricValidateDNSDuration,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Duration of ValidateDNS operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	CheckStatusDuration = *prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricCheckStatusDuration,
			Subsystem: pluginCore.WEBSITE_SERVICE,
			Help:      "Duration of CheckStatus operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)
}

func GetCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		CreateWebsiteTotal,
		GetWebsiteTotal,
		GetWebsiteByDomainTotal,
		ListWebsitesTotal,
		UpdateWebsiteTotal,
		DeleteWebsiteTotal,
		ValidateDNSTotal,
		CheckStatusTotal,

		CreateWebsiteDuration,
		GetWebsiteDuration,
		GetWebsiteByDomainDuration,
		ListWebsitesDuration,
		UpdateWebsiteDuration,
		DeleteWebsiteDuration,
		ValidateDNSDuration,
		CheckStatusDuration,
	}
}
