package filemanager

import (
	"github.com/prometheus/client_golang/prometheus"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
)

const (
	MetricListFiles                = "list_files_total"
	MetricListDirectoryContents    = "list_directory_contents_total"
	MetricGetBreadcrumbs           = "get_breadcrumbs_total"
	MetricCreateFilePath           = "create_file_path_total"
	MetricValidatePathCompleteness = "validate_path_completeness_total"
	MetricGetIncompletePins        = "get_incomplete_pins_total"
	MetricGetOrphanedPaths         = "get_orphaned_paths_total"
	MetricUpdateFilePath           = "update_file_path_total"
	MetricDeleteFilePath           = "delete_file_path_total"
	MetricDeleteFilePathSmart      = "delete_file_path_smart_total"
	MetricDeleteFilePathsByUserID  = "delete_file_paths_by_user_id_total"
	MetricHealthCheck              = "health_check_total"
	MetricListFilesDuration                = "list_files_duration_seconds"
	MetricListDirectoryContentsDuration    = "list_directory_contents_duration_seconds"
	MetricGetBreadcrumbsDuration           = "get_breadcrumbs_duration_seconds"
	MetricCreateFilePathDuration           = "create_file_path_duration_seconds"
	MetricValidatePathCompletenessDuration = "validate_path_completeness_duration_seconds"
	MetricGetIncompletePinsDuration        = "get_incomplete_pins_duration_seconds"
	MetricGetOrphanedPathsDuration         = "get_orphaned_paths_duration_seconds"
	MetricUpdateFilePathDuration           = "update_file_path_duration_seconds"
	MetricDeleteFilePathDuration           = "delete_file_path_duration_seconds"
	MetricDeleteFilePathSmartDuration      = "delete_file_path_smart_duration_seconds"
	MetricDeleteFilePathsByUserIDDuration  = "delete_file_paths_by_user_id_duration_seconds"
	MetricHealthCheckDuration              = "health_check_duration_seconds"
)

const (
	LabelStatusError   = "error"
	LabelStatusSuccess = "success"
)

var (
	ListFilesTotal                *prometheus.CounterVec
	ListDirectoryContentsTotal    *prometheus.CounterVec
	GetBreadcrumbsTotal           *prometheus.CounterVec
	CreateFilePathTotal           *prometheus.CounterVec
	ValidatePathCompletenessTotal *prometheus.CounterVec
	GetIncompletePinsTotal        *prometheus.CounterVec
	GetOrphanedPathsTotal         *prometheus.CounterVec
	UpdateFilePathTotal           *prometheus.CounterVec
	DeleteFilePathTotal           *prometheus.CounterVec
	DeleteFilePathSmartTotal      *prometheus.CounterVec
	DeleteFilePathsByUserIDTotal  *prometheus.CounterVec
	HealthCheckTotal              *prometheus.CounterVec

	ListFilesDuration                *prometheus.HistogramVec
	ListDirectoryContentsDuration    *prometheus.HistogramVec
	GetBreadcrumbsDuration           *prometheus.HistogramVec
	CreateFilePathDuration           *prometheus.HistogramVec
	ValidatePathCompletenessDuration *prometheus.HistogramVec
	GetIncompletePinsDuration        *prometheus.HistogramVec
	GetOrphanedPathsDuration         *prometheus.HistogramVec
	UpdateFilePathDuration           *prometheus.HistogramVec
	DeleteFilePathDuration           *prometheus.HistogramVec
	DeleteFilePathSmartDuration      *prometheus.HistogramVec
	DeleteFilePathsByUserIDDuration  *prometheus.HistogramVec
	HealthCheckDuration              *prometheus.HistogramVec
)

func init() {
	// Counters
	ListFilesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricListFiles,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Total number of ListFiles operations",
		},
		[]string{"status"},
	)

	ListDirectoryContentsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricListDirectoryContents,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Total number of ListDirectoryContents operations",
		},
		[]string{"status"},
	)

	GetBreadcrumbsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricGetBreadcrumbs,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Total number of GetBreadcrumbs operations",
		},
		[]string{"status"},
	)

	CreateFilePathTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricCreateFilePath,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Total number of CreateFilePath operations",
		},
		[]string{"status"},
	)

	ValidatePathCompletenessTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricValidatePathCompleteness,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Total number of ValidatePathCompleteness operations",
		},
		[]string{"status"},
	)

	GetIncompletePinsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricGetIncompletePins,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Total number of GetIncompletePins operations",
		},
		[]string{"status"},
	)

	GetOrphanedPathsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricGetOrphanedPaths,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Total number of GetOrphanedPaths operations",
		},
		[]string{"status"},
	)

	UpdateFilePathTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricUpdateFilePath,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Total number of UpdateFilePath operations",
		},
		[]string{"status"},
	)

	DeleteFilePathTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricDeleteFilePath,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Total number of DeleteFilePath operations",
		},
		[]string{"status"},
	)

	DeleteFilePathSmartTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricDeleteFilePathSmart,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Total number of DeleteFilePathSmart operations",
		},
		[]string{"status"},
	)

	DeleteFilePathsByUserIDTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricDeleteFilePathsByUserID,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Total number of DeleteFilePathsByUserID operations",
		},
		[]string{"status"},
	)

	HealthCheckTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricHealthCheck,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Total number of HealthCheck operations",
		},
		[]string{"status"},
	)

	// Histograms
	ListFilesDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricListFilesDuration,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Duration of ListFiles operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	ListDirectoryContentsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricListDirectoryContentsDuration,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Duration of ListDirectoryContents operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	GetBreadcrumbsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricGetBreadcrumbsDuration,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Duration of GetBreadcrumbs operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	CreateFilePathDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricCreateFilePathDuration,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Duration of CreateFilePath operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	ValidatePathCompletenessDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricValidatePathCompletenessDuration,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Duration of ValidatePathCompleteness operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	GetIncompletePinsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricGetIncompletePinsDuration,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Duration of GetIncompletePins operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	GetOrphanedPathsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricGetOrphanedPathsDuration,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Duration of GetOrphanedPaths operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	UpdateFilePathDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricUpdateFilePathDuration,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Duration of UpdateFilePath operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	DeleteFilePathDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDeleteFilePathDuration,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Duration of DeleteFilePath operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	DeleteFilePathSmartDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDeleteFilePathSmartDuration,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Duration of DeleteFilePathSmart operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	DeleteFilePathsByUserIDDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricDeleteFilePathsByUserIDDuration,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Duration of DeleteFilePathsByUserID operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)

	HealthCheckDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricHealthCheckDuration,
			Subsystem: pluginCore.FILE_MANAGER_SERVICE,
			Help:      "Duration of HealthCheck operations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{},
	)
}

func GetCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		ListFilesTotal,
		ListDirectoryContentsTotal,
		GetBreadcrumbsTotal,
		CreateFilePathTotal,
		ValidatePathCompletenessTotal,
		GetIncompletePinsTotal,
		GetOrphanedPathsTotal,
		UpdateFilePathTotal,
		DeleteFilePathTotal,
		DeleteFilePathSmartTotal,
		DeleteFilePathsByUserIDTotal,
		HealthCheckTotal,

		ListFilesDuration,
		ListDirectoryContentsDuration,
		GetBreadcrumbsDuration,
		CreateFilePathDuration,
		ValidatePathCompletenessDuration,
		GetIncompletePinsDuration,
		GetOrphanedPathsDuration,
		UpdateFilePathDuration,
		DeleteFilePathDuration,
		DeleteFilePathSmartDuration,
		DeleteFilePathsByUserIDDuration,
		HealthCheckDuration,
	}
}
