// Prometheus metrics for the workspace reconciler. These observe reconcile
// outcomes, reconcile duration, and the distribution of workspace lifecycle
// states. Metric labels carry only non-secret values (outcome/state), never
// passwords, tokens, or error detail.
package workspace

import (
	"github.com/prometheus/client_golang/prometheus"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
)

const (
	// MetricReconcileTotal counts reconcile attempts bucketed by outcome.
	MetricReconcileTotal = "reconcile_total"
	// MetricReconcileDurationSeconds histogram observes reconcile duration by outcome.
	MetricReconcileDurationSeconds = "reconcile_duration_seconds"
	// MetricWorkspaceState gauges the number of workspaces in each lifecycle state.
	MetricWorkspaceState = "workspace_state"
)

// Outcome label values for reconcile metrics. They are intentionally stable
// and coarse so they never encode secret or error detail.
const (
	LabelOutcomeSuccess   = "success"
	LabelOutcomeRetryable = "retryable"
	LabelOutcomePermanent = "permanent"
	LabelOutcomeDrift     = "drift"
)

var (
	// ReconcileTotal counts each reconcile pass by outcome.
	ReconcileTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:      MetricReconcileTotal,
			Subsystem: pluginCore.WORKSPACE_SERVICE,
			Help:      "Total number of workspace reconcile attempts, by outcome.",
		},
		[]string{"outcome"},
	)

	// ReconcileDurationSeconds observes the duration of each reconcile pass by
	// outcome.
	ReconcileDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      MetricReconcileDurationSeconds,
			Subsystem: pluginCore.WORKSPACE_SERVICE,
			Help:      "Duration of workspace reconcile attempts in seconds, by outcome.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"outcome"},
	)

	// WorkspaceState is the current number of workspaces in each lifecycle
	// state, refreshed on each reconcile pass.
	WorkspaceState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:      MetricWorkspaceState,
			Subsystem: pluginCore.WORKSPACE_SERVICE,
			Help:      "Current number of workspaces by lifecycle state.",
		},
		[]string{"state"},
	)
)

// GetCollectors returns this package's Prometheus collectors so the plugin can
// register them.
func GetCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		ReconcileTotal,
		ReconcileDurationSeconds,
		WorkspaceState,
	}
}
