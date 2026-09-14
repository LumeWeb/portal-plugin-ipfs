package coolify

import "testing"

func TestNormalizeResourceStatus(t *testing.T) {
	tests := []struct {
		raw  string
		want ResourceStatus
	}{
		// Plain (legacy) statuses pass through untouched.
		{"running", ResourceStatusRunning},
		{"ready", ResourceStatusReady},
		{"failed", ResourceStatusFailed},
		{"exited", ResourceStatusExited},
		{"", ""},
		// Colon-composite statuses from ContainerStatusAggregator.
		{"running:healthy", ResourceStatusRunning},
		{"running:unknown", ResourceStatusRunning},
		{"running:excluded", ResourceStatusRunning},
		{"starting:unknown", ResourceStatusStarting},
		{"restarting:unknown", "restarting"},
		{"degraded:unhealthy", ResourceStatusUnhealthy},
		// An explicit unhealthy health suffix wins over the base state.
		{"running:unhealthy", ResourceStatusUnhealthy},
		// Degenerate suffixes fall back to the base state.
		{"running:", ResourceStatusRunning},
		{"exited:0", ResourceStatusExited},
	}
	for _, tt := range tests {
		if got := normalizeResourceStatus(tt.raw); got != tt.want {
			t.Errorf("normalizeResourceStatus(%q) = %q, want %q", tt.raw, got, tt.want)
		}
	}
}
