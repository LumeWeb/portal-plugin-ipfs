package config

import (
	"time"

	"go.lumeweb.com/portal/config"
)

var _ config.APIConfig = (*APIConfig)(nil)

type APIConfig struct {
	// GatewaySecret is the shared secret for authenticating gateway requests
	GatewaySecret string `config:"gateway_secret"`

	// EventLogRetention is how long website lifecycle events are retained in the
	// durable event log. The gateway must reconcile and persist its cursor
	// within this window or it can no longer replay events lost to an SSE gap.
	EventLogRetention time.Duration `config:"event_log_retention"`

	// ChangesMaxEvents caps the number of events returned by a single
	// /internal/websites/changes reconciliation request. The gateway pages with
	// the returned high-water mark when more events remain.
	ChangesMaxEvents int `config:"changes_max_events"`
}

func (A APIConfig) Defaults() map[string]any {
	return map[string]any{
		"GatewaySecret":     "",
		"EventLogRetention": 24 * time.Hour,
		"ChangesMaxEvents":  1000,
	}
}
