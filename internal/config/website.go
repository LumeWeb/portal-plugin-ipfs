package config

import (
	"time"

	"go.lumeweb.com/portal/config"
)

var _ config.Defaults = (*WebsiteConfig)(nil)

// WebsiteConfig contains the configuration for the website hosting feature
type WebsiteConfig struct {
	// Janitor configuration
	JanitorEnabled      bool          `config:"janitor_enabled"`
	CheckInterval       time.Duration `config:"check_interval"` // How often to re-validate individual websites
	JanitorWorkerCount  int           `config:"janitor_worker_count"`
	JanitorBatchSize    int           `config:"janitor_batch_size"`

	// Validation configuration
	ValidationTokenTTL   time.Duration `config:"validation_token_ttl"`
	VerificationTokenKey string        `config:"verification_token_key"`

	// Notification configuration
	NotificationsEnabled bool   `config:"notifications_enabled"`
	AdminEmail           string `config:"admin_email"`
}

func (c WebsiteConfig) Defaults() map[string]any {
	return map[string]any{
		"JanitorEnabled":  true,
		"CheckInterval":   30 * time.Minute,
		"JanitorWorkerCount": 10,
		"JanitorBatchSize":   500,
		"ValidationTokenTTL":   24 * time.Hour,
		"VerificationTokenKey": "lumeweb-verify",
		"NotificationsEnabled": true,
		"AdminEmail":           "",
	}
}
