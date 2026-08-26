package config

import (
	"strings"
	"time"

	"go.lumeweb.com/portal/config"
)

// SanitizeDNSLabel converts a string into a valid DNS label per RFC 1035.
// Lowercases, strips invalid chars to hyphens, trims leading/trailing hyphens,
// truncates to 63 chars.
func SanitizeDNSLabel(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			b.WriteRune(c)
		} else if c == '-' || c == '_' || c == ' ' {
			b.WriteByte('-')
		}
	}
	result := strings.Trim(b.String(), "-")
	if len(result) > 63 {
		result = result[:63]
	}
	return strings.TrimRight(result, "-")
}

var _ config.Defaults = (*WebsiteConfig)(nil)

// WebsiteConfig contains the configuration for the website hosting feature
type WebsiteConfig struct {
	// Janitor configuration
	JanitorEnabled     bool          `config:"janitor_enabled"`
	CheckInterval      time.Duration `config:"check_interval"` // How often to re-validate individual websites
	JanitorWorkerCount int           `config:"janitor_worker_count"`
	JanitorBatchSize   int           `config:"janitor_batch_size"`
	// JanitorGracePeriod is how long after a website is created the janitor
	// waits before it may mark the site broken. It gives deployments time to
	// finish pinning / publishing the target so we don't flag freshly created
	// sites as bad. A value <= 0 disables the grace period.
	JanitorGracePeriod time.Duration `config:"janitor_grace_period"`

	// Validation configuration
	ValidationTokenTTL time.Duration `config:"validation_token_ttl"`

	// Notification configuration
	NotificationsEnabled bool   `config:"notifications_enabled"`
	AdminEmail           string `config:"admin_email"`
}

func (c WebsiteConfig) Defaults() map[string]any {
	return map[string]any{
		"JanitorEnabled":       true,
		"CheckInterval":        30 * time.Minute,
		"JanitorWorkerCount":   10,
		"JanitorBatchSize":     500,
		"JanitorGracePeriod":   1 * time.Hour,
		"ValidationTokenTTL":   24 * time.Hour,
		"NotificationsEnabled": true,
		"AdminEmail":           "",
	}
}
