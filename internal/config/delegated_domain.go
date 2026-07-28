package config

import (
	"go.lumeweb.com/portal/config"
)

var _ config.Defaults = (*DelegatedDomainConfig)(nil)

// DelegatedDomainConfig contains the configuration for the delegated domain service.
// Currently no user-tunable options; exists to satisfy config.ServiceConfig.
type DelegatedDomainConfig struct{}

func (c DelegatedDomainConfig) Defaults() map[string]any {
	return map[string]any{}
}
