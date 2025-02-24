package config

import "go.lumeweb.com/portal/config"

var _ config.APIConfig = (*APIConfig)(nil)

type APIConfig struct {
}

func (A APIConfig) Defaults() map[string]any {
	return nil
}
