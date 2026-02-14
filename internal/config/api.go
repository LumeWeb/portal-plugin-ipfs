package config

import "go.lumeweb.com/portal/config"

var _ config.APIConfig = (*APIConfig)(nil)

type APIConfig struct {
	// GatewaySecret is the shared secret for authenticating gateway requests
	GatewaySecret string `config:"gateway_secret"`
}

func (A APIConfig) Defaults() map[string]any {
	return map[string]any{
		"GatewaySecret": "",
	}
}
