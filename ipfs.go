package ipfs

import (
	"embed"
	_ "github.com/dnslink-std/go"
	_ "github.com/ipfs/boxo/namesys"
	"go.lumeweb.com/portal-plugin-ipfs/internal/metrics/adapter"
	"go.lumeweb.com/portal-plugin-ipfs/internal/plugin"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/service"
)

//go:embed templates/*
var mailerTemplates embed.FS

func init() {
	// Inject Prometheus adapter for go-metrics-interface to capture boxo/bitswap metrics
	// This must be done before any IPFS components are created
	pluginRegistry := core.PluginMetricsRegistry(internal.ProtocolName)
	if err := adapter.InjectPrometheusAdapter(pluginRegistry); err != nil {
		// If adapter injection fails, boxo will use noop metrics (no metrics collected)
		// This is not a critical error, so we log and continue
		// Note: We can't log here since logger isn't available yet
	}

	templates, err := service.MailerTemplatesFromEmbed(&mailerTemplates, "")
	if err != nil {
		panic(err)
	}

	core.RegisterPlugin(plugin.GetPluginInfoWithTemplates(templates))

	internal.RegisterHashes()
}
