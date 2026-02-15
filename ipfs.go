package ipfs

import (
	"embed"
	_ "embed"

	"github.com/prometheus/client_golang/prometheus"
	_ "github.com/dnslink-std/go"
	_ "github.com/ipfs/boxo/namesys"
	"go.lumeweb.com/portal-plugin-ipfs/build"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/metrics/adapter"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/block"
	boxo "go.lumeweb.com/portal-plugin-ipfs/internal/service/boxo"
	filemanager "go.lumeweb.com/portal-plugin-ipfs/internal/service/file_manager"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/ipns_key"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/pin"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/upload"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/website"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/service"
	"go.lumeweb.com/web/go/portal-plugin-ipfs"
)

//go:embed templates/*
var mailerTemplates embed.FS

// GetCollectors returns all Prometheus collectors from all services merged at the plugin level
func GetCollectors() []prometheus.Collector {
	var collectors []prometheus.Collector

	collectors = append(collectors, pin.GetCollectors()...)
	collectors = append(collectors, upload.GetCollectors()...)
	collectors = append(collectors, block.GetCollectors()...)
	collectors = append(collectors, filemanager.GetCollectors()...)
	collectors = append(collectors, website.GetCollectors()...)

	return collectors
}

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

	core.RegisterPlugin(core.PluginInfo{
		ID:       internal.ProtocolName,
		Version:  build.GetInfo(),
		API:      api.NewAPI,
		APIExtensions: func(ctx core.Context) ([]core.APIExtensionFactory, error) {
			return []core.APIExtensionFactory{
				api.NewAdminExtension(),
			}, nil
		},
		Protocol: protocol.NewProtocol,
		Services: func() ([]core.ServiceInfo, error) {
			return []core.ServiceInfo{
				{
					ID:      pluginCore.PIN_SERVICE,
					Factory: pin.NewPinService,
					Depends: []string{pluginCore.FILE_MANAGER_SERVICE},
				},

				{
					ID:      pluginCore.UPLOAD_SERVICE,
					Factory: upload.NewUploadService,
					Depends: []string{core.PIN_SERVICE, pluginCore.PIN_SERVICE},
				},
				{
					ID:      pluginCore.BLOCK_SERVICE,
					Factory: block.NewBlockService,
				},
				{
					ID:      pluginCore.FILE_MANAGER_SERVICE,
					Factory: filemanager.NewFileManagerService,
				},
				{
					ID:      pluginCore.IPNS_KEY_SERVICE,
					Factory: ipns_key.NewIPNSKeyService,
				},
				{
					ID:      pluginCore.IPNS_PUBLISHER_SERVICE,
					Factory: boxo.NewIPNSPublisherService,
				},
				{
					ID:      pluginCore.IPNS_REPUBLISHER_SERVICE,
					Factory: boxo.NewIPNSRepublisherService,
					Depends: []string{pluginCore.IPNS_PUBLISHER_SERVICE, pluginCore.IPNS_KEY_SERVICE},
				},
				{
					ID:      pluginCore.WEBSITE_SERVICE,
					Factory: website.NewWebsiteService,
					Depends: []string{pluginCore.PIN_SERVICE, pluginCore.IPNS_KEY_SERVICE},
				},
			}, nil
		},
		CronJobs: []core.PluginCronJob{
			{
				Name:    core.GetCronJobIdentifier(core.JobOriginPlugin, "ipfs.website_janitor"),
				Factory: func() (core.CronJob, error) { return website.NewWebsiteJanitorJob(), nil },
				Schedule: core.NewCronScheduleDefinition(core.CronScheduleTypeCron).
					WithCronExpression("*/30 * * * *"), // Every 30 minutes
			},
		},
		Models: []any{
			&db.IPFSPin{},
			&db.IPFSBlock{},
			&db.IPFSLinkedBlock{},
			&db.UnixFSNode{},
			&db.IPFSIPNSKey{},
			&db.Website{},
		},
		Metrics: GetCollectors(),
		Migrations: core.DBMigration{
			core.DB_TYPE_SQLITE: migrations.GetSQLite(),
			core.DB_TYPE_MYSQL:  migrations.GetMySQL(),
		},
		WebBundles: core.NewWebBundles(core.NewWebBundle(portal_plugin_ipfs.GetFS(), core.WithWebBundleTargetApps("dashboard"))),
		MailerTemplates: templates,
	})

	internal.RegisterHashes()
}
