package ipfs

import (
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
	filemanager "go.lumeweb.com/portal-plugin-ipfs/internal/service/file_manager"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/pin"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/upload"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/web/go/portal-plugin-ipfs"
)

// GetCollectors returns all Prometheus collectors from all services merged at the plugin level
func GetCollectors() []prometheus.Collector {
	var collectors []prometheus.Collector

	collectors = append(collectors, pin.GetCollectors()...)
	collectors = append(collectors, upload.GetCollectors()...)
	collectors = append(collectors, block.GetCollectors()...)
	collectors = append(collectors, filemanager.GetCollectors()...)

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

	core.RegisterPlugin(core.PluginInfo{
		ID:       internal.ProtocolName,
		Version:  build.GetInfo(),
		API:      api.NewAPI,
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
			}, nil
		},
		Models: []any{
			&db.IPFSPin{},
			&db.IPFSBlock{},
			&db.IPFSLinkedBlock{},
			&db.UnixFSNode{},
		},
		Metrics: GetCollectors(),
		Migrations: core.DBMigration{
			core.DB_TYPE_SQLITE: migrations.GetSQLite(),
			core.DB_TYPE_MYSQL:  migrations.GetMySQL(),
		},
		WebBundles: core.NewWebBundles(core.NewWebBundle(portal_plugin_ipfs.GetFS(), core.WithWebBundleTargetApps("dashboard"))),
	})

	internal.RegisterHashes()
}
