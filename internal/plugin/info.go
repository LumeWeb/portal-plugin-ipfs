package plugin

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.lumeweb.com/portal-plugin-ipfs/build"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/ipfs"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/block"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/dns"
	filemanager "go.lumeweb.com/portal-plugin-ipfs/internal/service/file_manager"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/ipns_key"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/pin"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/upload"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/website"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
	"go.lumeweb.com/portal/core"
	portal_plugin_ipfs "go.lumeweb.com/web/go/portal-plugin-ipfs"
)

func GetCollectors() []prometheus.Collector {
	var collectors []prometheus.Collector

	collectors = append(collectors, pin.GetCollectors()...)
	collectors = append(collectors, upload.GetCollectors()...)
	collectors = append(collectors, block.GetCollectors()...)
	collectors = append(collectors, filemanager.GetCollectors()...)
	collectors = append(collectors, website.GetCollectors()...)
	collectors = append(collectors, ipfs.GetMetricsCollectors()...)

	return collectors
}

func getPluginInfoWithoutTemplates() core.PluginInfo {
	return core.PluginInfo{
		ID:      internal.ProtocolName,
		Version: build.GetInfo(),
		API:     api.NewAPI,
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
					ID:      pluginCore.WEBSITE_SERVICE,
					Factory: website.NewWebsiteService,
					Depends: []string{pluginCore.PIN_SERVICE, pluginCore.IPNS_KEY_SERVICE, pluginCore.DNS_SERVICE, pluginCore.DELEGATED_DOMAIN_SERVICE},
				},
				{
					ID:      pluginCore.DELEGATED_DOMAIN_SERVICE,
					Factory: domain.NewDelegatedDomainServiceFactory,
					Depends: []string{pluginCore.DNS_SERVICE},
				},
				{
					ID:      pluginCore.DNS_SERVICE,
					Factory: dns.NewDNSService,
				},
			}, nil
		},
		CronJobs: []core.PluginCronJob{
			{
				Name:    "website_janitor",
				Factory: func() (core.CronJob, error) { return website.NewWebsiteJanitorJob(), nil },
				Schedule: core.NewCronScheduleDefinition(core.CronScheduleTypeCron).
					WithCronExpression("* * * * *"),
			},
		},
		Models: []any{
			&db.IPFSPin{},
			&db.IPFSBlock{},
			&db.IPFSLinkedBlock{},
			&db.UnixFSNode{},
			&db.IPFSIPNSKey{},
			&db.Website{},
			&db.WebsiteDomain{},
			&db.DNSZone{},
		},
		Metrics:         GetCollectors(),
		Migrations:      core.DBMigration{core.DB_TYPE_SQLITE: migrations.GetSQLite(), core.DB_TYPE_MYSQL: migrations.GetMySQL()},
		WebBundles:      core.NewWebBundles(core.NewWebBundle(portal_plugin_ipfs.GetFS(), core.WithWebBundleTargetApps("dashboard"))),
		MailerTemplates: nil,
	}
}

func GetPluginInfoWithTemplates(templates core.MailerTemplates) core.PluginInfo {
	info := getPluginInfoWithoutTemplates()
	info.MailerTemplates = templates
	return info
}
