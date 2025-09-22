package ipfs

import (
	"go.lumeweb.com/portal-plugin-ipfs/build"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/block"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/pin"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/upload"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/web/go/portal-plugin-ipfs"
)

func init() {
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
			}, nil
		},
		Models: []any{
			&db.IPFSPin{},
			&db.IPFSBlock{},
			&db.IPFSLinkedBlock{},
			&db.UnixFSNode{},
		},
		Migrations: core.DBMigration{
			core.DB_TYPE_SQLITE: migrations.GetSQLite(),
			core.DB_TYPE_MYSQL:  migrations.GetMySQL(),
		},
		WebBundles: core.NewWebBundles(core.NewWebBundle(portal_plugin_ipfs.GetFS(), core.WithWebBundleTargetApps("dashboard"))),
	})

	internal.RegisterHashes()
}
