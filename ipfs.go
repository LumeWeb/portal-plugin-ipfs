package ipfs

import (
	"fmt"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service"
	"go.lumeweb.com/portal/core"
	"gorm.io/gorm"
)

const pinViewName = "ipfs_pin_view"

func viewExists(db *gorm.DB) (bool, error) {
	var exists bool
	var err error

	dialectName := db.Dialector.Name()
	switch dialectName {
	case "sqlite":
		err = db.Raw("SELECT COUNT(*) > 0 FROM sqlite_master WHERE type = 'view' AND name = ?", pinViewName).Scan(&exists).Error
	case "mysql":
		err = db.Raw("SELECT COUNT(*) > 0 FROM information_schema.views WHERE table_name = ?", pinViewName).Scan(&exists).Error
	default:
		return false, fmt.Errorf("unsupported database type: %s", dialectName)
	}

	return exists, err
}

func createView(db *gorm.DB) error {
	// Define Common Table Expressions (CTEs)
	ipfsRequestDataCTE := db.Table("portal.ipfs_requests ir").
		Select(`
            ir.request_id,
            ir.name AS ir_name,
            ir.internal,
            ir.pin_request_id,
            ir.parent_pin_request_id,
            r.id AS r_id,
            r.status,
            r.hash AS r_hash,
            r.hash_type AS r_hash_type,
            r.metadata,
            r.user_id AS r_user_id,
            r.source_ip,
            r.created_at AS r_created_at,
            r.updated_at AS r_updated_at
        `).
		Joins("LEFT JOIN portal.requests r ON ir.request_id = r.id")

	pinUploadDataCTE := db.Table("portal.ipfs_pins ip").
		Select(`
            ip.request_id AS ip_request_id,
            ip.name AS ip_name,
            ip.created_at AS ip_created_at,
            ip.updated_at AS ip_updated_at,
            p.id AS pin_id,
            p.user_id AS pin_user_id,
            u.hash AS u_hash,
            u.hash_type AS u_hash_type,
            u.uploader_ip,
			ip.partial AS partial
        `).
		Joins("JOIN portal.pins p ON ip.id = p.id").
		Joins("JOIN portal.uploads u ON p.upload_id = u.id").
		Where("ip.deleted_at IS NULL AND p.deleted_at IS NULL AND u.deleted_at IS NULL")

	// Main query
	query := db.Table("(?) AS ird", ipfsRequestDataCTE).
		Select(`
            COALESCE(pud.pin_id, ird.r_id) AS id,
            ird.request_id,
            ird.status,
            COALESCE(pud.u_hash, ird.r_hash) AS hash,
            COALESCE(pud.u_hash_type, ird.r_hash_type) AS hash_type,
            COALESCE(pud.ip_name, ird.ir_name) AS name,
            ird.metadata AS meta,
            COALESCE(pud.pin_user_id, ird.r_user_id) AS user_id,
            COALESCE(ird.source_ip, pud.uploader_ip) AS uploader_ip,
            ird.internal,
			COALESCE(pud.partial, false) AS partial,
            ird.pin_request_id,
            ird.parent_pin_request_id,
            pud.pin_id,
            COALESCE(pud.ip_created_at, ird.r_created_at) AS created_at,
            COALESCE(pud.ip_updated_at, ird.r_updated_at) AS updated_at,
            NULL AS deleted_at
        `).
		Joins("LEFT JOIN (?) AS pud ON ird.r_hash = pud.u_hash AND ird.r_hash_type = pud.u_hash_type", pinUploadDataCTE).
		Distinct()

	return db.Migrator().CreateView(pinViewName, gorm.ViewOption{
		Query:   query,
		Replace: isMySQL(db),
	})
}

func isMySQL(db *gorm.DB) bool {
	return db.Dialector.Name() == "mysql"
}

func init() {
	core.RegisterPlugin(core.PluginInfo{
		ID: internal.ProtocolName,
		API: func() (core.API, []core.ContextBuilderOption, error) {
			return api.NewAPI()
		},
		Protocol: func() (core.Protocol, []core.ContextBuilderOption, error) {
			return protocol.NewProtocol()
		},
		Services: func() ([]core.ServiceInfo, error) {
			return []core.ServiceInfo{
				{
					ID:      service.UPLOAD_SERVICE,
					Factory: service.NewUploadService,
					Depends: []string{core.CRON_SERVICE},
				},
			}, nil
		},
		Models: []any{
			&db.IPFSPin{},
			&db.IPFSBlock{},
			&db.IPFSLinkedBlock{},
			&db.IPFSRequest{},
			&db.UnixFSNode{},
		},
		Migrations: []core.DBMigration{
			func(db *gorm.DB) error {
				// Check if view exists
				exists, err := viewExists(db)
				if err != nil {
					return fmt.Errorf("failed to check if view exists: %w", err)
				}

				if exists && !isMySQL(db) {
					return nil
				}

				// Create view
				if err = createView(db); err != nil {
					return fmt.Errorf("failed to create view: %w", err)
				}

				return nil
			},
		},
		Cron: func() core.CronFactory {
			return func(ctx core.Context) (core.Cronable, error) {
				return cron.NewCron(ctx), nil
			}
		},
	})
}
