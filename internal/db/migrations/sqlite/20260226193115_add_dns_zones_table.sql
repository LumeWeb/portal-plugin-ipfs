-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ipfs_dns_zones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    domain TEXT NOT NULL,
    status TEXT NOT NULL,
    powerdns_zone_id TEXT,
    last_nameserver_check_at TIMESTAMP NULL DEFAULT NULL,
    nameservers_verified_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL
);

CREATE UNIQUE INDEX idx_ipfs_dns_zones_domain ON ipfs_dns_zones(domain COLLATE NOCASE) WHERE deleted_at IS NULL;
CREATE INDEX idx_ipfs_dns_zones_user_id ON ipfs_dns_zones(user_id);
CREATE INDEX idx_ipfs_dns_zones_status ON ipfs_dns_zones(status);
CREATE INDEX idx_ipfs_dns_zones_deleted_at ON ipfs_dns_zones(deleted_at);

<<<<<<< HEAD
-- Add dns_zone_id foreign key to ipfs_websites
ALTER TABLE ipfs_websites ADD COLUMN dns_zone_id INTEGER;
-- SQLite doesn't support ADD CONSTRAINT in ALTER TABLE, FK enforcement handled in app layer
CREATE INDEX idx_ipfs_websites_dns_zone_id ON ipfs_websites(dns_zone_id);

-- Add ipns_key_id foreign key to ipfs_websites
ALTER TABLE ipfs_websites ADD COLUMN ipns_key_id INTEGER;
-- SQLite doesn't support ADD CONSTRAINT in ALTER TABLE, FK enforcement handled in app layer
CREATE INDEX idx_ipfs_websites_ipns_key_id ON ipfs_websites(ipns_key_id);
=======
-- Note: dns_zone_id column already exists in ipfs_websites table from migration 20260207010815
-- Index already created in that migration as well
>>>>>>> de08024f (feat(dns): add database migrations for DNS zones)
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_ipfs_websites_dns_zone_id;
-- Note: SQLite doesn't support DROP COLUMN, handled in app layer

DROP INDEX IF EXISTS idx_ipfs_dns_zones_deleted_at;
DROP INDEX IF EXISTS idx_ipfs_dns_zones_status;
DROP INDEX IF EXISTS idx_ipfs_dns_zones_user_id;
DROP INDEX IF EXISTS idx_ipfs_dns_zones_domain;
DROP TABLE IF EXISTS ipfs_dns_zones;
-- +goose StatementEnd
