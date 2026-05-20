-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ipfs_ipns_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    peer_id_multihash BLOB NOT NULL,
    private_key_encrypted BLOB NOT NULL,
    last_published_cid TEXT DEFAULT NULL,
    last_published_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL
);

CREATE UNIQUE INDEX idx_ipfs_ipns_keys_user_peer ON ipfs_ipns_keys(user_id, peer_id_multihash);
CREATE INDEX idx_ipfs_ipns_keys_user_id ON ipfs_ipns_keys(user_id);
CREATE INDEX idx_ipfs_ipns_keys_deleted_at ON ipfs_ipns_keys(deleted_at);

CREATE TABLE IF NOT EXISTS ipfs_websites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    domain TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_multihash BLOB NOT NULL,
    cid_version INTEGER,
    cid_type INTEGER,
    status TEXT NOT NULL,
    validation_token TEXT NOT NULL,
    validation_expires_at TIMESTAMP NULL DEFAULT NULL,
    last_checked_at TIMESTAMP NULL DEFAULT NULL,
    dns_zone_id INTEGER,
    ipns_key_id INTEGER,
    dns_enabled INTEGER DEFAULT 0,
    ssl_status TEXT DEFAULT 'pending',
    ssl_error TEXT,
    ssl_issued_at TIMESTAMP NULL DEFAULT NULL,
    ssl_last_updated_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL
);

CREATE INDEX idx_ipfs_websites_user_id ON ipfs_websites(user_id);
CREATE INDEX idx_ipfs_websites_domain ON ipfs_websites(domain);
CREATE INDEX idx_ipfs_websites_status ON ipfs_websites(status);
CREATE INDEX idx_ipfs_websites_last_checked_at ON ipfs_websites(last_checked_at);
CREATE INDEX idx_ipfs_websites_dns_zone_id ON ipfs_websites(dns_zone_id);
CREATE INDEX idx_ipfs_websites_ipns_key_id ON ipfs_websites(ipns_key_id);
CREATE INDEX idx_ipfs_websites_ssl_status ON ipfs_websites(ssl_status);
CREATE INDEX idx_ipfs_websites_ssl_issued_at ON ipfs_websites(ssl_issued_at);
CREATE INDEX idx_ipfs_websites_ssl_last_updated_at ON ipfs_websites(ssl_last_updated_at);
CREATE INDEX idx_ipfs_websites_deleted_at ON ipfs_websites(deleted_at);

-- Note: SQLite doesn't support CHECK constraints in ALTER TABLE, handled in app layer
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_ipfs_websites_deleted_at;
DROP INDEX IF EXISTS idx_ipfs_websites_ssl_last_updated_at;
DROP INDEX IF EXISTS idx_ipfs_websites_ssl_issued_at;
DROP INDEX IF EXISTS idx_ipfs_websites_ssl_status;
DROP INDEX IF EXISTS idx_ipfs_websites_ipns_key_id;
DROP INDEX IF EXISTS idx_ipfs_websites_dns_zone_id;
DROP INDEX IF EXISTS idx_ipfs_websites_last_checked_at;
DROP INDEX IF EXISTS idx_ipfs_websites_status;
DROP INDEX IF EXISTS idx_ipfs_websites_domain;
DROP INDEX IF EXISTS idx_ipfs_websites_user_id;
DROP TABLE IF EXISTS ipfs_websites;

DROP INDEX IF EXISTS idx_ipfs_ipns_keys_deleted_at;
DROP INDEX IF EXISTS idx_ipfs_ipns_keys_user_id;
DROP INDEX IF EXISTS idx_ipfs_ipns_keys_user_peer;
DROP TABLE IF EXISTS ipfs_ipns_keys;
-- +goose StatementEnd
