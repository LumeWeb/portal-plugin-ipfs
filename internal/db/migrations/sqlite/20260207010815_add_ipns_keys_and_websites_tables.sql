-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ipfs_ipns_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    ipns_name TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    private_key_encrypted BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL
);

CREATE UNIQUE INDEX idx_ipfs_ipns_keys_user_peer ON ipfs_ipns_keys(user_id, peer_id);
CREATE INDEX idx_ipfs_ipns_keys_user_id ON ipfs_ipns_keys(user_id);
CREATE INDEX idx_ipfs_ipns_keys_deleted_at ON ipfs_ipns_keys(deleted_at);

CREATE TABLE IF NOT EXISTS ipfs_websites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    domain TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    validation_token TEXT NOT NULL,
    validation_expires_at TIMESTAMP NULL DEFAULT NULL,
    last_checked_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL
);

CREATE INDEX idx_ipfs_websites_user_id ON ipfs_websites(user_id);
CREATE INDEX idx_ipfs_websites_domain ON ipfs_websites(domain);
CREATE INDEX idx_ipfs_websites_status ON ipfs_websites(status);
CREATE INDEX idx_ipfs_websites_last_checked_at ON ipfs_websites(last_checked_at);
CREATE INDEX idx_ipfs_websites_deleted_at ON ipfs_websites(deleted_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_ipfs_websites_deleted_at;
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
