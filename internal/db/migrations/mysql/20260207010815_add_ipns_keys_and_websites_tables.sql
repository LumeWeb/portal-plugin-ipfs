-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ipfs_ipns_keys (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    user_id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    ipns_name VARCHAR(255) NOT NULL,
    peer_id VARCHAR(255) NOT NULL,
    private_key_encrypted BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    
    UNIQUE KEY user_peer (user_id, peer_id),
    KEY idx_ipfs_ipns_keys_user_id (user_id),
    KEY idx_ipfs_ipns_keys_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ipfs_websites (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    user_id INTEGER NOT NULL,
    domain VARCHAR(255) NOT NULL,
    target_type VARCHAR(50) NOT NULL,
    target_hash VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    validation_token VARCHAR(255) NOT NULL,
    validation_expires_at TIMESTAMP NULL DEFAULT NULL,
    last_checked_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    
    KEY idx_ipfs_websites_user_id (user_id),
    KEY idx_ipfs_websites_domain (domain),
    KEY idx_ipfs_websites_status (status),
    KEY idx_ipfs_websites_last_checked_at (last_checked_at),
    KEY idx_ipfs_websites_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS ipfs_websites;
DROP TABLE IF EXISTS ipfs_ipns_keys;
-- +goose StatementEnd
