-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ipfs_ipns_keys (
    id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL,
    peer_id_multihash VARBINARY(64) NOT NULL,
    private_key_encrypted BLOB NOT NULL,
    last_published_cid VARCHAR(255) DEFAULT NULL,
    last_published_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,

    UNIQUE KEY user_peer (user_id, peer_id_multihash),
    KEY idx_ipfs_ipns_keys_user_id (user_id),
    KEY idx_ipfs_ipns_keys_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ipfs_websites (
    id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT UNSIGNED NOT NULL,
    domain VARCHAR(255) NOT NULL,
    target_type VARCHAR(50) NOT NULL,
    target_multihash VARBINARY(64) NOT NULL,
    cid_version TINYINT UNSIGNED,
    cid_type TINYINT UNSIGNED,
    status VARCHAR(50) NOT NULL,
    validation_token VARCHAR(255) NOT NULL,
    validation_expires_at TIMESTAMP NULL DEFAULT NULL,
    last_checked_at TIMESTAMP NULL DEFAULT NULL,
    dns_zone_id BIGINT UNSIGNED NULL,
    ipns_key_id BIGINT UNSIGNED NULL,
    dns_enabled TINYINT(1) DEFAULT 0,
    ssl_status VARCHAR(50) NOT NULL DEFAULT 'pending',
    ssl_error TEXT,
    ssl_issued_at TIMESTAMP NULL DEFAULT NULL,
    ssl_last_updated_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,

    KEY idx_ipfs_websites_user_id (user_id),
    KEY idx_ipfs_websites_domain (domain),
    KEY idx_ipfs_websites_status (status),
    KEY idx_ipfs_websites_last_checked_at (last_checked_at),
    KEY idx_ipfs_websites_dns_zone_id (dns_zone_id),
    KEY idx_ipfs_websites_ipns_key_id (ipns_key_id),
    KEY idx_ipfs_websites_ssl_status (ssl_status),
    KEY idx_ipfs_websites_ssl_issued_at (ssl_issued_at),
    KEY idx_ipfs_websites_ssl_last_updated_at (ssl_last_updated_at),
    KEY idx_ipfs_websites_deleted_at (deleted_at),

    -- Enforce: IPFS -> cid_version set, IPNS -> cid_version NULL
    CONSTRAINT chk_website_cid_version CHECK (
        (target_type = 'ipfs' AND cid_version IS NOT NULL) OR
        (target_type = 'ipns' AND cid_version IS NULL)
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS ipfs_websites;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS ipfs_ipns_keys;
-- +goose StatementEnd
