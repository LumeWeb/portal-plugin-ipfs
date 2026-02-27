-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ipfs_dns_zones (
    id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT UNSIGNED NOT NULL,
    domain VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    powerdns_zone_id VARCHAR(255),
    last_nameserver_check_at TIMESTAMP NULL DEFAULT NULL,
    nameservers_verified_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,

    UNIQUE KEY idx_dns_zones_domain (domain),
    KEY idx_dns_zones_user_id (user_id),
    KEY idx_dns_zones_status (status),
    KEY idx_dns_zones_deleted_at (deleted_at),

    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE ipfs_websites ADD COLUMN dns_zone_id BIGINT UNSIGNED NULL;
CREATE INDEX idx_websites_dns_zone_id ON ipfs_websites(dns_zone_id);
ALTER TABLE ipfs_websites ADD CONSTRAINT fk_websites_dns_zone FOREIGN KEY (dns_zone_id) REFERENCES ipfs_dns_zones(id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE ipfs_websites DROP FOREIGN KEY fk_websites_dns_zone;
DROP INDEX IF EXISTS idx_websites_dns_zone_id ON ipfs_websites;
ALTER TABLE ipfs_websites DROP COLUMN dns_zone_id;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS ipfs_dns_zones;
-- +goose StatementEnd
