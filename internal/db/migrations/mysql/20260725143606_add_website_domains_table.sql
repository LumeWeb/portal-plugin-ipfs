-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS website_domains (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    website_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    domain VARCHAR(255) NOT NULL,
    namespace VARCHAR(32) NOT NULL,
    zone_name VARCHAR(255),
    gateway_host VARCHAR(255),
    zone_id BIGINT UNSIGNED,
    status VARCHAR(32) NOT NULL DEFAULT 'draft',
    delegation_data JSON,
    protocol_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    UNIQUE KEY uk_domain_namespace (domain, namespace),
    INDEX idx_website_domains_website_id (website_id),
    INDEX idx_website_domains_user_id (user_id),
    INDEX idx_website_domains_zone_id (zone_id),
    INDEX idx_website_domains_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- +goose StatementEnd

-- Backfill existing records from ipfs_websites (old single-domain model, default to icann namespace)
-- +goose StatementBegin
INSERT IGNORE INTO website_domains (website_id, user_id, domain, namespace, created_at, updated_at, status)
SELECT 
    id, 
    user_id, 
    domain, 
    'icann', 
    COALESCE(created_at, CURRENT_TIMESTAMP), 
    COALESCE(updated_at, CURRENT_TIMESTAMP),
    'draft'
FROM ipfs_websites 
WHERE domain IS NOT NULL AND trim(domain) != '';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS website_domains;
-- +goose StatementEnd
