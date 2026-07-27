-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS website_domains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    website_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    domain TEXT NOT NULL,
    namespace TEXT NOT NULL,
    zone_name TEXT,
    gateway_host TEXT,
    zone_id INTEGER,
    status TEXT NOT NULL DEFAULT 'draft',
    delegation_data JSON,
    protocol_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    UNIQUE (domain, namespace)
);

CREATE INDEX idx_website_domains_website_id ON website_domains(website_id);
CREATE INDEX idx_website_domains_user_id ON website_domains(user_id);
CREATE INDEX idx_website_domains_zone_id ON website_domains(zone_id);
CREATE INDEX idx_website_domains_deleted_at ON website_domains(deleted_at);

-- Backfill existing records from ipfs_websites (old single-domain model, default to icann namespace)
INSERT OR IGNORE INTO website_domains (website_id, user_id, domain, namespace, created_at, updated_at, status)
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
DROP INDEX IF EXISTS idx_website_domains_deleted_at;
DROP INDEX IF EXISTS idx_website_domains_zone_id;
DROP INDEX IF EXISTS idx_website_domains_user_id;
DROP INDEX IF EXISTS idx_website_domains_website_id;
DROP TABLE IF EXISTS website_domains;
-- +goose StatementEnd
