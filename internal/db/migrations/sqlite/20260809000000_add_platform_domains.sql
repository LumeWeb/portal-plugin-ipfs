-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS platform_domains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    namespace TEXT NOT NULL,
    zone_id INTEGER,
    enabled INTEGER NOT NULL DEFAULT 1,
    deleted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Unique includes deleted_at (soft-delete tombstone) so a deleted root can
    -- be re-registered; live rows remain unique per (domain, namespace).
    UNIQUE (domain, namespace, deleted_at)
);
-- +goose StatementEnd

CREATE INDEX idx_platform_domains_zone_id ON platform_domains(zone_id);

ALTER TABLE website_domains ADD COLUMN platform_domain_id INTEGER;
CREATE INDEX idx_website_domains_platform_domain_id ON website_domains(platform_domain_id);

-- +goose Down
DROP INDEX IF EXISTS idx_website_domains_platform_domain_id;
ALTER TABLE website_domains DROP COLUMN platform_domain_id;
DROP INDEX IF EXISTS idx_platform_domains_zone_id;
DROP TABLE IF EXISTS platform_domains;
