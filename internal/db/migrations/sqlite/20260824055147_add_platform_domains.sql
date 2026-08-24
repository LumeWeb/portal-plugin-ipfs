-- +goose Up
-- Introduce platform-owned DNS roots for free subdomains.
--
-- platform_domains is a minimal registry of roots (e.g. "platform.test") that
-- the operator owns. Users may claim subdomains under these roots for their
-- websites. It exists purely as the trust anchor for the one-zone relaxation in
-- DelegatedDomainService.resolveManagedZone: a subdomain whose parent matches a
-- registered platform root is allowed to reuse the platform-owned zone even
-- when the binding's UserID differs from the zone owner.
--
-- website_domains.platform_domain_id is a nullable FK: NULL for user-owned
-- apex/normal bindings (today's behavior); set when the binding is a platform
-- subdomain minted under the referenced PlatformDomain.
--
-- website_domains keeps its original strict UNIQUE(domain, namespace) index:
-- live-row uniqueness stays DB-enforced (the (domain, namespace, deleted_at)
-- widening would let NULL deleted_at values coexist for duplicate live rows).
-- Re-registration after a soft delete works because CreateDomain purges the
-- tombstone (deleted_at IS NOT NULL) for the key before inserting a fresh
-- binding. Only the new nullable platform_domain_id column is added here.

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS platform_domains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    namespace TEXT NOT NULL,
    zone_id INTEGER,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    UNIQUE (domain, namespace, deleted_at)
);
-- +goose StatementEnd

CREATE INDEX idx_platform_domains_zone_id ON platform_domains(zone_id);

-- No table rebuild is needed: the strict unique index already exists and the
-- new column is nullable, so SQLite ALTER TABLE ADD COLUMN suffices.
ALTER TABLE website_domains ADD COLUMN platform_domain_id INTEGER;
CREATE INDEX idx_website_domains_platform_domain_id ON website_domains(platform_domain_id);

-- +goose Down
-- Drop the new column and the platform root registry.
DROP INDEX IF EXISTS idx_website_domains_platform_domain_id;

-- SQLite cannot drop a column in place portably, so rebuild the table without
-- platform_domain_id, preserving the original strict UNIQUE(domain, namespace).
-- +goose StatementBegin
CREATE TABLE website_domains_old (
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
    ssl_status TEXT NOT NULL DEFAULT 'pending',
    ssl_error TEXT,
    ssl_issued_at TIMESTAMP NULL DEFAULT NULL,
    ssl_last_updated_at TIMESTAMP NULL DEFAULT NULL,
    dns_hosting_enabled INTEGER NOT NULL DEFAULT 0,
    UNIQUE (domain, namespace)
);
-- +goose StatementEnd

-- +goose StatementBegin
INSERT INTO website_domains_old (id, website_id, user_id, domain, namespace, zone_name, gateway_host, zone_id, status, delegation_data, protocol_data, created_at, updated_at, deleted_at, ssl_status, ssl_error, ssl_issued_at, ssl_last_updated_at, dns_hosting_enabled)
SELECT id, website_id, user_id, domain, namespace, zone_name, gateway_host, zone_id, status, delegation_data, protocol_data, created_at, updated_at, deleted_at, ssl_status, ssl_error, ssl_issued_at, ssl_last_updated_at, dns_hosting_enabled
FROM website_domains;
-- +goose StatementEnd

DROP INDEX IF EXISTS idx_website_domains_website_id;
DROP INDEX IF EXISTS idx_website_domains_user_id;
DROP INDEX IF EXISTS idx_website_domains_zone_id;
DROP INDEX IF EXISTS idx_website_domains_deleted_at;
DROP INDEX IF EXISTS idx_website_domains_ssl_status;
DROP INDEX IF EXISTS idx_website_domains_ssl_issued_at;
DROP INDEX IF EXISTS idx_website_domains_ssl_last_updated_at;

DROP TABLE website_domains;

-- +goose StatementBegin
ALTER TABLE website_domains_old RENAME TO website_domains;
-- +goose StatementEnd

CREATE INDEX idx_website_domains_website_id ON website_domains(website_id);
CREATE INDEX idx_website_domains_user_id ON website_domains(user_id);
CREATE INDEX idx_website_domains_zone_id ON website_domains(zone_id);
CREATE INDEX idx_website_domains_deleted_at ON website_domains(deleted_at);
CREATE INDEX idx_website_domains_ssl_status ON website_domains(ssl_status);
CREATE INDEX idx_website_domains_ssl_issued_at ON website_domains(ssl_issued_at);
CREATE INDEX idx_website_domains_ssl_last_updated_at ON website_domains(ssl_last_updated_at);

DROP INDEX IF EXISTS idx_platform_domains_zone_id;
DROP TABLE IF EXISTS platform_domains;
