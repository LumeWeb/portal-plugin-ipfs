-- +goose Up
-- CreatePlatformDomain now auto-creates (idempotently) the operator's DNS zone
-- for every platform root and always stores the resulting zone ID on the
-- platform_domains row. A PlatformDomain without a provisioned zone is invalid,
-- so enforce the invariant at the schema level by making zone_id NOT NULL.
-- SQLite cannot modify a column constraint in place, so rebuild the table.
-- +goose StatementBegin
CREATE TABLE platform_domains_old (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    namespace TEXT NOT NULL,
    zone_id INTEGER,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    UNIQUE (domain, namespace)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE platform_domains_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    namespace TEXT NOT NULL,
    zone_id INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    UNIQUE (domain, namespace)
);
-- +goose StatementEnd

-- +goose StatementBegin
INSERT INTO platform_domains_new (id, domain, namespace, zone_id, enabled, created_at, updated_at, deleted_at)
SELECT id, domain, namespace, zone_id, enabled, created_at, updated_at, deleted_at
FROM platform_domains;
-- +goose StatementEnd

DROP INDEX IF EXISTS idx_platform_domains_zone_id;
DROP TABLE platform_domains;
ALTER TABLE platform_domains_new RENAME TO platform_domains;
CREATE INDEX idx_platform_domains_zone_id ON platform_domains(zone_id);

-- +goose Down
-- +goose StatementBegin
CREATE TABLE platform_domains_old (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    namespace TEXT NOT NULL,
    zone_id INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    UNIQUE (domain, namespace)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE platform_domains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    namespace TEXT NOT NULL,
    zone_id INTEGER,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    UNIQUE (domain, namespace)
);
-- +goose StatementEnd

-- +goose StatementBegin
INSERT INTO platform_domains (id, domain, namespace, zone_id, enabled, created_at, updated_at, deleted_at)
SELECT id, domain, namespace, zone_id, enabled, created_at, updated_at, deleted_at
FROM platform_domains_old;
-- +goose StatementEnd

DROP INDEX IF EXISTS idx_platform_domains_zone_id;
DROP TABLE platform_domains_old;
CREATE INDEX idx_platform_domains_zone_id ON platform_domains(zone_id);
