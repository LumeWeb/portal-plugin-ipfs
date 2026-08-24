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
-- platform_domains uses the same strict UNIQUE(domain, namespace) as
-- website_domains: live-row uniqueness stays DB-enforced. The (domain,
-- namespace, deleted_at) widening is intentionally avoided — NULL deleted_at
-- values are distinct in both MySQL and SQLite, so it would let duplicate live
-- rows coexist and defeat the duplicate-key race detection CreateDomain and the
-- createWebsite handler rely on. Re-registration after a soft delete works
-- because CreatePlatformDomain purges the tombstone (deleted_at IS NOT NULL)
-- for the key before inserting a fresh root, so the strict unique key never
-- blocks re-registration.

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS platform_domains (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    domain VARCHAR(255) NOT NULL,
    namespace VARCHAR(32) NOT NULL,
    zone_id BIGINT UNSIGNED NULL DEFAULT NULL,
    enabled TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    UNIQUE KEY uk_platform_domains_domain_namespace (domain, namespace),
    INDEX idx_platform_domains_zone_id (zone_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- +goose StatementEnd

-- website_domains already carries its strict UNIQUE(domain, namespace) index
-- from the original table migration; it is intentionally left untouched here
-- (see the note above on why the index is not widened to include deleted_at).
ALTER TABLE website_domains
    ADD COLUMN platform_domain_id BIGINT UNSIGNED NULL DEFAULT NULL;
CREATE INDEX idx_website_domains_platform_domain_id ON website_domains(platform_domain_id);

-- +goose Down
DROP INDEX idx_website_domains_platform_domain_id ON website_domains;
ALTER TABLE website_domains DROP COLUMN platform_domain_id;

DROP TABLE IF EXISTS platform_domains;
