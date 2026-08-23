-- +goose Up
-- Introduce platform-owned DNS roots for free subdomains.
--
-- platform_domains is a minimal registry of roots (e.g. "pinner.site") that
-- the operator owns. Users may claim subdomains under these roots for their
-- websites. It exists purely as the trust anchor for the one-zone relaxation in
-- DelegatedDomainService.resolveManagedZone: a subdomain whose parent matches a
-- registered platform root is allowed to reuse the platform-owned zone even
-- when the binding's UserID differs from the zone owner.
--
-- website_domains.platform_domain_id is a nullable FK: NULL for user-owned
-- apex/normal bindings (today's behavior); set when the binding is a platform
-- subdomain minted under the referenced PlatformDomain.

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS platform_domains (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    domain VARCHAR(255) NOT NULL,
    namespace VARCHAR(32) NOT NULL,
    zone_id BIGINT UNSIGNED NULL DEFAULT NULL,
    enabled TINYINT(1) NOT NULL DEFAULT 1,
    deleted_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    -- Unique includes deleted_at (soft-delete tombstone) so a deleted root can
    -- be re-registered; live rows remain unique per (domain, namespace).
    UNIQUE KEY uk_platform_domains_domain_namespace (domain, namespace, deleted_at),
    INDEX idx_platform_domains_zone_id (zone_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- +goose StatementEnd

ALTER TABLE website_domains
    ADD COLUMN platform_domain_id BIGINT UNSIGNED NULL DEFAULT NULL;
CREATE INDEX idx_website_domains_platform_domain_id ON website_domains(platform_domain_id);

-- +goose Down
DROP INDEX idx_website_domains_platform_domain_id ON website_domains;
ALTER TABLE website_domains DROP COLUMN platform_domain_id;
DROP TABLE IF EXISTS platform_domains;
