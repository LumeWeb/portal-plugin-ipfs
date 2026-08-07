-- +goose Up
-- Move all DNS-hosting state from ipfs_websites (one row per website) to
-- website_domains (one row per bound hostname). DNS hosting is a per-domain
-- property: each bound domain carries its own dns_hosting_enabled flag, DNS
-- zone, and IPNS key. The website's denormalized `domain` string is replaced
-- by a `primary_domain_id` FK to website_domains.id.
--
-- Mirrors 20260806001200_move_ssl_status_to_website_domains (SQLite variant).

-- 1. Add per-domain DNS columns to website_domains.
ALTER TABLE website_domains ADD COLUMN dns_hosting_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE website_domains ADD COLUMN dns_zone_id BIGINT NULL DEFAULT NULL;
ALTER TABLE website_domains ADD COLUMN ipns_key_id BIGINT NULL DEFAULT NULL;

CREATE INDEX idx_website_domains_dns_zone_id ON website_domains(dns_zone_id);
CREATE INDEX idx_website_domains_ipns_key_id ON website_domains(ipns_key_id);

-- +goose StatementBegin
-- 2. Backfill the apex (primary) domain binding from its owning website's DNS
--    state. SQLite has no UPDATE ... JOIN, so use a correlated subquery keyed
--    on the primary domain.
UPDATE website_domains
SET dns_hosting_enabled = (SELECT w.dns_enabled FROM ipfs_websites w WHERE w.id = website_domains.website_id AND w.domain = website_domains.domain),
    dns_zone_id = (SELECT w.dns_zone_id FROM ipfs_websites w WHERE w.id = website_domains.website_id AND w.domain = website_domains.domain),
    ipns_key_id = (SELECT w.ipns_key_id FROM ipfs_websites w WHERE w.id = website_domains.website_id AND w.domain = website_domains.domain)
WHERE EXISTS (
    SELECT 1 FROM ipfs_websites w WHERE w.id = website_domains.website_id AND w.domain = website_domains.domain
);
-- +goose StatementEnd

-- 3. Add primary_domain_id FK, backfill it, then drop the redundant
--    website-level domain/DNS columns.
ALTER TABLE ipfs_websites ADD COLUMN primary_domain_id BIGINT NULL DEFAULT NULL;
CREATE INDEX idx_ipfs_websites_primary_domain_id ON ipfs_websites(primary_domain_id);

-- +goose StatementBegin
UPDATE ipfs_websites
SET primary_domain_id = (SELECT wd.id FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.domain = ipfs_websites.domain)
WHERE EXISTS (
    SELECT 1 FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.domain = ipfs_websites.domain
);
-- +goose StatementEnd

DROP INDEX IF EXISTS idx_ipfs_websites_domain;
DROP INDEX IF EXISTS idx_ipfs_websites_dns_zone_id;
DROP INDEX IF EXISTS idx_ipfs_websites_ipns_key_id;

ALTER TABLE ipfs_websites DROP COLUMN domain;
ALTER TABLE ipfs_websites DROP COLUMN dns_zone_id;
ALTER TABLE ipfs_websites DROP COLUMN ipns_key_id;
ALTER TABLE ipfs_websites DROP COLUMN dns_enabled;

-- +goose Down
-- 1. Re-add the DNS columns to ipfs_websites.
ALTER TABLE ipfs_websites ADD COLUMN domain VARCHAR(255) NOT NULL DEFAULT '';
ALTER TABLE ipfs_websites ADD COLUMN dns_zone_id BIGINT NULL DEFAULT NULL;
ALTER TABLE ipfs_websites ADD COLUMN ipns_key_id BIGINT NULL DEFAULT NULL;
ALTER TABLE ipfs_websites ADD COLUMN dns_enabled INTEGER NOT NULL DEFAULT 0;

CREATE INDEX idx_ipfs_websites_domain ON ipfs_websites(domain);
CREATE INDEX idx_ipfs_websites_dns_zone_id ON ipfs_websites(dns_zone_id);
CREATE INDEX idx_ipfs_websites_ipns_key_id ON ipfs_websites(ipns_key_id);

-- +goose StatementBegin
-- 2. Restore website DNS state from the apex (primary) domain binding.
UPDATE ipfs_websites
SET domain = (SELECT wd.domain FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.id = ipfs_websites.primary_domain_id),
    dns_enabled = (SELECT wd.dns_hosting_enabled FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.id = ipfs_websites.primary_domain_id),
    dns_zone_id = (SELECT wd.dns_zone_id FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.id = ipfs_websites.primary_domain_id),
    ipns_key_id = (SELECT wd.ipns_key_id FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.id = ipfs_websites.primary_domain_id)
WHERE EXISTS (
    SELECT 1 FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.id = ipfs_websites.primary_domain_id
);
-- +goose StatementEnd

DROP INDEX IF EXISTS idx_ipfs_websites_primary_domain_id;
ALTER TABLE ipfs_websites DROP COLUMN primary_domain_id;

-- 3. Drop the per-domain DNS columns from website_domains.
DROP INDEX IF EXISTS idx_website_domains_dns_zone_id;
DROP INDEX IF EXISTS idx_website_domains_ipns_key_id;

ALTER TABLE website_domains DROP COLUMN dns_hosting_enabled;
ALTER TABLE website_domains DROP COLUMN dns_zone_id;
ALTER TABLE website_domains DROP COLUMN ipns_key_id;
