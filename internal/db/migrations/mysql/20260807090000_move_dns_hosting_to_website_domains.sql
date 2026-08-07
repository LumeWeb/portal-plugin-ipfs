-- +goose Up
-- Move DNS-hosting state from ipfs_websites (one row per website) to
-- website_domains (one row per bound hostname). DNS hosting is a per-domain
-- property: each bound domain carries its own dns_hosting_enabled flag and DNS
-- hosting zone, so one domain on a website can be fully DNS-managed while
-- another is not. The website's denormalized `domain` string is replaced by a
-- `primary_domain_id` FK to website_domains.id, and the website keeps its own
-- IPNS key (the key belongs to the site's target, not the domain).
--
-- Mirrors the prior SSL migration (20260806001200): add per-domain columns,
-- backfill the apex binding keyed on w.domain = wd.domain, then drop the now
-- redundant website-level columns.

-- 1. Add per-domain DNS columns to website_domains.
-- +goose StatementBegin
ALTER TABLE website_domains
    ADD COLUMN dns_hosting_enabled TINYINT(1) NOT NULL DEFAULT 0,
    ADD COLUMN dns_zone_id BIGINT UNSIGNED NULL DEFAULT NULL;
-- +goose StatementEnd
CREATE INDEX idx_website_domains_dns_zone_id ON website_domains(dns_zone_id);

-- +goose StatementBegin
-- 2. Backfill the apex (primary) domain binding from its owning website's DNS
--    state, preserving the existing website-level DNS hosting. Only the
--    binding whose domain matches the website's primary domain inherits it;
--    additional domain bindings stay disabled (they had no prior per-domain
--    DNS state, exactly as with the SSL migration).
UPDATE website_domains wd
JOIN ipfs_websites w ON w.id = wd.website_id AND w.domain = wd.domain
SET wd.dns_hosting_enabled = w.dns_enabled,
    wd.dns_zone_id = w.dns_zone_id;
-- +goose StatementEnd

-- 3. Add primary_domain_id FK, backfill it, then drop the now-redundant
--    website-level domain/DNS-host columns (drop the indexes first, then the
--    columns, for MySQL ordering). ipns_key_id stays on ipfs_websites.
ALTER TABLE ipfs_websites
    ADD COLUMN primary_domain_id BIGINT UNSIGNED NULL DEFAULT NULL;

-- +goose StatementBegin
UPDATE ipfs_websites w
JOIN website_domains wd ON wd.website_id = w.id AND wd.domain = w.domain
SET w.primary_domain_id = wd.id;
-- +goose StatementEnd

ALTER TABLE ipfs_websites
    DROP INDEX idx_ipfs_websites_domain,
    DROP INDEX idx_ipfs_websites_dns_zone_id;

ALTER TABLE ipfs_websites
    DROP COLUMN domain,
    DROP COLUMN dns_zone_id,
    DROP COLUMN dns_enabled;

-- +goose Down
-- +goose StatementBegin
-- 1. Re-add the DNS-host columns to ipfs_websites.
ALTER TABLE ipfs_websites
    ADD COLUMN domain VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN dns_zone_id BIGINT UNSIGNED NULL DEFAULT NULL,
    ADD COLUMN dns_enabled TINYINT(1) NOT NULL DEFAULT 0;
-- +goose StatementEnd
ALTER TABLE ipfs_websites
    ADD KEY idx_ipfs_websites_domain (domain),
    ADD KEY idx_ipfs_websites_dns_zone_id (dns_zone_id);

-- +goose StatementBegin
-- 2. Restore website DNS state from the apex (primary) domain binding.
UPDATE ipfs_websites w
JOIN website_domains wd ON wd.website_id = w.id AND wd.id = w.primary_domain_id
SET w.domain = wd.domain,
    w.dns_enabled = wd.dns_hosting_enabled,
    w.dns_zone_id = wd.dns_zone_id;
-- +goose StatementEnd

-- Drop the primary_domain_id FK column now that it has been read back.
ALTER TABLE ipfs_websites
    DROP INDEX idx_ipfs_websites_primary_domain_id,
    DROP COLUMN primary_domain_id;

-- +goose StatementBegin
-- 3. Drop the per-domain DNS columns from website_domains (indexes first, then
--    the columns, in a single ALTER for MySQL).
ALTER TABLE website_domains
    DROP INDEX idx_website_domains_dns_zone_id,
    DROP COLUMN dns_hosting_enabled,
    DROP COLUMN dns_zone_id;
-- +goose StatementEnd
