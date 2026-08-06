-- +goose Up
-- Move SSL certificate state from ipfs_websites (one row per website) to
-- website_domains (one row per bound hostname). SSL is a per-domain property:
-- each bound domain may carry its own certificate/issuance lifecycle, so the
-- granularity belongs on the domain binding. Caddy already reports SSL events
-- per-domain, so this aligns the storage grain with the data arriving.

-- 1. Add per-domain SSL columns to website_domains.
-- +goose StatementBegin
ALTER TABLE website_domains
    ADD COLUMN ssl_status VARCHAR(50) NOT NULL DEFAULT 'pending',
    ADD COLUMN ssl_error TEXT,
    ADD COLUMN ssl_issued_at TIMESTAMP NULL DEFAULT NULL,
    ADD COLUMN ssl_last_updated_at TIMESTAMP NULL DEFAULT NULL;
-- +goose StatementEnd

CREATE INDEX idx_website_domains_ssl_status ON website_domains(ssl_status);
CREATE INDEX idx_website_domains_ssl_issued_at ON website_domains(ssl_issued_at);
CREATE INDEX idx_website_domains_ssl_last_updated_at ON website_domains(ssl_last_updated_at);

-- +goose StatementBegin
-- 2. Backfill the apex (primary) domain binding from its owning website's SSL
--    state, preserving the existing website-level cert state. Only the binding
--    whose domain matches the website's primary domain inherits it; additional
--    domain bindings stay 'pending' (they had no prior per-domain cert state).
UPDATE website_domains wd
JOIN ipfs_websites w ON w.id = wd.website_id AND w.domain = wd.domain
SET wd.ssl_status = w.ssl_status,
    wd.ssl_error = w.ssl_error,
    wd.ssl_issued_at = w.ssl_issued_at,
    wd.ssl_last_updated_at = w.ssl_last_updated_at;
-- +goose StatementEnd

-- 3. Remove the now-migrated SSL columns from ipfs_websites (drop the indexes
--    first, then the columns, for MySQL ordering).
ALTER TABLE ipfs_websites
    DROP INDEX idx_ipfs_websites_ssl_status,
    DROP INDEX idx_ipfs_websites_ssl_issued_at,
    DROP INDEX idx_ipfs_websites_ssl_last_updated_at;

ALTER TABLE ipfs_websites
    DROP COLUMN ssl_status,
    DROP COLUMN ssl_error,
    DROP COLUMN ssl_issued_at,
    DROP COLUMN ssl_last_updated_at;

-- +goose Down
-- +goose StatementBegin
-- 1. Re-add the SSL columns to ipfs_websites.
ALTER TABLE ipfs_websites
    ADD COLUMN ssl_status VARCHAR(50) NOT NULL DEFAULT 'pending',
    ADD COLUMN ssl_error TEXT,
    ADD COLUMN ssl_issued_at TIMESTAMP NULL DEFAULT NULL,
    ADD COLUMN ssl_last_updated_at TIMESTAMP NULL DEFAULT NULL,
    ADD KEY idx_ipfs_websites_ssl_status (ssl_status),
    ADD KEY idx_ipfs_websites_ssl_issued_at (ssl_issued_at),
    ADD KEY idx_ipfs_websites_ssl_last_updated_at (ssl_last_updated_at);
-- +goose StatementEnd

-- +goose StatementBegin
-- 2. Restore website SSL state from the apex domain binding.
UPDATE ipfs_websites w
JOIN website_domains wd ON wd.website_id = w.id AND wd.domain = w.domain
SET w.ssl_status = wd.ssl_status,
    w.ssl_error = wd.ssl_error,
    w.ssl_issued_at = wd.ssl_issued_at,
    w.ssl_last_updated_at = wd.ssl_last_updated_at;
-- +goose StatementEnd

-- +goose StatementBegin
-- 3. Drop the per-domain SSL columns from website_domains (indexes first, then
--    the columns, in a single ALTER for MySQL).
ALTER TABLE website_domains
    DROP INDEX idx_website_domains_ssl_status,
    DROP INDEX idx_website_domains_ssl_issued_at,
    DROP INDEX idx_website_domains_ssl_last_updated_at,
    DROP COLUMN ssl_status,
    DROP COLUMN ssl_error,
    DROP COLUMN ssl_issued_at,
    DROP COLUMN ssl_last_updated_at;
-- +goose StatementEnd
