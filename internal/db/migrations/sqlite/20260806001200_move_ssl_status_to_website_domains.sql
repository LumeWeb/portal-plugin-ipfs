-- +goose Up
-- Move SSL certificate state from ipfs_websites (one row per website) to
-- website_domains (one row per bound hostname). SSL is a per-domain property:
-- each bound domain may carry its own certificate/issuance lifecycle, so the
-- granularity belongs on the domain binding.

-- 1. Add per-domain SSL columns to website_domains.
ALTER TABLE website_domains ADD COLUMN ssl_status TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE website_domains ADD COLUMN ssl_error TEXT NULL;
ALTER TABLE website_domains ADD COLUMN ssl_issued_at TIMESTAMP NULL DEFAULT NULL;
ALTER TABLE website_domains ADD COLUMN ssl_last_updated_at TIMESTAMP NULL DEFAULT NULL;

CREATE INDEX idx_website_domains_ssl_status ON website_domains(ssl_status);
CREATE INDEX idx_website_domains_ssl_issued_at ON website_domains(ssl_issued_at);
CREATE INDEX idx_website_domains_ssl_last_updated_at ON website_domains(ssl_last_updated_at);

-- +goose StatementBegin
-- 2. Backfill the apex (primary) domain binding from its owning website's SSL
--    state, preserving the existing website-level cert state. SQLite has no
--    UPDATE ... JOIN, so use a correlated subquery keyed on the primary domain.
UPDATE website_domains
SET ssl_status = (SELECT w.ssl_status FROM ipfs_websites w WHERE w.id = website_domains.website_id AND w.domain = website_domains.domain),
    ssl_error = (SELECT w.ssl_error FROM ipfs_websites w WHERE w.id = website_domains.website_id AND w.domain = website_domains.domain),
    ssl_issued_at = (SELECT w.ssl_issued_at FROM ipfs_websites w WHERE w.id = website_domains.website_id AND w.domain = website_domains.domain),
    ssl_last_updated_at = (SELECT w.ssl_last_updated_at FROM ipfs_websites w WHERE w.id = website_domains.website_id AND w.domain = website_domains.domain)
WHERE EXISTS (
    SELECT 1 FROM ipfs_websites w WHERE w.id = website_domains.website_id AND w.domain = website_domains.domain
);
-- +goose StatementEnd

-- 3. Remove the now-migrated SSL columns from ipfs_websites.
DROP INDEX IF EXISTS idx_ipfs_websites_ssl_status;
DROP INDEX IF EXISTS idx_ipfs_websites_ssl_issued_at;
DROP INDEX IF EXISTS idx_ipfs_websites_ssl_last_updated_at;

ALTER TABLE ipfs_websites DROP COLUMN ssl_status;
ALTER TABLE ipfs_websites DROP COLUMN ssl_error;
ALTER TABLE ipfs_websites DROP COLUMN ssl_issued_at;
ALTER TABLE ipfs_websites DROP COLUMN ssl_last_updated_at;

-- +goose Down
-- 1. Re-add the SSL columns to ipfs_websites.
ALTER TABLE ipfs_websites ADD COLUMN ssl_status TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE ipfs_websites ADD COLUMN ssl_error TEXT NULL;
ALTER TABLE ipfs_websites ADD COLUMN ssl_issued_at TIMESTAMP NULL DEFAULT NULL;
ALTER TABLE ipfs_websites ADD COLUMN ssl_last_updated_at TIMESTAMP NULL DEFAULT NULL;

CREATE INDEX idx_ipfs_websites_ssl_status ON ipfs_websites(ssl_status);
CREATE INDEX idx_ipfs_websites_ssl_issued_at ON ipfs_websites(ssl_issued_at);
CREATE INDEX idx_ipfs_websites_ssl_last_updated_at ON ipfs_websites(ssl_last_updated_at);

-- +goose StatementBegin
-- 2. Restore website SSL state from the apex domain binding.
UPDATE ipfs_websites
SET ssl_status = (SELECT wd.ssl_status FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.domain = ipfs_websites.domain),
    ssl_error = (SELECT wd.ssl_error FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.domain = ipfs_websites.domain),
    ssl_issued_at = (SELECT wd.ssl_issued_at FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.domain = ipfs_websites.domain),
    ssl_last_updated_at = (SELECT wd.ssl_last_updated_at FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.domain = ipfs_websites.domain)
WHERE EXISTS (
    SELECT 1 FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.domain = ipfs_websites.domain
);
-- +goose StatementEnd

-- 3. Drop the per-domain SSL columns from website_domains.
DROP INDEX IF EXISTS idx_website_domains_ssl_status;
DROP INDEX IF EXISTS idx_website_domains_ssl_issued_at;
DROP INDEX IF EXISTS idx_website_domains_ssl_last_updated_at;

ALTER TABLE website_domains DROP COLUMN ssl_status;
ALTER TABLE website_domains DROP COLUMN ssl_error;
ALTER TABLE website_domains DROP COLUMN ssl_issued_at;
ALTER TABLE website_domains DROP COLUMN ssl_last_updated_at;
