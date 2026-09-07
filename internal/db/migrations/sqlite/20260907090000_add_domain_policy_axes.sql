-- +goose Up
-- Persist the independent domain-policy axes as NULLABLE columns on
-- website_domains. Storage
-- representation only: legacy `status`, `dns_hosting_enabled`, `zone_id`, and
-- delegation data are untouched and remain authoritative for the legacy
-- mapper; the new columns are dual-written alongside them and left NULL on
-- ambiguous rows (no SQL backfill of ambiguous rows — an application backfill
-- derives and validates axes row by row instead).
--
-- Column values are the string forms of internal/domainpolicy enums
-- (lifecycle_status, authority_locus, resolution_route, hosting_request),
-- a backend identifier (resolution_backend), the profile identity/version
-- (policy_id, policy_version), and reconciliation_status ("mapped" when a
-- complete valid axis set is persisted, "error" when mapping failed and no
-- axis was guessed).
--
-- PRODUCTION ROLLBACK: old readers + leave these nullable columns in place.
-- Do NOT drop populated columns during incident rollback. The development-only
-- drop migration lives outside the embedded set in
-- internal/db/migrations/dev_only/ and is never applied by goose automatically.

ALTER TABLE website_domains ADD COLUMN lifecycle_status TEXT NULL;
ALTER TABLE website_domains ADD COLUMN authority_locus TEXT NULL;
ALTER TABLE website_domains ADD COLUMN resolution_route TEXT NULL;
ALTER TABLE website_domains ADD COLUMN resolution_backend TEXT NULL;
ALTER TABLE website_domains ADD COLUMN hosting_request TEXT NULL;
ALTER TABLE website_domains ADD COLUMN policy_id TEXT NULL;
ALTER TABLE website_domains ADD COLUMN policy_version INTEGER NULL;
ALTER TABLE website_domains ADD COLUMN reconciliation_status TEXT NULL;

-- Backfill scan support: the bounded application backfill selects unmapped
-- rows, so the filter is worth an index.
CREATE INDEX IF NOT EXISTS idx_website_domains_reconciliation_status ON website_domains(reconciliation_status);

-- +goose Down
DROP INDEX IF EXISTS idx_website_domains_reconciliation_status;

ALTER TABLE website_domains DROP COLUMN reconciliation_status;
ALTER TABLE website_domains DROP COLUMN policy_version;
ALTER TABLE website_domains DROP COLUMN policy_id;
ALTER TABLE website_domains DROP COLUMN hosting_request;
ALTER TABLE website_domains DROP COLUMN resolution_backend;
ALTER TABLE website_domains DROP COLUMN resolution_route;
ALTER TABLE website_domains DROP COLUMN authority_locus;
ALTER TABLE website_domains DROP COLUMN lifecycle_status;
