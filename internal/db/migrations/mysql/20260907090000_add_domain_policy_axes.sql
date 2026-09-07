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
-- Do NOT drop populated columns during incident rollback (pre-axes readers
-- ignore them). The development-only drop migration lives outside the
-- embedded set in internal/db/migrations/dev_only/ and is never applied by
-- goose automatically.

-- +goose StatementBegin
ALTER TABLE website_domains
    ADD COLUMN lifecycle_status VARCHAR(32) NULL DEFAULT NULL,
    ADD COLUMN authority_locus VARCHAR(32) NULL DEFAULT NULL,
    ADD COLUMN resolution_route VARCHAR(32) NULL DEFAULT NULL,
    ADD COLUMN resolution_backend VARCHAR(128) NULL DEFAULT NULL,
    ADD COLUMN hosting_request VARCHAR(32) NULL DEFAULT NULL,
    ADD COLUMN policy_id VARCHAR(128) NULL DEFAULT NULL,
    ADD COLUMN policy_version INT NULL DEFAULT NULL,
    ADD COLUMN reconciliation_status VARCHAR(32) NULL DEFAULT NULL;
-- +goose StatementEnd

-- Backfill scan support: the bounded application backfill selects unmapped
-- rows ordered by id, so the filter is worth an index.
CREATE INDEX idx_website_domains_reconciliation_status ON website_domains(reconciliation_status);

-- +goose Down
-- +goose StatementBegin
ALTER TABLE website_domains
    DROP INDEX idx_website_domains_reconciliation_status,
    DROP COLUMN lifecycle_status,
    DROP COLUMN authority_locus,
    DROP COLUMN resolution_route,
    DROP COLUMN resolution_backend,
    DROP COLUMN hosting_request,
    DROP COLUMN policy_id,
    DROP COLUMN policy_version,
    DROP COLUMN reconciliation_status;
-- +goose StatementEnd
