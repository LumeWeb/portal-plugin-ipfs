-- DEVELOPMENT-ONLY ROLLBACK MIGRATION (MySQL variant).
--
-- This file is deliberately OUTSIDE the embedded migration set
-- (migrations.go embeds only mysql/*.sql and sqlite/*.sql, so goose never
-- applies internal/db/migrations/dev_only/). Apply it by hand in a development
-- database only:
--
--   goose -dir internal/db/migrations/dev_only/mysql <db-conn> down/up ...
--
-- PRODUCTION ROLLBACK NEVER RUNS THIS: production rollback is "switch reads
-- and writes back to the legacy fields and leave the nullable columns in
-- place" — populated axis columns must not be dropped to restore
-- compatibility (see internal/db/migrations/mysql/20260907090000 header).
-- This migration exists so a developer machine can return to the exact
-- pre-axes schema and migration history after testing the axes feature.

-- +goose Up
-- Back to the pre-axes website_domains shape.
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

-- +goose Down
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
CREATE INDEX idx_website_domains_reconciliation_status ON website_domains(reconciliation_status);
