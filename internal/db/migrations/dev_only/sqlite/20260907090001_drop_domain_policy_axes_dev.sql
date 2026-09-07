-- DEVELOPMENT-ONLY ROLLBACK MIGRATION (SQLite variant).
--
-- This file is deliberately OUTSIDE the embedded migration set
-- (migrations.go embeds only mysql/*.sql and sqlite/*.sql, so goose never
-- applies internal/db/migrations/dev_only/). Apply it by hand in a development
-- database only:
--
--   goose -dir internal/db/migrations/dev_only/sqlite <db-conn> down/up ...
--
-- PRODUCTION ROLLBACK NEVER RUNS THIS: production rollback is "switch reads
-- and writes back to the legacy fields and leave the nullable columns in
-- place" — populated axis columns must not be dropped to restore
-- compatibility (see internal/db/migrations/sqlite/20260907090000 header).
-- This migration exists so a developer machine can return to the exact
-- pre-axes schema and migration history after testing the axes feature.

-- +goose Up
-- Back to the pre-axes website_domains shape.
DROP INDEX IF EXISTS idx_website_domains_reconciliation_status;

ALTER TABLE website_domains DROP COLUMN reconciliation_status;
ALTER TABLE website_domains DROP COLUMN policy_version;
ALTER TABLE website_domains DROP COLUMN policy_id;
ALTER TABLE website_domains DROP COLUMN hosting_request;
ALTER TABLE website_domains DROP COLUMN resolution_backend;
ALTER TABLE website_domains DROP COLUMN resolution_route;
ALTER TABLE website_domains DROP COLUMN authority_locus;
ALTER TABLE website_domains DROP COLUMN lifecycle_status;

-- +goose Down
ALTER TABLE website_domains ADD COLUMN lifecycle_status TEXT NULL;
ALTER TABLE website_domains ADD COLUMN authority_locus TEXT NULL;
ALTER TABLE website_domains ADD COLUMN resolution_route TEXT NULL;
ALTER TABLE website_domains ADD COLUMN resolution_backend TEXT NULL;
ALTER TABLE website_domains ADD COLUMN hosting_request TEXT NULL;
ALTER TABLE website_domains ADD COLUMN policy_id TEXT NULL;
ALTER TABLE website_domains ADD COLUMN policy_version INTEGER NULL;
ALTER TABLE website_domains ADD COLUMN reconciliation_status TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_website_domains_reconciliation_status ON website_domains(reconciliation_status);
