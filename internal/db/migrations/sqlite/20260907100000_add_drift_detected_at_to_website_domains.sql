-- +goose Up
-- Add the janitor's route-drift backoff marker to website_domains. The
-- janitor is report-only for route drift (manual on-chain conversion is the
-- only conversion path), so a drifted binding stays in the delegation
-- lifecycle pool; this timestamp throttles how often the janitor re-probes
-- its external route before reporting again.
ALTER TABLE website_domains ADD COLUMN drift_detected_at DATETIME NULL;

CREATE INDEX idx_website_domains_drift_detected_at ON website_domains(drift_detected_at);

-- +goose Down
DROP INDEX IF EXISTS idx_website_domains_drift_detected_at;

ALTER TABLE website_domains DROP COLUMN drift_detected_at;
