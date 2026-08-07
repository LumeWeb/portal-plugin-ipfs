-- +goose Up
-- Collapse duplicated references to one PowerDNS zone into canonical zone_id.
-- A binding has one zone. Historical code maintained ZoneID (delegation) and
-- dns_zone_id (website DNS) independently, so divergent rows can exist.
-- For delegation-owned bindings, preserve nonzero ZoneID because delegation
-- lifecycle reads it for DNSSEC, DS, SOA, and republish. Other rows use
-- dns_zone_id as the canonical reference.
-- +goose StatementBegin
UPDATE website_domains
SET zone_id = dns_zone_id
WHERE dns_zone_id IS NOT NULL
  AND (zone_id IS NULL OR zone_id = 0 OR zone_id != dns_zone_id)
  AND NOT (zone_id IS NOT NULL AND zone_id != 0);
-- +goose StatementEnd

DROP INDEX IF EXISTS idx_website_domains_dns_zone_id;
ALTER TABLE website_domains DROP COLUMN dns_zone_id;

-- +goose Down
ALTER TABLE website_domains ADD COLUMN dns_zone_id BIGINT NULL DEFAULT NULL;
CREATE INDEX idx_website_domains_dns_zone_id ON website_domains(dns_zone_id);

-- +goose StatementBegin
UPDATE website_domains
SET dns_zone_id = zone_id
WHERE zone_id IS NOT NULL AND zone_id != 0;
-- +goose StatementEnd
