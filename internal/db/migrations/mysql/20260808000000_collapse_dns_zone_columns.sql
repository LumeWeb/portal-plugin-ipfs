-- +goose Up
-- Collapse duplicated references to one PowerDNS zone into canonical zone_id.
-- A binding has one zone. Historical code maintained ZoneID (delegation) and
-- dns_zone_id (website DNS) independently, so divergent rows can exist.
--
-- For delegation-owned bindings, ZoneID is authoritative because delegation
-- lifecycle reads it for DNSSEC, DS, SOA, and republish. Preserve a nonzero
-- delegation ZoneID. For all other rows, dns_zone_id supplies the hosting-zone
-- reference. Rows with ZoneID = 0 or NULL are still backfilled from dns_zone_id.
-- +goose StatementBegin
UPDATE website_domains
SET zone_id = dns_zone_id
WHERE dns_zone_id IS NOT NULL
  AND (zone_id IS NULL OR zone_id = 0 OR zone_id != dns_zone_id)
  AND NOT (zone_id IS NOT NULL AND zone_id != 0);
-- +goose StatementEnd

DROP INDEX idx_website_domains_dns_zone_id ON website_domains;
ALTER TABLE website_domains DROP COLUMN dns_zone_id;

-- +goose Down
-- +goose StatementBegin
ALTER TABLE website_domains ADD COLUMN dns_zone_id BIGINT UNSIGNED NULL DEFAULT NULL;
-- +goose StatementEnd
CREATE INDEX idx_website_domains_dns_zone_id ON website_domains(dns_zone_id);

-- +goose StatementBegin
UPDATE website_domains
SET dns_zone_id = zone_id
WHERE zone_id IS NOT NULL AND zone_id != 0;
-- +goose StatementEnd
