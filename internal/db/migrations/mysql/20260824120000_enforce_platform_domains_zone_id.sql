-- +goose Up
-- CreatePlatformDomain now auto-creates (idempotently) the operator's DNS zone
-- for every platform root and always stores the resulting zone ID on the
-- platform_domains row. A PlatformDomain without a provisioned zone is invalid
-- (claims write DNSLink/apex records into a dangling zone), so enforce the
-- invariant at the schema level by making zone_id NOT NULL.
ALTER TABLE platform_domains
    MODIFY COLUMN zone_id BIGINT UNSIGNED NOT NULL;

-- +goose Down
ALTER TABLE platform_domains
    MODIFY COLUMN zone_id BIGINT UNSIGNED NULL DEFAULT NULL;
