-- +goose Up
-- Durable, replayable website lifecycle event log owned by the portal. The
-- gateway consumes this through the SSE Last-Event-ID cursor and the
-- /internal/websites/changes reconciliation endpoint. The auto-increment ID is
-- the durable cursor / high-water mark; it is never reused, so consumers can
-- advance past it without ambiguity.
CREATE TABLE IF NOT EXISTS ipfs_website_events (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_type VARCHAR(50) NOT NULL,
    domain VARCHAR(255) NOT NULL,
    cid VARCHAR(255) NOT NULL DEFAULT '',
    website_id BIGINT UNSIGNED NULL,
    user_id BIGINT UNSIGNED NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    KEY idx_ipfs_website_events_event_type (event_type),
    KEY idx_ipfs_website_events_domain (domain),
    KEY idx_ipfs_website_events_website_id (website_id),
    KEY idx_ipfs_website_events_user_id (user_id),
    KEY idx_ipfs_website_events_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- +goose Down
DROP TABLE IF EXISTS ipfs_website_events;
