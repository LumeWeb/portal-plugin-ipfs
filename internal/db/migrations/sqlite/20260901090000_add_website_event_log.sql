-- +goose Up
-- Durable, replayable website lifecycle event log owned by the portal. The
-- gateway consumes this through the SSE Last-Event-ID cursor and the
-- /internal/websites/changes reconciliation endpoint.
CREATE TABLE IF NOT EXISTS ipfs_website_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    domain TEXT NOT NULL,
    cid TEXT NOT NULL DEFAULT '',
    website_id INTEGER NULL,
    user_id INTEGER NULL,
    created_at DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ipfs_website_events_event_type ON ipfs_website_events(event_type);
CREATE INDEX IF NOT EXISTS idx_ipfs_website_events_domain ON ipfs_website_events(domain);
CREATE INDEX IF NOT EXISTS idx_ipfs_website_events_website_id ON ipfs_website_events(website_id);
CREATE INDEX IF NOT EXISTS idx_ipfs_website_events_user_id ON ipfs_website_events(user_id);
CREATE INDEX IF NOT EXISTS idx_ipfs_website_events_created_at ON ipfs_website_events(created_at);

-- +goose Down
DROP TABLE IF EXISTS ipfs_website_events;
