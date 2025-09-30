-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ipfs_pins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id BLOB UNIQUE,
    user_id INTEGER,
    status TEXT,
    cid BLOB,
    name TEXT,
    origins TEXT,
    meta TEXT,
    delegates TEXT,
    info TEXT,
    parent_request_id BLOB,
    created_at DATETIME,
    updated_at DATETIME,
    deleted_at DATETIME
);

CREATE TABLE IF NOT EXISTS ipfs_blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cid VARBINARY(64) UNIQUE,
    size INTEGER,
    last_announcement DATETIME,
    ready BOOLEAN DEFAULT FALSE,
    created_at DATETIME,
    updated_at DATETIME,
    deleted_at DATETIME
);

CREATE INDEX IF NOT EXISTS idx_ipfs_blocks_last_announcement ON ipfs_blocks (last_announcement);
CREATE INDEX IF NOT EXISTS idx_ipfs_blocks_cid_last_announcement ON ipfs_blocks (cid, last_announcement);
CREATE INDEX IF NOT EXISTS idx_ipfs_blocks_ready ON ipfs_blocks (ready) WHERE ready = 0;

CREATE INDEX IF NOT EXISTS idx_ipfs_pins_created_at ON ipfs_pins (created_at);

CREATE TABLE IF NOT EXISTS ipfs_linked_blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER,
    child_id INTEGER,
    link_index INTEGER,
    created_at DATETIME,
    updated_at DATETIME,
    deleted_at DATETIME,
    FOREIGN KEY (parent_id) REFERENCES ipfs_blocks(id),
    FOREIGN KEY (child_id) REFERENCES ipfs_blocks(id),
    UNIQUE (parent_id, child_id, link_index)
);

CREATE INDEX IF NOT EXISTS ipfs_idx_linked_blocks_unique ON ipfs_linked_blocks (parent_id, child_id, link_index);

CREATE TABLE IF NOT EXISTS ipfs_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER,
    pin_request_id BLOB,
    parent_pin_request_id BLOB,
    created_at DATETIME,
    updated_at DATETIME,
    deleted_at DATETIME
);

CREATE TABLE IF NOT EXISTS ipfs_unixfs_nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    block_id INTEGER UNIQUE,
    name TEXT,
    type INTEGER,
    block_size INTEGER,
    child_cid TEXT,
    created_at DATETIME,
    updated_at DATETIME,
    deleted_at DATETIME,
    FOREIGN KEY (block_id) REFERENCES ipfs_blocks(id)
);

CREATE TABLE IF NOT EXISTS ipfs_file_paths (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    cid BLOB NOT NULL,
    path TEXT NOT NULL,
    name TEXT NOT NULL,
    type INTEGER NOT NULL,
    size INTEGER,
    is_directory BOOLEAN DEFAULT FALSE,
    parent_path TEXT,
    depth INTEGER DEFAULT 0,
    is_orphan BOOLEAN DEFAULT FALSE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    deleted_at DATETIME,
    
    UNIQUE (user_id, cid, path, deleted_at)
);

CREATE INDEX IF NOT EXISTS idx_ipfs_file_paths_user_path ON ipfs_file_paths (user_id, path);
CREATE INDEX IF NOT EXISTS idx_ipfs_file_paths_user_parent_path ON ipfs_file_paths (user_id, parent_path);
CREATE INDEX IF NOT EXISTS idx_ipfs_file_paths_user_parent_name ON ipfs_file_paths (user_id, parent_path, name);
CREATE INDEX IF NOT EXISTS idx_ipfs_file_paths_user_directory_depth ON ipfs_file_paths (user_id, is_directory, depth);
CREATE INDEX IF NOT EXISTS idx_ipfs_file_paths_user_orphan ON ipfs_file_paths (user_id, is_orphan);
CREATE INDEX IF NOT EXISTS idx_ipfs_file_paths_deleted_at ON ipfs_file_paths (deleted_at) WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_ipfs_unixfs_nodes_block_id ON ipfs_unixfs_nodes (block_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_ipfs_blocks_last_announcement;
DROP INDEX IF EXISTS idx_ipfs_blocks_cid_last_announcement;
DROP INDEX IF EXISTS idx_ipfs_blocks_ready;
DROP INDEX IF EXISTS ipfs_idx_linked_blocks_unique;
DROP INDEX IF EXISTS idx_ipfs_unixfs_nodes_block_id;
DROP INDEX IF EXISTS idx_ipfs_file_paths_deleted_at;
DROP INDEX IF EXISTS idx_ipfs_pins_created_at;

DROP TABLE IF EXISTS ipfs_pins;
DROP TABLE IF EXISTS ipfs_blocks;
DROP TABLE IF EXISTS ipfs_linked_blocks;
DROP TABLE IF EXISTS ipfs_requests;
DROP TABLE IF EXISTS ipfs_unixfs_nodes;
DROP TABLE IF EXISTS ipfs_file_paths;
-- +goose StatementEnd
