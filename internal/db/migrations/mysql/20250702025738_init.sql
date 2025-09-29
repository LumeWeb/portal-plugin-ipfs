-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ipfs_pins (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    request_id BINARY(16) UNIQUE,
    user_id INTEGER,
    status TEXT,
    cid VARBINARY(64),
    name TEXT,
    origins TEXT,
    meta TEXT,
    delegates TEXT,
    info TEXT,
    parent_request_id BINARY(16),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ipfs_blocks (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    cid VARBINARY(64) UNIQUE,
    size INTEGER,
    last_announcement TIMESTAMP,
    ready BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE INDEX idx_ipfs_blocks_last_announcement ON ipfs_blocks (last_announcement);
CREATE INDEX idx_ipfs_blocks_cid_last_announcement ON ipfs_blocks (cid, last_announcement);
CREATE INDEX idx_ipfs_blocks_ready ON ipfs_blocks (ready);

CREATE INDEX idx_ipfs_pins_created_at ON ipfs_pins (created_at);
CREATE INDEX idx_ipfs_pins_status_user_created_at ON ipfs_pins (status, user_id, created_at);

CREATE TABLE IF NOT EXISTS ipfs_linked_blocks (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    parent_id INTEGER,
    child_id INTEGER,
    link_index INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP,
    FOREIGN KEY (parent_id) REFERENCES ipfs_blocks(id),
    FOREIGN KEY (child_id) REFERENCES ipfs_blocks(id),
    UNIQUE (parent_id, child_id, link_index)
);

CREATE TABLE IF NOT EXISTS ipfs_requests (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    request_id BIGINT UNSIGNED UNIQUE,
    pin_request_id VARBINARY(64),
    parent_pin_request_id VARBINARY(64),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE INDEX idx_ipfs_requests_pin_request_id ON ipfs_requests (pin_request_id);
CREATE INDEX idx_ipfs_requests_parent_pin_request_id ON ipfs_requests (parent_pin_request_id);

CREATE TABLE IF NOT EXISTS ipfs_unixfs_nodes (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    block_id INTEGER UNIQUE,
    name TEXT,
    type INTEGER,
    block_size INTEGER,
    child_cid TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP,
    FOREIGN KEY (block_id) REFERENCES ipfs_blocks(id)
);

CREATE TABLE IF NOT EXISTS ipfs_file_paths (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    user_id INTEGER NOT NULL,
    cid VARBINARY(64) NOT NULL,
    path VARCHAR(1000) NOT NULL,
    name VARCHAR(255) NOT NULL,
    type TINYINT NOT NULL,
    size BIGINT,
    is_directory BOOLEAN DEFAULT FALSE,
    parent_path VARCHAR(1000),
    depth INTEGER DEFAULT 0,
    is_orphan BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    
    UNIQUE KEY (user_id, cid),
    KEY (user_id, path),
    KEY (user_id, parent_path),
    KEY (user_id, parent_path, name),
    KEY (user_id, is_directory, depth),
    KEY (user_id, is_orphan)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE ipfs_blocks DROP INDEX idx_ipfs_blocks_last_announcement;
ALTER TABLE ipfs_blocks DROP INDEX idx_ipfs_blocks_cid_last_announcement;
ALTER TABLE ipfs_blocks DROP INDEX idx_ipfs_blocks_ready;
ALTER TABLE ipfs_pins DROP INDEX idx_ipfs_pins_created_at;
ALTER TABLE ipfs_pins DROP INDEX idx_ipfs_pins_status_user_created_at;
ALTER TABLE ipfs_linked_blocks DROP INDEX ipfs_idx_linked_blocks_unique;
ALTER TABLE ipfs_requests DROP INDEX idx_ipfs_requests_pin_request_id;
ALTER TABLE ipfs_requests DROP INDEX idx_ipfs_requests_parent_pin_request_id;

DROP TABLE ipfs_linked_blocks;
DROP TABLE ipfs_unixfs_nodes;
DROP TABLE ipfs_file_paths;
DROP TABLE ipfs_pins;
DROP TABLE ipfs_requests;
DROP TABLE ipfs_blocks;
-- +goose StatementEnd
