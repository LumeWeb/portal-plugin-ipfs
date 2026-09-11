-- +goose Up
-- Workspace: a persistent, resource-limited authoring environment for an
-- existing website, provisioned on a runtime provider (Coolify for the MVP).
--
-- Runtime-agnostic entity; see workspace.go and the MySQL twin of this
-- migration for the full rationale. Unique keys are STRICT (no deleted_at):
-- SQLite treats NULL deleted_at values as distinct inside a UNIQUE index, so
-- widening the key with deleted_at would allow duplicate live rows and defeat
-- duplicate-key race detection. A soft-deleted workspace keeps its keys until
-- the delete slice purges the tombstone (deleted_at IS NOT NULL) before
-- re-provisioning.
--
-- Reconcile scheduling state is part of the initial schema because it is
-- created together with the table (pre-release):
--   - retry_count       : consecutive reconcile failures, drives bounded backoff.
--   - next_retry_at     : the earliest time a failed workspace is re-selected.
--   - last_reconcile_at : when the reconciler last processed the workspace,
--                         used to decide when a ready/suspended workspace needs
--                         a drift check.
--   The reverse-proxy Basic Auth credentials (proxy_username/proxy_password)
--   are defined inline as part of this pre-release consolidated initial
--   schema (they were previously a follow-up ALTER migration).
--
-- +goose StatementBegin
-- The workspace uses ONE shared, Coolify-managed MySQL/MariaDB resource
-- (configured in WorkspaceConfig.Database.ResourceID); there is no per-workspace
-- database_resource_id column. Only the logical database/user provisioned on
-- that shared server are recorded (database_name/database_user); the database
-- password is never stored (it is derived with HKDF-SHA256 keyed by the portal
-- identity key plus this workspace's per-workspace salt, which alone is stored
-- as database_password_salt).
CREATE TABLE IF NOT EXISTS workspaces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    website_id INTEGER NULL,
    platform_domain_id INTEGER NOT NULL,
    label TEXT NOT NULL,
    database_name TEXT,
    database_user TEXT,
    database_password_salt TEXT,
    application_resource_id TEXT,
    api_key_id INTEGER,
    proxy_username TEXT,
    proxy_password TEXT,
    status TEXT NOT NULL DEFAULT 'provisioning',
    last_error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at DATETIME,
    last_reconcile_at DATETIME,
    created_at DATETIME,
    updated_at DATETIME,
    deleted_at DATETIME,

    UNIQUE (website_id),
    UNIQUE (platform_domain_id, label),
    UNIQUE (application_resource_id),

    FOREIGN KEY (website_id) REFERENCES ipfs_websites(id),
    FOREIGN KEY (platform_domain_id) REFERENCES platform_domains(id)
);

CREATE INDEX idx_workspaces_user_id ON workspaces(user_id);
-- +goose StatementEnd

CREATE INDEX idx_workspaces_status ON workspaces(status);
CREATE INDEX idx_workspaces_api_key_id ON workspaces(api_key_id);
CREATE INDEX idx_workspaces_deleted_at ON workspaces(deleted_at);
CREATE INDEX idx_workspaces_next_retry_at ON workspaces(next_retry_at);
CREATE INDEX idx_workspaces_last_reconcile_at ON workspaces(last_reconcile_at);

-- +goose Down
DROP TABLE IF EXISTS workspaces;
