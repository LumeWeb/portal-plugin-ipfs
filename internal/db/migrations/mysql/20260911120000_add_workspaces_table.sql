-- +goose Up
-- Workspace: a persistent, resource-limited authoring environment for an
-- existing website, provisioned on a runtime provider (Coolify for the MVP).
--
-- The entity is runtime-agnostic: no WordPress-specific names appear here.
-- A workspace links to the Website that receives published output and the
-- PlatformDomain used for its authoring hostname, and records the provider
-- application/database IDs plus the workspace-scoped API credential ID once
-- each is created by the external system (all nullable because provisioning
-- crosses system boundaries).
--
-- Live-uniqueness (see also the platform_domains and website_domains keys):
-- the unique keys are STRICT and do not include deleted_at. MySQL treats NULL
-- deleted_at values as distinct inside a UNIQUE index, so a key widened with
-- deleted_at would let duplicate live rows coexist and defeat the
-- duplicate-key race detection the create path relies on. The cost of a strict
-- key is that a soft-deleted workspace still occupies its keys; the delete
-- slice purges the tombstone for the reclaimed keys before re-provisioning,
-- so the strict keys never permanently block re-provisioning.
--
-- Reconcile scheduling state is part of the initial schema because it is
-- created together with the table (pre-release):
--   - retry_count       : consecutive reconcile failures, drives bounded backoff.
--   - next_retry_at     : the earliest time a failed workspace is re-selected.
--   - last_reconcile_at : when the reconciler last processed the workspace,
--                         used to decide when a ready/suspended workspace needs
--                         a drift check.
--
-- Constraints:
--   - ownership is authoritative on the workspace   user_id NOT NULL
--   - one live workspace per ATTACHED website       UNIQUE(website_id), nullable:
--     NULL values are distinct inside a SQL UNIQUE index, so any number of
--     unattached workspaces (website_id NULL) may coexist on MySQL/SQLite
--     while an attached website is claimed by at most one live workspace.
--   - one hostname per platform root            UNIQUE(platform_domain_id, label)
--   - provider application UUIDs are never shared  UNIQUE(application_resource_id)
--   - website / platform-domain references are FK-constrained
--   (No per-workspace database resource ID: every workspace shares one
--   Coolify-managed MySQL/MariaDB resource configured in WorkspaceConfig.
--   Only the logical database/user names are stored, and only the workspace's
--   own logical database is created/dropped by the portal.)
--   The reverse-proxy Basic Auth credentials (proxy_username/proxy_password)
--   are defined inline as part of this pre-release consolidated initial
--   schema (they were previously a follow-up ALTER migration). They
--   authenticate the portal's own access to the workspace and must persist
--   across service restarts; they are never the database password.
--
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS workspaces (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT UNSIGNED NOT NULL,
    website_id BIGINT UNSIGNED NULL,
    platform_domain_id BIGINT UNSIGNED NOT NULL,
    label VARCHAR(63) NOT NULL,
    database_name VARCHAR(64) NULL,
    database_user VARCHAR(64) NULL,
    database_password_salt VARCHAR(255) NULL,
    application_resource_id VARCHAR(64) NULL,
    api_key_id BIGINT UNSIGNED NULL,
    proxy_username VARCHAR(255) NULL,
    proxy_password VARCHAR(255) NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'provisioning',
    last_error TEXT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMP NULL DEFAULT NULL,
    last_reconcile_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL,

    UNIQUE KEY uk_workspaces_website_id (website_id),
    UNIQUE KEY uk_workspaces_platform_domain_label (platform_domain_id, label),
    UNIQUE KEY uk_workspaces_application_resource_id (application_resource_id),
    INDEX idx_workspaces_user_id (user_id),
    INDEX idx_workspaces_status (status),
    INDEX idx_workspaces_api_key_id (api_key_id),
    INDEX idx_workspaces_deleted_at (deleted_at),
    INDEX idx_workspaces_next_retry_at (next_retry_at),
    INDEX idx_workspaces_last_reconcile_at (last_reconcile_at),

    CONSTRAINT fk_workspaces_website FOREIGN KEY (website_id) REFERENCES ipfs_websites(id),
    CONSTRAINT fk_workspaces_platform_domain FOREIGN KEY (platform_domain_id) REFERENCES platform_domains(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS workspaces;
