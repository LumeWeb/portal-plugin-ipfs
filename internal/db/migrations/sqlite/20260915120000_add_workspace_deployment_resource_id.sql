-- +goose Up
-- Persist the LAST queued deployment UUID for the workspace application. The
-- reconciler queues a Coolify deployment on every provisioning pass; queueing
-- is not idempotent on the Coolify side (POST /start never dedupes), so a
-- reconcile re-entry while the queued deployment has not yet moved the
-- application out of "exited" must observe the recorded deployment instead of
-- queueing a duplicate. A start whose deployment has already reached a
-- terminal state (or whose record is gone) starts normally.
ALTER TABLE workspaces ADD COLUMN deployment_resource_id VARCHAR(64) NULL;

-- +goose Down
ALTER TABLE workspaces DROP COLUMN deployment_resource_id;
