-- +goose Up
-- Route registration recreates these policies for the admin API at startup.
DELETE FROM access_rules
WHERE ptype = 'p'
  AND (
    (v0 IN ('user', 'admin') AND v1 LIKE 'ipfs.%')
    OR (v0 = 'user' AND v1 LIKE 'admin.%')
  )
  AND (
    (v3 = 'POST' AND v2 IN (
      '/api/ipfs/websites/:id/block',
      '/api/ipfs/websites/:id/unblock',
      '/api/ipfs/ipns/republish',
      '/api/ipfs/platform-domains',
      '/api/ipfs/platform-domains/:id/bind'
    ))
    OR (v3 = 'GET' AND v2 = '/api/ipfs/platform-domains')
    OR (v3 IN ('PATCH', 'DELETE') AND v2 = '/api/ipfs/platform-domains/:id')
  );

-- +goose Down
-- Each application version registers its own route policies at startup.
SELECT 1;
