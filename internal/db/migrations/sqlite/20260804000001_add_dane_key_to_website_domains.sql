-- +goose Up
-- +goose StatementBegin
ALTER TABLE website_domains ADD COLUMN tls_private_key TEXT NULL;
ALTER TABLE website_domains ADD COLUMN tls_cert_pem TEXT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE website_domains DROP COLUMN tls_private_key;
ALTER TABLE website_domains DROP COLUMN tls_cert_pem;
-- +goose StatementEnd
