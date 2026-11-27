-- The idempotency keys of a domain service's creating POSTs (ADR 0062): each key with its caller
-- and its operation, the digest of the request's body, and the response stored in the
-- transaction that committed the effect. The periodic removal finds the old keys by created_at.

-- +goose Up
CREATE TABLE idempotency_keys (
    caller       text        NOT NULL,
    operation    text        NOT NULL,
    key          text        NOT NULL,
    digest       bytea       NOT NULL,
    status       integer     NOT NULL,
    content_type text        NOT NULL,
    body         bytea       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (caller, operation, key)
);

CREATE INDEX idempotency_keys_created_at ON idempotency_keys (created_at);

-- +goose Down
DROP TABLE idempotency_keys;
