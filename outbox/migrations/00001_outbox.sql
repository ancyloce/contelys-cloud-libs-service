-- The outbox of a domain service (ADR 0034): events committed with the business state, waiting
-- for the relay, and the last position that the relay published in each partition, from which a
-- service reads its own topic again after a restore (Identity ADR 0008, Organization Access
-- ADR 0002).

-- +goose Up
CREATE TABLE outbox (
    id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic       text        NOT NULL,
    key         text        NOT NULL,
    event_id    uuid        NOT NULL UNIQUE,
    type        text        NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    payload     bytea       NOT NULL,
    traceparent text        NOT NULL DEFAULT '',
    tracestate  text        NOT NULL DEFAULT ''
);

CREATE TABLE outbox_positions (
    topic       text    NOT NULL,
    partition   integer NOT NULL,
    last_offset bigint  NOT NULL,
    PRIMARY KEY (topic, partition)
);

-- +goose Down
DROP TABLE outbox_positions;
DROP TABLE outbox;
