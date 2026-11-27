-- The inbox of a subscribing domain service (ADR 0034): the event identifiers of every record
-- that a subscription has accepted, kept for 8 days (ADR 0029), so that a repeated or replayed
-- event changes nothing, and, for a subscription that keeps its position, the offset of the last
-- record of each partition whose marker committed, from which it starts again after a restore.

-- +goose Up
CREATE TABLE inbox (
    subscription text        NOT NULL,
    source       text        NOT NULL,
    event_id     text        NOT NULL,
    received_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subscription, source, event_id)
);

CREATE INDEX inbox_received_at ON inbox (received_at);

CREATE TABLE inbox_positions (
    subscription text    NOT NULL,
    topic        text    NOT NULL,
    partition    integer NOT NULL,
    last_offset  bigint  NOT NULL,
    PRIMARY KEY (subscription, topic, partition)
);

-- +goose Down
DROP TABLE inbox_positions;
DROP TABLE inbox;
