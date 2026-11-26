-- The minimum authority versions of a registered domain service (ADR 0050, ADR 0068): for each
-- principal and each membership, the lowest authority version that a protected write may carry.

-- +goose Up
CREATE TABLE minimum_authority_versions (
    kind    text   NOT NULL CHECK (kind IN ('principal', 'membership')),
    id      text   NOT NULL,
    minimum bigint NOT NULL CHECK (minimum >= 1),
    PRIMARY KEY (kind, id)
);

-- +goose Down
DROP TABLE minimum_authority_versions;
