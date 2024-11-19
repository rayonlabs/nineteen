-- migrate:up
CREATE TABLE IF NOT EXISTS contenders (
    contender_id TEXT PRIMARY KEY,
    node_hotkey TEXT NOT NULL,
    node_id INTEGER NOT NULL,
    netuid INTEGER NOT NULL,
    task TEXT NOT NULL,
    validator_hotkey TEXT NOT NULL,
    raw_capacity FLOAT NOT NULL,
    capacity FLOAT NOT NULL,
    capacity_to_score FLOAT NOT NULL,
    consumed_capacity FLOAT NOT NULL,
    total_requests_made INTEGER NOT NULL DEFAULT 0,
    requests_429 INTEGER NOT NULL DEFAULT 0,
    requests_500 INTEGER NOT NULL DEFAULT 0,
    consecutive_fails INTEGER NOT NULL DEFAULT 0,
    period_score FLOAT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
);

CREATE TABLE IF NOT EXISTS contenders_history (
    id SERIAL PRIMARY KEY,
    contender_id TEXT NOT NULL,
    node_hotkey TEXT NOT NULL,
    node_id INTEGER NOT NULL,
    netuid INTEGER NOT NULL,
    task TEXT NOT NULL,
    validator_hotkey TEXT NOT NULL,
    raw_capacity FLOAT NOT NULL,
    capacity FLOAT NOT NULL,
    capacity_to_score FLOAT NOT NULL,
    consumed_capacity FLOAT NOT NULL,
    total_requests_made INTEGER NOT NULL DEFAULT 0,
    requests_429 INTEGER NOT NULL DEFAULT 0,
    requests_500 INTEGER NOT NULL DEFAULT 0,
    period_score FLOAT,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    expired_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
);

-- migrate:down
DROP TABLE IF EXISTS contenders;

DROP TABLE IF EXISTS contenders_history;