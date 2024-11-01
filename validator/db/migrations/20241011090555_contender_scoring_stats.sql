-- migrate:up

CREATE TABLE IF NOT EXISTS contenders_weights_stats (
    id SERIAL PRIMARY KEY,
    version_key INTEGER NOT NULL,
    netuid INTEGER NOT NULL,
    validator_hotkey TEXT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    node_hotkey TEXT NOT NULL,
    task TEXT NOT NULL,
    average_quality_score FLOAT NOT NULL,
    metric_bonus FLOAT NOT NULL,
    combined_quality_score FLOAT NOT NULL,
    period_score_multiplier FLOAT NOT NULL,
    normalised_period_score FLOAT NOT NULL,
    contender_capacity FLOAT NOT NULL,
    normalised_net_score FLOAT NOT NULL
);

-- migrate:down
DROP TABLE IF EXISTS contenders_weights_stats;
