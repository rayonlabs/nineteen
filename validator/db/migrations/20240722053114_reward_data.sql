-- migrate:up
CREATE TABLE IF NOT EXISTS reward_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task TEXT NOT NULL,
    node_id INTEGER NOT NULL,
    quality_score FLOAT NOT NULL,
    validator_hotkey TEXT NOT NULL,
    node_hotkey TEXT NOT NULL,
    synthetic_query BOOLEAN NOT NULL,
    speed_scoring_factor FLOAT,
    response_time FLOAT,
    volume FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- migrate:down
DROP TABLE IF EXISTS reward_data;