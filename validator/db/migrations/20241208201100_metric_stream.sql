-- migrate:up
ALTER TABLE reward_data ADD COLUMN stream_metric FLOAT DEFAULT 0;
ALTER TABLE contenders_weights_stats ADD COLUMN stream_metric FLOAT NOT NULL DEFAULT 0;
ALTER TABLE contenders_weights_stats ALTER COLUMN stream_metric DROP DEFAULT;

-- migrate:down
ALTER TABLE reward_data DROP COLUMN stream_metric;
ALTER TABLE contenders_weights_stats DROP COLUMN stream_metric;