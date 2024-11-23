-- migrate:up

ALTER TABLE contenders_weights_stats ADD COLUMN metric FLOAT NOT NULL DEFAULT -1;
ALTER TABLE contenders_weights_stats ALTER COLUMN metric DROP DEFAULT;

-- migrate:down

ALTER TABLE contenders_weights_stats DROP COLUMN metric;
