-- migrate:up
ALTER TABLE reward_data ADD COLUMN metric FLOAT;
ALTER TABLE reward_data DROP COLUMN speed_scoring_factor;


-- migrate:down
ALTER TABLE reward_data DROP COLUMN metric;
ALTER TABLE reward_data ADD COLUMN speed_scoring_factor FLOAT;
