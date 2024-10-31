-- migrate:up
ALTER TABLE reward_data ADD COLUMN response_time_scoring_factor FLOAT DEFAULT 1;

-- migrate:down
ALTER TABLE reward_data DROP COLUMN response_time_scoring_factor;
