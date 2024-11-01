-- migrate:up
ALTER TABLE reward_data ADD COLUMN response_time_scoring_factor FLOAT DEFAULT 1;
ALTER TABLE contenders_weights_stats ADD COLUMN average_response_time_penalty_multiplier FLOAT DEFAULT 1;

-- migrate:down
ALTER TABLE reward_data DROP COLUMN response_time_scoring_factor;
ALTER TABLE contenders_weights_stats DROP COLUMN average_response_time_penalty_multiplier;
