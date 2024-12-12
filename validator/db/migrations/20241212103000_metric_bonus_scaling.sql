-- migrate:up
ALTER TABLE reward_data ADD COLUMN response_time_penalty_multiplier FLOAT DEFAULT 1;
ALTER TABLE contenders_weights_stats ADD COLUMN average_response_time_penalty_multiplier FLOAT DEFAULT 1;
ALTER TABLE reward_data ALTER COLUMN response_time_penalty_multiplier DROP DEFAULT;
ALTER TABLE contenders_weights_stats ALTER COLUMN average_response_time_penalty_multiplier DROP DEFAULT;

-- migrate:down
ALTER TABLE reward_data DROP COLUMN response_time_penalty_multiplier;
ALTER TABLE contenders_weights_stats DROP COLUMN average_response_time_penalty_multiplier;
