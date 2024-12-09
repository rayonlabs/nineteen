-- migrate:up
ALTER TABLE reward_data ADD COLUMN checking_data TEXT;


-- migrate:down
ALTER TABLE reward_data DROP COLUMN checking_data;
