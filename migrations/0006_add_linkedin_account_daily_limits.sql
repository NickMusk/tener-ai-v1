ALTER TABLE linkedin_accounts
    ADD COLUMN IF NOT EXISTS daily_message_limit INTEGER;

ALTER TABLE linkedin_accounts
    ADD COLUMN IF NOT EXISTS daily_connect_limit INTEGER;
