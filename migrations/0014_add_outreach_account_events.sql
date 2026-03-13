CREATE TABLE IF NOT EXISTS outreach_account_events (
    id BIGSERIAL PRIMARY KEY,
    event_key TEXT NOT NULL UNIQUE,
    account_id BIGINT NOT NULL REFERENCES linkedin_accounts(id),
    job_id BIGINT REFERENCES jobs(id),
    candidate_id BIGINT REFERENCES candidates(id),
    conversation_id BIGINT REFERENCES conversations(id),
    event_type TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outreach_account_events_account_created
    ON outreach_account_events(account_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_outreach_account_events_account_candidate
    ON outreach_account_events(account_id, candidate_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_outreach_account_events_job_created
    ON outreach_account_events(job_id, created_at DESC, id DESC);
