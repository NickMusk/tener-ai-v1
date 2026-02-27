CREATE TABLE IF NOT EXISTS job_culture_profiles (
    job_id BIGINT PRIMARY KEY REFERENCES jobs(id),
    status TEXT NOT NULL,
    company_name TEXT,
    company_website TEXT,
    profile_json JSONB,
    sources_json JSONB,
    warnings_json JSONB,
    search_queries_json JSONB,
    error TEXT,
    generated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS pre_resume_sessions (
    session_id TEXT PRIMARY KEY,
    conversation_id BIGINT UNIQUE NOT NULL REFERENCES conversations(id),
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    candidate_id BIGINT NOT NULL REFERENCES candidates(id),
    status TEXT NOT NULL,
    language TEXT,
    last_intent TEXT,
    followups_sent INTEGER NOT NULL DEFAULT 0,
    turns INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    resume_links JSONB,
    next_followup_at TIMESTAMPTZ,
    state_json JSONB NOT NULL,
    instruction TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS pre_resume_events (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    conversation_id BIGINT NOT NULL REFERENCES conversations(id),
    event_type TEXT NOT NULL,
    intent TEXT,
    inbound_text TEXT,
    outbound_text TEXT,
    state_status TEXT,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pre_resume_events_session_created
    ON pre_resume_events(session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pre_resume_events_conversation_created
    ON pre_resume_events(conversation_id, created_at DESC);

CREATE TABLE IF NOT EXISTS webhook_events (
    id BIGSERIAL PRIMARY KEY,
    event_key TEXT UNIQUE NOT NULL,
    source TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS linkedin_accounts (
    id BIGSERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    provider_account_id TEXT NOT NULL UNIQUE,
    provider_user_id TEXT,
    label TEXT,
    status TEXT NOT NULL,
    metadata JSONB,
    connected_at TIMESTAMPTZ,
    last_synced_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_linkedin_accounts_status
    ON linkedin_accounts(status);

CREATE TABLE IF NOT EXISTS linkedin_onboarding_sessions (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL UNIQUE,
    provider TEXT NOT NULL,
    status TEXT NOT NULL,
    state_nonce TEXT NOT NULL,
    state_expires_at TIMESTAMPTZ NOT NULL,
    redirect_uri TEXT,
    connect_url TEXT,
    provider_account_id TEXT,
    error TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_linkedin_onboarding_sessions_status
    ON linkedin_onboarding_sessions(status);
CREATE INDEX IF NOT EXISTS idx_linkedin_onboarding_sessions_provider_account
    ON linkedin_onboarding_sessions(provider_account_id);

CREATE TABLE IF NOT EXISTS outbound_actions (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    candidate_id BIGINT NOT NULL REFERENCES candidates(id),
    conversation_id BIGINT NOT NULL REFERENCES conversations(id),
    action_type TEXT NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    not_before TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    account_id BIGINT,
    payload_json JSONB NOT NULL,
    result_json JSONB,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbound_actions_status_due
    ON outbound_actions(status, not_before, priority DESC, id ASC);
CREATE INDEX IF NOT EXISTS idx_outbound_actions_job
    ON outbound_actions(job_id, status, id DESC);

CREATE TABLE IF NOT EXISTS linkedin_account_daily_counters (
    account_id BIGINT NOT NULL,
    day_utc TEXT NOT NULL,
    connect_sent INTEGER NOT NULL DEFAULT 0,
    new_threads_sent INTEGER NOT NULL DEFAULT 0,
    replies_sent INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(account_id, day_utc)
);

CREATE TABLE IF NOT EXISTS linkedin_account_weekly_counters (
    account_id BIGINT NOT NULL,
    week_start_utc TEXT NOT NULL,
    connect_sent INTEGER NOT NULL DEFAULT 0,
    new_threads_sent INTEGER NOT NULL DEFAULT 0,
    replies_sent INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(account_id, week_start_utc)
);

CREATE TABLE IF NOT EXISTS job_linkedin_account_assignments (
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    account_id BIGINT NOT NULL REFERENCES linkedin_accounts(id),
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(job_id, account_id)
);
CREATE INDEX IF NOT EXISTS idx_job_linkedin_account_assignments_job
    ON job_linkedin_account_assignments(job_id, account_id);
