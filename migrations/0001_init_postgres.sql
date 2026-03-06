CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    company TEXT,
    company_website TEXT,
    jd_text TEXT NOT NULL,
    location TEXT,
    preferred_languages JSONB,
    seniority TEXT,
    linkedin_routing_mode TEXT NOT NULL DEFAULT 'auto',
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS candidates (
    id BIGSERIAL PRIMARY KEY,
    linkedin_id TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    headline TEXT,
    location TEXT,
    languages JSONB,
    skills JSONB,
    years_experience INTEGER,
    source TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS candidate_job_matches (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    candidate_id BIGINT NOT NULL REFERENCES candidates(id),
    score DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL,
    verification_notes JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE(job_id, candidate_id)
);

CREATE TABLE IF NOT EXISTS conversations (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    candidate_id BIGINT NOT NULL REFERENCES candidates(id),
    channel TEXT NOT NULL,
    status TEXT NOT NULL,
    external_chat_id TEXT UNIQUE,
    linkedin_account_id BIGINT,
    last_message_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id BIGSERIAL PRIMARY KEY,
    conversation_id BIGINT NOT NULL REFERENCES conversations(id),
    direction TEXT NOT NULL,
    candidate_language TEXT,
    content TEXT NOT NULL,
    meta JSONB,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS operation_logs (
    id BIGSERIAL PRIMARY KEY,
    operation TEXT NOT NULL,
    entity_type TEXT,
    entity_id TEXT,
    status TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_operation_logs_entity
    ON operation_logs(entity_type, entity_id, created_at DESC);

CREATE TABLE IF NOT EXISTS job_step_progress (
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    step TEXT NOT NULL,
    status TEXT NOT NULL,
    output_json JSONB,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(job_id, step)
);

CREATE TABLE IF NOT EXISTS candidate_agent_assessments (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    candidate_id BIGINT NOT NULL REFERENCES candidates(id),
    agent_key TEXT NOT NULL,
    agent_name TEXT NOT NULL,
    stage_key TEXT NOT NULL,
    score DOUBLE PRECISION,
    status TEXT NOT NULL,
    reason TEXT,
    instruction TEXT,
    details JSONB,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE(job_id, candidate_id, agent_key, stage_key)
);

CREATE TABLE IF NOT EXISTS organizations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    full_name TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS memberships (
    org_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    role TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(org_id, user_id),
    FOREIGN KEY(org_id) REFERENCES organizations(id),
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS roles (
    key TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    permissions_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY,
    org_id TEXT NOT NULL REFERENCES organizations(id),
    user_id TEXT NOT NULL REFERENCES users(id),
    name TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    prefix TEXT NOT NULL,
    scopes_json JSONB NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    expires_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_api_keys_org_user
    ON api_keys(org_id, user_id);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    org_id TEXT NOT NULL REFERENCES organizations(id),
    user_id TEXT NOT NULL REFERENCES users(id),
    token_hash TEXT NOT NULL UNIQUE,
    scopes_json JSONB NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    expires_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_org_user
    ON sessions(org_id, user_id);

CREATE TABLE IF NOT EXISTS auth_audit_events (
    id BIGSERIAL PRIMARY KEY,
    org_id TEXT,
    user_id TEXT,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL,
    details_json JSONB,
    created_at TIMESTAMPTZ NOT NULL
);

