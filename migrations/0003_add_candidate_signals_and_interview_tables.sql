CREATE TABLE IF NOT EXISTS candidate_signals (
    id BIGSERIAL PRIMARY KEY,
    signal_key TEXT NOT NULL UNIQUE,
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    candidate_id BIGINT NOT NULL REFERENCES candidates(id),
    conversation_id BIGINT,
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    signal_category TEXT,
    title TEXT NOT NULL,
    detail TEXT,
    impact_score DOUBLE PRECISION,
    confidence DOUBLE PRECISION,
    signal_meta JSONB,
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_candidate_signals_job_observed
    ON candidate_signals(job_id, observed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_candidate_signals_candidate_observed
    ON candidate_signals(candidate_id, observed_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS interview_sessions (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT UNIQUE NOT NULL,
    job_id BIGINT NOT NULL,
    candidate_id BIGINT NOT NULL,
    candidate_name TEXT,
    conversation_id BIGINT,
    provider TEXT NOT NULL,
    provider_assessment_id TEXT,
    provider_invitation_id TEXT,
    provider_candidate_id TEXT,
    status TEXT NOT NULL,
    language TEXT,
    entry_token_hash TEXT UNIQUE NOT NULL,
    entry_token_expires_at TIMESTAMPTZ NOT NULL,
    provider_interview_url TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    scored_at TIMESTAMPTZ,
    last_sync_at TIMESTAMPTZ,
    last_error_code TEXT,
    last_error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_interview_sessions_job_status
    ON interview_sessions(job_id, status);
CREATE INDEX IF NOT EXISTS idx_interview_sessions_candidate
    ON interview_sessions(candidate_id);
CREATE INDEX IF NOT EXISTS idx_interview_sessions_provider
    ON interview_sessions(provider, provider_invitation_id);

CREATE TABLE IF NOT EXISTS interview_results (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    provider_result_id TEXT,
    result_version INTEGER NOT NULL DEFAULT 1,
    technical_score DOUBLE PRECISION,
    soft_skills_score DOUBLE PRECISION,
    culture_fit_score DOUBLE PRECISION,
    total_score DOUBLE PRECISION,
    score_confidence DOUBLE PRECISION,
    pass_recommendation TEXT,
    normalized_json JSONB NOT NULL,
    raw_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE(session_id, result_version)
);

CREATE TABLE IF NOT EXISTS interview_events (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS candidate_interview_summary (
    job_id BIGINT NOT NULL,
    candidate_id BIGINT NOT NULL,
    candidate_name TEXT,
    session_id TEXT NOT NULL,
    interview_status TEXT NOT NULL,
    technical_score DOUBLE PRECISION,
    soft_skills_score DOUBLE PRECISION,
    culture_fit_score DOUBLE PRECISION,
    total_score DOUBLE PRECISION,
    score_confidence DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(job_id, candidate_id)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    route TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    response_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(route, idempotency_key)
);

CREATE TABLE IF NOT EXISTS job_interview_assessments (
    job_id BIGINT PRIMARY KEY,
    provider TEXT NOT NULL,
    provider_assessment_id TEXT NOT NULL,
    assessment_name TEXT,
    generation_hash TEXT,
    generated_questions_json JSONB,
    meta_json JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
