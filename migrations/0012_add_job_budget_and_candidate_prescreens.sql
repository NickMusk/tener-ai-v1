ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS salary_min DOUBLE PRECISION;

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS salary_max DOUBLE PRECISION;

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS salary_currency TEXT;

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS work_authorization_required BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS candidate_prescreens (
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    candidate_id BIGINT NOT NULL REFERENCES candidates(id),
    conversation_id BIGINT REFERENCES conversations(id),
    status TEXT NOT NULL,
    must_have_answers_json JSONB,
    salary_expectation_min DOUBLE PRECISION,
    salary_expectation_max DOUBLE PRECISION,
    salary_expectation_currency TEXT,
    location_confirmed BOOLEAN,
    work_authorization_confirmed BOOLEAN,
    cv_received BOOLEAN NOT NULL DEFAULT FALSE,
    summary TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(job_id, candidate_id)
);

CREATE INDEX IF NOT EXISTS idx_candidate_prescreens_candidate
    ON candidate_prescreens(candidate_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_candidate_prescreens_conversation
    ON candidate_prescreens(conversation_id);
