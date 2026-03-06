CREATE TABLE IF NOT EXISTS resume_assets (
    id BIGSERIAL PRIMARY KEY,
    asset_key TEXT NOT NULL UNIQUE,
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    candidate_id BIGINT NOT NULL REFERENCES candidates(id),
    conversation_id BIGINT,
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    provider TEXT,
    provider_message_id TEXT,
    file_name TEXT,
    mime_type TEXT,
    file_size_bytes BIGINT,
    remote_url TEXT,
    storage_path TEXT,
    content_sha256 TEXT,
    processing_status TEXT NOT NULL,
    processing_error TEXT,
    extracted_text TEXT,
    parsed_json JSONB,
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_resume_assets_job_observed
    ON resume_assets(job_id, observed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_resume_assets_candidate_observed
    ON resume_assets(candidate_id, observed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_resume_assets_processing
    ON resume_assets(processing_status, updated_at DESC, id DESC);
