ALTER TABLE candidates
    ADD COLUMN IF NOT EXISTS provider_id TEXT;

ALTER TABLE candidates
    ADD COLUMN IF NOT EXISTS unipile_profile_id TEXT;

ALTER TABLE candidates
    ADD COLUMN IF NOT EXISTS attendee_provider_id TEXT;

CREATE INDEX IF NOT EXISTS idx_candidates_provider_id
    ON candidates(provider_id);

CREATE INDEX IF NOT EXISTS idx_candidates_unipile_profile_id
    ON candidates(unipile_profile_id);

CREATE INDEX IF NOT EXISTS idx_candidates_attendee_provider_id
    ON candidates(attendee_provider_id);
