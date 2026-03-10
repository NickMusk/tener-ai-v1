ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS questionable_skills JSONB DEFAULT '[]'::jsonb;
