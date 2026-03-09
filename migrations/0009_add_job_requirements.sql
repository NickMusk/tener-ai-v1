ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS must_have_skills JSONB DEFAULT '[]'::jsonb;

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS nice_to_have_skills JSONB DEFAULT '[]'::jsonb;
