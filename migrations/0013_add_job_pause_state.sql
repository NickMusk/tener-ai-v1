ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS job_state TEXT;

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS paused_at TIMESTAMPTZ;

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS pause_reason TEXT;

UPDATE jobs
SET job_state = CASE
    WHEN archived_at IS NOT NULL THEN 'archived'
    WHEN paused_at IS NOT NULL THEN 'paused'
    ELSE 'active'
END
WHERE job_state IS NULL OR BTRIM(job_state) = '';

ALTER TABLE jobs
    ALTER COLUMN job_state SET DEFAULT 'active';

UPDATE jobs
SET job_state = 'active'
WHERE job_state IS NULL OR BTRIM(job_state) = '';
