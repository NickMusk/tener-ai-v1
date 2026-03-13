DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'candidate_job_matches'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'job_candidates'
    ) THEN
        ALTER TABLE candidate_job_matches RENAME TO job_candidates;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname = 'idx_candidate_matches_job_score'
    ) THEN
        EXECUTE 'ALTER INDEX idx_candidate_matches_job_score RENAME TO idx_job_candidates_job_score';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_job_candidates_job_score
    ON job_candidates(job_id, score DESC, id DESC);
