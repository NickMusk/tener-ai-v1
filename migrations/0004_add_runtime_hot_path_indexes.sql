CREATE INDEX IF NOT EXISTS idx_conversations_job_last_message
    ON conversations(job_id, last_message_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_conversations_candidate_last_message
    ON conversations(candidate_id, last_message_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_conversations_status_last_message
    ON conversations(status, last_message_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_conversations_job_candidate_channel
    ON conversations(job_id, candidate_id, channel, id DESC);

CREATE INDEX IF NOT EXISTS idx_messages_conversation_id_id
    ON messages(conversation_id, id DESC);

CREATE INDEX IF NOT EXISTS idx_candidate_matches_job_score
    ON candidate_job_matches(job_id, score DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_candidate_assessments_candidate_job_updated
    ON candidate_agent_assessments(candidate_id, job_id, updated_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_candidate_assessments_job_updated
    ON candidate_agent_assessments(job_id, updated_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_pre_resume_sessions_status_followup
    ON pre_resume_sessions(status, next_followup_at, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_pre_resume_sessions_job_updated
    ON pre_resume_sessions(job_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_pre_resume_sessions_candidate_updated
    ON pre_resume_sessions(candidate_id, updated_at DESC);

