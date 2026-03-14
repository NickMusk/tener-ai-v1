ALTER TABLE conversations
    ADD COLUMN IF NOT EXISTS ai_enabled BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE conversations
    ADD COLUMN IF NOT EXISTS operator_attention_required BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE conversations
    ADD COLUMN IF NOT EXISTS terminal_reason TEXT;

ALTER TABLE conversations
    ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ;

ALTER TABLE conversations
    ADD COLUMN IF NOT EXISTS closed_by TEXT;
