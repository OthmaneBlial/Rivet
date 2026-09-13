ALTER TABLE remote_attempts
    ADD COLUMN recovery_attempts INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS remote_attempts_recovery_idx
    ON remote_attempts(recovery_attempts, created_at);
