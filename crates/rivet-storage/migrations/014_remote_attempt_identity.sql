ALTER TABLE remote_attempts
    ADD COLUMN attempt_id TEXT NOT NULL DEFAULT '';

UPDATE remote_attempts
SET attempt_id = build_id
WHERE attempt_id = '';

CREATE UNIQUE INDEX IF NOT EXISTS remote_attempts_attempt_id_idx
    ON remote_attempts(attempt_id);
