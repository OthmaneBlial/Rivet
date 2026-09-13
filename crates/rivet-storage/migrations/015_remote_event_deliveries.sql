CREATE TABLE IF NOT EXISTS remote_event_deliveries (
    build_id TEXT NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
    attempt_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    event_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(build_id, attempt_id, sequence)
);

CREATE INDEX IF NOT EXISTS remote_event_deliveries_build_idx
    ON remote_event_deliveries(build_id, created_at);
