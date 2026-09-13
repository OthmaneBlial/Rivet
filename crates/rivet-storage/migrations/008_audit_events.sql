CREATE TABLE IF NOT EXISTS audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    actor_id TEXT,
    action TEXT NOT NULL,
    resource TEXT NOT NULL,
    outcome TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS audit_events_timestamp_idx
    ON audit_events(timestamp DESC, sequence DESC);
