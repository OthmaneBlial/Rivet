CREATE TABLE IF NOT EXISTS build_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    build_id TEXT NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
    timestamp TEXT NOT NULL,
    event_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS build_events_build_sequence_idx
    ON build_events(build_id, sequence);
