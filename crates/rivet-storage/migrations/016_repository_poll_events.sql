CREATE TABLE IF NOT EXISTS repository_poll_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    revision TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    build_id TEXT REFERENCES builds(id),
    build_number INTEGER
);

CREATE INDEX IF NOT EXISTS repository_poll_events_project_checked_idx
    ON repository_poll_events(project_id, checked_at DESC);
