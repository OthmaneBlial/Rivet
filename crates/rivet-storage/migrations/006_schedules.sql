CREATE TABLE IF NOT EXISTS schedules (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    expression TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
    next_run_at TEXT NOT NULL,
    last_run_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(project_id, name)
);

CREATE INDEX IF NOT EXISTS schedules_due_idx
    ON schedules(enabled, next_run_at);

CREATE INDEX IF NOT EXISTS schedules_project_idx
    ON schedules(project_id, name);
