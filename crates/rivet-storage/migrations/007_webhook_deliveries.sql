CREATE TABLE IF NOT EXISTS webhook_deliveries (
    event_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    received_at TEXT NOT NULL,
    build_id TEXT REFERENCES builds(id) ON DELETE SET NULL,
    build_number INTEGER
);

CREATE INDEX IF NOT EXISTS webhook_deliveries_project_idx
    ON webhook_deliveries(project_id, received_at DESC);
