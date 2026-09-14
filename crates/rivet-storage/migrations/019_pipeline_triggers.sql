CREATE TABLE IF NOT EXISTS pipeline_triggers (
    id TEXT PRIMARY KEY NOT NULL,
    upstream_project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    downstream_project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
    created_at TEXT NOT NULL,
    UNIQUE(upstream_project_id, downstream_project_id)
);

CREATE INDEX IF NOT EXISTS pipeline_triggers_upstream_idx
    ON pipeline_triggers(upstream_project_id, enabled, created_at, id);

CREATE INDEX IF NOT EXISTS pipeline_triggers_downstream_idx
    ON pipeline_triggers(downstream_project_id, created_at, id);

CREATE TABLE IF NOT EXISTS pipeline_trigger_deliveries (
    trigger_id TEXT NOT NULL REFERENCES pipeline_triggers(id) ON DELETE CASCADE,
    upstream_build_id TEXT NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
    downstream_build_id TEXT REFERENCES builds(id) ON DELETE SET NULL,
    triggered_at TEXT NOT NULL,
    PRIMARY KEY(trigger_id, upstream_build_id)
);

CREATE INDEX IF NOT EXISTS pipeline_trigger_deliveries_downstream_idx
    ON pipeline_trigger_deliveries(downstream_build_id, triggered_at);
