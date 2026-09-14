CREATE TABLE IF NOT EXISTS provider_triggers (
    id TEXT PRIMARY KEY NOT NULL,
    downstream_project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    source_repository TEXT NOT NULL,
    source_pipeline TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
    created_at TEXT NOT NULL,
    UNIQUE(downstream_project_id, provider, source_repository, source_pipeline)
);

CREATE INDEX IF NOT EXISTS provider_triggers_match_idx
    ON provider_triggers(downstream_project_id, provider, source_repository, source_pipeline, enabled);

CREATE TABLE IF NOT EXISTS provider_trigger_deliveries (
    trigger_id TEXT NOT NULL REFERENCES provider_triggers(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL,
    downstream_build_id TEXT REFERENCES builds(id) ON DELETE SET NULL,
    triggered_at TEXT NOT NULL,
    PRIMARY KEY(trigger_id, event_id)
);

CREATE INDEX IF NOT EXISTS provider_trigger_deliveries_build_idx
    ON provider_trigger_deliveries(downstream_build_id, triggered_at);
