CREATE TABLE IF NOT EXISTS build_annotations (
    id TEXT PRIMARY KEY NOT NULL,
    build_id TEXT NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
    stage_id TEXT REFERENCES build_stages(id) ON DELETE SET NULL,
    kind TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS build_annotations_build_idx
    ON build_annotations(build_id, created_at, id);
