CREATE TABLE IF NOT EXISTS build_artifacts (
    id TEXT PRIMARY KEY NOT NULL,
    build_id TEXT NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(build_id, name, relative_path)
);

CREATE INDEX IF NOT EXISTS build_artifacts_build_idx
    ON build_artifacts(build_id, name, relative_path);
