CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    repository_path TEXT NOT NULL,
    pipeline_path TEXT NOT NULL,
    pipeline_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS builds (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    number INTEGER NOT NULL,
    status TEXT NOT NULL,
    queued_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    UNIQUE(project_id, number)
);

CREATE INDEX IF NOT EXISTS builds_project_number_idx
    ON builds(project_id, number DESC);

CREATE TABLE IF NOT EXISTS build_stages (
    id TEXT PRIMARY KEY NOT NULL,
    build_id TEXT NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    UNIQUE(build_id, position)
);

CREATE TABLE IF NOT EXISTS build_steps (
    id TEXT PRIMARY KEY NOT NULL,
    stage_id TEXT NOT NULL REFERENCES build_stages(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    exit_code INTEGER,
    started_at TEXT,
    finished_at TEXT,
    UNIQUE(stage_id, position)
);

CREATE TABLE IF NOT EXISTS build_logs (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    build_id TEXT NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
    timestamp TEXT NOT NULL,
    stream TEXT NOT NULL,
    line TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS build_logs_build_sequence_idx
    ON build_logs(build_id, sequence);
