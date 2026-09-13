CREATE TABLE IF NOT EXISTS remote_attempts (
    build_id TEXT PRIMARY KEY NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL,
    requirements_json TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    pipeline_json TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS remote_attempts_project_idx
    ON remote_attempts(project_id, created_at);
