CREATE TABLE IF NOT EXISTS auth_sessions (
    id TEXT PRIMARY KEY NOT NULL,
    token_sha256 TEXT NOT NULL UNIQUE,
    principal_id TEXT NOT NULL,
    role TEXT NOT NULL,
    projects_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    revoked_at TEXT
);

CREATE INDEX IF NOT EXISTS auth_sessions_active_idx
    ON auth_sessions(token_sha256, expires_at, revoked_at);
