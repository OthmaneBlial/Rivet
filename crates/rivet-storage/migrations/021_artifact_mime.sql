ALTER TABLE build_artifacts
    ADD COLUMN mime_type TEXT NOT NULL DEFAULT 'application/octet-stream';
