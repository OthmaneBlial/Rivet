ALTER TABLE build_artifacts
    ADD COLUMN stage_id TEXT REFERENCES build_stages(id) ON DELETE SET NULL;
