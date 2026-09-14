ALTER TABLE schedules
    ADD COLUMN trigger TEXT NOT NULL DEFAULT 'build'
    CHECK(trigger IN ('build', 'repository_poll'));
