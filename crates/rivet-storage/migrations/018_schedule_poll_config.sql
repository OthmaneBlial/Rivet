ALTER TABLE schedules
    ADD COLUMN poll_remote TEXT NOT NULL DEFAULT 'origin';

ALTER TABLE schedules
    ADD COLUMN poll_fetch INTEGER NOT NULL DEFAULT 0
    CHECK(poll_fetch IN (0, 1));

ALTER TABLE schedules
    ADD COLUMN poll_credential_id TEXT;
