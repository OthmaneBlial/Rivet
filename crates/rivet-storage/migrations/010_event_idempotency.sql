ALTER TABLE build_events ADD COLUMN event_hash TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS build_events_event_hash_idx
    ON build_events(build_id, event_hash)
    WHERE event_hash IS NOT NULL;
