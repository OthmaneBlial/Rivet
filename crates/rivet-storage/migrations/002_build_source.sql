ALTER TABLE builds ADD COLUMN source_provider TEXT;
ALTER TABLE builds ADD COLUMN source_revision TEXT;
ALTER TABLE builds ADD COLUMN source_reference TEXT;
ALTER TABLE builds ADD COLUMN source_remote TEXT;
ALTER TABLE builds ADD COLUMN source_dirty INTEGER;
