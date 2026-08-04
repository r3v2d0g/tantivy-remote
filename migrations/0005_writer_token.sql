-- Fencing token for the exclusive index writer.
--
-- Set when the INDEX_WRITER_LOCK advisory lock is acquired, cleared on release,
-- and checked inside mutating metadata transactions so a writer that lost its
-- lock (or was superseded) cannot publish further changes.
ALTER TABLE tantivy.directories
ADD COLUMN writer_token UUID;
