-- Segment-file bundling: when enabled, the (non-empty, non-`.del`) component files
-- of a segment are concatenated into a single object instead of one object per
-- file.
--
-- A bundled file's bytes live inside its segment's bundle object whose path is
-- derived from the file (`<segment_uuid>.bundle`) at the byte range
-- `[byte_offset, byte_offset + byte_length)`. A file is bundled iff
-- `byte_length IS NOT NULL`, otherwise it is a standalone object whose bytes are
-- the whole object at `path`.
ALTER TABLE tantivy.files
ADD COLUMN byte_offset BIGINT NOT NULL DEFAULT 0,
ADD COLUMN byte_length BIGINT;
