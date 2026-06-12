-- Whether a (non-metadata) file is "logically empty": it stored some bytes, but
-- those bytes were the default/empty serialization for its `SegmentComponent`, so we
-- skipped writing them to the object store / inner directory and reconstruct them from
-- memory on read.
ALTER TABLE tantivy.files
ADD COLUMN is_empty BOOLEAN NOT NULL DEFAULT FALSE;
