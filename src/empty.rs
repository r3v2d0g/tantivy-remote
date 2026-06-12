//! Detection and reconstruction of *logically empty* segment component files.
//!
//! Every file tantivy writes ends with a [footer][1] (`{"version":…,"crc":…}` + a
//! length + a magic number), so even a segment component that holds no data is
//! still a non-empty *byte* sequence: a tiny structural header followed by that
//! footer. We call such a file *logically empty* — it has bytes, but they encode
//! the default/empty value for its [`SegmentComponent`][2].
//!
//! When tantivy writes a segment, a [`SegmentComponent`][2] ends up logically empty
//! whenever the schema/segment has nothing to store for it, for instance:
//! - [`Positions`][`.pos`] when no field records positions,
//! - [`FastFields`][`.fast`] when there is no fast field,
//! - [`FieldNorms`][`.fieldnorm`] when no field has fieldnorms,
//! - [`Postings`][`.idx`] / [`Terms`][`.term`] for a field that indexed nothing.
//!
//! These empty serializations are *constant* for a given tantivy version: the
//! structural header is fixed, the footer's version is fixed, and its CRC is the
//! CRC of that fixed body. We capture them verbatim and use byte-for-byte equality
//! both to *detect* an empty file on write and to *reconstruct* it on read.
//!
//! Matching against the exact captured bytes makes this safe across tantivy
//! upgrades: if a future version changes the format, the new empty files simply
//! stop matching, so the optimization disables itself rather than ever corrupting
//! data.
//!
//! Two distinct representations cover every component that can be empty:
//! - [`Empty::Composite`]: the empty [`CompositeFile`][3], shared by `.idx`,
//!   `.pos`, `.term` and `.fieldnorm` (they all serialize byte-identically when
//!   empty).
//! - [`Empty::Fast`]: the empty columnar fast-field store (`.fast`).
//!
//! The remaining components have no fixed empty representation and are always
//! stored normally: `.store` always encodes the (per-document) doc store, and
//! `.del` is only ever written when there actually are deletes.
//!
//! [1]: <https://docs.rs/tantivy/0.26/src/tantivy/directory/footer.rs.html>
//! [2]: tantivy::SegmentComponent
//! [3]: <https://docs.rs/tantivy/0.26/src/tantivy/directory/composite_file.rs.html>
//! [`.pos`]: Empty::Composite
//! [`.fast`]: Empty::Fast
//! [`.fieldnorm`]: Empty::Composite
//! [`.idx`]: Empty::Composite
//! [`.term`]: Empty::Composite

use std::{cmp, path::Path};

/// The empty [`CompositeFile`][1] serialization (`.idx`, `.pos`, `.term`,
/// `.fieldnorm`), as written by tantivy `0.26`.
///
/// [1]: <https://docs.rs/tantivy/0.26/src/tantivy/directory/composite_file.rs.html>
const EMPTY_COMPOSITE: &[u8] = include_bytes!("empty/composite.bin");

/// The empty columnar fast-field serialization (`.fast`), as written by tantivy
/// `0.26`.
const EMPTY_FAST: &[u8] = include_bytes!("empty/fast.bin");

/// The logically-empty serialization of a [`SegmentComponent`][1].
///
/// Obtained from a path via [`Empty::for_path`] and from written content via
/// [`Empty::detect`]; the corresponding bytes are returned by [`Empty::bytes`].
///
/// [1]: tantivy::SegmentComponent
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Empty {
    /// The empty [`CompositeFile`] shared by the `.idx`, `.pos`, `.term` and
    /// `.fieldnorm` components.
    Composite,

    /// The empty columnar fast-field store (`.fast`).
    Fast,
}

impl Empty {
    /// The largest number of bytes a logically-empty file can occupy.
    ///
    /// Used as the buffering cap when deciding, on write, whether a file is empty: once
    /// more than this many bytes have been written, the file cannot be empty.
    pub fn max_len() -> usize {
        cmp::max(EMPTY_COMPOSITE.len(), EMPTY_FAST.len())
    }

    /// The exact bytes of this empty serialization.
    pub fn bytes(self) -> &'static [u8] {
        match self {
            Empty::Composite => EMPTY_COMPOSITE,
            Empty::Fast => EMPTY_FAST,
        }
    }

    /// The empty representation that *could* apply to the file at `path`, based on its
    /// segment component (file extension).
    ///
    /// Returns `None` for components that never have a fixed empty representation
    /// (`.store`, `.del`, metadata files, …).
    pub fn for_path(path: &Path) -> Option<Empty> {
        match path.extension()?.to_str()? {
            "idx" | "pos" | "term" | "fieldnorm" => Some(Empty::Composite),
            "fast" => Some(Empty::Fast),
            _ => None,
        }
    }

    /// Returns the empty representation for `path` iff `content` is exactly the
    /// logically-empty serialization for that file's component.
    pub fn detect(path: &Path, content: &[u8]) -> Option<Empty> {
        let empty = Empty::for_path(path)?;
        (content == empty.bytes()).then_some(empty)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, env, fs, path::Path};

    use eyre::Result;
    use tantivy::{
        Index, IndexSettings, TantivyDocument,
        directory::MmapDirectory,
        doc,
        schema::{FAST, INDEXED, Schema, SchemaBuilder, TEXT},
    };

    use super::*;

    /// Builds a one-segment index over an [`MmapDirectory`][1] in a temp dir, returning
    /// every segment file's bytes keyed by extension.
    ///
    /// [1]: tantivy::directory::MmapDirectory
    fn segment_files<T>(
        label: &str,
        build: impl FnOnce(&mut SchemaBuilder) -> T,
        doc: impl FnOnce(&Schema) -> Result<TantivyDocument>,
    ) -> Result<BTreeMap<String, Vec<u8>>> {
        let tmp = env::temp_dir().join(format!("tantivy_remote_empty_{label}"));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp)?;

        let mut builder = SchemaBuilder::new();
        build(&mut builder);
        let schema = builder.build();

        let dir = MmapDirectory::open(&tmp)?;
        let index = Index::create(dir, schema.clone(), IndexSettings::default())?;
        let mut writer = index.writer(15_000_000)?;
        let document = doc(&schema)?;
        writer.add_document(document)?;
        writer.commit()?;

        let mut files = BTreeMap::new();
        for entry in fs::read_dir(&tmp)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some((_, ext)) = name.rsplit_once('.') {
                if matches!(ext, "json" | "lock") {
                    continue;
                }

                let file = fs::read(entry.path())?;
                files.insert(ext.to_string(), file);
            }
        }

        Ok(files)
    }

    /// The captured constants must match exactly what tantivy writes for an all-empty
    /// segment.
    ///
    /// If tantivy changes its format/version this fails loudly, signalling that the
    /// constants need to be regenerated.
    #[test]
    fn captured_constants_match_tantivy() -> Result<()> {
        // A single fast u64 field: `.idx`, `.pos`, `.term` and `.fieldnorm` are all
        // empty composites, so we can verify the composite template from any of them.
        let files = segment_files(
            "fast_only",
            |sb| sb.add_u64_field("n", FAST),
            |s| Ok(doc!(s.get_field("n")? => 42u64)),
        )?;

        for ext in ["idx", "pos", "term", "fieldnorm"] {
            assert_eq!(
                files[ext].as_slice(),
                EMPTY_COMPOSITE,
                "empty `.{ext}` no longer matches the captured composite template",
            );
        }

        // A single indexed (non-fast) u64 field has no fast field, so `.fast` is empty.
        let files = segment_files(
            "indexed_only",
            |sb| sb.add_u64_field("n", INDEXED),
            |s| Ok(doc!(s.get_field("n")? => 42u64)),
        )?;
        assert_eq!(
            files["fast"].as_slice(),
            EMPTY_FAST,
            "empty `.fast` no longer matches the captured fast-field template",
        );

        Ok(())
    }

    /// A component that actually holds data must *not* be detected as empty.
    #[test]
    fn populated_components_are_not_empty() -> Result<()> {
        let files = segment_files(
            "text",
            |sb| sb.add_text_field("body", TEXT),
            |s| Ok(doc!(s.get_field("body")? => "hello world hello")),
        )?;

        // A text field with one document populates the postings, positions, terms,
        // fieldnorms and doc store.
        for ext in ["idx", "pos", "term", "fieldnorm", "store"] {
            let path = format!("seg.{ext}");
            assert_eq!(
                Empty::detect(Path::new(&path), &files[ext]),
                None,
                "populated `.{ext}` was wrongly detected as empty",
            );
        }

        // The same segment has no fast field, so its `.fast` is genuinely empty.
        assert_eq!(
            Empty::detect(Path::new("seg.fast"), &files["fast"]),
            Some(Empty::Fast),
            "`.fast` with no fast field should be logically empty",
        );

        Ok(())
    }

    #[test]
    fn detect_matches_only_the_right_extension() {
        // The composite bytes are empty for `.idx` but meaningless for `.store`/`.del`.
        assert_eq!(
            Empty::detect(Path::new("seg.idx"), Empty::Composite.bytes()),
            Some(Empty::Composite),
        );

        assert_eq!(
            Empty::detect(Path::new("seg.fast"), Empty::Fast.bytes()),
            Some(Empty::Fast),
        );

        // Right bytes, wrong component => not empty.
        assert_eq!(
            Empty::detect(Path::new("seg.idx"), Empty::Fast.bytes()),
            None,
        );

        assert_eq!(
            Empty::detect(Path::new("seg.store"), Empty::Composite.bytes()),
            None,
        );

        assert_eq!(Empty::detect(Path::new("seg.0.del"), &[]), None);
    }
}
