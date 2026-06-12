//! Segment-file *bundling*: concatenating a segment's component files into a single
//! object instead of storing one object per file.
//!
//! Tantivy writes each [`SegmentComponent`][1] of a segment as its own file
//! (`<segment_uuid>.<ext>`). An index with many small segments therefore produces a
//! huge number of tiny objects, which is expensive to write and read over an object
//! store. When bundling is enabled, the non-empty, non-`.del` component files of a
//! segment are buffered in memory as they are written and, at [`sync_directory`][2]
//! time, concatenated into a single `<segment_uuid>.bundle` object. Each component
//! then lives at a byte range inside that object, recorded in PostgreSQL.
//!
//! The actual bytes are written to the object store ([`FullDirectory`][3]) or the
//! inner directory ([`LightDirectory`][4]) - the [`Bundler`] here only holds them
//! transiently, between a file being finalized and the next sync.
//!
//! `.del` files are never bundled: they are mutable and written after the segment,
//! so they remain standalone objects. Files larger than
//! [`Context::bundle_max_file_bytes`] are also kept standalone, so a large (merge)
//! segment never has to be held in memory.
//!
//! [1]: tantivy::SegmentComponent
//! [2]: tantivy::Directory::sync_directory
//! [3]: crate::FullDirectory
//! [4]: crate::LightDirectory

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::utils::FastConcurrentMap;

/// The file extension of a bundle object (`<segment_uuid>.bundle`).
pub(crate) const BUNDLE_EXT: &str = "bundle";

/// Returns whether the file at `filepath` may be bundled.
///
/// Every component except the deletes file (`.del`) is immutable once written and
/// can be bundled. Deletes are written after the segment and may be rewritten, so
/// they stay standalone.
pub(crate) fn is_bundleable(filepath: &Path) -> bool {
    filepath.extension().and_then(|ext| ext.to_str()) != Some("del")
}

/// The (index-relative) path of the bundle object that a component file is, or
/// would be, stored in.
///
/// The bundle object's path is always derivable from a component's path this way,
/// so it is never stored in PostgreSQL.
pub(crate) fn object(filepath: &Path) -> Option<PathBuf> {
    let segment = filepath.file_stem()?.to_str()?;
    Some(PathBuf::from(format!("{segment}.{BUNDLE_EXT}")))
}

/// Holds the bytes of bundle-eligible files that have been finalized but not yet
/// synced.
///
/// The bytes live here only transiently: [`Bundler::drain`] hands them to the
/// directory at sync time, which writes them to the backing store as one object per
/// segment.
#[derive(Clone, Debug, Default)]
pub(crate) struct Bundler {
    /// Maps an (index-relative) component path to its buffered bytes.
    pending: Arc<FastConcurrentMap<PathBuf, Vec<u8>>>,
}

impl Bundler {
    /// Buffers `bytes` for the component at `path`, awaiting the next sync.
    pub async fn buffer(&self, path: PathBuf, bytes: Vec<u8>) {
        let _ = self.pending.insert_async(path, bytes).await;
    }

    /// Returns the buffered bytes for `path`.
    pub async fn get(&self, path: &Path) -> Option<Vec<u8>> {
        self.pending
            .read_async(path, |_, bytes| bytes.clone())
            .await
    }

    /// Removes and returns every pending file, grouped into one [`Bundle`] per segment.
    pub async fn drain(&self) -> Vec<Bundle> {
        let mut files = Vec::new();
        self.pending
            .retain_async(|path, bytes| {
                files.push((path.clone(), std::mem::take(bytes)));
                false
            })
            .await;

        // Group by segment ID, keeping a deterministic order both across segments and
        // within a segment.
        let mut by_segment: BTreeMap<String, Vec<(PathBuf, Vec<u8>)>> = BTreeMap::new();
        for (path, bytes) in files {
            let Some(segment) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };

            by_segment
                .entry(segment.to_string())
                .or_default()
                .push((path, bytes));
        }

        by_segment
            .into_iter()
            .map(|(segment, mut files)| {
                files.sort_by(|(a, _), (b, _)| a.cmp(b));

                let mut bytes = Vec::new();
                let mut entries = Vec::with_capacity(files.len());
                for (path, file) in files {
                    let offset = bytes.len() as u64;
                    bytes.extend_from_slice(&file);
                    entries.push(BundleEntry {
                        path,
                        offset,
                        length: file.len() as u64,
                    });
                }

                let path = PathBuf::from(format!("{segment}.{BUNDLE_EXT}"));
                Bundle {
                    path,
                    bytes,
                    entries,
                }
            })
            .collect()
    }
}

/// A segment's component files concatenated into a single object.
#[derive(Clone, Debug)]
pub(crate) struct Bundle {
    /// The (index-relative) path of the bundle object, `<segment_uuid>.bundle`.
    pub path: PathBuf,

    /// The concatenated bytes of every component, in `entries` order.
    pub bytes: Vec<u8>,

    /// Where each component lives inside `bytes`.
    pub entries: Vec<BundleEntry>,
}

/// The location of one component file inside a [`Bundle`].
#[derive(Clone, Debug)]
pub(crate) struct BundleEntry {
    /// The (index-relative) path of the component file.
    pub path: PathBuf,

    /// The component's byte offset within the bundle object.
    pub offset: u64,

    /// The component's byte length.
    pub length: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_bundleable_excludes_deletes() {
        assert!(is_bundleable(Path::new("seg.idx")));
        assert!(is_bundleable(Path::new("seg.store")));
        assert!(!is_bundleable(Path::new("seg.123.del")));
    }

    #[tokio::test]
    async fn drain_groups_by_segment_and_lays_out_offsets() {
        let bundler = Bundler::default();
        bundler.buffer(PathBuf::from("a.idx"), vec![1, 2, 3]).await;
        bundler.buffer(PathBuf::from("a.term"), vec![4, 5]).await;
        bundler.buffer(PathBuf::from("b.idx"), vec![9]).await;

        let mut bundles = bundler.drain().await;
        bundles.sort_by(|x, y| x.path.cmp(&y.path));
        assert_eq!(bundles.len(), 2);

        // Segment `a`: `a.idx` (sorts before `a.term`) then `a.term`.
        let a = &bundles[0];
        assert_eq!(a.path, PathBuf::from("a.bundle"));
        assert_eq!(a.bytes, vec![1, 2, 3, 4, 5]);
        assert_eq!(a.entries.len(), 2);
        assert_eq!(a.entries[0].path, PathBuf::from("a.idx"));
        assert_eq!((a.entries[0].offset, a.entries[0].length), (0, 3));
        assert_eq!(a.entries[1].path, PathBuf::from("a.term"));
        assert_eq!((a.entries[1].offset, a.entries[1].length), (3, 2));

        let b = &bundles[1];
        assert_eq!(b.path, PathBuf::from("b.bundle"));
        assert_eq!(b.bytes, vec![9]);

        // Draining consumed everything.
        assert!(bundler.drain().await.is_empty());
    }
}
