use std::path::{Path, PathBuf};

use uuid::Uuid;

/// Default threshold for storing metadata files in PostgreSQL.
///
/// Files above this size are stored in the remote object storage instead of
/// PostgreSQL.
pub(crate) const DEFAULT_THRESHOLD: usize = 512 * 1024; // 512 KB

/// Configuration shared across a [`FullDirectory`][1], its [`MetadataStore`][2] and
/// the [`File`][3] handles it hands out.
///
/// [1]: crate::FullDirectory
/// [2]: crate::metadata::MetadataStore
/// [3]: crate::File
#[derive(Clone, Debug)]
pub(crate) struct Context {
    /// The ID of the index this directory is operating on.
    pub index: Uuid,

    /// The threshold, in bytes, above which metadata files are stored remotely instead
    /// of in PostgreSQL.
    pub threshold: usize,

    /// Defines the size of the chunks which should be read from the storage backend.
    pub read_chunks: Option<usize>,

    /// Defines the size of the chunks which should be written to the storage backend.
    pub write_chunks: Option<usize>,

    /// Defines the number of concurrent requests to make when reading a file from the
    /// storage backend.
    pub read_concurrency: Option<usize>,

    /// Defines the number of concurrent requests to make when writing a file to the
    /// storage backend.
    pub write_concurrency: Option<usize>,
}

impl Context {
    /// Creates a new context for the given index.
    pub fn new(index: Uuid) -> Self {
        Self {
            index,
            threshold: DEFAULT_THRESHOLD,
            read_chunks: None,
            write_chunks: None,
            read_concurrency: None,
            write_concurrency: None,
        }
    }

    /// Returns the path that should be used for the file at `path` for the index.
    ///
    /// This should not be used for metadata files, unless they are stored remotely.
    pub fn path(&self, path: impl AsRef<Path>) -> PathBuf {
        let base = format!("idx-{}", self.index);
        let mut base = PathBuf::from(base);
        base.push(path);
        base
    }
}
