use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use derive_more::Deref;
use opendal::Metadata;
use scc::hash_map::Entry;
use tantivy::directory::error::{OpenReadError, OpenWriteError};

use crate::{File, empty::Empty, metadata::FileRecord, utils::FastConcurrentMap};

// TODO(MLB): clean up the cache when a file is closed/after some time?

/// Caches opened files and their metadata, as well as the list of files which have
/// been created and whether they have been flushed.
#[derive(Clone, Debug, Default)]
pub(crate) struct Cache {
    /// Keeps track of the files which have been created, and whether they have been
    /// flushed, until the directory containing them is synced.
    created: Arc<CreatedCache>,

    /// Caches the files which have been opened.
    files: Arc<FilesCache>,

    /// Caches the metadata which have been fetched.
    metadata: Arc<MetadataCache>,
}

/// In-process cache of successful [`FileRecord`][1] lookups for one index.
///
/// Only positive hits are stored: a missing path is never cached, so a concurrent
/// writer creating a file cannot permanently poison a reader. Memory is
/// `O(number of cached file rows)` for the index (small: path + a few integers per
/// component).
///
/// Shared across clones via [`Arc`]. All accessors are synchronous so callers can
/// consult the cache from `block_on_place` paths without holding a lock across
/// `.await`. Prefetch publishes a fully built map via [`replace_all`][2], so
/// concurrent lookups never observe a half-empty cache.
///
/// [1]: crate::metadata::FileRecord
/// [2]: Self::replace_all
#[derive(Clone, Debug, Default)]
pub(crate) struct FileLookupCache {
    /// The live record map. Readers take a short `RwLock` read guard; prefetch swaps
    /// the inner [`Arc`] under a write guard after building the replacement off to
    /// the side, so the clear-then-fill window is never visible.
    records: Arc<RwLock<Arc<FastConcurrentMap<String, FileRecord>>>>,
}

impl FileLookupCache {
    /// Returns the cached [`FileRecord`][1] for `path`, if any.
    ///
    /// [1]: crate::metadata::FileRecord
    pub fn get(&self, path: &str) -> Option<FileRecord> {
        let records = self
            .records
            .read()
            .unwrap_or_else(|error| error.into_inner());

        records.read_sync(path, |_, record| record.clone())
    }

    /// Inserts or replaces the cached record for `path`.
    pub fn insert(&self, path: String, record: FileRecord) {
        let records = self
            .records
            .read()
            .unwrap_or_else(|error| error.into_inner());

        let _ = records.upsert_sync(path, record);
    }

    /// Removes the cached record for `path`, if any.
    pub fn remove(&self, path: &str) {
        let records = self
            .records
            .read()
            .unwrap_or_else(|error| error.into_inner());

        let _ = records.remove_sync(path);
    }

    /// Atomically replaces the entire cache with `entries` (used by prefetch).
    ///
    /// Builds the replacement map first, then swaps it in under a write lock so
    /// concurrent [`get`][1]/[`insert`][2]/[`remove`][3] never observe an empty or
    /// partially rebuilt cache.
    ///
    /// [1]: Self::get
    /// [2]: Self::insert
    /// [3]: Self::remove
    pub fn replace_all(&self, entries: impl IntoIterator<Item = (String, FileRecord)>) {
        let next = FastConcurrentMap::default();
        for (path, record) in entries {
            let _ = next.insert_sync(path, record);
        }

        let mut records = self
            .records
            .write()
            .unwrap_or_else(|error| error.into_inner());

        *records = Arc::new(next);
    }
}

/// Caches the paths of the files which have been created, until the directory
/// containing them is synced.
#[derive(Debug, Default, Deref)]
struct CreatedCache {
    /// Contains, for each path created, its current [`CreatedState`].
    #[deref]
    cache: FastConcurrentMap<PathBuf, CreatedState>,
}

/// The state of a created-but-not-yet-synced file.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CreatedState {
    /// Whether the file has been flushed and closed.
    pub flushed: bool,

    /// If the file was detected to be [logically empty][1], which empty representation
    /// it is (so it can be reconstructed from memory on read).
    ///
    /// [1]: crate::empty
    pub empty: Option<Empty>,
}

/// An entry into the cache of created files, used to record that the file has been
/// flushed and whether it turned out to be empty.
pub(crate) struct CreatedEntry {
    path: PathBuf,
    cache: Arc<CreatedCache>,

    /// Whether [`done()`][1] has been called.
    ///
    /// [1]: Self::done
    done: bool,
}

/// Caches the [`File`]s which have been opened.
#[derive(Debug, Default, Deref)]
pub(crate) struct FilesCache {
    #[deref]
    cache: FastConcurrentMap<PathBuf, Arc<File>>,
}

/// Caches the [`Metadata`]s which have been fetched.
#[derive(Debug, Default, Deref)]
struct MetadataCache {
    #[deref]
    cache: FastConcurrentMap<PathBuf, Arc<Metadata>>,
}

impl Cache {
    /// Gets the [`File`] for the given path from the cache, returning `None` if it is
    /// not cached.
    pub async fn get_file(&self, path: &Path) -> Option<Arc<File>> {
        self.files
            .read_async(path, |_, file| Arc::clone(file))
            .await
    }

    /// Fetches the metadata for the given path from the cache, fetching it and
    /// populating the cache using the provided closure if it is not already cached.
    pub async fn metadata(
        &self,
        path: &Path,
        fetch: impl AsyncFnOnce() -> Result<Metadata, OpenReadError>,
    ) -> Result<Arc<Metadata>, OpenReadError> {
        self.metadata.fetch(path, fetch).await
    }

    /// Fetches the [`File`] for the given path from the cache, opening it and
    /// populating the cache using the provided closure if it is not already cached.
    pub async fn file(
        &self,
        path: &Path,
        open: impl AsyncFnOnce() -> Result<Arc<File>, OpenReadError>,
    ) -> Result<Arc<File>, OpenReadError> {
        // fast path: try to get the file handle from the cache – this does not lock other
        //            readers.
        if let Some(file) = self.get_file(path).await {
            return Ok(file);
        }

        // slow path: get the entry and insert into it if it is still missing.
        let entry = self.files.entry_sync(path.to_path_buf());
        let entry = match entry {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(entry) => {
                // TODO(MLB): avoid keeping the lock while opening the file?
                let file = open().await?;
                entry.insert_entry(file)
            }
        };

        Ok(Arc::clone(entry.get()))
    }

    /// Returns the [`CreatedState`] of the file if it is being created and/or has been
    /// flushed but not yet synced.
    ///
    /// Returns `None` if it is not tracked as created.
    pub async fn created_state(&self, filepath: &Path) -> Option<CreatedState> {
        self.created.read_async(filepath, |_, state| *state).await
    }

    /// Marks the file at the given path as having been created, returning a
    /// [`CreatedEntry`] for it so that it can later be marked as having been flushed.
    pub async fn created(&self, filepath: PathBuf) -> Result<CreatedEntry, OpenWriteError> {
        let filepath = filepath.to_path_buf();
        let result = self
            .created
            .insert_async(filepath.clone(), CreatedState::default())
            .await;

        match result {
            Ok(_) => Ok(CreatedEntry {
                path: filepath,
                cache: Arc::clone(&self.created),
                done: false,
            }),

            Err(_) => Err(OpenWriteError::FileAlreadyExists(filepath)),
        }
    }

    /// Iterates over all of the created files, returning the ones which have been
    /// flushed and closed, together with their [logically empty][1] representation (if
    /// any).
    ///
    /// [1]: crate::empty
    pub async fn sync(&self) -> Vec<(PathBuf, Option<Empty>)> {
        let mut flushed = vec![];
        self.created
            .iter_mut_async(|entry| {
                if entry.flushed {
                    let empty = entry.empty;
                    let (path, _) = entry.consume();
                    flushed.push((path, empty));
                }

                true
            })
            .await;

        flushed
    }
}

impl CreatedEntry {
    /// Marks the file as having been flushed and closed, recording its [logically
    /// empty][1] representation if it was detected to be empty.
    ///
    /// [1]: crate::empty
    pub fn done(&mut self, empty: Option<Empty>) {
        if !self.done {
            self.done = true;
            self.cache.update_sync(&self.path, |_, state| {
                state.flushed = true;
                state.empty = empty;
            });
        }
    }

    /// Removes the created entry entirely, without marking it flushed.
    ///
    /// Used for [bundled][1] files, which are tracked by the [bundler][2] instead of the
    /// created cache, so `sync_directory` must not also see them here.
    ///
    /// [1]: crate::bundle
    /// [2]: crate::bundle::Bundler
    pub fn remove(&mut self) {
        if !self.done {
            self.done = true;
            self.cache.remove_sync(&self.path);
        }
    }
}

impl MetadataCache {
    /// Gets the metadata for the given path from the cache.
    async fn get(&self, path: &Path) -> Option<Arc<Metadata>> {
        self.read_async(path, |_, metadata| Arc::clone(metadata))
            .await
    }

    /// Fetches the metadata for the given path from the cache, populating it using the
    /// provided async closure if it is not already cached.
    async fn fetch(
        &self,
        path: &Path,
        fetch: impl AsyncFnOnce() -> Result<Metadata, OpenReadError>,
    ) -> Result<Arc<Metadata>, OpenReadError> {
        // fast path: try to read the metadata from the cache – this does not lock other readers.
        if let Some(metadata) = self.get(path).await {
            return Ok(metadata);
        }

        // slow path: get the entry for the file and insert if it is still missing
        let entry = self.entry_sync(path.to_path_buf());
        let entry = match entry {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(entry) => {
                // TODO(MLB): cache whether the file exists or not?
                // TODO(MLB): avoid keeping the lock while fetching the metadata?
                let metadata = fetch().await.map(Arc::new)?;
                entry.insert_entry(metadata)
            }
        };

        Ok(Arc::clone(entry.get()))
    }
}
