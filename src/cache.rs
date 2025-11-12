use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use derive_more::Deref;
use opendal::Metadata;
use scc::hash_map::Entry;
use tantivy::directory::{
    FileHandle,
    error::{OpenReadError, OpenWriteError},
};

use crate::utils::FastConcurrentMap;

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

/// Caches the paths of the files which have been created, until the directory
/// containing them is synced.
#[derive(Debug, Default, Deref)]
struct CreatedCache {
    /// Contains, for each path created, whether the file has been flushed and closed.
    #[deref]
    cache: FastConcurrentMap<PathBuf, bool>,
}

/// An entry into the cache of created files, used to save that the file has been
/// flushed.
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
    cache: FastConcurrentMap<PathBuf, Arc<dyn FileHandle>>,
}

/// Caches the [`Metadata`]s which have been fetched.
#[derive(Debug, Default, Deref)]
struct MetadataCache {
    #[deref]
    cache: FastConcurrentMap<PathBuf, Arc<Metadata>>,
}

impl Cache {
    /// Fetches the metadata for the given path from the cache, fetching it and
    /// populating the cache using the provided closure if it is not already cached.
    pub async fn metadata(
        &self,
        path: &Path,
        fetch: impl AsyncFnOnce() -> Result<Metadata, OpenReadError>,
    ) -> Result<Arc<Metadata>, OpenReadError> {
        self.metadata.fetch(path, fetch).await
    }

    /// Fetches the [`FileHandle`] for the given path from the cache, opening it and
    /// populating the cache using the provided closure if it is not already cached.
    pub async fn file(
        &self,
        path: &Path,
        open: impl AsyncFnOnce() -> Result<Arc<dyn FileHandle>, OpenReadError>,
    ) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        // fast path: try to get the file handle from the cache – this does not lock other
        //            readers.
        if let Some(file) = self.files.read_sync(path, |_, file| Arc::clone(file)) {
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

    /// Marks the file at the given path as having been created, returning a
    /// [`CreatedEntry`] for it so that it can later be marked as having been flushed.
    pub async fn created(&self, filepath: PathBuf) -> Result<CreatedEntry, OpenWriteError> {
        let filepath = filepath.to_path_buf();
        let result = self.created.insert_async(filepath.clone(), false).await;
        match result {
            Ok(_) => Ok(CreatedEntry {
                path: filepath,
                cache: Arc::clone(&self.created),
                done: false,
            }),

            Err(_) => Err(OpenWriteError::FileAlreadyExists(filepath)),
        }
    }
}

impl CreatedEntry {
    /// Marks the file as having been flushed and closed.
    pub fn done(&mut self) {
        if !self.done {
            self.done = true;
            self.cache.update_sync(&self.path, |_, done| *done = true);
        }
    }
}

impl MetadataCache {
    /// Fetches the metadata for the given path from the cache, populating it using the
    /// provided async closure if it is not already cached.
    async fn fetch(
        &self,
        path: &Path,
        fetch: impl AsyncFnOnce() -> Result<Metadata, OpenReadError>,
    ) -> Result<Arc<Metadata>, OpenReadError> {
        // fast path: try to read the metadata from the cache – this does not lock other readers.
        if let Some(metadata) = self
            .read_async(path, |_, metadata| Arc::clone(metadata))
            .await
        {
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

impl Drop for CreatedEntry {
    fn drop(&mut self) {
        self.cache.remove_sync(&self.path);
    }
}
