use std::{io, path::Path, sync::Arc};

use block_on_place::HandleExt;
use derive_more::Debug;
use eyre::Result;
use sqlx::PgPool;
use tantivy::{
    Directory,
    directory::{
        DirectoryLock, FileHandle, Lock, WatchCallback, WatchHandle, WritePtr,
        error::{DeleteError, LockError, OpenReadError, OpenWriteError},
    },
};
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::{
    context::Context,
    directory::is_metadata,
    metadata::MetadataStore,
    utils::{PathExt, WrapIoErrorExt},
};

/// A [`Directory`] implementation that delegates to an inner [`Directory`] for
/// everything except the metadata files (`meta.json` and `.managed.json`), which it
/// stores in PostgreSQL (and, above a threshold, in a remote object storage) the same
/// way [`FullDirectory`][1] does.
///
/// This makes it possible to keep the "normal" files (segments, …) on a fast local
/// directory such as [`MmapDirectory`][2] while still sharing the small but
/// frequently rewritten metadata files through PostgreSQL.
///
/// Concretely, only [`atomic_read`][3], [`atomic_write`][4] and the metadata-file
/// branch of [`exists`][5] are handled here; every other call is forwarded verbatim to
/// the wrapped directory.
///
/// Just like [`FullDirectory`][1], this does not support watching for updates to the
/// metadata files: the readers using this directory should be created using
/// [`ReloadPolicy::Manual`][6] and reloaded manually.
///
/// [1]: crate::FullDirectory
/// [2]: tantivy::directory::MmapDirectory
/// [3]: Self::atomic_read
/// [4]: Self::atomic_write
/// [5]: Self::exists
/// [6]: tantivy::ReloadPolicy::Manual
#[derive(Clone, Debug)]
#[debug("LightDirectory {{ index: {}, inner: {inner:?} }}", metadata.context.index)]
pub struct LightDirectory<D> {
    /// A handle to the tokio runtime, used to perform async operations in a sync
    /// context.
    rt: Handle,

    /// Stores the metadata files for the directory.
    metadata: MetadataStore,

    /// The wrapped directory, handling every non-metadata operation.
    inner: D,
}

impl<D> LightDirectory<D> {
    /// Wraps `inner`, storing the metadata files for the given index in PostgreSQL.
    ///
    /// If the index does not exist, it creates it.
    ///
    /// ## Panics
    ///
    /// This will panic if called from outside of the context of a `tokio` runtime.
    pub async fn open(
        inner: D,
        index: Uuid,
        operator: opendal::Operator,
        pool: PgPool,
    ) -> Result<Self> {
        let context = Context::new(index);
        let metadata = MetadataStore::open(&context, pool, operator).await?;

        Ok(Self {
            rt: Handle::current(),
            metadata,
            inner,
        })
    }

    /// Defines the threshold for storing metadata files in the remote storage.
    ///
    /// Files smaller than this threshold will be stored in PostgreSQL.
    ///
    /// Default is 512 KB.
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.metadata.context.threshold = threshold;
        self
    }

    /// Defines the size of the chunks which should be read from the storage backend.
    pub fn with_read_chunks(mut self, chunks: usize) -> Self {
        self.metadata.context.read_chunks = Some(chunks);
        self
    }

    /// Defines the size of the chunks which should be written to the storage backend.
    pub fn with_write_chunks(mut self, chunks: usize) -> Self {
        self.metadata.context.write_chunks = Some(chunks);
        self
    }

    /// Defines the number of concurrent requests to make when reading a file from the
    /// storage backend.
    pub fn with_read_concurrency(mut self, concurrency: usize) -> Self {
        self.metadata.context.read_concurrency = Some(concurrency);
        self
    }

    /// Defines the number of concurrent requests to make when writing a file to the
    /// storage backend.
    pub fn with_write_concurrency(mut self, concurrency: usize) -> Self {
        self.metadata.context.write_concurrency = Some(concurrency);
        self
    }
}

impl<D: Directory + Clone> Directory for LightDirectory<D> {
    #[inline]
    fn get_file_handle(&self, filepath: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.inner.get_file_handle(filepath)
    }

    #[inline]
    fn delete(&self, filepath: &Path) -> Result<(), DeleteError> {
        self.inner.delete(filepath)
    }

    fn exists(&self, filepath: &Path) -> Result<bool, OpenReadError> {
        if is_metadata(filepath) {
            let path = filepath.try_to_str::<OpenReadError>()?;
            return self
                .rt
                .block_on_place(self.metadata.metadata_exists(path))
                .map_err(OpenReadError::wrapper(filepath));
        }

        self.inner.exists(filepath)
    }

    #[inline]
    fn open_write(&self, filepath: &Path) -> Result<WritePtr, OpenWriteError> {
        self.inner.open_write(filepath)
    }

    fn atomic_read(&self, filepath: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.rt
            .block_on_place(self.metadata.read_metadata(filepath))
            .map_err(OpenReadError::wrapper(filepath))?
            .ok_or_else(|| OpenReadError::FileDoesNotExist(filepath.into()))
    }

    fn atomic_write(&self, filepath: &Path, data: &[u8]) -> io::Result<()> {
        self.rt
            .block_on_place(self.metadata.write_metadata(filepath, data))
    }

    #[inline]
    fn sync_directory(&self) -> io::Result<()> {
        self.inner.sync_directory()
    }

    #[inline]
    fn watch(&self, callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(callback)
    }

    #[inline]
    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        self.inner.acquire_lock(lock)
    }
}
