use std::{io, path::Path, sync::Arc};

use block_on_place::HandleExt;
use derive_more::Debug;
use eyre::Result;
use sqlx::PgPool;
use tantivy::{
    Directory, TantivyError,
    directory::{
        DirectoryLock, FileHandle, Lock, WatchCallback, WatchHandle, WritePtr,
        error::{DeleteError, LockError, OpenReadError, OpenWriteError},
    },
};
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::{
    cache::Cache,
    context::Context,
    directory::is_metadata,
    file::File,
    metadata::MetadataStore,
    operator::Operator,
    utils::{PathExt, WrapIoErrorExt},
    writer::Writer,
};

/// A [`Directory`] implementation that reads and writes files to a remote object
/// storage using [`opendal`], with metadata stored in PostgreSQL.
///
/// This does not support watching for updates to the metadata files. Instead, the
/// readers using this directory should be created using [`ReloadPolicy::Manual`][1]
/// and reloaded manually.
///
/// This also does not implement any locking logic. It is up to the user of this
/// directory to make sure that there can only be one index writer using it at any
/// given time.
///
/// [1]: tantivy::ReloadPolicy::Manual
#[derive(Clone, Debug)]
#[debug("FullDirectory {{ index: {} }}", context.index)]
pub struct FullDirectory {
    /// A handle to the tokio runtime, used to perform async operations in a sync
    /// context.
    rt: Handle,

    /// Caches file handles and metadata.
    cache: Cache,

    /// The underlying Opendal operator used to read and write files.
    operator: Operator,

    /// Stores the metadata for the directory and its files.
    metadata: MetadataStore,

    /// The configuration shared with the metadata store and the file handles.
    ///
    /// The metadata store keeps its own clone; the two are kept in sync by the
    /// `with_*` builder methods.
    context: Context,
}

impl FullDirectory {
    /// Creates a new directory to read/write from/to the given index.
    ///
    /// If the index does not exist, it creates it.
    ///
    /// ## Panics
    ///
    /// This will panic if called from outside of the context of a `tokio` runtime.
    pub async fn open(index: Uuid, operator: opendal::Operator, pool: PgPool) -> Result<Self> {
        let context = Context::new(index);
        let metadata = MetadataStore::open(&context, pool, operator.clone()).await?;

        Ok(Self {
            rt: Handle::current(),
            cache: Cache::default(),
            operator: Operator::from(operator),
            metadata,
            context,
        })
    }

    /// Defines the size of the chunks which should be read from the storage backend.
    pub fn with_read_chunks(mut self, chunks: usize) -> Self {
        self.context.read_chunks = Some(chunks);
        self.metadata.context.read_chunks = Some(chunks);
        self
    }

    /// Defines the size of the chunks which should be written to the storage backend.
    pub fn with_write_chunks(mut self, chunks: usize) -> Self {
        self.context.write_chunks = Some(chunks);
        self.metadata.context.write_chunks = Some(chunks);
        self
    }

    /// Defines the number of concurrent requests to make when reading a file from the
    /// storage backend.
    pub fn with_read_concurrency(mut self, concurrency: usize) -> Self {
        self.context.read_concurrency = Some(concurrency);
        self.metadata.context.read_concurrency = Some(concurrency);
        self
    }

    /// Defines the number of concurrent requests to make when writing a file to the
    /// storage backend.
    pub fn with_write_concurrency(mut self, concurrency: usize) -> Self {
        self.context.write_concurrency = Some(concurrency);
        self.metadata.context.write_concurrency = Some(concurrency);
        self
    }

    /// Defines the threshold for storing metadata files in the remote storage.
    ///
    /// Files smaller than this threshold will be stored in PostgreSQL.
    ///
    /// Default is 512 KB.
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.context.threshold = threshold;
        self.metadata.context.threshold = threshold;
        self
    }

    /// Opens the file at `filepath` for reading, returning a [`File`] handle for it.
    pub async fn get_file(&self, filepath: &Path) -> Result<Arc<File>, OpenReadError> {
        let path = filepath.try_to_str::<OpenReadError>()?;
        let filepath = self.context.path(filepath);

        if let Some(file) = self.cache.get_file(&filepath).await {
            return Ok(file);
        }

        // We haven't already opened the file, so we need to validate that it exists.
        let exists = if self.cache.is_created(&filepath).await {
            true
        } else {
            self.metadata
                .file_exists(path)
                .await
                .map_err(OpenReadError::wrapper(path))?
        };

        if !exists {
            return Err(OpenReadError::FileDoesNotExist(path.into()));
        }

        let open = async || {
            let metadata = self
                .cache
                .metadata(&filepath, || self.operator.metadata(&filepath))
                .await?;

            let path = filepath.try_to_str::<OpenReadError>()?;
            let file = File::open(
                path,
                metadata,
                self.rt.clone(),
                self.operator.clone(),
                &self.context,
            );

            Ok(file)
        };

        self.cache.file(&filepath, open).await
    }
}

impl Directory for FullDirectory {
    #[inline]
    fn get_file_handle(&self, filepath: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file = self.rt.block_on_place(self.get_file(filepath))?;

        Ok(file)
    }

    fn delete(&self, filepath: &Path) -> Result<(), DeleteError> {
        let path = filepath.try_to_str::<DeleteError>()?;
        let deleted = self
            .rt
            .block_on_place(self.metadata.delete_file(path))
            .map_err(DeleteError::wrapper(filepath))?;

        if !deleted {
            return Err(DeleteError::FileDoesNotExist(filepath.into()));
        }

        // TODO(MLB): add a TLL to the files in S3?

        Ok(())
    }

    fn exists(&self, filepath: &Path) -> Result<bool, OpenReadError> {
        let path = filepath.try_to_str::<OpenReadError>()?;

        if is_metadata(filepath) {
            return self
                .rt
                .block_on_place(self.metadata.metadata_exists(path))
                .map_err(OpenReadError::wrapper(filepath));
        }

        self.rt
            .block_on_place(self.metadata.file_exists(path))
            .map_err(OpenReadError::wrapper(filepath))
    }

    fn open_write(&self, filepath: &Path) -> Result<WritePtr, OpenWriteError> {
        // We first have to make sure that the file does not already exist.
        let path = filepath.try_to_str::<OpenWriteError>()?;
        let exists = self
            .rt
            .block_on_place(self.metadata.file_exists(path))
            .map_err(OpenWriteError::wrapper(filepath))?;

        if exists {
            return Err(OpenWriteError::FileAlreadyExists(filepath.into()));
        }

        let filepath = self.context.path(filepath);
        let path = filepath.try_to_str::<OpenWriteError>()?;

        let writer = self.rt.block_on_place(async {
            let mut writer = self.operator.writer_with(path).append(false);
            if let Some(chunks) = self.context.write_chunks {
                writer = writer.chunk(chunks);
            }

            if let Some(concurrency) = self.context.write_concurrency {
                writer = writer.concurrent(concurrency);
            }

            let writer = match writer.await {
                Ok(writer) => writer,
                Err(error) => {
                    let filepath = filepath.to_path_buf();
                    if error.kind() == opendal::ErrorKind::AlreadyExists {
                        return Err(OpenWriteError::FileAlreadyExists(filepath));
                    } else {
                        return Err(OpenWriteError::wrap_other(error, filepath));
                    }
                }
            };

            let filepath = filepath.to_path_buf();
            let entry = self.cache.created(filepath).await?;

            Ok(Writer::new(entry, writer, self.rt.clone()))
        })?;

        let writer = Box::new(writer);
        let ptr = WritePtr::new(writer);

        Ok(ptr)
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

    fn sync_directory(&self) -> io::Result<()> {
        let flushed = self.rt.block_on_place(self.cache.sync());
        if flushed.is_empty() {
            return Ok(());
        }

        for path in flushed {
            // We have to remove the first part of the path, as it contains the index ID, which
            // we don't include in PSQL.
            let mut components = path.components();
            components.next();

            let filepath = components.as_path();
            let path = filepath.try_to_str::<io::Error>()?;

            self.rt
                .block_on_place(self.metadata.create_file(path))
                .map_err(io::Error::wrapper(filepath))?;
        }

        // TODO(MLB): remove from the cache

        Ok(())
    }

    fn watch(&self, _cb: WatchCallback) -> tantivy::Result<WatchHandle> {
        let error =
            "watching is not supported by this directory, use `ReloadingPolicy::Manual`".into();

        Err(TantivyError::InternalError(error))
    }

    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(())))
    }
}
