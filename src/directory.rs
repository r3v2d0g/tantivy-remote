use std::{
    io,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use derive_more::Debug;
use eyre::Result;
use opendal::Metadata;
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
    file::File,
    metadata::MetadataStore,
    operator::Operator,
    utils::{PathExt, WrapIoErrorExt},
    writer::Writer,
};

// TODO(MLB): replace with `const`s once the `const` version of `Path::new` is stabilized
static META_JSON: LazyLock<&'static Path> = LazyLock::new(|| Path::new("meta.json"));
static MANAGED_JSON: LazyLock<&'static Path> = LazyLock::new(|| Path::new(".managed.json"));

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
#[debug("RemoteDirectory {{ index: {index} }}")]
pub struct RemoteDirectory {
    /// The ID of the index for which this is storing data.
    index: Uuid,

    /// A handle to the tokio runtime, used to perform async operations in a sync
    /// context.
    rt: Handle,

    /// Caches file handles and metadata.
    cache: Cache,

    /// The underlying Opendal operator used to read and write files.
    operator: Operator,

    /// Stores the metadata that is being written and read using [`atomic_read()`][1]
    /// and [`atomic_write()`][2].
    ///
    /// [1]: Directory::atomic_read()
    /// [2]: Directory::atomic_write()
    metadata: MetadataStore,
}

impl RemoteDirectory {
    /// Creates a new directory to read/write from/to the given index.
    ///
    /// If the index does not exist, it creates it.
    ///
    /// ## Panics
    ///
    /// This will panic if called from outside of the context of a `tokio` runtime.
    pub async fn open(index: Uuid, operator: opendal::Operator, pool: PgPool) -> Result<Self> {
        let metadata = MetadataStore::open(index, pool).await?;

        Ok(Self {
            index,
            rt: Handle::current(),
            cache: Cache::default(),
            operator: Operator::from(operator),
            metadata,
        })
    }

    /// Returns the path that should be used for the file at `path` for the index.
    ///
    /// This should not be used for metadata files.
    fn path(&self, path: impl AsRef<Path>) -> PathBuf {
        let base = format!("idx-{}", self.index);
        let mut base = PathBuf::from(base);
        base.push(path);
        base
    }

    /// Fetches the metadata for the given path.
    async fn metadata(&self, path: &Path) -> Result<Arc<Metadata>, OpenReadError> {
        // TODO(MLB): check whether the file exists + has not been deleted in PSQL

        let fetch = async || self.operator.metadata(path).await;
        self.cache.metadata(path, fetch).await
    }
}

impl Directory for RemoteDirectory {
    fn get_file_handle(&self, filepath: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        // We first have to make sure that the file is supposed to exist, either because it
        // has been created although the directory hasn't been synced yet, or because it is
        // present in PSQL and hasn't been marked as deleted.
        let is_created = self.rt.block_on(async {
            let filepath = self.path(filepath);
            println!("{}", filepath.display());
            self.cache.is_created(&filepath).await
        });

        let exists = if is_created {
            true
        } else {
            let path = filepath.try_to_str::<OpenReadError>()?;
            self.rt
                .block_on(self.metadata.file_exists(path))
                .map_err(OpenReadError::wrapper(filepath))?
        };

        if !exists {
            return Err(OpenReadError::FileDoesNotExist(filepath.into()));
        }

        let filepath = self.path(filepath);
        self.rt.block_on(async {
            let open = async || {
                let metadata = self.metadata(&filepath).await?;

                let path = filepath.try_to_str::<OpenReadError>()?;
                let file = File::open(path, metadata, self.rt.clone(), self.operator.clone());

                Ok(file)
            };

            self.cache.file(&filepath, open).await
        })
    }

    fn delete(&self, filepath: &Path) -> Result<(), DeleteError> {
        let path = filepath.try_to_str::<DeleteError>()?;
        let deleted = self
            .rt
            .block_on(self.metadata.delete_file(&path))
            .map_err(DeleteError::wrapper(filepath))?;

        if !deleted {
            return Err(DeleteError::FileDoesNotExist(filepath.into()));
        }

        // TODO(MLB): add a TLL to the files in S3?

        Ok(())
    }

    fn exists(&self, filepath: &Path) -> Result<bool, OpenReadError> {
        let path = filepath.try_to_str::<OpenReadError>()?;

        if filepath == *META_JSON || filepath == *MANAGED_JSON {
            return self
                .rt
                .block_on(self.metadata.metadata_exists(path))
                .map_err(OpenReadError::wrapper(filepath));
        }

        self.rt
            .block_on(self.metadata.file_exists(path))
            .map_err(OpenReadError::wrapper(filepath))
    }

    fn open_write(&self, filepath: &Path) -> Result<WritePtr, OpenWriteError> {
        // We first have to make sure that the file does not already exist.
        let path = filepath.try_to_str::<OpenWriteError>()?;
        let exists = self
            .rt
            .block_on(self.metadata.file_exists(path))
            .map_err(OpenWriteError::wrapper(filepath))?;

        if exists {
            return Err(OpenWriteError::FileAlreadyExists(filepath.into()));
        }

        let filepath = self.path(filepath);
        let path = filepath.try_to_str::<OpenWriteError>()?;

        let writer = self.rt.block_on(async {
            let result = self.operator.writer_with(path).append(false).await;
            let writer = match result {
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
        let path = filepath.try_to_str::<OpenReadError>()?;

        self.rt
            .block_on(self.metadata.read_metadata(path))
            .map_err(OpenReadError::wrapper(filepath))?
            .ok_or_else(|| OpenReadError::FileDoesNotExist(filepath.into()))
    }

    fn atomic_write(&self, filepath: &Path, data: &[u8]) -> io::Result<()> {
        let path = filepath.try_to_str::<io::Error>()?;

        self.rt
            .block_on(self.metadata.write_metadata(path, data))
            .map_err(io::Error::wrapper(filepath))
    }

    fn sync_directory(&self) -> io::Result<()> {
        let flushed = self.rt.block_on(self.cache.sync());
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
                .block_on(self.metadata.create_file(path))
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
