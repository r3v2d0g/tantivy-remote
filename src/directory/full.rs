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
    bundle::{self, Bundler},
    cache::Cache,
    context::Context,
    directory::is_metadata,
    empty::Empty,
    file::File,
    metadata::MetadataStore,
    operator::Operator,
    utils::{PathExt, WrapIoErrorExt},
    writer::{OnDone, OpenSink, OpendalSink, Outcome, Writer},
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
/// ## File-lookup cache
///
/// Successful PostgreSQL `file_lookup` results are cached in-process for the
/// lifetime of this directory (shared across clones). Call [`prefetch_files`][2]
/// once before a cold open/reload to avoid one SELECT per segment component.
/// Missing paths are not cached; after another process commits, prefetch again on
/// this directory instance.
///
/// [1]: tantivy::ReloadPolicy::Manual
/// [2]: Self::prefetch_files
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

    /// Buffers bundle-eligible files until they are written as one object per segment
    /// at sync time.
    ///
    /// Empty if bundling is not enabled.
    bundle: Bundler,

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
            bundle: Bundler::default(),
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

    /// Enables [bundling][1]: a segment's (non-empty, non-`.del`) component files are
    /// concatenated into a single `<segment_uuid>.bundle` object instead of one object
    /// per file.
    ///
    /// This drastically cuts the number of objects for indexes with many small segments.
    ///
    /// Disabled by default.
    ///
    /// A component file larger than [`with_bundle_max_file_bytes`][2] is left as its
    /// own object so a large (merge) segment is never held in memory.
    ///
    /// [1]: crate::bundle
    /// [2]: Self::with_bundle_max_file_bytes
    pub fn with_bundling(mut self) -> Self {
        self.context.bundle = true;
        self.metadata.context.bundle = true;
        self
    }

    /// Sets the per-file size cap for [bundling][1]: files larger than this are written
    /// as their own object instead of being bundled.
    ///
    /// Default is 16 MiB.
    ///
    /// [1]: crate::bundle
    pub fn with_bundle_max_file_bytes(mut self, bytes: usize) -> Self {
        self.context.bundle_max_file_bytes = bytes;
        self.metadata.context.bundle_max_file_bytes = bytes;
        self
    }

    /// Loads all non-deleted file records for this index into the local lookup cache in
    /// one PostgreSQL query.
    ///
    /// Returns the number of rows loaded. Safe to call multiple times: each call
    /// **replaces** the cache with a fresh snapshot.
    ///
    /// Call this once at the start of a reader open/reload so subsequent
    /// [`get_file_handle`][1] calls do not issue per-path `SELECT`s for known
    /// empty/bundled/standalone metadata rows.
    ///
    /// Memory: `O(number of file rows)` for this index. After another process commits
    /// new or deleted files, call this again before relying on the cache for those
    /// paths.
    ///
    /// [1]: Directory::get_file_handle
    pub async fn prefetch_files(&self) -> sqlx::Result<usize> {
        self.metadata.prefetch_files().await
    }

    /// Returns how many PostgreSQL queries `file_lookup` has issued on this directory's
    /// metadata store (cache hits do not count).
    ///
    /// Useful for metrics and for verifying that [`prefetch_files`][1] eliminated
    /// per-path lookups.
    ///
    /// [1]: Self::prefetch_files
    pub fn file_lookup_query_count(&self) -> u64 {
        self.metadata.file_lookup_query_count()
    }

    /// Opens the file at `filepath` for reading, returning a [`File`] handle for it.
    pub async fn get_file(&self, filepath: &Path) -> Result<Arc<File>, OpenReadError> {
        let path = filepath.try_to_str::<OpenReadError>()?;
        let prefixed = self.context.path(filepath);

        if let Some(file) = self.cache.get_file(&prefixed).await {
            return Ok(file);
        }

        // A bundle-eligible file that has been written but not yet synced lives only in
        // the in-memory bundler – serve it from there.
        if let Some(bytes) = self.bundle.get(filepath).await {
            let path = prefixed.to_string_lossy().into_owned();
            let file = File::memory_owned(path, bytes);
            return self.cache.file(&prefixed, async || Ok(file)).await;
        }

        // Otherwise resolve it: created this session (standalone or empty), or recorded
        // in the metadata store (standalone, empty, or bundled).
        enum Source {
            Empty(Empty),
            Standalone,
            Bundled { offset: u64, len: u64 },
        }

        let source = match self.cache.created_state(&prefixed).await {
            Some(state) => match state.empty {
                Some(empty) => Source::Empty(empty),
                None => Source::Standalone,
            },
            None => match self
                .metadata
                .file_lookup(path)
                .await
                .map_err(OpenReadError::wrapper(path))?
            {
                None => return Err(OpenReadError::FileDoesNotExist(path.into())),
                Some(record) if record.is_empty => Source::Empty(
                    Empty::for_path(filepath)
                        .ok_or_else(|| OpenReadError::FileDoesNotExist(path.into()))?,
                ),
                Some(record) => match record.byte_length {
                    Some(len) => Source::Bundled {
                        offset: record.byte_offset as u64,
                        len: len as u64,
                    },
                    None => Source::Standalone,
                },
            },
        };

        let open = async || match source {
            // Logically empty: reconstructed from memory, never read from the store.
            Source::Empty(empty) => {
                let path = prefixed.to_string_lossy().into_owned();
                Ok(File::memory(path, empty.bytes()))
            }

            // Bundled: a byte range of the segment's bundle object.
            Source::Bundled { offset, len } => {
                let object = bundle::object(filepath)
                    .ok_or_else(|| OpenReadError::FileDoesNotExist(path.into()))?;
                let object = self.context.path(object);
                let object = object.to_string_lossy().into_owned();
                Ok(File::bundled(
                    object,
                    offset,
                    len,
                    self.rt.clone(),
                    self.operator.clone(),
                    &self.context,
                ))
            }

            // Standalone: the whole object at the file's own path.
            Source::Standalone => {
                let path = prefixed.to_string_lossy().into_owned();
                let metadata = self
                    .cache
                    .metadata(&prefixed, || self.operator.metadata(&prefixed))
                    .await?;

                Ok(File::open(
                    path,
                    metadata.content_length(),
                    self.rt.clone(),
                    self.operator.clone(),
                    &self.context,
                ))
            }
        };

        self.cache.file(&prefixed, open).await
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
        // We first have to make sure that the file does not already exist, then register
        // it as created so it is visible before the directory is synced.
        let path = filepath.try_to_str::<OpenWriteError>()?;
        let mut entry = self.rt.block_on_place(async {
            let exists = self
                .metadata
                .file_exists(path)
                .await
                .map_err(OpenWriteError::wrapper(filepath))?;

            if exists {
                return Err(OpenWriteError::FileAlreadyExists(filepath.into()));
            }

            self.cache.created(self.context.path(filepath)).await
        })?;

        let prefixed = self.context.path(filepath);
        let path = prefixed.to_string_lossy().into_owned();

        // The object-store writer is opened lazily: a logically empty or bundled file
        // never opens it, so its bytes are never sent to the object store individually.
        let operator = self.operator.clone();
        let rt = self.rt.clone();
        let chunks = self.context.write_chunks;
        let concurrency = self.context.write_concurrency;

        let open: OpenSink = Box::new(move || {
            rt.clone().block_on_place(async {
                let mut writer = operator.writer_with(&path).append(false);
                if let Some(chunks) = chunks {
                    writer = writer.chunk(chunks);
                }

                if let Some(concurrency) = concurrency {
                    writer = writer.concurrent(concurrency);
                }

                let writer = writer.await.map_err(io::Error::other)?;
                Ok(OpendalSink::boxed(writer, rt.clone()))
            })
        });

        // When bundling, a small bundle-eligible file is buffered (not streamed) and
        // bundled at sync; everything else behaves as without bundling.
        let eligible = self.context.bundle && bundle::is_bundleable(filepath);
        let cap = if eligible {
            self.context.bundle_max_file_bytes
        } else {
            Empty::max_len()
        };

        // Once finalized, record the outcome so `sync_directory` can persist it: empty /
        // standalone files stay on the cache entry; bundled bytes go to the bundler.
        let bundler = self.bundle.clone();
        let rt = self.rt.clone();
        let relative = filepath.to_path_buf();
        let on_done: OnDone = Box::new(move |outcome| {
            match outcome {
                Outcome::Empty(empty) => entry.done(Some(empty)),
                Outcome::Standalone => entry.done(None),
                Outcome::Bundled(bytes) => {
                    entry.remove();
                    rt.block_on_place(bundler.buffer(relative, bytes));
                }
            }

            Ok(())
        });

        let writer = Writer::new(prefixed, cap, eligible, open, on_done);
        Ok(WritePtr::new(Box::new(writer)))
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
        // Standalone and empty files: record each in the metadata store. The cache keys
        // are prefixed with the index ID, which we strip before storing.
        for (path, empty) in self.rt.block_on_place(self.cache.sync()) {
            let mut components = path.components();
            components.next();

            let filepath = components.as_path();
            let path = filepath.try_to_str::<io::Error>()?;

            self.rt
                .block_on_place(self.metadata.create_file(path, empty.is_some(), None))
                .map_err(io::Error::wrapper(filepath))?;
        }

        // Bundled files: write one object per segment, then record each component's byte
        // range. The bundler is keyed by index-relative paths.
        for bundle in self.rt.block_on_place(self.bundle.drain()) {
            let object = self.context.path(&bundle.path);
            let object = object.try_to_str::<io::Error>()?;

            self.rt.block_on_place(async {
                let mut writer = self.operator.writer_with(object).append(false).await?;
                writer.write_from(bundle.bytes.as_slice()).await?;
                writer.close().await?;
                io::Result::Ok(())
            })?;

            for entry in bundle.entries {
                let path = entry.path.try_to_str::<io::Error>()?;
                self.rt
                    .block_on_place(self.metadata.create_file(
                        path,
                        false,
                        Some((entry.offset, entry.length)),
                    ))
                    .map_err(io::Error::wrapper(&entry.path))?;
            }
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
