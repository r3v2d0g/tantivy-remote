use std::{io, io::Write, path::Path, sync::Arc};

use block_on_place::HandleExt;
use derive_more::Debug;
use eyre::Result;
use sqlx::PgPool;
use tantivy::{
    Directory,
    directory::{
        DirectoryLock, FileHandle, Lock, TerminatingWrite, WatchCallback, WatchHandle, WritePtr,
        error::{DeleteError, LockError, OpenReadError, OpenWriteError},
    },
};
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::{
    bundle::{self, Bundler},
    context::Context,
    directory::is_metadata,
    empty::Empty,
    file::{File, Slice},
    metadata::{FileRecord, MetadataStore},
    utils::{PathExt, WrapIoErrorExt},
    writer::{OnDone, OpenSink, Outcome, Sink, Writer},
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

    /// Buffers bundle-eligible files until they are written as one object per segment to
    /// the inner directory at sync time.
    ///
    /// Empty if bundling is not enabled.
    bundle: Bundler,

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
            bundle: Bundler::default(),
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

    /// Enables [bundling][1]: a segment's (non-empty, non-`.del`) component files are
    /// concatenated into a single `<segment_uuid>.bundle` file in the inner directory
    /// instead of one file per component.
    ///
    /// Disabled by default.
    ///
    /// A component file larger than [`with_bundle_max_file_bytes`][2] is left as its
    /// own file so a large (merge) segment is never held in memory.
    ///
    /// [1]: crate::bundle
    /// [2]: Self::with_bundle_max_file_bytes
    pub fn with_bundling(mut self) -> Self {
        self.metadata.context.bundle = true;
        self
    }

    /// Sets the per-file size cap for [bundling][1]: files larger than this are written
    /// as their own file instead of being bundled.
    ///
    /// Default is 16 MiB.
    ///
    /// [1]: crate::bundle
    pub fn with_bundle_max_file_bytes(mut self, bytes: usize) -> Self {
        self.metadata.context.bundle_max_file_bytes = bytes;
        self
    }
}

impl<D: Directory + Clone> Directory for LightDirectory<D> {
    fn get_file_handle(&self, filepath: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        // Standalone files live in the inner directory; empty and bundled files were
        // never written there as themselves, so a miss falls back to the metadata store.
        match self.inner.get_file_handle(filepath) {
            Err(OpenReadError::FileDoesNotExist(_)) => (),
            result => return result,
        }

        let path = filepath.try_to_str::<OpenReadError>()?;

        // A bundle-eligible file written but not yet synced lives only in the in-memory
        // bundler; serve it from there.
        if let Some(bytes) = self.rt.block_on_place(self.bundle.get(filepath)) {
            return Ok(File::memory_owned(path, bytes));
        }

        let record = self
            .rt
            .block_on_place(self.metadata.file_lookup(path))
            .map_err(OpenReadError::wrapper(filepath))?;

        match record {
            // Reconstruct the logically empty file's bytes from memory.
            Some(record) if record.is_empty => {
                let empty = Empty::for_path(filepath)
                    .ok_or_else(|| OpenReadError::FileDoesNotExist(filepath.into()))?;
                Ok(File::memory(path, empty.bytes()))
            }

            // Bundled: a byte range of the inner directory's bundle file.
            Some(FileRecord {
                byte_offset,
                byte_length: Some(byte_length),
                ..
            }) => {
                let object = bundle::object(filepath)
                    .ok_or_else(|| OpenReadError::FileDoesNotExist(filepath.into()))?;
                let handle = self.inner.get_file_handle(&object)?;
                Ok(Slice::new(
                    handle,
                    byte_offset as usize,
                    byte_length as usize,
                ))
            }

            // Standalone files are never tracked here (they are in the inner directory), and
            // `None` means the file genuinely does not exist.
            _ => Err(OpenReadError::FileDoesNotExist(filepath.into())),
        }
    }

    fn delete(&self, filepath: &Path) -> Result<(), DeleteError> {
        // Logically empty files are not in the inner directory; delete them from the
        // metadata store instead.
        match self.inner.delete(filepath) {
            Err(DeleteError::FileDoesNotExist(_)) => (),
            result => return result,
        }

        let path = filepath.try_to_str::<DeleteError>()?;
        let deleted = self
            .rt
            .block_on_place(self.metadata.delete_file(path))
            .map_err(DeleteError::wrapper(filepath))?;

        if deleted {
            Ok(())
        } else {
            Err(DeleteError::FileDoesNotExist(filepath.into()))
        }
    }

    fn exists(&self, filepath: &Path) -> Result<bool, OpenReadError> {
        if is_metadata(filepath) {
            let path = filepath.try_to_str::<OpenReadError>()?;
            return self
                .rt
                .block_on_place(self.metadata.metadata_exists(path))
                .map_err(OpenReadError::wrapper(filepath));
        }

        if self.inner.exists(filepath)? {
            return Ok(true);
        }

        // It might be a logically empty file, tracked only in the metadata store.
        let path = filepath.try_to_str::<OpenReadError>()?;
        let found = self
            .rt
            .block_on_place(self.metadata.file_lookup(path))
            .map_err(OpenReadError::wrapper(filepath))?;

        Ok(found.is_some())
    }

    fn open_write(&self, filepath: &Path) -> Result<WritePtr, OpenWriteError> {
        // The inner writer is opened lazily: empty and bundled files are never written to
        // the inner directory as themselves.
        let inner = self.inner.clone();
        let target = filepath.to_path_buf();
        let open: OpenSink = Box::new(move || {
            let writer = inner.open_write(&target).map_err(io::Error::other)?;
            Ok(Box::new(writer) as Sink)
        });

        // When bundling, a small bundle-eligible file is buffered (not written to the
        // inner directory) and bundled at sync; everything else behaves as without it.
        let eligible = self.metadata.context.bundle && bundle::is_bundleable(filepath);
        let cap = if eligible {
            self.metadata.context.bundle_max_file_bytes
        } else {
            Empty::max_len()
        };

        let metadata = self.metadata.clone();
        let bundler = self.bundle.clone();
        let rt = self.rt.clone();
        let path = filepath.to_path_buf();

        let on_done: OnDone = Box::new(move |outcome| {
            match outcome {
                // Empty files are recorded in the metadata store and reconstructed on read.
                Outcome::Empty(_) => {
                    let path = path.try_to_str::<io::Error>()?;
                    rt.block_on_place(metadata.create_file(path, true, None))
                        .map_err(io::Error::other)?;
                }

                // Standalone files already live in the inner directory; nothing to record.
                Outcome::Standalone => {}

                // Bundled bytes are buffered until the next sync.
                Outcome::Bundled(bytes) => rt.block_on_place(bundler.buffer(path, bytes)),
            }

            Ok(())
        });

        let writer = Writer::new(filepath.to_path_buf(), cap, eligible, open, on_done);
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
        // Bundled files: write one object per segment to the inner directory, then record
        // each component's byte range in the metadata store.
        for bundle in self.rt.block_on_place(self.bundle.drain()) {
            let mut writer = self
                .inner
                .open_write(&bundle.path)
                .map_err(io::Error::other)?;
            writer.write_all(&bundle.bytes)?;
            writer.terminate()?;

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
