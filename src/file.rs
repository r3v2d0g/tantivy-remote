use std::{
    fmt::{self, Debug, Formatter},
    io,
    ops::Range,
    sync::Arc,
};

use async_trait::async_trait;
use block_on_place::HandleExt;
use tantivy::{
    HasLen,
    directory::{FileHandle, OwnedBytes},
};
use tokio::runtime::Handle;

use crate::{context::Context, operator::Operator};

/// A [`FileHandle`] implementation for remote files, with automatic caching.
///
/// A file's bytes come from one of:
/// - the object store, as the whole object ([`File::open`]);
/// - the object store, as a byte range of a [bundle][1] object ([`File::bundled`]);
/// - memory, for files that are [logically empty][2] or whose bundle has not been synced
///   yet.
///
/// [1]: crate::bundle
/// [2]: crate::empty
#[derive(Clone)]
pub struct File {
    /// The path of the object this reads from (the file's own object, or the bundle
    /// object it lives in).
    path: String,

    /// Where the file's bytes come from.
    backend: Backend,
}

/// The source of a [`File`]'s bytes.
#[derive(Clone)]
enum Backend {
    /// The bytes live in the object storage and are read on demand, as the byte range
    /// `[offset, offset + len)` of the object at [`File::path`].
    ///
    /// For a standalone file `offset` is `0` and `len` is the whole object. For a
    /// bundled file they locate the component inside its bundle object.
    Remote {
        rt: Handle,

        /// The storage backend the object this is reading is located in.
        operator: Operator,

        /// The offset of the file's bytes within the object.
        offset: u64,

        /// The length of the file's bytes.
        len: u64,

        /// Defines the size of the chunks which should be read from the storage backend.
        chunks: Option<usize>,

        /// Defines the number of concurrent requests to make when reading a file from
        /// the storage backend.
        concurrency: Option<usize>,
    },

    /// The bytes are kept in memory and served directly, without any remote read.
    ///
    /// Used for [logically empty][1] files and for [bundled][2] files that have not yet
    /// been synced to the backing store.
    ///
    /// [1]: crate::empty
    /// [2]: crate::bundle
    Memory { bytes: OwnedBytes },
}

impl File {
    /// Opens the remote file at `path` for reading the whole object, returning a
    /// [`File`] handle for it.
    ///
    /// `len` is the object's length (from its metadata).
    pub(crate) fn open(
        path: impl Into<String>,
        len: u64,
        rt: Handle,
        operator: Operator,
        context: &Context,
    ) -> Arc<Self> {
        Arc::new(Self {
            path: path.into(),
            backend: Backend::Remote {
                rt,
                operator,
                offset: 0,
                len,
                chunks: context.read_chunks,
                concurrency: context.read_concurrency,
            },
        })
    }

    /// Opens a [bundled][1] file: the byte range `[offset, offset + len)` of the bundle
    /// object at `path`.
    ///
    /// [1]: crate::bundle
    pub(crate) fn bundled(
        path: impl Into<String>,
        offset: u64,
        len: u64,
        rt: Handle,
        operator: Operator,
        context: &Context,
    ) -> Arc<Self> {
        Arc::new(Self {
            path: path.into(),
            backend: Backend::Remote {
                rt,
                operator,
                offset,
                len,
                chunks: context.read_chunks,
                concurrency: context.read_concurrency,
            },
        })
    }

    /// Builds an in-memory handle for a [logically empty][1] file, serving the static
    /// `bytes` directly instead of reading anything from the storage backend.
    ///
    /// [1]: crate::empty
    pub(crate) fn memory(path: impl Into<String>, bytes: &'static [u8]) -> Arc<Self> {
        Arc::new(Self {
            path: path.into(),
            backend: Backend::Memory {
                bytes: OwnedBytes::new(bytes),
            },
        })
    }

    /// Builds an in-memory handle serving owned `bytes` directly.
    ///
    /// Used for a [bundled][1] file whose bundle object has not been synced yet.
    ///
    /// [1]: crate::bundle
    pub(crate) fn memory_owned(path: impl Into<String>, bytes: Vec<u8>) -> Arc<Self> {
        Arc::new(Self {
            path: path.into(),
            backend: Backend::Memory {
                bytes: OwnedBytes::new(bytes),
            },
        })
    }

    /// Returns the path of the object this reads from.
    #[inline]
    pub fn path(&self) -> &str {
        &self.path
    }
}

#[async_trait]
impl FileHandle for File {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        match &self.backend {
            Backend::Remote { rt, .. } => rt.block_on_place(self.read_bytes_async(range)),
            Backend::Memory { bytes } => Ok(bytes.slice(range)),
        }
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let (operator, offset, chunks, concurrency) = match &self.backend {
            Backend::Remote {
                operator,
                offset,
                chunks,
                concurrency,
                ..
            } => (operator, *offset, chunks, concurrency),

            // Memory-backed files never touch the network.
            Backend::Memory { bytes } => return Ok(bytes.slice(range)),
        };

        let mut reader = operator.reader_with(&self.path);
        if let Some(chunks) = chunks {
            reader = reader.chunk(*chunks);
        }

        if let Some(concurrency) = concurrency {
            reader = reader.concurrent(*concurrency);
        }

        let reader = reader.await.map_err(io::Error::other)?;
        // Shift the requested range by the file's offset within the object so that a
        // bundled file only ever reads its own slice.
        let range = Range {
            start: offset + range.start as u64,
            end: offset + range.end as u64,
        };

        let buffer = reader.read(range).await.map_err(io::Error::other)?;
        // TODO(MLB): avoid copying
        let bytes = buffer.to_vec();
        let bytes = OwnedBytes::new(bytes);

        Ok(bytes)
    }
}

impl HasLen for File {
    fn len(&self) -> usize {
        match &self.backend {
            Backend::Remote { len, .. } => *len as usize,
            Backend::Memory { bytes } => bytes.len(),
        }
    }
}

impl Debug for File {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut f = f.debug_struct("File");
        f.field("path", &self.path);

        match &self.backend {
            Backend::Remote { offset, len, .. } => f.field("offset", offset).field("len", len),
            Backend::Memory { bytes } => f.field("memory_len", &bytes.len()),
        };

        f.finish()
    }
}

/// A [`FileHandle`] exposing the byte range `[offset, offset + len)` of an inner
/// handle.
///
/// Used by [`LightDirectory`][1] to present one [bundled][2] component as a slice
/// of the inner directory's bundle-object handle.
///
/// [1]: crate::LightDirectory
/// [2]: crate::bundle
pub(crate) struct Slice {
    inner: Arc<dyn FileHandle>,
    offset: usize,
    len: usize,
}

impl Slice {
    /// Wraps `inner`, exposing only `[offset, offset + len)`.
    pub fn new(inner: Arc<dyn FileHandle>, offset: usize, len: usize) -> Arc<Self> {
        Arc::new(Self { inner, offset, len })
    }
}

#[async_trait]
impl FileHandle for Slice {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.inner
            .read_bytes(self.offset + range.start..self.offset + range.end)
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.inner
            .read_bytes_async(self.offset + range.start..self.offset + range.end)
            .await
    }
}

impl HasLen for Slice {
    fn len(&self) -> usize {
        self.len
    }
}

impl Debug for Slice {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Slice")
            .field("offset", &self.offset)
            .field("len", &self.len)
            .finish()
    }
}
