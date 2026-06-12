use std::{
    fmt::{self, Debug, Formatter},
    io,
    ops::Range,
    sync::Arc,
};

use async_trait::async_trait;
use block_on_place::HandleExt;
use opendal::Metadata;
use tantivy::{
    HasLen,
    directory::{FileHandle, OwnedBytes},
};
use tokio::runtime::Handle;

use crate::{context::Context, operator::Operator};

/// A [`FileHandle`] implementation for remote files, with automatic caching.
///
/// A file is either backed by the remote object storage ([`File::open`]) or, for
/// files that were detected to be [logically empty][1], reconstructed from an
/// in-memory constant ([`File::memory`]) instead of being read over the network.
///
/// [1]: crate::empty
#[derive(Clone)]
pub struct File {
    /// The path of the file this is reading.
    path: String,

    /// Where the file's bytes come from.
    backend: Backend,
}

/// The source of a [`File`]'s bytes.
#[derive(Clone)]
enum Backend {
    /// The bytes live in the object storage and are read on demand.
    Remote {
        rt: Handle,

        /// The storage backend the file this is reading is located in.
        operator: Operator,

        /// The metadata of the file this is reading.
        metadata: Arc<Metadata>,

        /// Defines the size of the chunks which should be read from the storage backend.
        chunks: Option<usize>,

        /// Defines the number of concurrent requests to make when reading a file from
        /// the storage backend.
        concurrency: Option<usize>,
    },

    /// The file is logically empty: its bytes are a static constant kept in memory, so
    /// no remote read is ever performed.
    Memory { bytes: &'static [u8] },
}

impl File {
    /// Opens the remote file at `path` for reading, returning a [`File`] handle for it.
    pub(crate) fn open(
        path: impl Into<String>,
        metadata: Arc<Metadata>,
        rt: Handle,
        operator: Operator,
        context: &Context,
    ) -> Arc<Self> {
        Arc::new(Self {
            path: path.into(),
            backend: Backend::Remote {
                rt,
                operator,
                metadata,
                chunks: context.read_chunks,
                concurrency: context.read_concurrency,
            },
        })
    }

    /// Builds an in-memory handle for a [logically empty][1] file, serving `bytes`
    /// directly instead of reading anything from the storage backend.
    ///
    /// [1]: crate::empty
    pub(crate) fn memory(path: impl Into<String>, bytes: &'static [u8]) -> Arc<Self> {
        Arc::new(Self {
            path: path.into(),
            backend: Backend::Memory { bytes },
        })
    }

    /// Returns the path of the file.
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
            Backend::Memory { bytes } => Ok(OwnedBytes::new(*bytes).slice(range)),
        }
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let (operator, chunks, concurrency) = match &self.backend {
            Backend::Remote {
                operator,
                chunks,
                concurrency,
                ..
            } => (operator, chunks, concurrency),

            // Memory-backed files never touch the network.
            Backend::Memory { bytes } => return Ok(OwnedBytes::new(*bytes).slice(range)),
        };

        let mut reader = operator.reader_with(&self.path);
        if let Some(chunks) = chunks {
            reader = reader.chunk(*chunks);
        }

        if let Some(concurrency) = concurrency {
            reader = reader.concurrent(*concurrency);
        }

        let reader = reader.await.map_err(io::Error::other)?;
        let range = Range {
            start: range.start as u64,
            end: range.end as u64,
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
            Backend::Remote { metadata, .. } => metadata.content_length() as usize,
            Backend::Memory { bytes } => bytes.len(),
        }
    }
}

impl Debug for File {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut f = f.debug_struct("File");
        f.field("path", &self.path);

        match &self.backend {
            Backend::Remote { metadata, .. } => f.field("metadata", metadata),
            Backend::Memory { bytes } => f.field("empty_len", &bytes.len()),
        };

        f.finish()
    }
}
