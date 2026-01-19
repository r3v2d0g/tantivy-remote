use std::{
    fmt::{self, Debug, Formatter},
    io,
    ops::Range,
    sync::Arc,
};

use async_trait::async_trait;
use opendal::Metadata;
use tantivy::{
    HasLen,
    directory::{FileHandle, OwnedBytes},
};
use tokio::runtime::Handle;

use crate::operator::Operator;

/// A [`FileHandle`] implementation for remote files, with automatic caching.
#[derive(Clone)]
pub struct File {
    rt: Handle,

    /// The storage backend the file this is reading is located in.
    operator: Operator,

    /// The path of the file this is reading.
    path: String,

    /// The metadata of the file this is reading.
    metadata: Arc<Metadata>,

    /// Defines the size of the chunks which should be read from the storage backend.
    chunks: Option<usize>,

    /// Defines the number of concurrent requests to make when reading a file from the
    /// storage backend.
    concurrency: Option<usize>,
}

impl File {
    pub(crate) fn open(
        path: impl Into<String>,
        metadata: Arc<Metadata>,
        rt: Handle,
        operator: Operator,
        chunks: Option<usize>,
        concurrency: Option<usize>,
    ) -> Arc<dyn FileHandle> {
        Arc::new(Self {
            rt,
            operator,
            path: path.into(),
            metadata,
            chunks,
            concurrency,
        })
    }
}

#[async_trait]
impl FileHandle for File {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.rt.block_on(self.read_bytes_async(range))
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let mut reader = self.operator.reader_with(&self.path);
        if let Some(chunks) = self.chunks {
            reader = reader.chunk(chunks);
        }

        if let Some(concurrency) = self.concurrency {
            reader = reader.concurrent(concurrency);
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
        self.metadata.content_length() as usize
    }
}

impl Debug for File {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("File")
            .field("path", &self.path)
            .field("metadata", &self.metadata)
            .finish()
    }
}
