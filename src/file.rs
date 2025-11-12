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
    operator: Operator,

    path: String,
    metadata: Arc<Metadata>,
}

impl File {
    pub(crate) fn open(
        path: impl Into<String>,
        metadata: Arc<Metadata>,
        rt: Handle,
        operator: Operator,
    ) -> Arc<dyn FileHandle> {
        Arc::new(Self {
            rt,
            operator,
            path: path.into(),
            metadata,
        })
    }
}

#[async_trait]
impl FileHandle for File {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.rt.block_on(self.read_bytes_async(range))
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        // TODO(MLB): cache?
        let reader = self
            .operator
            .reader(&self.path)
            .await
            .map_err(io::Error::other)?;

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
