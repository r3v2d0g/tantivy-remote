use std::{
    io::{self, Write},
    pin::Pin,
    task::{Context, Poll},
};

use opendal::FuturesAsyncWriter;
use pin_project_lite::pin_project;
use tantivy::directory::{AntiCallToken, TerminatingWrite};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    runtime::Handle,
};
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};

use crate::cache::CreatedEntry;

pin_project! {
    pub(crate) struct Writer {
        rt: Handle,
        #[pin]
        writer: Compat<FuturesAsyncWriter>,
        entry: CreatedEntry,
    }
}

impl Writer {
    pub fn new(entry: CreatedEntry, writer: opendal::Writer, rt: Handle) -> Self {
        let writer = writer.into_futures_async_write();
        let writer = writer.compat_write();

        Self { rt, writer, entry }
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.rt.block_on(async { self.writer.write(buf).await })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.rt.block_on(async { self.writer.flush().await })
    }
}

impl AsyncWrite for Writer {
    fn poll_write(self: Pin<&mut Self>, ctx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.project();
        this.writer.poll_write(ctx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<io::Result<()>> {
        let this = self.project();
        this.writer.poll_flush(ctx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<io::Result<()>> {
        let this = self.project();
        match this.writer.poll_shutdown(ctx)? {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => {
                this.entry.done();
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl TerminatingWrite for Writer {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        // TODO(MLB): flush as well?
        self.rt.block_on(async { self.writer.shutdown().await })
    }
}
