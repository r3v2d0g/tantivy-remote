use std::{
    io::{self, Write},
    mem,
    path::PathBuf,
};

use block_on_place::HandleExt;
use opendal::FuturesAsyncWriter;
use tantivy::directory::{AntiCallToken, TerminatingWrite};
use tokio::{io::AsyncWriteExt, runtime::Handle};
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};

use crate::empty::Empty;

/// A boxed downstream sink.
///
/// This is where a non-empty file's bytes are ultimately written (the object store
/// for [`FullDirectory`][1], the inner [`Directory`][2] for [`LightDirectory`][3]).
///
/// [1]: crate::FullDirectory
/// [2]: tantivy::Directory
/// [3]: crate::LightDirectory
pub(crate) type Sink = Box<dyn TerminatingWrite + Send + Sync>;

/// Lazily opens the downstream [`Sink`].
///
/// Only called once, and only if the file turns out *not* to be logically empty, so
/// empty files never open anything downstream.
pub(crate) type OpenSink = Box<dyn FnOnce() -> io::Result<Sink> + Send + Sync>;

/// Called exactly once when the file is finalized, with `Some` if it was detected
/// to be [logically empty][1] (and thus *not* written downstream).
///
/// [1]: crate::empty
pub(crate) type OnDone = Box<dyn FnOnce(Option<Empty>) -> io::Result<()> + Send + Sync>;

/// A [`TerminatingWrite`] that defers the decision of whether a segment-component
/// file is [logically empty][1] until it is finalized.
///
/// It buffers up to [`Empty::max_len`] bytes in memory. If the file is closed while
/// still within that cap *and* its bytes are exactly the empty serialization for its
/// component, it is logically empty: the downstream sink is never opened, and [`OnDone`]
/// is invoked with the matching [`Empty`]. Otherwise the buffer is flushed to the
/// downstream sink (opened lazily), any further bytes are streamed straight through, and
/// [`OnDone`] is invoked with `None`.
///
/// [1]: crate::empty
pub(crate) struct Writer {
    /// The (index-relative) path of the file, used to pick the empty representation that
    /// could apply to its component.
    path: PathBuf,

    /// The maximum number of bytes that may be buffered before the file is necessarily
    /// non-empty ([`Empty::max_len`]).
    cap: usize,

    /// The current state of the writer.
    state: State,

    /// Opens the downstream sink; taken the first time it is needed.
    open: Option<OpenSink>,

    /// Invoked once when the file is finalized; taken on termination.
    on_done: Option<OnDone>,
}

/// The state machine of a [`Writer`].
enum State {
    /// Still buffering, undecided whether the file is empty.
    Buffering(Vec<u8>),

    /// The file is known to be non-empty; bytes stream straight to the sink.
    Streaming(Sink),

    /// The file has been finalized.
    Done,
}

impl Writer {
    /// Creates a writer for the file at `path`.
    ///
    /// `open` lazily opens the downstream sink and is only ever called for
    /// non-empty files; `on_done` records the outcome once the file is finalized.
    pub fn new(path: PathBuf, open: OpenSink, on_done: OnDone) -> Self {
        Self {
            path,
            cap: Empty::max_len(),
            state: State::Buffering(Vec::new()),
            open: Some(open),
            on_done: Some(on_done),
        }
    }

    /// Transitions to [`State::Streaming`] if still buffering, opening the downstream
    /// sink and flushing any buffered bytes into it.
    fn ensure_streaming(&mut self) -> io::Result<()> {
        if let State::Buffering(buffer) = &mut self.state {
            let buffer = mem::take(buffer);
            let open = self
                .open
                .take()
                .expect("the downstream sink can only be opened once");

            let mut sink = open()?;
            sink.write_all(&buffer)?;
            self.state = State::Streaming(sink);
        }

        Ok(())
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Fast path: still buffering, and the bytes still fit under the cap.
        if let State::Buffering(buffer) = &mut self.state
            && buffer.len() + buf.len() <= self.cap
        {
            buffer.extend_from_slice(buf);
            return Ok(buf.len());
        }

        // Over the cap (or already streaming): the file cannot be empty.
        self.ensure_streaming()?;
        match &mut self.state {
            State::Streaming(sink) => {
                sink.write_all(buf)?;
                Ok(buf.len())
            }

            // `ensure_streaming` left us streaming, unless we were already finalized.
            State::Buffering(_) => unreachable!("ensure_streaming did not start streaming"),
            State::Done => Err(io::Error::other("write after the file was finalized")),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.state {
            // Do not force the sink open: the emptiness decision is deferred to
            // `terminate`, so buffered bytes must stay buffered.
            State::Buffering(_) | State::Done => Ok(()),
            State::Streaming(sink) => sink.flush(),
        }
    }
}

impl TerminatingWrite for Writer {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        let empty = match mem::replace(&mut self.state, State::Done) {
            State::Buffering(buffer) => match Empty::detect(&self.path, &buffer) {
                // Logically empty: skip the downstream sink entirely.
                Some(empty) => Some(empty),

                // Non-empty but small: flush the buffer downstream and finalize it.
                None => {
                    let open = self
                        .open
                        .take()
                        .expect("the downstream sink can only be opened once");

                    let mut sink = open()?;
                    sink.write_all(&buffer)?;
                    sink.terminate()?;

                    None
                }
            },

            State::Streaming(sink) => {
                sink.terminate()?;
                None
            }

            State::Done => return Err(io::Error::other("the file was already finalized")),
        };

        let on_done = self
            .on_done
            .take()
            .expect("a file can only be finalized once");

        on_done(empty)
    }
}

/// A downstream [`Sink`] backed by an [`opendal`] writer, used by
/// [`FullDirectory`][1].
///
/// [1]: crate::FullDirectory
pub(crate) struct OpendalSink {
    rt: Handle,
    writer: Compat<FuturesAsyncWriter>,
}

impl OpendalSink {
    /// Wraps an [`opendal::Writer`] as a boxed [`Sink`].
    pub fn boxed(writer: opendal::Writer, rt: Handle) -> Sink {
        let writer = writer.into_futures_async_write().compat_write();
        Box::new(Self { rt, writer })
    }
}

impl Write for OpendalSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.rt
            .block_on_place(async { self.writer.write(buf).await })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.rt.block_on_place(async { self.writer.flush().await })
    }
}

impl TerminatingWrite for OpendalSink {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.rt
            .clone()
            .block_on_place(async { self.writer.shutdown().await })
    }
}
