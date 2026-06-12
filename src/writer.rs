use std::{
    cmp,
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

/// Called exactly once when the file is finalized, with the [`Outcome`] of buffering it.
pub(crate) type OnDone = Box<dyn FnOnce(Outcome) -> io::Result<()> + Send + Sync>;

/// How a finalized file was handled, reported to [`OnDone`].
pub(crate) enum Outcome {
    /// The file was [logically empty][1]: nothing was written downstream.
    ///
    /// [1]: crate::empty
    Empty(Empty),

    /// The file's bytes were written to the downstream [`Sink`] as their own object.
    Standalone,

    /// The file is [bundle][1]-eligible and was small enough to buffer entirely: its
    /// bytes are handed back here to be bundled at sync time, and nothing was written
    /// downstream.
    ///
    /// [1]: crate::bundle
    Bundled(Vec<u8>),
}

/// A [`TerminatingWrite`] that defers, until the file is finalized, the decision of how
/// to store a segment-component file.
///
/// It buffers up to `cap` bytes in memory, then at finalization:
/// - if the bytes are exactly the [logically empty][1] serialization for the component,
///   nothing is written downstream ([`Outcome::Empty`]);
/// - else if the file is [bundle][2]-eligible and stayed within `cap`, the buffered bytes
///   are handed back to be bundled ([`Outcome::Bundled`]);
/// - else the buffer is flushed to the downstream sink (opened lazily) and any further
///   bytes are streamed straight through ([`Outcome::Standalone`]).
///
/// With bundling disabled, `cap` is just [`Empty::max_len`] and the bundle branch is
/// never taken, so only the empty/standalone outcomes occur.
///
/// [1]: crate::empty
/// [2]: crate::bundle
pub(crate) struct Writer {
    /// The (index-relative) path of the file, used to pick the empty representation that
    /// could apply to its component.
    path: PathBuf,

    /// The maximum number of bytes that may be buffered before the file is necessarily
    /// non-empty or non-bundleable and must be streamed.
    cap: usize,

    /// Whether the file may be [bundled][1] if it stays within `cap` and is non-empty.
    ///
    /// [1]: crate::bundle
    bundle: bool,

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
    /// `cap` is the most bytes to buffer before the file must be streamed standalone,
    /// `bundle` is whether the file may be bundled (when within `cap` and non-empty),
    /// `open` lazily opens the downstream sink and is only ever called for streamed or
    /// standalone files, and `on_done` records the [`Outcome`] once the file is
    /// finalized.
    ///
    /// `cap` is raised to at least [`Empty::max_len`] so that a [logically empty][1]
    /// file is always fully buffered and can be detected; a smaller cap would stream it
    /// out before the check could run.
    ///
    /// [1]: crate::empty
    pub fn new(path: PathBuf, cap: usize, bundle: bool, open: OpenSink, on_done: OnDone) -> Self {
        Self {
            path,
            cap: cmp::max(cap, Empty::max_len()),
            bundle,
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
        let outcome = match mem::replace(&mut self.state, State::Done) {
            State::Buffering(buffer) => {
                if let Some(empty) = Empty::detect(&self.path, &buffer) {
                    // Logically empty: skip the downstream sink entirely.
                    Outcome::Empty(empty)
                } else if self.bundle {
                    // Bundle-eligible and within `cap`: hand the bytes back to be bundled.
                    Outcome::Bundled(buffer)
                } else {
                    // Standalone: flush the buffer downstream and finalize it.
                    let open = self
                        .open
                        .take()
                        .expect("the downstream sink can only be opened once");

                    let mut sink = open()?;
                    sink.write_all(&buffer)?;
                    sink.terminate()?;

                    Outcome::Standalone
                }
            }

            State::Streaming(sink) => {
                sink.terminate()?;
                Outcome::Standalone
            }

            State::Done => return Err(io::Error::other("the file was already finalized")),
        };

        let on_done = self
            .on_done
            .take()
            .expect("a file can only be finalized once");

        on_done(outcome)
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
