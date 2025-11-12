use std::{error, io, path::PathBuf, sync::Arc};

use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};

/// Extension trait for [`tantivy`]'s errors, to create an error from an [`io::Error`].
pub trait WrapIoErrorExt: Sized {
    /// Wraps the given [`io::Error`].
    fn wrap(error: io::Error, path: impl Into<PathBuf>) -> Self;

    /// Creates an [`io::Error`] from the given error and wraps it.
    fn wrap_other(
        error: impl Into<Box<dyn error::Error + Send + Sync>>,
        path: impl Into<PathBuf>,
    ) -> Self {
        let error = io::Error::other(error);
        Self::wrap(error, path)
    }

    /// Returns a closure that will wrap an error.
    fn wrapper<E>(path: impl Into<PathBuf>) -> impl Fn(E) -> Self
    where
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        let path = path.into();
        move |error: E| {
            let error = io::Error::other(error);
            Self::wrap(error, path.clone())
        }
    }

    /// Creates an error indicating that the provided file path is invalid.
    fn invalid_filename(path: impl Into<PathBuf>) -> Self {
        let kind = io::ErrorKind::InvalidFilename;
        let error = io::Error::new(kind, "invalid file name");

        Self::wrap(error, path)
    }
}

impl WrapIoErrorExt for io::Error {
    fn wrap(error: io::Error, _path: impl Into<PathBuf>) -> Self {
        error
    }
}

impl WrapIoErrorExt for OpenReadError {
    fn wrap(error: io::Error, path: impl Into<PathBuf>) -> Self {
        Self::IoError {
            io_error: Arc::new(error),
            filepath: path.into(),
        }
    }
}

impl WrapIoErrorExt for OpenWriteError {
    fn wrap(error: io::Error, path: impl Into<PathBuf>) -> Self {
        Self::IoError {
            io_error: Arc::new(error),
            filepath: path.into(),
        }
    }
}

impl WrapIoErrorExt for DeleteError {
    fn wrap(error: io::Error, path: impl Into<PathBuf>) -> Self {
        Self::IoError {
            io_error: Arc::new(error),
            filepath: path.into(),
        }
    }
}
