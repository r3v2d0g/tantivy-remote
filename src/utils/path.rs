use std::path::{Path, PathBuf};

use super::WrapIoErrorExt;

/// Extension trait for [`Path`], integrating it with [`WrapIoErrorExt`].
pub trait PathExt {
    fn try_to_str<E: WrapIoErrorExt>(&self) -> Result<&str, E>;
}

impl PathExt for Path {
    fn try_to_str<E: WrapIoErrorExt>(&self) -> Result<&str, E> {
        self.to_str().ok_or_else(|| E::invalid_filename(self))
    }
}

impl PathExt for PathBuf {
    fn try_to_str<E: WrapIoErrorExt>(&self) -> Result<&str, E> {
        self.to_str().ok_or_else(|| E::invalid_filename(self))
    }
}
