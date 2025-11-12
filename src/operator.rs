use std::{
    fmt::{self, Debug, Formatter},
    io,
    path::Path,
    sync::Arc,
};

use derive_more::{Deref, From};
use opendal::{ErrorKind, Metadata};
use tantivy::directory::error::OpenReadError;

#[derive(Clone, Deref, From)]
pub(crate) struct Operator {
    #[deref]
    operator: opendal::Operator,
}

impl Operator {
    /// Fetches the metadata for the file at the given path.
    ///
    /// Fails if the path does not exist, or if it is not pointing to a file.
    pub async fn metadata(&self, filepath: &Path) -> Result<Metadata, OpenReadError> {
        let Some(path) = filepath.to_str() else {
            let filepath = filepath.to_path_buf();
            return Err(OpenReadError::FileDoesNotExist(filepath));
        };

        match self.operator.stat(path).await {
            Ok(metadata) => {
                if metadata.is_file() {
                    Ok(metadata)
                } else {
                    let filepath = filepath.to_path_buf();
                    Err(OpenReadError::FileDoesNotExist(filepath))
                }
            }

            Err(error) => {
                let filepath = filepath.to_path_buf();
                let error = if error.kind() == ErrorKind::NotFound {
                    OpenReadError::FileDoesNotExist(filepath)
                } else {
                    OpenReadError::IoError {
                        io_error: Arc::new(io::Error::other(error)),
                        filepath,
                    }
                };

                Err(error)
            }
        }
    }
}

impl Debug for Operator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Debug::fmt(&self.operator, f)
    }
}
