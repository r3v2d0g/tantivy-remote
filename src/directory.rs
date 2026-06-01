mod full;
mod light;

use std::{path::Path, sync::LazyLock};

pub use self::{full::FullDirectory, light::LightDirectory};

// TODO(MLB): replace with `const`s once the `const` version of `Path::new` is stabilized
pub(crate) static META_JSON: LazyLock<&'static Path> = LazyLock::new(|| Path::new("meta.json"));
pub(crate) static MANAGED_JSON: LazyLock<&'static Path> =
    LazyLock::new(|| Path::new(".managed.json"));

/// Returns `true` if `filepath` refers to one of tantivy's metadata files
/// ([`meta.json`][1] or [`.managed.json`][2]).
///
/// [1]: META_JSON
/// [2]: MANAGED_JSON
pub(crate) fn is_metadata(filepath: &Path) -> bool {
    filepath == *META_JSON || filepath == *MANAGED_JSON
}
