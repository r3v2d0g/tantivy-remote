mod error;
mod path;

pub use self::{error::WrapIoErrorExt, path::PathExt};

/// A hasher builder which is faster than the one in the standard library.
pub type FastBuildHasher = gxhash::GxBuildHasher;

/// A concurrent hash map using a fast hasher.
pub type FastConcurrentMap<K, V> = scc::HashMap<K, V, FastBuildHasher>;
