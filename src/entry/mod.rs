mod traits;
pub use traits::{ContentEq, Digest, FileAttr};

mod generic;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::Entry;

#[cfg(not(unix))]
pub use generic::Entry;
