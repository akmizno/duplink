mod generic;
mod traits;
pub use traits::{ContentEq, Digest, FileAttr, LinkTo};

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::Entry;

#[cfg(windows)]
mod win;
#[cfg(windows)]
pub use win::Entry;
