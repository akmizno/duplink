mod traits;
pub use traits::{ContentEq, Digest, FileAttr, LinkTo};

mod generic;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::Entry;

#[cfg(windows)]
mod win;
#[cfg(windows)]
pub use win::Entry;

#[cfg(not(any(unix, windows)))]
pub use generic::Entry;
