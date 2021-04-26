use std::path::Path;

pub trait Entry {
    fn len(&self) -> u64;
    fn path(&self) -> &Path;
}
pub trait OsEntry {
    fn dev(&self) -> u64;
    fn ino(&self) -> u64;
}

// trait Digest {
//     fn fast_digest() -> u64;
//     fn digest() -> u64;
// }

