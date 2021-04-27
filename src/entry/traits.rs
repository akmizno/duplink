use std::path::Path;

pub trait FileAttr {
    fn path(&self) -> &Path;
    fn len(&self) -> u64;
    fn dev(&self) -> u64;
    fn ino(&self) -> u64;
    fn readonly(&self) -> bool;
}

// trait Digest {
//     fn fast_digest() -> u64;
//     fn digest() -> u64;
// }

