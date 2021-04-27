use std::path::Path;

pub trait FileAttr {
    fn path(&self) -> &Path;
    fn len(&self) -> u64;
    fn readonly(&self) -> bool;
    fn dev(&self) -> Option<u64>;
    fn ino(&self) -> Option<u64>;
}

// trait Digest {
//     fn fast_digest() -> u64;
//     fn digest() -> u64;
// }
