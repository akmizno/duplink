use async_trait::async_trait;
use std::io;
use std::path::Path;

pub trait FileAttr {
    fn path(&self) -> &Path;
    fn size(&self) -> u64;
    fn readonly(&self) -> bool;
    fn dev(&self) -> Option<u64>;
    fn ino(&self) -> Option<u64>;
}

#[async_trait]
pub trait Digest {
    async fn fast_digest(&self) -> io::Result<u64>;
    async fn digest(&self) -> io::Result<u64>;
}

#[async_trait]
pub trait ContentEq {
    async fn eq_content_path(&self, path: &Path) -> io::Result<bool>;
    async fn eq_content<P: AsRef<Path> + Send + Sync>(&self, path: P) -> io::Result<bool> {
        self.eq_content_path(path.as_ref()).await
    }
}
