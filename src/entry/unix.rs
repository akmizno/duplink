use async_trait::async_trait;
use std::path::Path;
use tokio::io;
use walkdir::DirEntry;

use std::os::unix::fs::MetadataExt;

use super::generic;
use super::{ContentEq, Digest, FileAttr};

#[derive(Debug, Clone)]
pub struct Entry {
    entry: generic::Entry,
    dev: u64,
    ino: u64,
}

impl Entry {
    pub(crate) fn from_path<P: AsRef<Path>>(p: P) -> Option<Self> {
        let meta = p.as_ref().symlink_metadata();
        if let Err(e) = &meta {
            log::warn!("{}", e);
            return None;
        }
        let meta = meta.unwrap();

        if !meta.is_file() {
            return None;
        }

        let entry = generic::Entry::new(p, meta.len(), meta.permissions().readonly());

        Some(Entry {
            entry,
            dev: meta.dev(),
            ino: meta.ino(),
        })
    }

    pub(crate) fn from_direntry(d: DirEntry) -> Option<Self> {
        let meta = d.metadata();
        if let Err(e) = &meta {
            log::warn!("{}", e);
            return None;
        }
        let meta = meta.unwrap();

        if !meta.is_file() {
            return None;
        }

        let entry = generic::Entry::new(d.path(), meta.len(), meta.permissions().readonly());

        Some(Entry {
            entry,
            dev: meta.dev(),
            ino: meta.ino(),
        })
    }
}
impl FileAttr for Entry {
    fn size(&self) -> u64 {
        self.entry.size()
    }
    fn path(&self) -> &Path {
        self.entry.path()
    }
    fn readonly(&self) -> bool {
        self.entry.readonly()
    }
    fn dev(&self) -> Option<u64> {
        Some(self.dev)
    }
    fn ino(&self) -> Option<u64> {
        Some(self.ino)
    }
}

#[async_trait]
impl Digest for Entry {
    async fn fast_digest(&mut self) -> io::Result<u64> {
        self.entry.fast_digest().await
    }
    async fn digest(&mut self) -> io::Result<u64> {
        self.entry.digest().await
    }
}

#[async_trait]
impl ContentEq for Entry {
    async fn eq_bytes(&self, other: &Self) -> io::Result<bool> {
        self.entry.eq_bytes(&other.entry).await
    }
}

#[cfg(test)]
mod tests {
    use super::Entry;
    use super::{ContentEq, Digest, FileAttr};

    #[test]
    fn from_regular_path() {
        let p = "files/softlink/original";
        let e = Entry::from_path(p).unwrap();
        assert_eq!(e.path().as_os_str(), p);
        assert_eq!(e.size(), 9);
        assert!(!e.readonly());
        assert!(e.dev().is_some());
        assert!(e.ino().is_some());
    }
    #[test]
    fn from_link_path() {
        let e = Entry::from_path("files/softlink/original_link");
        assert!(e.is_none());
    }
    #[test]
    fn from_dir_path() {
        let e = Entry::from_path("files/softlink");
        assert!(e.is_none());
    }
    #[test]
    fn from_nonexist_path() {
        let p = "files/nonexist-path";
        let e = Entry::from_path(p);
        assert!(e.is_none());
    }

    #[tokio::test]
    async fn fast_digest_eq() {
        let p = "files/softlink/original";
        let mut e1 = Entry::from_path(p).unwrap();
        let mut e2 = Entry::from_path(p).unwrap();
        let d1 = e1.fast_digest().await.unwrap();
        let d2 = e2.fast_digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test]
    async fn fast_digest_eq_multiple_time() {
        let p = "files/softlink/original";
        let mut e = Entry::from_path(p).unwrap();
        let d1 = e.fast_digest().await.unwrap();
        let d2 = e.fast_digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test]
    async fn fast_digest_ne() {
        let mut e1 = Entry::from_path("files/small-uniques/unique1").unwrap();
        let mut e2 = Entry::from_path("files/small-uniques/unique2").unwrap();
        let d1 = e1.fast_digest().await.unwrap();
        let d2 = e2.fast_digest().await.unwrap();
        assert_ne!(d1, d2);
    }

    #[tokio::test]
    async fn digest_eq() {
        let p = "files/softlink/original";
        let mut e1 = Entry::from_path(p).unwrap();
        let mut e2 = Entry::from_path(p).unwrap();
        let d1 = e1.digest().await.unwrap();
        let d2 = e2.digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test]
    async fn digest_eq_multiple_time() {
        let p = "files/softlink/original";
        let mut e = Entry::from_path(p).unwrap();
        let d1 = e.digest().await.unwrap();
        let d2 = e.digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test]
    async fn digest_ne() {
        let mut e1 = Entry::from_path("files/large-uniques/fill_00_16k").unwrap();
        let mut e2 = Entry::from_path("files/large-uniques/fill_ff_16k").unwrap();
        let d1 = e1.digest().await.unwrap();
        let d2 = e2.digest().await.unwrap();
        assert_ne!(d1, d2);
    }
    #[tokio::test]
    async fn bytes_eq() {
        let e1 = Entry::from_path("files/large-uniques/fill_00_16k").unwrap();
        let e2 = Entry::from_path("files/large-uniques/fill_00_16k").unwrap();
        assert!(e1.eq_bytes(&e2).await.unwrap());
    }
    #[tokio::test]
    async fn bytes_ne() {
        let e1 = Entry::from_path("files/large-uniques/fill_00_16k").unwrap();
        let e2 = Entry::from_path("files/large-uniques/fill_ff_16k").unwrap();
        assert!(!e1.eq_bytes(&e2).await.unwrap());
    }
}
