use std::io;
use std::path::Path;
use walkdir::DirEntry;

use std::os::windows::fs::MetadataExt;

use super::generic;
use super::{ContentEq, Digest, FileAttr, LinkTo};

#[derive(Debug)]
pub struct Entry {
    entry: generic::Entry,
    vol: Option<u32>,
    idx: Option<u64>,
}

impl Entry {
    pub(crate) fn from_path<P: AsRef<Path>>(p: P) -> io::Result<Option<Self>> {
        let meta = p.as_ref().metadata();
        if let Err(e) = meta {
            return Err(e);
        }
        let meta = meta.unwrap();

        if !meta.is_file() {
            return Ok(None);
        }

        let entry = generic::Entry::new(p, meta.len(), meta.permissions().readonly());

        Ok(Some(Entry {
            entry,
            vol: meta.volume_serial_number(),
            idx: meta.file_index(),
        }))
    }

    pub(crate) fn from_direntry(d: DirEntry) -> io::Result<Option<Self>> {
        // Call Self::from_path() because
        // volume serial number and file index can not be obtained from
        // Metadata created by walkdir::DirEntry.
        Entry::from_path(d.path())
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
        match self.vol {
            None => None,
            Some(v) => Some(v as u64),
        }
    }
    fn ino(&self) -> Option<u64> {
        self.idx
    }
}

impl Digest for Entry {}

impl ContentEq for Entry {}

#[cfg(test)]
mod tests {
    use super::Entry;
    use super::{ContentEq, Digest, FileAttr};

    #[test]
    fn from_regular_path() {
        let p = "files/softlink/original";
        let e = Entry::from_path(p).unwrap().unwrap();
        assert_eq!(e.path().as_os_str(), p);
        assert_eq!(e.size(), 10);
        assert!(!e.readonly());
        assert!(e.dev().is_some());
        assert!(e.ino().is_some());
    }
    #[test]
    fn from_link_path() {
        let e = Entry::from_path("files/softlink/original_link").unwrap();
        assert!(e.is_some());
    }
    #[test]
    fn from_dir_path() {
        let e = Entry::from_path("files/softlink").unwrap();
        assert!(e.is_none());
    }
    #[test]
    fn from_nonexist_path() {
        let p = "files/nonexist-path";
        let e = Entry::from_path(p);
        assert!(e.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn fast_digest_eq() {
        let p = "files/softlink/original";
        let e1 = Entry::from_path(p).unwrap().unwrap();
        let e2 = Entry::from_path(p).unwrap().unwrap();
        let d1 = e1.fast_digest().await.unwrap();
        let d2 = e2.fast_digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn fast_digest_eq_multiple_time() {
        let p = "files/softlink/original";
        let e = Entry::from_path(p).unwrap().unwrap();
        let d1 = e.fast_digest().await.unwrap();
        let d2 = e.fast_digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn fast_digest_ne() {
        let e1 = Entry::from_path("files/small-uniques/unique1")
            .unwrap()
            .unwrap();
        let e2 = Entry::from_path("files/small-uniques/unique2")
            .unwrap()
            .unwrap();
        let d1 = e1.fast_digest().await.unwrap();
        let d2 = e2.fast_digest().await.unwrap();
        assert_ne!(d1, d2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn digest_eq() {
        let p = "files/softlink/original";
        let e1 = Entry::from_path(p).unwrap().unwrap();
        let e2 = Entry::from_path(p).unwrap().unwrap();
        let d1 = e1.digest().await.unwrap();
        let d2 = e2.digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn digest_eq_multiple_time() {
        let p = "files/softlink/original";
        let e = Entry::from_path(p).unwrap().unwrap();
        let d1 = e.digest().await.unwrap();
        let d2 = e.digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn digest_ne() {
        let e1 = Entry::from_path("files/large-uniques/fill_00_16k")
            .unwrap()
            .unwrap();
        let e2 = Entry::from_path("files/large-uniques/fill_ff_16k")
            .unwrap()
            .unwrap();
        let d1 = e1.digest().await.unwrap();
        let d2 = e2.digest().await.unwrap();
        assert_ne!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn content_eq() {
        let e = Entry::from_path("files/large-uniques/fill_00_16k")
            .unwrap()
            .unwrap();
        let p = Entry::from_path("files/large-uniques/fill_00_16k")
            .unwrap()
            .unwrap();
        assert!(e.eq_content(&p).await.unwrap());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn content_ne() {
        let e = Entry::from_path("files/large-uniques/fill_00_16k")
            .unwrap()
            .unwrap();
        let p = Entry::from_path("files/large-uniques/fill_ff_16k")
            .unwrap()
            .unwrap();
        assert!(!e.eq_content(&p).await.unwrap());
    }
}
