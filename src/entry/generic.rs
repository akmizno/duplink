use std::io;
use std::path::{Path, PathBuf};
use walkdir::DirEntry;

use super::{ContentEq, Digest, FileAttr};

#[derive(Debug)]
pub struct Entry {
    path: PathBuf,
    len: u64,
    readonly: bool,
}

impl Entry {
    #[allow(dead_code)]
    pub fn from_path<P: AsRef<Path>>(p: P) -> io::Result<Option<Self>> {
        let path = p.as_ref();

        let meta = path.metadata();
        if let Err(e) = meta {
            return Err(e);
        }
        let meta = meta.unwrap();

        if !meta.is_file() {
            return Ok(None);
        }

        Ok(Some(Entry::new(path, meta.len(), meta.permissions().readonly())))
    }

    #[allow(dead_code)]
    pub(crate) fn from_direntry(d: DirEntry) -> io::Result<Option<Entry>> {
        let meta = d.metadata();
        if let walkdir::Result::Err(we) = meta {
            let westr = format!("{}", we);
            let err = match we.into_io_error() {
                None => io::Error::new(io::ErrorKind::Other, westr),
                Some(e) => e
            };
            return Err(err);
        }
        let meta = meta.unwrap();

        if !meta.is_file() {
            return Ok(None);
        }

        Ok(Some(Entry::new(d.path(), meta.len(), meta.permissions().readonly())))
    }

    pub fn new<P: AsRef<Path>>(p: P, len: u64, readonly: bool) -> Self {
        Entry {
            path: PathBuf::from(p.as_ref()),
            len,
            readonly,
        }
    }

}
impl FileAttr for Entry {
    fn size(&self) -> u64 {
        self.len
    }
    fn path(&self) -> &Path {
        self.path.as_path()
    }
    fn readonly(&self) -> bool {
        self.readonly
    }
    fn dev(&self) -> Option<u64> {
        None
    }
    fn ino(&self) -> Option<u64> {
        None
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
        #[cfg(unix)]
        assert_eq!(e.size(), 9);
        #[cfg(windows)]
        assert_eq!(e.size(), 10);
        assert!(!e.readonly());
        assert!(e.dev().is_none());
        assert!(e.ino().is_none());
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
        let e1 = Entry::from_path("files/small-uniques/unique1").unwrap().unwrap();
        let e2 = Entry::from_path("files/small-uniques/unique2").unwrap().unwrap();
        let d1 = e1.fast_digest().await.unwrap();
        let d2 = e2.fast_digest().await.unwrap();
        assert_ne!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn fast_digest_small() {
        let e = Entry::from_path("files/small-uniques/unique1").unwrap().unwrap();
        let f = e.fast_digest().await.unwrap();
        let d = e.digest().await.unwrap();
        assert_eq!(f, d);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn fast_digest_large() {
        let e = Entry::from_path("files/large-uniques/fill_00_16k").unwrap().unwrap();
        let f = e.fast_digest().await.unwrap();
        let d = e.digest().await.unwrap();
        assert_ne!(f, d);
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
        let e1 = Entry::from_path("files/large-uniques/fill_00_16k").unwrap().unwrap();
        let e2 = Entry::from_path("files/large-uniques/fill_ff_16k").unwrap().unwrap();
        let d1 = e1.digest().await.unwrap();
        let d2 = e2.digest().await.unwrap();
        assert_ne!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn content_eq() {
        let e = Entry::from_path("files/large-uniques/fill_00_16k").unwrap().unwrap();
        let p = Entry::from_path("files/large-uniques/fill_00_16k").unwrap().unwrap();
        assert!(e.eq_content(&p).await.unwrap());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn content_ne() {
        let e = Entry::from_path("files/large-uniques/fill_00_16k").unwrap().unwrap();
        let p = Entry::from_path("files/large-uniques/fill_ff_16k").unwrap().unwrap();
        assert!(!e.eq_content(&p).await.unwrap());
    }
}
