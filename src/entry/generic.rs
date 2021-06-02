use async_trait::async_trait;
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, BufReader};
use twox_hash::XxHash64;
use walkdir::DirEntry;
use memmap::MmapOptions;

use super::{ContentEq, Digest, FileAttr};
use crate::util::THRESHOLD;

const BUFSIZE: usize = THRESHOLD as usize;

// make uninitialized buffer
fn make_buffer() -> Box<[u8]> {
    unsafe {
        let mut v = Vec::with_capacity(BUFSIZE);
        v.set_len(BUFSIZE);
        v.into_boxed_slice()
    }
}

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
            match we.into_io_error() {
                None => return Ok(None),
                Some(e) => return Err(e)
            }
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

    async fn calc_hash_async(p: PathBuf, size: usize) -> io::Result<u64> {
        let mut h: XxHash64 = Default::default();
        if size == 0 {
            return Ok(h.finish());
        }

        let f = File::open(p).await?;
        let mut reader = BufReader::new(f);
        let mut buffer = make_buffer();

        let mut size_count = 0;
        while size_count < size {
            let n = reader.read(&mut buffer[..]).await?;
            if n == 0 {
                break;
            }
            h.write(&buffer[..n]);
            size_count += n;
        }

        Ok(h.finish())
    }
    fn calc_hash_mmap(p: PathBuf, size: usize) -> io::Result<u64> {
        let mut h: XxHash64 = Default::default();
        if size == 0 {
            return Ok(h.finish());
        }
        let f = std::fs::File::open(p)?;
        let mmap = unsafe{ MmapOptions::new().map(&f)? };
        h.write(&mmap[..size]);

        Ok(h.finish())
    }
    async fn calc_hash(&self, size: usize) -> io::Result<u64> {
        let path = PathBuf::from(self.path());
        if size <= BUFSIZE {
            Entry::calc_hash_async(path, size).await
        } else {
            tokio::task::block_in_place(move|| Entry::calc_hash_mmap(path, size))
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

#[async_trait]
impl Digest for Entry {
    async fn fast_digest(&self) -> io::Result<u64> {
        if self.size() as usize <= BUFSIZE {
            return self.digest().await;
        }

        self.calc_hash(BUFSIZE).await
    }
    async fn digest(&self) -> io::Result<u64> {
        self.calc_hash(self.size() as usize).await
    }
}

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
