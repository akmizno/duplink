use async_trait::async_trait;
use memmap::MmapOptions;
use std::hash::Hasher;
use std::io;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;
use twox_hash::XxHash64;

use crate::util::THRESHOLD;
const BUFSIZE: usize = THRESHOLD as usize;

pub trait FileAttr {
    fn path(&self) -> &Path;
    fn size(&self) -> u64;
    fn readonly(&self) -> bool;
    fn dev(&self) -> Option<u64>;
    fn ino(&self) -> Option<u64>;
}

async fn calc_hash_async(p: PathBuf, size: usize) -> io::Result<u64> {
    let mut h: XxHash64 = Default::default();
    if size == 0 {
        return Ok(h.finish());
    }

    let f = tokio::fs::File::open(p).await?;
    let mut reader = tokio::io::BufReader::new(f);
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
    let mmap = unsafe { MmapOptions::new().map(&f)? };
    h.write(&mmap[..size]);

    Ok(h.finish())
}
async fn calc_hash<P: AsRef<Path>>(path: P, size: usize) -> io::Result<u64> {
    let p = PathBuf::from(path.as_ref());
    if size <= BUFSIZE {
        calc_hash_async(p, size).await
    } else {
        tokio::task::block_in_place(move || calc_hash_mmap(p, size))
    }
}
#[async_trait]
pub trait Digest: FileAttr {
    async fn fast_digest(&self) -> io::Result<u64> {
        if self.size() as usize <= BUFSIZE {
            return self.digest().await;
        }

        calc_hash(self.path(), BUFSIZE).await
    }
    async fn digest(&self) -> io::Result<u64> {
        calc_hash(self.path(), self.size() as usize).await
    }
}

// make uninitialized buffer
fn make_buffer() -> Box<[u8]> {
    unsafe {
        let mut v = Vec::with_capacity(BUFSIZE);
        v.set_len(BUFSIZE);
        v.into_boxed_slice()
    }
}
async fn eq_content_async(p1: PathBuf, p2: PathBuf) -> io::Result<bool> {
    let f1 = tokio::fs::File::open(p1).await?;
    let f2 = tokio::fs::File::open(p2).await?;
    let mut reader1 = tokio::io::BufReader::new(f1);
    let mut reader2 = tokio::io::BufReader::new(f2);

    let mut buffer1 = make_buffer();
    let mut buffer2 = make_buffer();

    loop {
        let n1 = reader1.read(&mut buffer1[..]).await?;
        let n2 = reader2.read(&mut buffer2[..]).await?;

        if n1 == 0 && n2 == 0 {
            return Ok(true);
        }

        if buffer1[..n1] != buffer2[..n2] {
            return Ok(false);
        }
    }
}
fn eq_content_mmap(p1: PathBuf, p2: PathBuf) -> io::Result<bool> {
    let f1 = std::fs::File::open(p1)?;
    let f2 = std::fs::File::open(p2)?;
    let mmap1 = unsafe { MmapOptions::new().map(&f1)? };
    let mmap2 = unsafe { MmapOptions::new().map(&f2)? };
    Ok(mmap1[..] == mmap2[..])
}
#[async_trait]
pub trait ContentEq: FileAttr {
    async fn eq_content(&self, other: &Self) -> io::Result<bool> {
        let self_path = PathBuf::from(self.path());
        let other_path = PathBuf::from(other.path());

        if self.size() as usize <= BUFSIZE {
            eq_content_async(self_path, other_path).await
        } else {
            tokio::task::block_in_place(move || eq_content_mmap(self_path, other_path))
        }
    }
}
