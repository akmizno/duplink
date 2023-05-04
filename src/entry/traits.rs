use async_trait::async_trait;
use futures::future::Future;
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

/// Create a link dst pointing to src.
/// Both src and dst must be existing files.
async fn link<P, F, Fut>(src: P, dst: P, f: F) -> io::Result<()>
where
    P: AsRef<Path>,
    F: FnOnce(PathBuf, PathBuf) -> Fut,
    Fut: Future<Output = io::Result<()>>,
{
    let src = src.as_ref();
    let dst = dst.as_ref();

    let src_abspath = tokio::fs::canonicalize(src).await?;
    let dst_abspath = tokio::fs::canonicalize(dst).await?;

    if src_abspath == dst_abspath || !src_abspath.is_file() || !dst_abspath.is_file() {
        // Do nothing to avoid creating invalid links.
        return Ok(());
    }

    let abspath = dst_abspath;
    let dir = abspath.parent().unwrap();
    let filename = src.file_name().unwrap();

    // Try to create a link.
    // Make a temporary link before modifying the dst.
    let tmpdir = tempfile::tempdir_in(dir)?;
    let tmplink = tmpdir.path().join(filename);
    let tmplink_result = f(src.to_owned(), tmplink.to_owned()).await;
    match tmplink_result {
        Ok(_) => {
            log::info!(
                "Create a temporary link {} to {}.",
                tmplink.display(),
                src.display()
            );
        }
        Err(e) => {
            log::error!(
                "Fail to create a temporary link {} to {}.",
                tmplink.display(),
                src.display()
            );
            return Err(e);
        }
    };

    // Try to rename the created link as dst.
    let link_result = tokio::fs::rename(&tmplink, &dst).await;
    match link_result {
        Ok(_) => {
            log::info!("Create a link {} to {}.", dst.display(), src.display());
            Ok(())
        }
        Err(e) => {
            log::error!(
                "Fail to create a link {} to {}.",
                dst.display(),
                src.display()
            );
            Err(e)
        }
    }
}
async fn link_hard<P>(src: P, dst: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    link(src, dst, tokio::fs::hard_link).await
}
#[async_trait]
pub trait LinkTo: FileAttr {
    async fn hardlink(&self, to: &Path) -> io::Result<()> {
        link_hard(to, self.path()).await
    }
}

#[cfg(unix)]
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::Permissions;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use test_log::test;

    fn make_tempdir() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let f1 = dir.path().join("f1");
        let f2 = dir.path().join("f2");
        std::fs::File::create(f1.as_path()).unwrap();
        std::fs::File::create(f2.as_path()).unwrap();
        (dir, f1, f2)
    }

    #[test(tokio::test)]
    async fn link_hard_ok() {
        let (_tmpdir, src, dst) = make_tempdir();
        let src_meta = std::fs::symlink_metadata(&src).unwrap();
        let src_type = src_meta.file_type();
        let dst_meta = std::fs::symlink_metadata(&dst).unwrap();
        let dst_type = dst_meta.file_type();
        assert!(src_type.is_file() && dst.exists());
        assert!(dst_type.is_file() && dst.exists());
        assert_eq!(src_meta.dev(), dst_meta.dev());
        assert_ne!(src_meta.ino(), dst_meta.ino());

        link_hard(&src, &dst).await.unwrap();

        let src_meta = std::fs::metadata(&src).unwrap();
        let src_type = src_meta.file_type();
        let dst_meta = std::fs::symlink_metadata(&dst).unwrap();
        let dst_type = dst_meta.file_type();
        assert!(src_type.is_file() && dst.exists());
        assert!(dst_type.is_file() && dst.exists());
        assert_eq!(src_meta.dev(), dst_meta.dev());
        assert_eq!(src_meta.ino(), dst_meta.ino());
    }
    #[test(tokio::test)]
    async fn link_hard_nonexistent_src() {
        let (_tmpdir, src, dst) = make_tempdir();
        std::fs::remove_file(&src).unwrap();

        assert!(!src.exists());
        assert!(dst.is_file() && dst.exists());

        let result = link_hard(&src, &dst).await;

        assert!(result.is_err());
    }
    #[test(tokio::test)]
    async fn link_hard_nonexistent_dst() {
        let (_tmpdir, src, dst) = make_tempdir();
        std::fs::remove_file(&dst).unwrap();

        assert!(src.is_file() && src.exists());
        assert!(!dst.exists());

        let result = link_hard(&src, &dst).await;

        assert!(result.is_err());
    }
    #[test(tokio::test)]
    async fn link_hard_permissions_err() {
        let (tmpdir, src, dst) = make_tempdir();
        assert!(src.is_file() && src.exists());
        assert!(dst.is_file() && dst.exists());

        let perm = Permissions::from_mode(0o555);
        std::fs::set_permissions(tmpdir.path(), perm).unwrap();
        let result = link_hard(&src, &dst).await;

        assert!(result.is_err());

        let perm = Permissions::from_mode(0o755);
        std::fs::set_permissions(&tmpdir.path(), perm).unwrap();
    }
}
