use std::io::Result;
use std::path::{Path, PathBuf};

use std::os::unix::fs::MetadataExt;

trait Entry {
    fn len(&self) -> u64;
    fn path(&self) -> &Path;
}
trait OsEntry {
    fn dev(&self) -> u64;
    fn ino(&self) -> u64;
}

// trait Digest {
//     fn fast_digest() -> u64;
//     fn digest() -> u64;
// }

#[derive(Debug)]
pub struct UnixEntry {
    path: PathBuf,
    len: u64,
    dev: u64,
    ino: u64,
    fast_digest: Option<u64>,
    digest: Option<u64>,
}

impl UnixEntry {
    pub fn from_path<P: AsRef<Path>>(p: P) -> Result<Option<Self>> {
        let path = p.as_ref();
        let meta = path.symlink_metadata()?;

        if !meta.is_file() {
            // path is not regular file
            return Ok(None);
        };

        let len = meta.len();
        let dev = meta.dev();
        let ino = meta.ino();

        Ok(Some(UnixEntry {
            path: PathBuf::from(path),
            len,
            dev,
            ino,
            fast_digest: None,
            digest: None,
        }))
    }
}
impl Entry for UnixEntry {
    fn len(&self) -> u64 {
        self.len
    }
    fn path(&self) -> &Path {
        self.path.as_path()
    }
}
impl OsEntry for UnixEntry {
    fn dev(&self) -> u64 {
        self.dev
    }
    fn ino(&self) -> u64 {
        self.ino
    }
}

pub type DEntry = UnixEntry;

#[cfg(test)]
mod tests {
    use super::DEntry;

    #[test]
    fn from_regular_path() {
        let e = DEntry::from_path("files/softlink/original");
        assert!(e.is_ok());
        assert!(e.unwrap().is_some());
    }
    #[test]
    fn from_link_path() {
        let e = DEntry::from_path("files/softlink/original_link");
        assert!(e.is_ok());
        assert!(e.unwrap().is_none());
    }
    #[test]
    fn from_dir_path() {
        let e = DEntry::from_path("files/softlink");
        assert!(e.is_ok());
        assert!(e.unwrap().is_none());
    }
    #[test]
    fn from_nonexist_path() {
        let e = DEntry::from_path("files/nonexist-path");
        assert!(e.is_err());
    }
}
