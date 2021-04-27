use std::io::Result;
use std::path::{Path, PathBuf};

use std::os::unix::fs::MetadataExt;

use super::FileAttr;

#[derive(Debug, Clone)]
pub struct Entry {
    path: PathBuf,
    len: u64,
    dev: u64,
    ino: u64,
    readonly: bool,

    fast_digest: Option<u64>,
    digest: Option<u64>,
}

impl Entry {
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
        let readonly = meta.permissions().readonly();

        Ok(Some(Entry {
            path: PathBuf::from(path),
            len,
            dev,
            ino,
            readonly,
            fast_digest: None,
            digest: None,
        }))
    }
}
impl FileAttr for Entry {
    fn len(&self) -> u64 {
        self.len
    }
    fn path(&self) -> &Path {
        self.path.as_path()
    }
    fn dev(&self) -> Option<u64> {
        Some(self.dev)
    }
    fn ino(&self) -> Option<u64> {
        Some(self.ino)
    }
    fn readonly(&self) -> bool {
        self.readonly
    }
}
