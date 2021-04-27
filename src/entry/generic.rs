use std::io::Result;
use std::path::{Path, PathBuf};

use super::FileAttr;

#[derive(Debug, Clone)]
pub struct Entry {
    path: PathBuf,
    len: u64,
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
        let readonly = meta.permissions().readonly();

        Ok(Some(Entry {
            path: PathBuf::from(path),
            len,
            readonly,
            fast_digest: None,
            digest: None,
        }))
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
