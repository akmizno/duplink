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
    #[allow(dead_code)]
    pub fn from_path<P: AsRef<Path>>(p: P) -> Option<Self> {
        let path = p.as_ref();

        let meta = path.symlink_metadata();
        if let Err(e) = &meta {
            log::warn!("{}", e);
            return None;
        }
        let meta = meta.unwrap();

        if !meta.is_file() {
            return None;
        }

        Some(Entry::new(path, meta.len(), meta.permissions().readonly()))
    }

    pub fn new<P: AsRef<Path>>(p: P, len: u64, readonly: bool) -> Self {
        Entry {
            path: PathBuf::from(p.as_ref()),
            len,
            readonly,
            fast_digest: None,
            digest: None,
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

#[cfg(test)]
mod tests {
    use super::Entry;
    use super::FileAttr;

    #[test]
    fn from_regular_path() {
        let p = "files/softlink/original";
        let e = Entry::from_path(p).unwrap();
        assert_eq!(e.path().as_os_str(), p);
        assert_eq!(e.size(), 9);
        assert!(!e.readonly());
        assert!(e.dev().is_none());
        assert!(e.ino().is_none());
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
}
