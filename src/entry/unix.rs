use std::path::Path;

use std::os::unix::fs::MetadataExt;

use super::generic;
use super::FileAttr;

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
}
