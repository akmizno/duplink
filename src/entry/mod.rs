use std::path::Path;

mod traits;
pub use traits::FileAttr;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::Entry;

#[cfg(not(unix))]
mod generic;
#[cfg(not(unix))]
pub use generic::Entry;

pub enum Node {
    Single(Entry),
    Multi(Vec<Entry>),
}

impl From<Entry> for Node {
    fn from(entry: Entry) -> Self {
        Node::Single(entry)
    }
}
impl From<Vec<Entry>> for Node {
    fn from(mut entries: Vec<Entry>) -> Self {
        assert!(!entries.is_empty());

        if 1 == entries.len() {
            Node::Single(entries.pop().unwrap())
        } else {
            Node::Multi(entries)
        }
    }
}

impl Node {
    fn entry(&self) -> &Entry {
        match self {
            Node::Single(e) => e,
            Node::Multi(v) => unsafe {
                debug_assert!(!v.is_empty());
                v.get_unchecked(0)
            },
        }
    }
}

impl FileAttr for Node {
    fn path(&self) -> &Path {
        self.entry().path()
    }
    fn size(&self) -> u64 {
        self.entry().size()
    }
    fn dev(&self) -> Option<u64> {
        self.entry().dev()
    }
    fn ino(&self) -> Option<u64> {
        self.entry().ino()
    }
    fn readonly(&self) -> bool {
        self.entry().readonly()
    }
}

#[cfg(test)]
mod tests {
    use super::Entry;
    use super::FileAttr;
    use super::Node;

    #[test]
    fn from_regular_path() {
        let e = Entry::from_path("files/softlink/original");
        assert!(e.is_ok());
        assert!(e.unwrap().is_some());
    }
    #[test]
    fn from_link_path() {
        let e = Entry::from_path("files/softlink/original_link");
        assert!(e.is_ok());
        assert!(e.unwrap().is_none());
    }
    #[test]
    fn from_dir_path() {
        let e = Entry::from_path("files/softlink");
        assert!(e.is_ok());
        assert!(e.unwrap().is_none());
    }
    #[test]
    fn from_nonexist_path() {
        let e = Entry::from_path("files/nonexist-path");
        assert!(e.is_err());
    }

    #[test]
    fn entry_fileattr_interface() {
        // Interface check
        let e = Entry::from_path("files/softlink/original");
        assert!(e.is_ok());
        let e = e.unwrap();
        assert!(e.is_some());
        let e = e.unwrap();

        assert!(e.path().exists());
        assert!(0 < e.size());
    }

    #[test]
    fn node_fileattr_interface() {
        // Interface check
        let e = Entry::from_path("files/softlink/original");
        assert!(e.is_ok());
        let e = e.unwrap();
        assert!(e.is_some());
        let e = e.unwrap();

        let v1 = vec![e.clone()];
        let v2 = vec![e.clone(), e.clone()];

        let snode = Node::from(e);
        let mnode1 = Node::from(v1);
        let mnode2 = Node::from(v2);

        assert_eq!(mnode1.path(), snode.path());
        assert_eq!(mnode1.size(), snode.size());
        assert_eq!(mnode1.dev(), snode.dev());
        assert_eq!(mnode1.ino(), snode.ino());
        assert_eq!(mnode1.readonly(), snode.readonly());

        assert_eq!(mnode1.path(), mnode2.path());
        assert_eq!(mnode1.size(), mnode2.size());
        assert_eq!(mnode1.dev(), mnode2.dev());
        assert_eq!(mnode1.ino(), mnode2.ino());
        assert_eq!(mnode1.readonly(), mnode2.readonly());
    }
}
