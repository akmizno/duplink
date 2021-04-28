use std::path::Path;

mod traits;
pub use traits::FileAttr;

mod generic;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::Entry;

#[cfg(not(unix))]
pub use generic::Entry;

fn have_all_same_dev(entries: &[Entry]) -> bool {
    if entries.len() < 2 {
        return true;
    }

    let dev = entries[0].dev();
    entries[1..].iter().all(|e| e.dev() == dev)
}

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
        debug_assert!(have_all_same_dev(&entries));

        if 1 == entries.len() {
            Node::Single(entries.pop().unwrap())
        } else {
            Node::Multi(entries)
        }
    }
}

impl Node {
    #[allow(dead_code)]
    pub(crate) fn from_path<P: AsRef<Path>>(p: P) -> Option<Self> {
        match Entry::from_path(p) {
            None => None,
            Some(e) => Some(Node::from(e)),
        }
    }

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
        let p = "files/softlink/original";
        let n = Node::from_path(p).unwrap();
        assert_eq!(n.path().as_os_str(), p);
        assert_eq!(n.size(), 9);
        assert!(!n.readonly());
    }
    #[test]
    fn from_link_path() {
        let n = Node::from_path("files/softlink/original_link");
        assert!(n.is_none());
    }
    #[test]
    fn from_dir_path() {
        let n = Node::from_path("files/softlink");
        assert!(n.is_none());
    }
    #[test]
    fn from_nonexist_path() {
        let p = "files/nonexist-path";
        let n = Node::from_path(p);
        assert!(n.is_none());
    }
    #[test]
    fn from_single_entry() {
        let p = "files/softlink/original";
        let e1 = Entry::from_path(p).unwrap();
        let e2 = Entry::from_path(p).unwrap();
        let n = Node::from(e1);
        assert_eq!(n.path(), e2.path());
        assert_eq!(n.size(), e2.size());
        assert_eq!(n.readonly(), e2.readonly());
        assert_eq!(n.dev(), e2.dev());
        assert_eq!(n.ino(), e2.ino());
    }
    #[test]
    fn from_multiple_entry() {
        let p = "files/softlink/original";
        let e1 = Entry::from_path(p).unwrap();
        let e2 = Entry::from_path(p).unwrap();
        let e3 = Entry::from_path(p).unwrap();
        let n = Node::from(vec![e1, e2]);
        assert_eq!(n.path(), e3.path());
        assert_eq!(n.size(), e3.size());
        assert_eq!(n.readonly(), e3.readonly());
        assert_eq!(n.dev(), e3.dev());
        assert_eq!(n.ino(), e3.ino());
    }
}
