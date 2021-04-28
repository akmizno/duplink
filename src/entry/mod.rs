use async_trait::async_trait;
use std::path::Path;
use tokio::io;

mod traits;
pub use traits::{ContentEq, Digest, FileAttr};

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
    fn entry_mut(&mut self) -> &mut Entry {
        match self {
            Node::Single(e) => e,
            Node::Multi(v) => unsafe {
                debug_assert!(!v.is_empty());
                v.get_unchecked_mut(0)
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

#[async_trait]
impl Digest for Node {
    async fn fast_digest(&mut self) -> io::Result<u64> {
        self.entry_mut().fast_digest().await
    }
    async fn digest(&mut self) -> io::Result<u64> {
        self.entry_mut().digest().await
    }
}

#[async_trait]
impl ContentEq for Node {
    async fn eq_bytes(&self, other: &Self) -> io::Result<bool> {
        self.entry().eq_bytes(&other.entry()).await
    }
}

#[cfg(test)]
mod tests {
    use super::Entry;
    use super::Node;
    use super::{ContentEq, Digest, FileAttr};

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

    #[tokio::test]
    async fn fast_digest_eq() {
        let p = "files/softlink/original";
        let mut e1 = Node::from_path(p).unwrap();
        let mut e2 = Node::from_path(p).unwrap();
        let d1 = e1.fast_digest().await.unwrap();
        let d2 = e2.fast_digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test]
    async fn fast_digest_eq_multiple_time() {
        let p = "files/softlink/original";
        let mut e = Node::from_path(p).unwrap();
        let d1 = e.fast_digest().await.unwrap();
        let d2 = e.fast_digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test]
    async fn fast_digest_ne() {
        let mut e1 = Node::from_path("files/small-uniques/unique1").unwrap();
        let mut e2 = Node::from_path("files/small-uniques/unique2").unwrap();
        let d1 = e1.fast_digest().await.unwrap();
        let d2 = e2.fast_digest().await.unwrap();
        assert_ne!(d1, d2);
    }

    #[tokio::test]
    async fn digest_eq() {
        let p = "files/softlink/original";
        let mut e1 = Node::from_path(p).unwrap();
        let mut e2 = Node::from_path(p).unwrap();
        let d1 = e1.digest().await.unwrap();
        let d2 = e2.digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test]
    async fn digest_eq_multiple_time() {
        let p = "files/softlink/original";
        let mut e = Node::from_path(p).unwrap();
        let d1 = e.digest().await.unwrap();
        let d2 = e.digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test]
    async fn digest_ne() {
        let mut e1 = Node::from_path("files/large-uniques/fill_00_16k").unwrap();
        let mut e2 = Node::from_path("files/large-uniques/fill_ff_16k").unwrap();
        let d1 = e1.digest().await.unwrap();
        let d2 = e2.digest().await.unwrap();
        assert_ne!(d1, d2);
    }
    #[tokio::test]
    async fn bytes_eq() {
        let e1 = Node::from_path("files/large-uniques/fill_00_16k").unwrap();
        let e2 = Node::from_path("files/large-uniques/fill_00_16k").unwrap();
        assert!(e1.eq_bytes(&e2).await.unwrap());
    }
    #[tokio::test]
    async fn bytes_ne() {
        let e1 = Node::from_path("files/large-uniques/fill_00_16k").unwrap();
        let e2 = Node::from_path("files/large-uniques/fill_ff_16k").unwrap();
        assert!(!e1.eq_bytes(&e2).await.unwrap());
    }
}
