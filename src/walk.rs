use async_trait::async_trait;
use tokio::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures::stream::{Stream, StreamExt};
use walkdir::WalkDir;
use tokio::task;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use log;
use itertools::Itertools;
use pin_project::pin_project;

use super::entry::{Entry, FileAttr, ContentEq, Digest};

fn have_all_same_dev(entries: &[Entry]) -> bool {
    if entries.len() < 2 {
        return true;
    }

    let dev = entries[0].dev();
    entries[1..].iter().all(|e| e.dev() == dev)
}

#[derive(Debug)]
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
    pub(crate) fn from_path<P: AsRef<Path>>(p: P) -> io::Result<Option<Self>> {
        let entry = Entry::from_path(p);
        if let Err(e) = entry {
            return Err(e);
        }
        match entry.unwrap() {
            None => Ok(None),
            Some(e) => Ok(Some(Node::from(e))),
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

fn make_walkdir_single<P: AsRef<Path>>(
    root: P,
    min_depth: Option<usize>,
    max_depth: Option<usize>,
    follow_links: bool,
    ) -> WalkDir {
    let w = WalkDir::new(root)
        .follow_links(follow_links);
    let w = match min_depth {
        None => w,
        Some(d) => w.min_depth(d)
    };
    let w = match max_depth {
        None => w,
        Some(d) => w.max_depth(d)
    };

    w
}

fn make_entry_stream(wds: Vec<WalkDir>) -> mpsc::UnboundedReceiver<Entry> {
    let (tx, rx) = mpsc::unbounded_channel();

    for wd in wds.into_iter() {
        let tx = tx.clone();
        task::spawn_blocking(move ||{
            for d in wd.into_iter() {
                if d.is_err() {
                    log::warn!("{}", d.unwrap_err());
                    continue;
                }

                let entry = Entry::from_direntry(d.unwrap());
                if entry.is_err() {
                    log::warn!("{}", entry.unwrap_err());
                    continue;
                }
                let entry = entry.unwrap();

                if entry.is_none() {
                    continue;
                }
                let entry = entry.unwrap();

                tx.send(entry).unwrap();
            }
        });
    }

    rx
}

#[derive(PartialOrd, Ord)]
struct DevInoCmp {
    ino: Option<u64>,
    dev: Option<u64>,
}
impl DevInoCmp {
    fn new(entry: &Entry) -> Self {
        DevInoCmp{ino: entry.ino(), dev: entry.dev()}
    }
}
impl PartialEq for DevInoCmp {
    fn eq(&self, other: &Self) -> bool {
        if self.ino.is_none()
            || self.dev.is_none()
            || other.dev.is_none()
            || other.dev.is_none() {
                return false;
        }

        (self.ino.unwrap(), self.dev.unwrap())
            == (other.ino.unwrap(), self.dev.unwrap())
    }
}
impl Eq for DevInoCmp {}


async fn make_nodes(wds: Vec<WalkDir>) -> Vec<Node> {
    let rx = make_entry_stream(wds);
    let entries: Vec<Entry> = UnboundedReceiverStream::new(rx).collect().await;
    let nodes = task::block_in_place(
        || entries.into_iter()
        .sorted_by_key(|e| DevInoCmp::new(&e))
        .group_by(|e| DevInoCmp::new(&e))
        .into_iter()
        .map(|(_, g)| g.collect_vec())
        .map(|g| Node::from(g))
        .collect_vec()
    );

    nodes
}


#[pin_project]
pub struct NodeStream {
    #[pin]
    inner: ReceiverStream<Node>,
}

impl NodeStream {
    pub fn new(wds: Vec<WalkDir>) -> NodeStream {
        // TODO
        // Consider buffer size of channel
        let (tx, rx) = mpsc::channel(256);

        task::spawn(async move {
            let nodes = make_nodes(wds).await;
            for node in nodes {
                tx.send(node).await.unwrap();
            }
        });

        NodeStream{ inner: ReceiverStream::new(rx) }
    }
}

impl Stream for NodeStream {
    type Item = Node;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

pub struct Walker {
    min_depth: Option<usize>,
    max_depth: Option<usize>,
    follow_links: bool,
}

impl Walker {
    pub fn new() -> Walker {
        Walker{ min_depth: None, max_depth: None, follow_links: true }
    }
    pub fn min_depth(mut self, depth: usize) -> Walker {
        self.min_depth = Some(depth);
        self
    }
    pub fn max_depth(mut self, depth: usize) -> Walker {
        self.max_depth = Some(depth);
        self
    }
    pub fn follow_links(mut self, f: bool) -> Walker {
        self.follow_links = f;
        self
    }

    pub fn walk<P: AsRef<Path>>(self, roots: &[P]) -> NodeStream {
        let wds = roots.iter()
            .map(|p| make_walkdir_single(p, self.min_depth, self.max_depth, self.follow_links))
            .collect_vec();

        NodeStream::new(wds)
    }
}


#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use itertools::Itertools;
    use futures::stream::StreamExt;

    use crate::entry::{Entry, ContentEq, Digest, FileAttr};
    use super::Node;
    use super::Walker;

    fn canonical_path<P: AsRef<Path>>(p: P) -> PathBuf {
        p.as_ref().canonicalize().unwrap()
    }

    #[test]
    fn from_regular_path() {
        let p = "files/softlink/original";
        let n = Node::from_path(p).unwrap().unwrap();
        assert_eq!(n.path().as_os_str(), p);
        #[cfg(unix)]
        assert!(n.size() == 9);
        #[cfg(windows)]
        assert!(n.size() == 10);
        assert!(!n.readonly());
    }
    #[test]
    fn from_link_path() {
        let n = Node::from_path("files/softlink/original_link").unwrap();
        assert!(n.is_none());
    }
    #[test]
    fn from_dir_path() {
        let n = Node::from_path("files/softlink").unwrap();
        assert!(n.is_none());
    }
    #[test]
    fn from_nonexist_path() {
        let p = "files/nonexist-path";
        let n = Node::from_path(p);
        assert!(n.is_err());
    }
    #[test]
    fn from_single_entry() {
        let p = "files/softlink/original";
        let e1 = Entry::from_path(p).unwrap().unwrap();
        let e2 = Entry::from_path(p).unwrap().unwrap();
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
        let e1 = Entry::from_path(p).unwrap().unwrap();
        let e2 = Entry::from_path(p).unwrap().unwrap();
        let e3 = Entry::from_path(p).unwrap().unwrap();
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
        let mut e1 = Node::from_path(p).unwrap().unwrap();
        let mut e2 = Node::from_path(p).unwrap().unwrap();
        let d1 = e1.fast_digest().await.unwrap();
        let d2 = e2.fast_digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test]
    async fn fast_digest_eq_multiple_time() {
        let p = "files/softlink/original";
        let mut e = Node::from_path(p).unwrap().unwrap();
        let d1 = e.fast_digest().await.unwrap();
        let d2 = e.fast_digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test]
    async fn fast_digest_ne() {
        let mut e1 = Node::from_path("files/small-uniques/unique1").unwrap().unwrap();
        let mut e2 = Node::from_path("files/small-uniques/unique2").unwrap().unwrap();
        let d1 = e1.fast_digest().await.unwrap();
        let d2 = e2.fast_digest().await.unwrap();
        assert_ne!(d1, d2);
    }
    #[tokio::test]
    async fn digest_eq() {
        let p = "files/softlink/original";
        let mut e1 = Node::from_path(p).unwrap().unwrap();
        let mut e2 = Node::from_path(p).unwrap().unwrap();
        let d1 = e1.digest().await.unwrap();
        let d2 = e2.digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test]
    async fn digest_eq_multiple_time() {
        let p = "files/softlink/original";
        let mut e = Node::from_path(p).unwrap().unwrap();
        let d1 = e.digest().await.unwrap();
        let d2 = e.digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test]
    async fn digest_ne() {
        let mut e1 = Node::from_path("files/large-uniques/fill_00_16k").unwrap().unwrap();
        let mut e2 = Node::from_path("files/large-uniques/fill_ff_16k").unwrap().unwrap();
        let d1 = e1.digest().await.unwrap();
        let d2 = e2.digest().await.unwrap();
        assert_ne!(d1, d2);
    }
    #[tokio::test]
    async fn bytes_eq() {
        let e1 = Node::from_path("files/large-uniques/fill_00_16k").unwrap().unwrap();
        let e2 = Node::from_path("files/large-uniques/fill_00_16k").unwrap().unwrap();
        assert!(e1.eq_bytes(&e2).await.unwrap());
    }
    #[tokio::test]
    async fn bytes_ne() {
        let e1 = Node::from_path("files/large-uniques/fill_00_16k").unwrap().unwrap();
        let e2 = Node::from_path("files/large-uniques/fill_ff_16k").unwrap().unwrap();
        assert!(!e1.eq_bytes(&e2).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir() {
        let p = "files/small-uniques";
        let paths = Walker::new()
            .walk(&[p])
            .collect::<Vec<Node>>().await
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        assert_eq!(paths.len(), 3);
        assert!(paths.contains(&canonical_path("files/small-uniques/unique1")));
        assert!(paths.contains(&canonical_path("files/small-uniques/unique2")));
        assert!(paths.contains(&canonical_path("files/small-uniques/unique3")));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_nonexist() {
        let p = "files/nonexist";
        let paths = Walker::new()
            .walk(&[p])
            .collect::<Vec<Node>>().await
            .into_iter()
            .collect_vec();

        assert_eq!(paths.len(), 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_multiple() {
        let p = "files/small-uniques";
        let paths = Walker::new()
            .walk(&[p, p])
            .collect::<Vec<Node>>().await
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        #[cfg(unix)]
        assert_eq!(paths.len(), 3);
        // FIX IT
        #[cfg(windows)]
        assert_eq!(paths.len(), 6);

        assert!(paths.contains(&canonical_path("files/small-uniques/unique1")));
        assert!(paths.contains(&canonical_path("files/small-uniques/unique2")));
        assert!(paths.contains(&canonical_path("files/small-uniques/unique3")));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_multiple2() {
        let p1 = "files/small-uniques";
        let p2 = "files/large-uniques";
        let paths = Walker::new()
            .walk(&[p1, p2])
            .collect::<Vec<Node>>().await
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        assert_eq!(paths.len(), 7);
        assert!(paths.contains(&canonical_path("files/small-uniques/unique1")));
        assert!(paths.contains(&canonical_path("files/small-uniques/unique2")));
        assert!(paths.contains(&canonical_path("files/small-uniques/unique3")));
        assert!(paths.contains(&canonical_path("files/large-uniques/fill_00_16k")));
        assert!(paths.contains(&canonical_path("files/large-uniques/fill_00_32k")));
        assert!(paths.contains(&canonical_path("files/large-uniques/fill_ff_16k")));
        assert!(paths.contains(&canonical_path("files/large-uniques/fill_ff_32k")));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_follow_links() {
        let p = "files/softlink-dir";
        let paths = Walker::new()
            .walk(&[p])
            .collect::<Vec<Node>>().await
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        #[cfg(unix)]
        assert_eq!(paths.len(), 2);
        // FIX IT
        #[cfg(windows)]
        assert_eq!(paths.len(), 4);

        assert!(paths.contains(&canonical_path("files/softlink-dir/dir/file")));
        assert!(paths.contains(&canonical_path("files/softlink/original")));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_no_follow_links() {
        let p = "files/softlink-dir";
        let paths = Walker::new()
            .follow_links(false)
            .walk(&[p])
            .collect::<Vec<Node>>().await
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        assert_eq!(paths.len(), 1);
        assert!(paths.contains(&canonical_path("files/softlink-dir/dir/file")));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_min_depth() {
        let p = "files/depth-uniques";
        let paths = Walker::new()
            .min_depth(4)
            .walk(&[p])
            .collect::<Vec<Node>>().await
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        assert_eq!(paths.len(), 1);
        assert!(paths.contains(&canonical_path("files/depth-uniques/level1/level2/level3/unique3")));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_max_depth() {
        let p = "files/depth-uniques";
        let paths = Walker::new()
            .max_depth(1)
            .walk(&[p])
            .collect::<Vec<Node>>().await
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        assert_eq!(paths.len(), 1);
        assert!(paths.contains(&canonical_path("files/depth-uniques/unique0")));
    }

    fn make_tempdir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let f1 = dir.path().join("original");
        let f2 = dir.path().join("hardlink");
        std::fs::File::create(f1.as_path()).unwrap();
        std::fs::hard_link(f1, f2).unwrap();
        dir
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_hardlink() {
        let dir = make_tempdir();
        let paths = Walker::new()
            .walk(&[dir.path()])
            .collect::<Vec<Node>>().await
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();


        #[cfg(unix)]
        assert_eq!(paths.len(), 1);
        // FIX IT
        #[cfg(windows)]
        assert_eq!(paths.len(), 2);

        assert!(
            paths.contains(&canonical_path(dir.path().join("original")))
            || paths.contains(&canonical_path(dir.path().join("hardlink")))
            );
    }
}
