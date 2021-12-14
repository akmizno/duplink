use async_trait::async_trait;
use futures::stream::StreamExt;
use itertools::Itertools;
use std::path::Path;
use tokio::io;
use tokio::sync::mpsc;
use tokio::task;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use walkdir::WalkDir;

use super::entry::{ContentEq, Digest, Entry, FileAttr};

fn have_all_same_dev(entries: &[Entry]) -> bool {
    if entries.len() < 2 {
        return true;
    }

    let dev = entries[0].dev();
    entries[1..].iter().all(|e| e.dev() == dev)
}

#[derive(Debug)]
pub struct Node {
    entries: Vec<Entry>,
}

impl Node {
    fn new(entries: Vec<Entry>) -> Self {
        assert!(!entries.is_empty());
        debug_assert!(have_all_same_dev(&entries));
        Node { entries }
    }

    #[allow(dead_code)]
    fn from_path<P: AsRef<Path>>(p: P) -> io::Result<Option<Self>> {
        let entry = Entry::from_path(p);
        if let Err(e) = entry {
            return Err(e);
        }
        match entry.unwrap() {
            None => Ok(None),
            Some(e) => Ok(Some(Node::from_entry(e))),
        }
    }

    fn from_entry(entry: Entry) -> Self {
        Node::new(vec![entry])
    }

    fn from_entries(entries: Vec<Entry>) -> Self {
        Node::new(entries)
    }

    fn entry(&self) -> &Entry {
        debug_assert!(!self.entries.is_empty());
        unsafe { self.entries.get_unchecked(0) }
    }

    pub fn into_entries(self) -> Vec<Entry> {
        self.entries
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
    async fn fast_digest(&self) -> io::Result<u64> {
        self.entry().fast_digest().await
    }
    async fn digest(&self) -> io::Result<u64> {
        self.entry().digest().await
    }
}

impl ContentEq for Node {}

fn make_walkdir_single<P: AsRef<Path>>(
    root: P,
    min_depth: Option<usize>,
    max_depth: Option<usize>,
    follow_links: bool,
) -> WalkDir {
    let w = WalkDir::new(root).follow_links(follow_links);

    let w = match min_depth {
        None => w,
        Some(d) => w.min_depth(d),
    };

    match max_depth {
        None => w,
        Some(d) => w.max_depth(d),
    }
}

fn into_entry_stream(wds: Vec<WalkDir>) -> mpsc::UnboundedReceiver<Entry> {
    let (tx, rx) = mpsc::unbounded_channel();

    for wd in wds.into_iter() {
        let tx = tx.clone();
        task::spawn_blocking(move || {
            for d in wd.into_iter() {
                if let walkdir::Result::Err(e) = d {
                    log::warn!("{}", e);
                    continue;
                }

                let entry = Entry::from_direntry(d.unwrap());
                if let Err(e) = entry {
                    log::warn!("{}", e);
                    continue;
                }

                let entry = entry.unwrap();

                if entry.is_none() {
                    // Directory entry.
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
        DevInoCmp {
            ino: entry.ino(),
            dev: entry.dev(),
        }
    }
}
impl PartialEq for DevInoCmp {
    fn eq(&self, other: &Self) -> bool {
        if self.ino.is_none() || self.dev.is_none() || other.dev.is_none() || other.dev.is_none() {
            return false;
        }

        (self.ino.unwrap(), self.dev.unwrap()) == (other.ino.unwrap(), self.dev.unwrap())
    }
}
impl Eq for DevInoCmp {}

fn entries2nodes(entries: Vec<Entry>) -> Vec<Node> {
    entries
        .into_iter()
        .sorted_unstable_by_key(DevInoCmp::new)
        .group_by(DevInoCmp::new)
        .into_iter()
        .map(|(_, g)| g.collect_vec())
        .map(Node::from_entries)
        .collect_vec()
}

pub type NodeStream = ReceiverStream<Node>;

pub struct DirWalker {
    min_depth: Option<usize>,
    max_depth: Option<usize>,
    follow_links: bool,
}

impl DirWalker {
    pub fn new() -> DirWalker {
        DirWalker {
            min_depth: None,
            max_depth: None,
            follow_links: true,
        }
    }
    pub fn min_depth(mut self, depth: Option<usize>) -> DirWalker {
        self.min_depth = depth;
        self
    }
    pub fn max_depth(mut self, depth: Option<usize>) -> DirWalker {
        self.max_depth = depth;
        self
    }
    pub fn follow_links(mut self, f: bool) -> DirWalker {
        self.follow_links = f;
        self
    }

    pub fn walk<P: AsRef<Path>>(self, roots: &[P]) -> NodeStream {
        let wds = roots
            .iter()
            .map(|p| make_walkdir_single(p, self.min_depth, self.max_depth, self.follow_links))
            .collect_vec();

        let (tx, rx) = mpsc::channel(512);

        task::spawn(async move {
            let entries = into_entry_stream(wds);
            let entries: Vec<Entry> = UnboundedReceiverStream::new(entries).collect().await;

            let nodes = task::block_in_place(|| entries2nodes(entries));

            for node in nodes.into_iter() {
                let tx = tx.clone();
                task::spawn(async move {
                    tx.send(node).await.unwrap();
                });
            }
        });

        NodeStream::new(rx)
    }
}

impl Default for DirWalker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use itertools::Itertools;
    use std::path::{Path, PathBuf};

    use super::DirWalker;
    use super::Node;
    use crate::entry::{ContentEq, Digest, Entry, FileAttr};

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
        assert!(n.is_some());
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
        let n = Node::from_entry(e1);
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
        let n = Node::from_entries(vec![e1, e2]);
        assert_eq!(n.path(), e3.path());
        assert_eq!(n.size(), e3.size());
        assert_eq!(n.readonly(), e3.readonly());
        assert_eq!(n.dev(), e3.dev());
        assert_eq!(n.ino(), e3.ino());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn fast_digest_eq() {
        let p = "files/softlink/original";
        let e1 = Node::from_path(p).unwrap().unwrap();
        let e2 = Node::from_path(p).unwrap().unwrap();
        let d1 = e1.fast_digest().await.unwrap();
        let d2 = e2.fast_digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn fast_digest_eq_multiple_time() {
        let p = "files/softlink/original";
        let e = Node::from_path(p).unwrap().unwrap();
        let d1 = e.fast_digest().await.unwrap();
        let d2 = e.fast_digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn fast_digest_ne() {
        let e1 = Node::from_path("files/small-uniques/unique1")
            .unwrap()
            .unwrap();
        let e2 = Node::from_path("files/small-uniques/unique2")
            .unwrap()
            .unwrap();
        let d1 = e1.fast_digest().await.unwrap();
        let d2 = e2.fast_digest().await.unwrap();
        assert_ne!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn digest_eq() {
        let p = "files/softlink/original";
        let e1 = Node::from_path(p).unwrap().unwrap();
        let e2 = Node::from_path(p).unwrap().unwrap();
        let d1 = e1.digest().await.unwrap();
        let d2 = e2.digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn digest_eq_multiple_time() {
        let p = "files/softlink/original";
        let e = Node::from_path(p).unwrap().unwrap();
        let d1 = e.digest().await.unwrap();
        let d2 = e.digest().await.unwrap();
        assert_eq!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn digest_ne() {
        let e1 = Node::from_path("files/large-uniques/fill_00_16k")
            .unwrap()
            .unwrap();
        let e2 = Node::from_path("files/large-uniques/fill_ff_16k")
            .unwrap()
            .unwrap();
        let d1 = e1.digest().await.unwrap();
        let d2 = e2.digest().await.unwrap();
        assert_ne!(d1, d2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn content_eq() {
        let e = Node::from_path("files/large-uniques/fill_00_16k")
            .unwrap()
            .unwrap();
        let p = Node::from_path("files/large-uniques/fill_00_16k")
            .unwrap()
            .unwrap();
        assert!(e.eq_content(&p).await.unwrap());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn content_ne() {
        let e = Node::from_path("files/large-uniques/fill_00_16k")
            .unwrap()
            .unwrap();
        let p = Node::from_path("files/large-uniques/fill_ff_16k")
            .unwrap()
            .unwrap();
        assert!(!e.eq_content(&p).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir() {
        let p = "files/small-uniques";
        let paths = DirWalker::new()
            .walk(&[p])
            .collect::<Vec<Node>>()
            .await
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
        let paths = DirWalker::new()
            .walk(&[p])
            .collect::<Vec<Node>>()
            .await
            .into_iter()
            .collect_vec();

        assert_eq!(paths.len(), 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_multiple() {
        let p = "files/small-uniques";
        let nodes = DirWalker::new().walk(&[p, p]).collect::<Vec<Node>>().await;

        // println!("{:?}", nodes);

        let paths = nodes
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        assert_eq!(paths.len(), 3);

        assert!(paths.contains(&canonical_path("files/small-uniques/unique1")));
        assert!(paths.contains(&canonical_path("files/small-uniques/unique2")));
        assert!(paths.contains(&canonical_path("files/small-uniques/unique3")));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_multiple2() {
        let p1 = "files/small-uniques";
        let p2 = "files/large-uniques";
        let paths = DirWalker::new()
            .walk(&[p1, p2])
            .collect::<Vec<Node>>()
            .await
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
    // Disable on windows because symlinks can not be created by git clone.
    #[cfg(not(windows))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_follow_links() {
        let p = "files/softlink-dir";
        let nodes = DirWalker::new().walk(&[p]).collect::<Vec<Node>>().await;
        let paths = nodes
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        assert_eq!(paths.len(), 2);

        assert!(paths.contains(&canonical_path("files/softlink-dir/dir/file")));
        assert!(paths.contains(&canonical_path("files/softlink/original")));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_no_follow_links() {
        let p = "files/softlink-dir";
        let paths = DirWalker::new()
            .follow_links(false)
            .walk(&[p])
            .collect::<Vec<Node>>()
            .await
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        assert_eq!(paths.len(), 1);
        assert!(paths.contains(&canonical_path("files/softlink-dir/dir/file")));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_min_depth() {
        let p = "files/depth-uniques";
        let paths = DirWalker::new()
            .min_depth(Some(4))
            .walk(&[p])
            .collect::<Vec<Node>>()
            .await
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        assert_eq!(paths.len(), 1);
        assert!(paths.contains(&canonical_path(
            "files/depth-uniques/level1/level2/level3/unique3"
        )));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_max_depth() {
        let p = "files/depth-uniques";
        let paths = DirWalker::new()
            .max_depth(Some(1))
            .walk(&[p])
            .collect::<Vec<Node>>()
            .await
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
        let paths = DirWalker::new()
            .walk(&[dir.path()])
            .collect::<Vec<Node>>()
            .await
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        #[cfg(unix)]
        assert_eq!(paths.len(), 1);

        assert!(
            paths.contains(&canonical_path(dir.path().join("original")))
                || paths.contains(&canonical_path(dir.path().join("hardlink")))
        );
    }
}
