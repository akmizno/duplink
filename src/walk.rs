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

use super::entry::{Entry, Node, FileAttr};

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

pub struct NodeStreamBuilder {
    min_depth: Option<usize>,
    max_depth: Option<usize>,
    follow_links: bool,
}

impl NodeStreamBuilder {
    pub fn new() -> NodeStreamBuilder {
        NodeStreamBuilder{ min_depth: None, max_depth: None, follow_links: true }
    }
    pub fn min_depth(mut self, depth: usize) -> NodeStreamBuilder {
        self.min_depth = Some(depth);
        self
    }
    pub fn max_depth(mut self, depth: usize) -> NodeStreamBuilder {
        self.max_depth = Some(depth);
        self
    }
    pub fn follow_links(mut self, f: bool) -> NodeStreamBuilder {
        self.follow_links = f;
        self
    }

    pub fn build<P: AsRef<Path>>(self, roots: &[P]) -> NodeStream {
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
    use super::super::entry::FileAttr;
    use super::super::entry::Node;
    use super::NodeStreamBuilder;
    use futures::stream::StreamExt;

    fn canonical_path<P: AsRef<Path>>(p: P) -> PathBuf {
        p.as_ref().canonicalize().unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir() {
        let p = "files/small-uniques";
        let paths = NodeStreamBuilder::new()
            .build(&[p])
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
        let paths = NodeStreamBuilder::new()
            .build(&[p])
            .collect::<Vec<Node>>().await
            .into_iter()
            .collect_vec();

        assert_eq!(paths.len(), 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn walk_dir_multiple() {
        let p = "files/small-uniques";
        let paths = NodeStreamBuilder::new()
            .build(&[p, p])
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
        let paths = NodeStreamBuilder::new()
            .build(&[p1, p2])
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
        let paths = NodeStreamBuilder::new()
            .build(&[p])
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
        let paths = NodeStreamBuilder::new()
            .follow_links(false)
            .build(&[p])
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
        let paths = NodeStreamBuilder::new()
            .min_depth(4)
            .build(&[p])
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
        let paths = NodeStreamBuilder::new()
            .max_depth(1)
            .build(&[p])
            .collect::<Vec<Node>>().await
            .into_iter()
            .map(|n| canonical_path(n.path()))
            .collect_vec();

        assert_eq!(paths.len(), 1);
        assert!(paths.contains(&canonical_path("files/depth-uniques/unique0")));
    }
}
