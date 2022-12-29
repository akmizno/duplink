use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use super::entry::{FileAttr, LinkTo};
use super::find;
use super::util::semaphore::Semaphore;
use super::walk::Node;

pub type DupLinkStream<T> = ReceiverStream<T>;
pub type DuplicateStream = DupLinkStream<Vec<Node>>;
pub type UniqueStream = DupLinkStream<Node>;

pub struct DupFinder {
    sem_small: Semaphore,
    sem_large: Semaphore,
    ignore_dev: bool,
}

impl DupFinder {
    pub fn new(sem_small: Semaphore, sem_large: Semaphore) -> DupFinder {
        DupFinder {
            sem_small,
            sem_large,
            ignore_dev: false,
        }
    }

    pub fn ignore_dev(mut self, ignore: bool) -> Self {
        self.ignore_dev = ignore;
        self
    }

    pub fn find_dups(self, nodes: Vec<Node>) -> (DuplicateStream, UniqueStream) {
        let (dups_tx, dups_rx) = mpsc::channel(nodes.len());
        let (uniqs_tx, uniqs_rx) = mpsc::channel(nodes.len());

        find::find_dups(
            nodes,
            self.sem_small,
            self.sem_large,
            dups_tx,
            uniqs_tx,
            self.ignore_dev,
        );

        (DuplicateStream::new(dups_rx), UniqueStream::new(uniqs_rx))
    }
}

pub struct DedupPipe {
    sem: Semaphore,
    buffer: usize,
}

impl DedupPipe {
    pub fn new(sem: Semaphore, buffer: usize) -> DedupPipe {
        DedupPipe { sem, buffer }
    }

    pub fn dedup(self, mut dups: DuplicateStream) -> DuplicateStream {
        let (tx, rx) = mpsc::channel(self.buffer);

        task::spawn(async move {
            while let Some(nodes) = dups.next().await {
                let sem = self.sem.clone();
                let tx = tx.clone();
                task::spawn(async move {
                    Self::link_nodes(nodes, sem, tx).await;
                });
            }
        });

        DuplicateStream::new(rx)
    }

    async fn link_nodes(nodes: Vec<Node>, sem: Semaphore, sender: Sender<Vec<Node>>) {
        assert!(1 < nodes.len());

        let first = &nodes[0];
        let to = first.path();
        let entries = nodes[1..].iter().flat_map(|node| node.entries());

        {
            let _p = sem.acquire().await.unwrap();

            for entry in entries {
                let res = entry.hardlink(to).await;

                if res.is_err() {
                    // Log the error and continue linking.
                    log::warn!("{}: {}", res.unwrap_err(), entry.path().display());
                }
            }
        }

        sender.send(nodes).await.unwrap();
    }
}
