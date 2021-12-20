use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

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
