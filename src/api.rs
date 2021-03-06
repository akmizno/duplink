use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::find;
use super::util::semaphore::Semaphore;
use super::walk::Node;

pub type DuplicateStream = ReceiverStream<Vec<Node>>;
pub type UniqueStream = ReceiverStream<Node>;

pub struct DupLink {
    sem_small: Semaphore,
    sem_large: Semaphore,
    ignore_dev: bool,
}

impl DupLink {
    pub fn new(sem_small: Semaphore, sem_large: Semaphore) -> DupLink {
        DupLink {
            sem_small,
            sem_large,
            ignore_dev: false,
        }
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

        (ReceiverStream::new(dups_rx), ReceiverStream::new(uniqs_rx))
    }
}
