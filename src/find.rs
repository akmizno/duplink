use tokio::task;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Vacant, Occupied};
use itertools::Itertools;
use log;

use super::entry::{FileAttr, Digest, ContentEq};
use super::walk::Node;
use super::util::{group_by_key_map, group_by_key};
use super::util::semaphore::Semaphore;

const THRESHOLD: u64 = 8192;

fn group_by_size(nodes: Vec<Node>) -> Vec<Vec<Node>> {
    group_by_key(nodes, |n| n.size())
}

fn find_dupes_by_size(nodes: Vec<Node>, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    if nodes.len() == 0 {
        return;
    }

    task::spawn(async move{
        let groups = task::block_in_place(|| group_by_size(nodes));

        for group in groups.into_iter() {
            debug_assert!(0 < group.len());
            if group.len() == 1 {
                let uniqs = uniqs.clone();
                task::spawn(async move {
                    uniqs.send(group.into_iter().next().unwrap()).await.unwrap();
                });
            } else {
                let dupes = dupes.clone();
                task::spawn(async move {
                    dupes.send(group).await.unwrap();
                });
            }
        }
    });
}


async fn group_by_fast_digest(nodes: Vec<Node>, sem: Semaphore) -> Vec<Vec<Node>> {
    if nodes.len() == 0 {
        return Vec::new();
    }

    let rx = {
        let (tx, rx) = mpsc::channel(nodes.len());

        for node in nodes.into_iter() {
            let tx = tx.clone();
            let sem = sem.clone();
            task::spawn(async move {
                let d = {
                    let _p = sem.acquire().await.unwrap();
                    node.fast_digest().await.unwrap();
                };
                tx.send((d, node)).await.unwrap();
            });
        }
        rx
    };

    let dn = ReceiverStream::new(rx).collect().await;

    task::block_in_place(|| group_by_key_map(dn, |&(d, _)| d, |(_, n)| n))
}

async fn group_by_digest(nodes: Vec<Node>, sem: Semaphore) -> Vec<Vec<Node>> {
    if nodes.len() == 0 {
        return Vec::new();
    }

    let rx = {
        let (tx, rx) = mpsc::channel(nodes.len());

        for node in nodes.into_iter() {
            let tx = tx.clone();
            let sem = sem.clone();
            task::spawn(async move {
                let _p = sem.acquire().await.unwrap();
                let d = node.digest().await.unwrap();
                tx.send((d, node)).await.unwrap();
            });
        }
        rx
    };

    let dn = ReceiverStream::new(rx).collect().await;

    task::block_in_place(|| group_by_key_map(dn, |&(d, _)| d, |(_, n)| n))
}

fn find_dupes_by_fast_digest_impl(nodes: Vec<Node>, sem: Semaphore, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    if nodes.len() == 0 {
        return;
    }

    task::spawn(async move{
        let groups = group_by_fast_digest(nodes, sem).await;

        for mut group in groups.into_iter() {
            debug_assert!(0 < group.len());
            if group.len() == 1 {
                let uniqs = uniqs.clone();
                task::spawn(async move {
                    uniqs.send(group.pop().unwrap()).await.unwrap();
                });
            } else {
                let dupes = dupes.clone();
                task::spawn(async move {
                    dupes.send(group).await.unwrap();
                });
            }
        }
    });
}

fn find_dupes_by_digest_impl(nodes: Vec<Node>, sem: Semaphore, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    if nodes.len() == 0 {
        return;
    }

    task::spawn(async move{
        let groups = group_by_digest(nodes, sem).await;

        for mut group in groups.into_iter() {
            debug_assert!(0 < group.len());
            if group.len() == 1 {
                let uniqs = uniqs.clone();
                task::spawn(async move {
                    uniqs.send(group.pop().unwrap()).await.unwrap();
                });
            } else {
                let dupes = dupes.clone();
                task::spawn(async move {
                    dupes.send(group).await.unwrap();
                });
            }
        }
    });
}

fn find_dupes_by_digest_small(nodes: Vec<Node>, sem: Semaphore, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    find_dupes_by_digest_impl(nodes, sem, dupes, uniqs);
}

fn find_dupes_by_digest_large(nodes: Vec<Node>, sem_small: Semaphore, sem_large: Semaphore, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    if nodes.len() == 0 {
        return;
    }

    let (fast_dupes_tx, mut fast_dupes_rx) = mpsc::channel(nodes.len());

    find_dupes_by_fast_digest_impl(nodes, sem_small.clone(), fast_dupes_tx, uniqs.clone());

    task::spawn(async move {
        while let Some(nodes) = fast_dupes_rx.recv().await {
            let sem = sem_large.clone();
            let dupes = dupes.clone();
            let uniqs = uniqs.clone();
            task::spawn(async move {
                find_dupes_by_digest_impl(nodes, sem, dupes, uniqs);
            });
        }
    });
}


async fn collect_content_eq<P: AsRef<Path>>(path: P, nodes: Vec<Node>, sem: Semaphore) -> (Vec<Node>, Vec<Node>) {
    debug_assert!(0 < nodes.len());

    let (eq_rx, ne_rx) = {
        let (eq_tx, eq_rx) = mpsc::channel(nodes.len());
        let (ne_tx, ne_rx) = mpsc::channel(nodes.len());

        for node in nodes.into_iter() {
            let path = PathBuf::from(path.as_ref());
            let eq_tx = eq_tx.clone();
            let ne_tx = ne_tx.clone();
            let sem = sem.clone();
            task::spawn(async move{
                let eq = {
                    let _p = sem.acquire_many(2).await.unwrap();
                    let eq = node.eq_content(path).await;
                    if eq.is_err() {
                        log::error!("{}", eq.unwrap_err());
                        return;
                    }
                    eq.unwrap()
                };

                if eq {
                    eq_tx.send(node).await.unwrap();
                } else {
                    ne_tx.send(node).await.unwrap();
                }
            });
        }

        (eq_rx, ne_rx)
    };

    let eqs = ReceiverStream::new(eq_rx).collect().await;
    let nes = ReceiverStream::new(ne_rx).collect().await;
    (eqs, nes)
}

async fn group_by_content(mut nodes: Vec<Node>, sem: Semaphore) -> Vec<Vec<Node>> {
    if nodes.len() == 0 {
        return Vec::new();
    }

    let mut groups = Vec::new();

    while 1 < nodes.len() {
        let top = nodes.pop().unwrap();
        let mut rem = Vec::new();
        std::mem::swap(&mut rem, &mut nodes);

        let (mut eqs, nes) = collect_content_eq(top.path(), rem, sem.clone()).await;

        eqs.push(top);
        groups.push(eqs);
        let _ = std::mem::replace(&mut nodes, nes);
    }

    if nodes.len() == 1 {
        groups.push(nodes);
    }

    debug_assert!(groups.iter().all(|v| 0 < v.len()));
    groups
}

fn find_dupes_by_content(nodes: Vec<Node>, sem: Semaphore, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    if nodes.len() == 0 {
        return;
    }
    if nodes.len() == 1 {
        task::spawn(async move {
            let mut nodes = nodes;
            uniqs.send(nodes.pop().unwrap()).await.unwrap();
        });
        return;
    }

    task::spawn(async move{
        let groups = group_by_content(nodes, sem).await;

        for mut group in groups.into_iter() {
            debug_assert!(0 < group.len());
            if group.len() == 1 {
                let uniqs = uniqs.clone();
                task::spawn(async move {
                    uniqs.send(group.pop().unwrap()).await.unwrap();
                });
            } else {
                let dupes = dupes.clone();
                task::spawn(async move {
                    dupes.send(group).await.unwrap();
                });
            }
        }
    });
}

fn split_by_size(nodes: Vec<Node>, empty: Sender<Node>, small: Sender<Node>, large: Sender<Node>) {
    for node in nodes.into_iter() {
        if node.size() == 0 {
            let empty = empty.clone();
            task::spawn(async move {
                empty.send(node).await.unwrap();
            });
        } else if THRESHOLD < node.size() {
            let large = large.clone();
            task::spawn(async move {
                large.send(node).await.unwrap();
            });
        } else {
            let small = small.clone();
            task::spawn(async move {
                small.send(node).await.unwrap();
            });
        }
    }
}

fn find_dupes_large(nodes: Vec<Node>, sem_small: Semaphore, sem_large: Semaphore, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    if nodes.len() == 0 {
        return;
    }

    let channel_size = nodes.len();

    let mut size_dupes = {
        let (size_dupes_tx, size_dupes_rx) = mpsc::channel(channel_size);
        find_dupes_by_size(nodes, size_dupes_tx, uniqs.clone());
        size_dupes_rx
    };

    let mut digest_dupes = {
        let sem_small = sem_small.clone();
        let sem_large = sem_large.clone();
        let uniqs = uniqs.clone();

        let (digest_dupes_tx, digest_dupes_rx) = mpsc::channel(channel_size);

        task::spawn(async move {
            while let Some(nodes) = size_dupes.recv().await {
                debug_assert!(0 < nodes.len());
                debug_assert!(THRESHOLD < nodes[0].size());
                debug_assert!(nodes.iter().map(Node::size).all_equal());
                find_dupes_by_digest_large(nodes, sem_small.clone(), sem_large.clone(), digest_dupes_tx.clone(), uniqs.clone());
            }
        });

        digest_dupes_rx
    };

    task::spawn(async move {
        while let Some(nodes) = digest_dupes.recv().await {
            find_dupes_by_content(nodes, sem_large.clone(), dupes.clone(), uniqs.clone());
        }
    });
}
fn find_dupes_small(nodes: Vec<Node>, sem: Semaphore, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    if nodes.len() == 0 {
        return;
    }

    let channel_size = nodes.len();

    let mut size_dupes = {
        let (size_dupes_tx, size_dupes_rx) = mpsc::channel(channel_size);
        find_dupes_by_size(nodes, size_dupes_tx, uniqs.clone());
        size_dupes_rx
    };

    let mut digest_dupes = {
        let sem = sem.clone();
        let uniqs = uniqs.clone();

        let (digest_dupes_tx, digest_dupes_rx) = mpsc::channel(channel_size);

        task::spawn(async move {
            while let Some(nodes) = size_dupes.recv().await {
                debug_assert!(0 < nodes.len());
                debug_assert!(nodes[0].size() <= THRESHOLD);
                debug_assert!(nodes.iter().map(Node::size).all_equal());
                find_dupes_by_digest_small(nodes, sem.clone(), digest_dupes_tx.clone(), uniqs.clone());
            }
        });

        digest_dupes_rx
    };

    task::spawn(async move {
        while let Some(nodes) = digest_dupes.recv().await {
            find_dupes_by_content(nodes, sem.clone(), dupes.clone(), uniqs.clone());
        }
    });
}
fn find_dupes_core(nodes: Vec<Node>, sem_small: Semaphore, sem_large: Semaphore, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    if nodes.len() == 0 {
        return;
    }

    let channel_size = nodes.len();

    let (empty_tx, empty_rx) = mpsc::channel(channel_size);
    let (small_tx, small_rx) = mpsc::channel(channel_size);
    let (large_tx, large_rx) = mpsc::channel(channel_size);
    split_by_size(nodes, empty_tx, small_tx, large_tx);

    // empty
    {
        let dupes = dupes.clone();
        let uniqs = uniqs.clone();
        task::spawn(async move {
            let mut empty_nodes: Vec<Node> = ReceiverStream::new(empty_rx).collect().await;
            if empty_nodes.len() == 1 {
                uniqs.send(empty_nodes.pop().unwrap()).await.unwrap();
            } else if 1 < empty_nodes.len() {
                dupes.send(empty_nodes).await.unwrap();
            }
        });
    }

    // small
    {
        let dupes = dupes.clone();
        let uniqs = uniqs.clone();
        let sem = sem_small.clone();
        task::spawn(async move {
            let small_nodes = ReceiverStream::new(small_rx).collect().await;
            find_dupes_small(small_nodes, sem, dupes, uniqs);
        });
    }

    // large
    {
        task::spawn(async move {
            let large_nodes = ReceiverStream::new(large_rx).collect().await;
            find_dupes_large(large_nodes, sem_small, sem_large, dupes, uniqs);
        });
    }
}
pub(crate) fn find_dupes(nodes: Vec<Node>, sem_small: Semaphore, sem_large: Semaphore, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>, ignore_dev: bool) {
    if nodes.len() == 0 {
        return;
    }

    if ignore_dev {
        find_dupes_core(nodes, sem_small, sem_large, dupes, uniqs);
    } else {
        task::spawn(async move {
            let channel_size = nodes.len();
            let mut dev_task: HashMap<u64, Sender<Node>> = HashMap::new();
            let mut none_devs = Vec::new();

            for node in nodes.into_iter() {
                let dev = node.dev();
                if dev.is_none() {
                    none_devs.push(node);
                    continue;
                }

                match dev_task.entry(dev.unwrap()) {
                    Vacant(v) => {
                        let sem_small = sem_small.clone();
                        let sem_large = sem_large.clone();
                        let dupes = dupes.clone();
                        let uniqs = uniqs.clone();
                        let (tx, rx) = mpsc::channel(channel_size);

                        tx.send(node).await.unwrap();
                        v.insert(tx);

                        task::spawn(async move {
                            let nodes = ReceiverStream::new(rx).collect().await;
                            find_dupes_core(nodes, sem_small, sem_large, dupes, uniqs);
                        });
                    },
                    Occupied(o) => {
                        o.get().send(node).await.unwrap();
                    },
                };
            }

            if 0 < none_devs.len() {
                find_dupes_core(none_devs, sem_small, sem_large, dupes, uniqs);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::super::walk::{DirWalker, Node};
    use super::*;

    use super::super::util::semaphore::{SemaphoreBuilder, SmallSemaphore, LargeSemaphore};

    fn new_semaphore() -> (SmallSemaphore, LargeSemaphore) {
        SemaphoreBuilder::new()
            .max_concurrency(Some(2))
            .large_concurrency(false)
            .build()
    }

    async fn collect_nodes<P: AsRef<Path>>(paths: &[P]) -> Vec<Node> {
        DirWalker::new()
            .walk(paths)
            .collect().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn find_dupes_empty() {
        let p = "files/empty-duplicates";
        let nodes = collect_nodes(&[p]).await;

        let (dupes_tx, dupes_rx) = mpsc::channel(nodes.len());
        let (uniqs_tx, uniqs_rx) = mpsc::channel(nodes.len());
        let (sem_small, sem_large) = new_semaphore();

        find_dupes(nodes, sem_small, sem_large, dupes_tx, uniqs_tx, false);

        let uniqs = ReceiverStream::new(uniqs_rx).collect::<Vec<Node>>().await;
        let dupes = ReceiverStream::new(dupes_rx).collect::<Vec<Vec<Node>>>().await;

        assert_eq!(dupes.len(), 1);
        assert_eq!(dupes[0].len(), 3);

        assert_eq!(uniqs.len(), 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn find_dupes_small_uniqs() {
        let p = "files/small-uniques";
        let nodes = collect_nodes(&[p]).await;

        let (dupes_tx, dupes_rx) = mpsc::channel(nodes.len());
        let (uniqs_tx, uniqs_rx) = mpsc::channel(nodes.len());
        let (sem_small, sem_large) = new_semaphore();

        find_dupes(nodes, sem_small, sem_large, dupes_tx, uniqs_tx, false);

        let uniqs = ReceiverStream::new(uniqs_rx).collect::<Vec<Node>>().await;
        let dupes = ReceiverStream::new(dupes_rx).collect::<Vec<Vec<Node>>>().await;

        assert_eq!(dupes.len(), 0);
        assert_eq!(uniqs.len(), 3);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn find_dupes_large_uniqs() {
        let p = "files/large-uniques";
        let nodes = collect_nodes(&[p]).await;

        let (dupes_tx, dupes_rx) = mpsc::channel(nodes.len());
        let (uniqs_tx, uniqs_rx) = mpsc::channel(nodes.len());
        let (sem_small, sem_large) = new_semaphore();

        find_dupes(nodes, sem_small, sem_large, dupes_tx, uniqs_tx, false);

        let uniqs = ReceiverStream::new(uniqs_rx).collect::<Vec<Node>>().await;
        let dupes = ReceiverStream::new(dupes_rx).collect::<Vec<Vec<Node>>>().await;

        assert_eq!(dupes.len(), 0);
        assert_eq!(uniqs.len(), 4);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn find_dupes_small_duplicates() {
        let p = "files/small-duplicates";
        let nodes = collect_nodes(&[p]).await;

        let (dupes_tx, dupes_rx) = mpsc::channel(nodes.len());
        let (uniqs_tx, uniqs_rx) = mpsc::channel(nodes.len());
        let (sem_small, sem_large) = new_semaphore();

        find_dupes(nodes, sem_small, sem_large, dupes_tx, uniqs_tx, false);

        let uniqs = ReceiverStream::new(uniqs_rx).collect::<Vec<Node>>().await;
        let dupes = ReceiverStream::new(dupes_rx).collect::<Vec<Vec<Node>>>().await;

        assert_eq!(dupes.len(), 1);
        assert_eq!(dupes[0].len(), 3);

        assert_eq!(uniqs.len(), 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn find_dupes_large_duplicates() {
        let p = "files/large-duplicates";
        let nodes = collect_nodes(&[p]).await;

        let (dupes_tx, dupes_rx) = mpsc::channel(nodes.len());
        let (uniqs_tx, uniqs_rx) = mpsc::channel(nodes.len());
        let (sem_small, sem_large) = new_semaphore();

        find_dupes(nodes, sem_small, sem_large, dupes_tx, uniqs_tx, false);

        let uniqs = ReceiverStream::new(uniqs_rx).collect::<Vec<Node>>().await;
        let dupes = ReceiverStream::new(dupes_rx).collect::<Vec<Vec<Node>>>().await;

        assert_eq!(dupes.len(), 1);
        assert_eq!(dupes[0].len(), 3);

        assert_eq!(uniqs.len(), 0);
    }
}
