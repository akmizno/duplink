use tokio::task;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use log;

use super::entry::{FileAttr, Digest, ContentEq};
use super::walk::Node;
use super::util::{group_by_key_map, group_by_key};

const THRESHOLD: u64 = 8192;

fn group_by_dev(nodes: Vec<Node>) -> Vec<Vec<Node>> {
    group_by_key(nodes, |n| n.dev().unwrap())
}

fn find_dupes_by_dev(nodes: Vec<Node>, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    task::spawn(async move {
        let mut some_nodes = Vec::new();
        for node in nodes.into_iter() {
            if node.dev().is_none() {
                // Exclude if dev() can not return Some.
                let uniqs = uniqs.clone();
                task::spawn(async move {
                    uniqs.send(node).await.unwrap();
                });
            } else {
                some_nodes.push(node);
            }
        }

        let groups = task::block_in_place(|| group_by_dev(some_nodes));

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


fn group_by_size(nodes: Vec<Node>) -> Vec<Vec<Node>> {
    group_by_key(nodes, |n| n.size())
}

fn split_zeros(nodes: Vec<Node>) -> (Vec<Node>, Vec<Node>) {
    let mut zeros = Vec::new();
    let mut nonzeros = Vec::new();

    for node in nodes.into_iter() {
        if node.size() == 0 {
            zeros.push(node);
        } else {
            nonzeros.push(node);
        }
    }

    (nonzeros, zeros)
}

fn find_dupes_by_size(nodes: Vec<Node>, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    task::spawn(async move{
        let nodes = {
            let dupes = dupes.clone();
            let uniqs = uniqs.clone();

            let (nonzeros, zeros) = task::block_in_place(|| split_zeros(nodes));

            if zeros.len() == 1 {
                task::spawn(async move {
                    uniqs.send(zeros.into_iter().next().unwrap()).await.unwrap();
                });
            } else if 1 < zeros.len() {
                task::spawn(async move {
                    dupes.send(zeros).await.unwrap();
                });
            }

            nonzeros
        };

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


async fn group_by_fast_digest(nodes: Vec<Node>, sem: Arc<Semaphore>) -> Vec<Vec<Node>> {
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

async fn group_by_digest(nodes: Vec<Node>, sem: Arc<Semaphore>) -> Vec<Vec<Node>> {
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

fn find_dupes_by_fast_digest_impl(nodes: Vec<Node>, sem: Arc<Semaphore>, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
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

fn find_dupes_by_digest_impl(nodes: Vec<Node>, sem: Arc<Semaphore>, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
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

fn find_dupes_by_digest(nodes: Vec<Node>, sem: Arc<Semaphore>, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    let (fast_dupes_tx, mut fast_dupes_rx) = mpsc::channel(nodes.len());

    find_dupes_by_fast_digest_impl(nodes, sem.clone(), fast_dupes_tx, uniqs.clone());

    task::spawn(async move {
        while let Some(nodes) = fast_dupes_rx.recv().await {
            let sem = sem.clone();
            let dupes = dupes.clone();
            let uniqs = uniqs.clone();
            task::spawn(async move {
                find_dupes_by_digest_impl(nodes, sem, dupes, uniqs);
            });
        }
    });
}


async fn collect_content_eq<P: AsRef<Path>>(path: P, nodes: Vec<Node>, sem: &Arc<Semaphore>) -> (Vec<Node>, Vec<Node>) {
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
                    node.eq_content(path).await
                };

                if eq.is_err() {
                    log::error!("{}", eq.unwrap_err());
                    return;
                }
                let eq = eq.unwrap();

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

async fn group_by_content(mut nodes: Vec<Node>, sem_small: Arc<Semaphore>, sem_large: Arc<Semaphore>) -> Vec<Vec<Node>> {
    debug_assert!(1 < nodes.len());

    let mut groups = Vec::new();

    while 1 < nodes.len() {
        let top = nodes.pop().unwrap();
        let rem = nodes.drain(..).collect();

        let sem = if THRESHOLD < top.size() { &sem_large } else { &sem_small };
        let (mut eqs, nes) = collect_content_eq(top.path(), rem, sem).await;

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

fn find_dupes_by_content(nodes: Vec<Node>, sem_small: Arc<Semaphore>, sem_large: Arc<Semaphore>, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    task::spawn(async move{
        let groups = group_by_content(nodes, sem_small, sem_large).await;

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

fn find_dupes_core(nodes: Vec<Node>, sem_small: Arc<Semaphore>, sem_large: Arc<Semaphore>, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    let channel_size = nodes.len();

    let mut size_dupes = {
        let (size_dupes_tx, size_dupes_rx) = mpsc::channel(channel_size);
        find_dupes_by_size(nodes, size_dupes_tx, uniqs.clone());
        size_dupes_rx
    };

    let mut digest_dupes = {
        let sem = sem_small.clone();
        let uniqs = uniqs.clone();

        let (digest_dupes_tx, digest_dupes_rx) = mpsc::channel(channel_size);

        task::spawn(async move {
            while let Some(nodes) = size_dupes.recv().await {
                find_dupes_by_digest(nodes, sem.clone(), digest_dupes_tx.clone(), uniqs.clone());
            }
        });

        digest_dupes_rx
    };

    task::spawn(async move {
        while let Some(nodes) = digest_dupes.recv().await {
            find_dupes_by_content(nodes, sem_small.clone(), sem_large.clone(), dupes.clone(), uniqs.clone());
        }
    });
}
pub(crate) fn find_dupes(nodes: Vec<Node>, sem_small: Arc<Semaphore>, sem_large: Arc<Semaphore>, dupes: Sender<Vec<Node>>, uniqs: Sender<Node>, ignore_dev: bool) {
    if ignore_dev {
        find_dupes_core(nodes, sem_small, sem_large, dupes, uniqs);
    } else {
        let (dev_dupes_tx, mut dev_dupes_rx) = mpsc::channel(nodes.len());
        let (dev_uniqs_tx, dev_uniqs_rx) = mpsc::channel(nodes.len());
        find_dupes_by_dev(nodes, dev_dupes_tx, dev_uniqs_tx);

        let sem_small2 = sem_small.clone();
        let sem_large2 = sem_large.clone();
        let dupes2 = dupes.clone();
        let uniqs2 = uniqs.clone();

        task::spawn(async move {
            while let Some(nodes) = dev_dupes_rx.recv().await {
                find_dupes_core(nodes, sem_small.clone(), sem_large.clone(), dupes.clone(), uniqs.clone());
            }
        });
        task::spawn(async move {
            let nodes = ReceiverStream::new(dev_uniqs_rx).collect().await;
            find_dupes_core(nodes, sem_small2.clone(), sem_large2.clone(), dupes2.clone(), uniqs2.clone());
        });
    }
}

#[cfg(test)]
mod tests {
    use super::super::walk::{DirWalker, Node};
    use super::*;

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
        let sem_small = Arc::new(Semaphore::new(2));
        let sem_large = Arc::new(Semaphore::new(2));

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
        let sem_small = Arc::new(Semaphore::new(2));
        let sem_large = Arc::new(Semaphore::new(2));

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
        let sem_small = Arc::new(Semaphore::new(2));
        let sem_large = Arc::new(Semaphore::new(2));

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
        let sem_small = Arc::new(Semaphore::new(2));
        let sem_large = Arc::new(Semaphore::new(2));

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
        let sem_small = Arc::new(Semaphore::new(2));
        let sem_large = Arc::new(Semaphore::new(2));

        find_dupes(nodes, sem_small, sem_large, dupes_tx, uniqs_tx, false);

        let uniqs = ReceiverStream::new(uniqs_rx).collect::<Vec<Node>>().await;
        let dupes = ReceiverStream::new(dupes_rx).collect::<Vec<Vec<Node>>>().await;

        assert_eq!(dupes.len(), 1);
        assert_eq!(dupes[0].len(), 3);

        assert_eq!(uniqs.len(), 0);
    }
}
