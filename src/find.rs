use itertools::Itertools;
use std::cmp::Ordering;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use super::entry::{ContentEq, Digest, FileAttr};
use super::util::semaphore::Semaphore;
use super::util::THRESHOLD;
use super::util::{group_by_key, group_by_key_map};
use super::walk::Node;

fn group_by_size(nodes: Vec<Node>) -> Vec<(u64, Vec<Node>)> {
    group_by_key(nodes, |n| n.size())
}

fn find_dups_by_size(nodes: Vec<Node>, dups: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    if nodes.is_empty() {
        return;
    }

    task::spawn(async move {
        let groups = task::block_in_place(|| group_by_size(nodes));

        let mut uniq_count = 0;
        for (size, group) in groups.into_iter() {
            debug_assert!(!group.is_empty());
            if group.len() == 1 {
                let uniqs = uniqs.clone();
                task::spawn(async move {
                    uniqs.send(group.into_iter().next().unwrap()).await.unwrap();
                });
                uniq_count += 1;
            } else {
                let dup_count = group.len();
                let dups = dups.clone();
                task::spawn(async move {
                    dups.send(group).await.unwrap();
                });
                log::debug!("{} files have same size; {}B.", dup_count, size);
            }
        }
        if 0 < uniq_count {
            log::debug!("{} files have unique size.", uniq_count);
        }
    });
}

async fn group_by_fast_digest(nodes: Vec<Node>, sem: Semaphore) -> Vec<(u64, Vec<Node>)> {
    if nodes.is_empty() {
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
                    node.fast_digest().await
                };

                let d = match d {
                    Err(e) => {
                        log::warn!("{}: {}", e, node.path().display());
                        return;
                    }
                    Ok(d) => d,
                };

                tx.send((d, node)).await.unwrap();
            });
        }
        rx
    };

    let dn = ReceiverStream::new(rx).collect().await;

    task::block_in_place(|| group_by_key_map(dn, |&(d, _)| d, |(_, n)| n))
}

async fn group_by_digest(nodes: Vec<Node>, sem: Semaphore) -> Vec<(u64, Vec<Node>)> {
    if nodes.is_empty() {
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
                    node.digest().await
                };

                let d = match d {
                    Err(e) => {
                        log::warn!("{}: {}", e, node.path().display());
                        return;
                    }
                    Ok(d) => d,
                };

                tx.send((d, node)).await.unwrap();
            });
        }
        rx
    };

    let dn = ReceiverStream::new(rx).collect().await;

    task::block_in_place(|| group_by_key_map(dn, |&(d, _)| d, |(_, n)| n))
}

fn find_dups_by_fast_digest_impl(
    nodes: Vec<Node>,
    sem: Semaphore,
    dups: Sender<Vec<Node>>,
    uniqs: Sender<Node>,
) {
    if nodes.is_empty() {
        return;
    }

    task::spawn(async move {
        let groups = group_by_fast_digest(nodes, sem).await;

        let mut uniq_count = 0;
        for (digest, mut group) in groups.into_iter() {
            debug_assert!(!group.is_empty());
            if group.len() == 1 {
                let uniqs = uniqs.clone();
                task::spawn(async move {
                    uniqs.send(group.pop().unwrap()).await.unwrap();
                });
                uniq_count += 1;
            } else {
                let dup_count = group.len();
                let dups = dups.clone();
                task::spawn(async move {
                    dups.send(group).await.unwrap();
                });
                log::debug!("{} files have same digests at beginning {}B; {:#018x}", dup_count, THRESHOLD, digest);
            }
        }
        if 0 < uniq_count {
            log::debug!("{} files have unique digests at beginning {}B.", uniq_count, THRESHOLD);
        }
    });
}

fn find_dups_by_digest_impl(
    nodes: Vec<Node>,
    sem: Semaphore,
    dups: Sender<Vec<Node>>,
    uniqs: Sender<Node>,
) {
    if nodes.is_empty() {
        return;
    }

    task::spawn(async move {
        let groups = group_by_digest(nodes, sem).await;

        let mut uniq_count = 0;
        for (digest, mut group) in groups.into_iter() {
            debug_assert!(!group.is_empty());
            if group.len() == 1 {
                let uniqs = uniqs.clone();
                task::spawn(async move {
                    uniqs.send(group.pop().unwrap()).await.unwrap();
                });
                uniq_count += 1;
            } else {
                let dup_count = group.len();
                let dups = dups.clone();
                task::spawn(async move {
                    dups.send(group).await.unwrap();
                });
                log::debug!("{} files have same digests; {:#018x}", dup_count, digest);
            }
        }
        if 0 < uniq_count {
            log::debug!("{} files have unique digests.", uniq_count);
        }
    });
}

fn find_dups_by_digest_small(
    nodes: Vec<Node>,
    sem: Semaphore,
    dups: Sender<Vec<Node>>,
    uniqs: Sender<Node>,
) {
    find_dups_by_digest_impl(nodes, sem, dups, uniqs);
}

fn find_dups_by_digest_large(
    nodes: Vec<Node>,
    sem_small: Semaphore,
    sem_large: Semaphore,
    dups: Sender<Vec<Node>>,
    uniqs: Sender<Node>,
) {
    if nodes.is_empty() {
        return;
    }

    let (fast_dups_tx, mut fast_dups_rx) = mpsc::channel(nodes.len());

    find_dups_by_fast_digest_impl(nodes, sem_small, fast_dups_tx, uniqs.clone());

    task::spawn(async move {
        while let Some(nodes) = fast_dups_rx.recv().await {
            let sem = sem_large.clone();
            let dups = dups.clone();
            let uniqs = uniqs.clone();
            task::spawn(async move {
                find_dups_by_digest_impl(nodes, sem, dups, uniqs);
            });
        }
    });
}

async fn collect_content_eq(
    base_node: Node,
    nodes: Vec<Node>,
    sem: Semaphore,
) -> (Vec<Node>, Vec<Node>) {
    debug_assert!(!nodes.is_empty());

    let base_node = Arc::new(base_node);

    let (eq_rx, ne_rx) = {
        let (eq_tx, eq_rx) = mpsc::channel(nodes.len());
        let (ne_tx, ne_rx) = mpsc::channel(nodes.len());

        for node in nodes.into_iter().rev() {
            let base_node = base_node.clone();
            let eq_tx = eq_tx.clone();
            let ne_tx = ne_tx.clone();
            let sem = sem.clone();
            task::spawn(async move {
                let eq = {
                    let _p = sem.acquire_double().await.unwrap();
                    base_node.eq_content(&node).await
                };

                match eq {
                    Err(e) => {
                        log::warn!(
                            "{}: {} or {}",
                            e,
                            base_node.path().display(),
                            node.path().display()
                        );
                        ne_tx.send(node).await.unwrap();
                    }
                    Ok(eq) => {
                        if eq {
                            eq_tx.send(node).await.unwrap();
                        } else {
                            ne_tx.send(node).await.unwrap();
                        }
                    }
                };
            });
        }

        (eq_rx, ne_rx)
    };

    let mut eqs: Vec<Node> = ReceiverStream::new(eq_rx).collect().await;
    let nes = ReceiverStream::new(ne_rx).collect().await;

    // The base_node can be unwrapped because
    // it is clear that all of spawned tasks have been end at this time.
    eqs.push(Arc::try_unwrap(base_node).unwrap());
    (eqs, nes)
}

async fn group_by_content(mut nodes: Vec<Node>, sem: Semaphore) -> Vec<Vec<Node>> {
    if nodes.is_empty() {
        return Vec::new();
    }

    let mut groups = Vec::new();

    while 1 < nodes.len() {
        let top = nodes.pop().unwrap();
        let mut rem = Vec::new();
        std::mem::swap(&mut rem, &mut nodes);

        let (eqs, nes) = collect_content_eq(top, rem, sem.clone()).await;

        groups.push(eqs);
        let _ = std::mem::replace(&mut nodes, nes);
    }

    if nodes.len() == 1 {
        groups.push(nodes);
    }

    debug_assert!(groups.iter().all(|v| !v.is_empty()));
    groups
}

fn find_dups_by_content(
    nodes: Vec<Node>,
    sem: Semaphore,
    dups: Sender<Vec<Node>>,
    uniqs: Sender<Node>,
) {
    if nodes.is_empty() {
        return;
    }
    if nodes.len() == 1 {
        task::spawn(async move {
            let mut nodes = nodes;
            uniqs.send(nodes.pop().unwrap()).await.unwrap();
            log::debug!("1 file have unique contents.");
        });
        return;
    }

    task::spawn(async move {
        let groups = group_by_content(nodes, sem).await;

        let mut uniq_count = 0;
        for mut group in groups.into_iter() {
            debug_assert!(!group.is_empty());
            if group.len() == 1 {
                let uniqs = uniqs.clone();
                task::spawn(async move {
                    uniqs.send(group.pop().unwrap()).await.unwrap();
                });
                uniq_count += 1;
            } else {
                let dup_count = group.len();
                let dups = dups.clone();
                task::spawn(async move {
                    dups.send(group).await.unwrap();
                });
                log::debug!("{} files have same contents.", dup_count);
            }
        }
        if 0 < uniq_count {
            log::debug!("{} files have unique contents.", uniq_count);
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

fn find_dups_large(
    nodes: Vec<Node>,
    sem_small: Semaphore,
    sem_large: Semaphore,
    dups: Sender<Vec<Node>>,
    uniqs: Sender<Node>,
) {
    if nodes.is_empty() {
        return;
    }

    let channel_size = nodes.len();

    let mut size_dups = {
        let (size_dups_tx, size_dups_rx) = mpsc::channel(channel_size);
        find_dups_by_size(nodes, size_dups_tx, uniqs.clone());
        size_dups_rx
    };

    let mut digest_dups = {
        let sem_large = sem_large.clone();
        let uniqs = uniqs.clone();

        let (digest_dups_tx, digest_dups_rx) = mpsc::channel(channel_size);

        task::spawn(async move {
            while let Some(nodes) = size_dups.recv().await {
                debug_assert!(!nodes.is_empty());
                debug_assert!(THRESHOLD < nodes[0].size());
                debug_assert!(nodes.iter().map(Node::size).all_equal());
                find_dups_by_digest_large(
                    nodes,
                    sem_small.clone(),
                    sem_large.clone(),
                    digest_dups_tx.clone(),
                    uniqs.clone(),
                );
            }
        });

        digest_dups_rx
    };

    task::spawn(async move {
        while let Some(nodes) = digest_dups.recv().await {
            find_dups_by_content(nodes, sem_large.clone(), dups.clone(), uniqs.clone());
        }
    });
}
fn find_dups_small(nodes: Vec<Node>, sem: Semaphore, dups: Sender<Vec<Node>>, uniqs: Sender<Node>) {
    if nodes.is_empty() {
        return;
    }

    let channel_size = nodes.len();

    let mut size_dups = {
        let (size_dups_tx, size_dups_rx) = mpsc::channel(channel_size);
        find_dups_by_size(nodes, size_dups_tx, uniqs.clone());
        size_dups_rx
    };

    let mut digest_dups = {
        let sem = sem.clone();
        let uniqs = uniqs.clone();

        let (digest_dups_tx, digest_dups_rx) = mpsc::channel(channel_size);

        task::spawn(async move {
            while let Some(nodes) = size_dups.recv().await {
                debug_assert!(!nodes.is_empty());
                debug_assert!(nodes[0].size() <= THRESHOLD);
                debug_assert!(nodes.iter().map(Node::size).all_equal());
                find_dups_by_digest_small(
                    nodes,
                    sem.clone(),
                    digest_dups_tx.clone(),
                    uniqs.clone(),
                );
            }
        });

        digest_dups_rx
    };

    task::spawn(async move {
        while let Some(nodes) = digest_dups.recv().await {
            find_dups_by_content(nodes, sem.clone(), dups.clone(), uniqs.clone());
        }
    });
}
fn find_dups_core(
    nodes: Vec<Node>,
    sem_small: Semaphore,
    sem_large: Semaphore,
    dups: Sender<Vec<Node>>,
    uniqs: Sender<Node>,
) {
    if nodes.is_empty() {
        return;
    }

    let channel_size = nodes.len();

    let (empty_tx, empty_rx) = mpsc::channel(channel_size);
    let (small_tx, small_rx) = mpsc::channel(channel_size);
    let (large_tx, large_rx) = mpsc::channel(channel_size);
    split_by_size(nodes, empty_tx, small_tx, large_tx);

    // empty
    {
        let dups = dups.clone();
        let uniqs = uniqs.clone();
        task::spawn(async move {
            let mut empty_nodes: Vec<Node> = ReceiverStream::new(empty_rx).collect().await;
            let empty_count = empty_nodes.len();
            match empty_nodes.len().cmp(&1) {
                Ordering::Equal => {
                    uniqs.send(empty_nodes.pop().unwrap()).await.unwrap();
                },
                Ordering::Greater => {
                    dups.send(empty_nodes).await.unwrap();
                },
                _ => (),
            };
            log::debug!("{} empty files are detected.", empty_count);
        });
    }

    // small
    {
        let dups = dups.clone();
        let uniqs = uniqs.clone();
        let sem = sem_small.clone();
        task::spawn(async move {
            let small_nodes = ReceiverStream::new(small_rx).collect().await;
            find_dups_small(small_nodes, sem, dups, uniqs);
        });
    }

    // large
    {
        task::spawn(async move {
            let large_nodes = ReceiverStream::new(large_rx).collect().await;
            find_dups_large(large_nodes, sem_small, sem_large, dups, uniqs);
        });
    }
}
pub(crate) fn find_dups(
    nodes: Vec<Node>,
    sem_small: Semaphore,
    sem_large: Semaphore,
    dups: Sender<Vec<Node>>,
    uniqs: Sender<Node>,
    ignore_dev: bool,
) {
    if nodes.is_empty() {
        return;
    }

    if ignore_dev {
        find_dups_core(nodes, sem_small, sem_large, dups, uniqs);
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
                        let dups = dups.clone();
                        let uniqs = uniqs.clone();
                        let (tx, rx) = mpsc::channel(channel_size);

                        tx.send(node).await.unwrap();
                        v.insert(tx);

                        task::spawn(async move {
                            let nodes = ReceiverStream::new(rx).collect().await;
                            find_dups_core(nodes, sem_small, sem_large, dups, uniqs);
                        });
                    }
                    Occupied(o) => {
                        o.get().send(node).await.unwrap();
                    }
                };
            }

            if !none_devs.is_empty() {
                find_dups_core(none_devs, sem_small, sem_large, dups, uniqs);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::super::walk::{DirWalker, Node};
    use super::*;

    use super::super::util::semaphore::{LargeSemaphore, SemaphoreBuilder, SmallSemaphore};

    fn new_semaphore() -> (SmallSemaphore, LargeSemaphore) {
        SemaphoreBuilder::new()
            .max_concurrency(Some(2))
            .large_concurrency(false)
            .build()
    }

    async fn collect_nodes<P: AsRef<Path>>(paths: &[P]) -> Vec<Node> {
        DirWalker::new().walk(paths).collect().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn find_dups_empty() {
        let p = "files/empty-duplicates";
        let nodes = collect_nodes(&[p]).await;

        let (dups_tx, dups_rx) = mpsc::channel(nodes.len());
        let (uniqs_tx, uniqs_rx) = mpsc::channel(nodes.len());
        let (sem_small, sem_large) = new_semaphore();

        find_dups(nodes, sem_small, sem_large, dups_tx, uniqs_tx, false);

        let uniqs = ReceiverStream::new(uniqs_rx).collect::<Vec<Node>>().await;
        let dups = ReceiverStream::new(dups_rx)
            .collect::<Vec<Vec<Node>>>()
            .await;

        assert_eq!(dups.len(), 1);
        assert_eq!(dups[0].len(), 3);

        assert_eq!(uniqs.len(), 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn find_dups_small_uniqs() {
        let p = "files/small-uniques";
        let nodes = collect_nodes(&[p]).await;

        let (dups_tx, dups_rx) = mpsc::channel(nodes.len());
        let (uniqs_tx, uniqs_rx) = mpsc::channel(nodes.len());
        let (sem_small, sem_large) = new_semaphore();

        find_dups(nodes, sem_small, sem_large, dups_tx, uniqs_tx, false);

        let uniqs = ReceiverStream::new(uniqs_rx).collect::<Vec<Node>>().await;
        let dups = ReceiverStream::new(dups_rx)
            .collect::<Vec<Vec<Node>>>()
            .await;

        assert_eq!(dups.len(), 0);
        assert_eq!(uniqs.len(), 3);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn find_dups_large_uniqs() {
        let p = "files/large-uniques";
        let nodes = collect_nodes(&[p]).await;

        let (dups_tx, dups_rx) = mpsc::channel(nodes.len());
        let (uniqs_tx, uniqs_rx) = mpsc::channel(nodes.len());
        let (sem_small, sem_large) = new_semaphore();

        find_dups(nodes, sem_small, sem_large, dups_tx, uniqs_tx, false);

        let uniqs = ReceiverStream::new(uniqs_rx).collect::<Vec<Node>>().await;
        let dups = ReceiverStream::new(dups_rx)
            .collect::<Vec<Vec<Node>>>()
            .await;

        assert_eq!(dups.len(), 0);
        assert_eq!(uniqs.len(), 4);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn find_dups_small_duplicates() {
        let p = "files/small-duplicates";
        let nodes = collect_nodes(&[p]).await;

        let (dups_tx, dups_rx) = mpsc::channel(nodes.len());
        let (uniqs_tx, uniqs_rx) = mpsc::channel(nodes.len());
        let (sem_small, sem_large) = new_semaphore();

        find_dups(nodes, sem_small, sem_large, dups_tx, uniqs_tx, false);

        let uniqs = ReceiverStream::new(uniqs_rx).collect::<Vec<Node>>().await;
        let dups = ReceiverStream::new(dups_rx)
            .collect::<Vec<Vec<Node>>>()
            .await;

        assert_eq!(dups.len(), 1);
        assert_eq!(dups[0].len(), 3);

        assert_eq!(uniqs.len(), 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn find_dups_large_duplicates() {
        let p = "files/large-duplicates";
        let nodes = collect_nodes(&[p]).await;

        let (dups_tx, dups_rx) = mpsc::channel(nodes.len());
        let (uniqs_tx, uniqs_rx) = mpsc::channel(nodes.len());
        let (sem_small, sem_large) = new_semaphore();

        find_dups(nodes, sem_small, sem_large, dups_tx, uniqs_tx, false);

        let uniqs = ReceiverStream::new(uniqs_rx).collect::<Vec<Node>>().await;
        let dups = ReceiverStream::new(dups_rx)
            .collect::<Vec<Vec<Node>>>()
            .await;

        assert_eq!(dups.len(), 1);
        assert_eq!(dups[0].len(), 3);

        assert_eq!(uniqs.len(), 0);
    }
}
