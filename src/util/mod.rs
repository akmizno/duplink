// use tokio::sync::Semaphore;
use itertools::Itertools;

pub mod progress;
pub mod semaphore;

pub(crate) fn group_by_key_map<T, V, K, F, M>(
    items: Vec<T>,
    mut cmp: F,
    mut mapper: M,
) -> Vec<(K, Vec<V>)>
where
    K: Ord,
    F: FnMut(&T) -> K,
    M: FnMut(T) -> V,
{
    if items.is_empty() {
        return Vec::new();
    }

    items
        .into_iter()
        .sorted_unstable_by_key(|i| cmp(i))
        .group_by(|i| cmp(i))
        .into_iter()
        .map(|(k, g)| (k, g.map(&mut mapper).collect_vec()))
        .collect_vec()
}

pub(crate) fn group_by_key<T, K, F>(items: Vec<T>, cmp: F) -> Vec<(K, Vec<T>)>
where
    K: Ord,
    F: FnMut(&T) -> K,
{
    group_by_key_map(items, cmp, |i| i)
}

pub const THRESHOLD: u64 = 8192;
