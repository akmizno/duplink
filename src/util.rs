use std::sync::Arc;
use std::cmp;
use tokio::sync::Semaphore;
use itertools::Itertools;

use fdlimit;
use num_cpus;

pub(crate) fn group_by_key_map<T, V, K, F, M>(items: Vec<T>, mut cmp: F, mut mapper: M) -> Vec<Vec<V>>
where
    K: Ord,
    F: FnMut(&T) -> K,
    M: FnMut(T) -> V,
{
    if items.len() == 0 {
        return Vec::new();
    }

    items.into_iter()
        .sorted_by_key(|i| cmp(i))
        .group_by(|i| cmp(i))
        .into_iter()
        .map(|(_, g)| g.map(|i| mapper(i)).collect_vec())
        .collect_vec()
}

pub(crate) fn group_by_key<T, K, F>(items: Vec<T>, cmp: F) -> Vec<Vec<T>>
where
    K: Ord,
    F: FnMut(&T) -> K,
{
    group_by_key_map(items, cmp, |i|i)
}


fn system_fdlimit() -> usize {
    const MAX: usize = std::usize::MAX;
    match fdlimit::raise_fd_limit() {
        Some(n) => cmp::min(n, MAX as u64) as usize,
        None => MAX,
    }
}
fn max_open(user_limit: Option<usize>) -> usize {
    let sys_limit = cmp::min(
        10*num_cpus::get(),
        system_fdlimit()
    );

    debug_assert!(3 <= sys_limit);
    let sys_limit = sys_limit - 3;  // stdin, stdout, stderr

    let user_limit = user_limit.unwrap_or(sys_limit);

    cmp::max(2, cmp::min(sys_limit, user_limit))
}


pub(crate) type SmallSemaphore = Arc<Semaphore>;
pub(crate) type LargeSemaphore = Arc<Semaphore>;

pub(crate) struct SemaphoreBuilder {
    max_concurrency: Option<usize>,
    large_concurrency: Option<usize>,
}

impl SemaphoreBuilder {
    pub(crate) fn new() -> SemaphoreBuilder {
        SemaphoreBuilder {
            max_concurrency: None,
            large_concurrency: None
        }
    }
    pub(crate) fn max_concurrency(mut self, con: Option<usize>) -> Self {
        self.max_concurrency = con;
        self
    }
    pub(crate) fn large_concurrency(mut self, con: Option<usize>) -> Self {
        self.large_concurrency = con;
        self
    }

    fn build_shared(self) -> (SmallSemaphore, LargeSemaphore) {
        debug_assert!(self.large_concurrency.is_none());
        let con = max_open(self.max_concurrency);
        let small_sem = Arc::new(Semaphore::new(con));
        let large_sem = small_sem.clone();
        (small_sem, large_sem)
    }
    fn build_split(self) -> (SmallSemaphore, LargeSemaphore) {
        debug_assert!(self.large_concurrency.is_some());
        let large_con = self.large_concurrency.unwrap();
        let small_con = max_open(self.max_concurrency) - large_con;

        let small_sem = Arc::new(Semaphore::new(small_con));
        let large_sem = Arc::new(Semaphore::new(large_con));
        (small_sem, large_sem)

    }
    pub(crate) fn build(self) -> (SmallSemaphore, LargeSemaphore) {
        match self.large_concurrency {
            None => self.build_shared(),
            Some(_) => self.build_split(),
        }
    }
}
