use fdlimit;
use num_cpus;
use std::cmp;
use std::sync::Arc;

const MIN_FDS: usize = 2; // Two files will be opened at a time to compare them.

fn system_fdlimit() -> usize {
    const MAX: usize = std::usize::MAX;
    match fdlimit::raise_fd_limit() {
        Some(n) => cmp::min(n, MAX as u64) as usize,
        None => MAX,
    }
}
fn max_concurrency(user_limit: Option<usize>) -> usize {
    let default_concurrency = 10 * num_cpus::get();
    cmp::max(
        MIN_FDS,
        cmp::min(
            system_fdlimit() - 3, // 3 = (stdin, stdout, stderr)
            cmp::min(
                default_concurrency,
                user_limit.unwrap_or(default_concurrency),
            ),
        ),
    )
}

type TSemaphore = tokio::sync::Semaphore;
type TPermit<'a> = tokio::sync::SemaphorePermit<'a>;
type TMutex = tokio::sync::Mutex<()>;
type TMutexGuard<'a> = tokio::sync::MutexGuard<'a, ()>;

#[derive(Debug)]
pub struct SemaphorePermit<'a> {
    normal: TPermit<'a>,
    large: Option<TMutexGuard<'a>>,
}

pub type AcquireError = tokio::sync::AcquireError;

#[derive(Debug)]
pub struct SemaphoreImpl {
    normal: TSemaphore,
    large: Option<TMutex>,
}

impl SemaphoreImpl {
    fn new(normal_permits: usize, large_mutex: bool) -> SemaphoreImpl {
        let normal = TSemaphore::new(normal_permits);
        let large = if large_mutex {
            Some(TMutex::new(()))
        } else {
            None
        };
        SemaphoreImpl { normal, large }
    }
    async fn acquire_small_many(&self, n: u32) -> Result<SemaphorePermit<'_>, AcquireError> {
        let normal = self.normal.acquire_many(n).await?;
        Ok(SemaphorePermit {
            normal,
            large: None,
        })
    }
    async fn acquire_large_many(&self, n: u32) -> Result<SemaphorePermit<'_>, AcquireError> {
        // Acquire lock from large prior to normal.
        let large = match &self.large {
            None => None,
            Some(mtx) => Some(mtx.lock().await),
        };
        let normal = self.normal.acquire_many(n).await?;
        Ok(SemaphorePermit { normal, large })
    }
}

#[derive(Debug, Clone)]
pub enum Semaphore {
    Small(Arc<SemaphoreImpl>),
    Large(Arc<SemaphoreImpl>),
}

impl Semaphore {
    pub async fn acquire_many(&self, n: u32) -> Result<SemaphorePermit<'_>, AcquireError> {
        match self {
            Semaphore::Small(sem) => sem.acquire_small_many(n).await,
            Semaphore::Large(sem) => sem.acquire_large_many(n).await,
        }
    }
    pub async fn acquire(&self) -> Result<SemaphorePermit<'_>, AcquireError> {
        match self {
            Semaphore::Small(sem) => sem.acquire_small_many(1).await,
            Semaphore::Large(sem) => sem.acquire_large_many(1).await,
        }
    }
}

pub type SmallSemaphore = Semaphore;
pub type LargeSemaphore = Semaphore;

pub struct SemaphoreBuilder {
    max_concurrency: Option<usize>,
    large_concurrency: bool,
}

impl SemaphoreBuilder {
    pub fn new() -> SemaphoreBuilder {
        SemaphoreBuilder {
            max_concurrency: None,
            large_concurrency: true,
        }
    }
    #[allow(dead_code)]
    pub fn max_concurrency(mut self, con: Option<usize>) -> Self {
        self.max_concurrency = if con.is_none() {
            None
        } else if con.unwrap() == 0 {
            None
        } else if con.unwrap() < MIN_FDS {
            Some(MIN_FDS)
        } else {
            con
        };
        self
    }
    #[allow(dead_code)]
    pub fn large_concurrency(mut self, enable: bool) -> Self {
        self.large_concurrency = enable;
        self
    }

    fn build_single(self, normal_con: usize) -> (SmallSemaphore, LargeSemaphore) {
        let small_sem = Arc::new(SemaphoreImpl::new(normal_con, false));
        let large_sem = small_sem.clone();
        (Semaphore::Small(small_sem), Semaphore::Large(large_sem))
    }
    fn build_double(self, normal_con: usize) -> (SmallSemaphore, LargeSemaphore) {
        let large_con = self.large_concurrency;

        if large_con {
            return self.build_single(normal_con);
        }

        let small_sem = Arc::new(SemaphoreImpl::new(normal_con, true));
        let large_sem = small_sem.clone();
        (Semaphore::Small(small_sem), Semaphore::Large(large_sem))
    }
    pub fn build(self) -> (SmallSemaphore, LargeSemaphore) {
        let normal_con = max_concurrency(self.max_concurrency);
        if self.large_concurrency {
            self.build_single(normal_con)
        } else {
            self.build_double(normal_con)
        }
    }
}
