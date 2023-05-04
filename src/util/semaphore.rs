use compact_rc::Arc;
use std::cmp;

const MIN_FDS: usize = 2; // Two files are opened at a time to compare them.

fn system_fdlimit() -> usize {
    const MAX: usize = std::usize::MAX;
    match fdlimit::raise_fd_limit() {
        Some(n) => cmp::min(n, MAX as u64) as usize,
        None => MAX,
    }
}
fn max_concurrency(user_limit: Option<usize>) -> usize {
    let default_concurrency = 4 * num_cpus::get();
    cmp::max(
        MIN_FDS,
        cmp::min(
            system_fdlimit() - 4, // 4 = (stdin, stdout, stderr, output file)
            cmp::min(
                default_concurrency,
                user_limit.unwrap_or(default_concurrency),
            ),
        ),
    )
}

type TokSemaphore = tokio::sync::Semaphore;
type TokPermit<'a> = tokio::sync::SemaphorePermit<'a>;
type TokMutex = tokio::sync::Mutex<()>;
type TokMutexGuard<'a> = tokio::sync::MutexGuard<'a, ()>;

#[must_use]
#[derive(Debug)]
pub struct SemaphorePermit<'a> {
    _normal: TokPermit<'a>,
    _large: Option<TokMutexGuard<'a>>,
}

pub type AcquireError = tokio::sync::AcquireError;

#[derive(Debug)]
pub struct SemaphoreImpl {
    // Semaphore to control overall concurrency.
    normal: TokSemaphore,
    // Mutex limits concurrency level of large files if used.
    large: Option<TokMutex>,
}

impl SemaphoreImpl {
    fn new(normal_permits: usize, large_mutex: bool) -> SemaphoreImpl {
        let normal = TokSemaphore::new(normal_permits);
        let large = if large_mutex {
            Some(TokMutex::new(()))
        } else {
            None
        };
        SemaphoreImpl { normal, large }
    }
    async fn acquire_small_many(&self, n: u32) -> Result<SemaphorePermit<'_>, AcquireError> {
        let normal = self.normal.acquire_many(n).await?;
        Ok(SemaphorePermit {
            _normal: normal,
            _large: None,
        })
    }
    async fn acquire_large_many(&self, n: u32) -> Result<SemaphorePermit<'_>, AcquireError> {
        // Acquire the mutex to reduce concurrency between large files.
        let large = match &self.large {
            None => None,
            Some(mtx) => Some(mtx.lock().await),
        };
        // Acquire
        let normal = self.normal.acquire_many(n).await?;
        Ok(SemaphorePermit {
            _normal: normal,
            _large: large,
        })
    }
}

#[derive(Debug, Clone)]
pub enum Semaphore {
    Small(Arc<SemaphoreImpl>),
    Large(Arc<SemaphoreImpl>),
}

impl Semaphore {
    pub async fn acquire_double(&self) -> Result<SemaphorePermit<'_>, AcquireError> {
        match self {
            Semaphore::Small(sem) => sem.acquire_small_many(2).await,
            Semaphore::Large(sem) => sem.acquire_large_many(2).await,
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
        self.max_concurrency = if let Some(c) = con {
            if c == 0 {
                None
            } else {
                Some(std::cmp::min(c, MIN_FDS))
            }
        } else {
            None
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
