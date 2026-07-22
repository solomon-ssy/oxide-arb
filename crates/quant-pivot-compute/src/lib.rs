//! Process-wide CPU, job, and offline-memory governance.

use std::{
    any::Any,
    panic::{self, AssertUnwindSafe},
    sync::Arc,
};

use quant_pivot_allocator as _;
use quant_pivot_error::{QuantError, QuantResult, infra::InfraError, research::ResearchError};
use rayon::{ThreadPool, ThreadPoolBuilder};
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    sync::{
        OwnedSemaphorePermit, Semaphore,
        oneshot::{self, Receiver},
    },
};
use tokio_util::sync::CancellationToken;

pub const SERVING_THREADS: usize = 2;
pub const OFFLINE_THREADS: usize = 2;
pub const OFFLINE_MEMORY_BYTES: usize = 10 * 1024 * 1024 * 1024;

const SERVING_CPU_PERMITS: u32 = 2;
const OFFLINE_CPU_PERMITS: u32 = 2;
const MIB_BYTES: usize = 1024 * 1024;
const OFFLINE_MEMORY_MIB: usize = 10 * 1024;
const OFFLINE_MEMORY_PERMITS: u32 = 10 * 1024;

/// Explicit logical peak-memory reservation for one offline computation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OfflineMemory {
    permits_mib: u32,
}

impl OfflineMemory {
    /// Reserve a whole number of GiB from the fixed 10 GiB offline budget.
    pub fn try_gib(gib: u32) -> QuantResult<Self> {
        let permits_mib = gib
            .checked_mul(1024)
            .ok_or_else(|| InfraError::ComputeExecution {
                detail: format!("offline memory reservation {gib} GiB overflowed"),
            })?;
        Self::from_mib(permits_mib)
    }

    /// Round a byte estimate up to MiB permits.
    pub fn try_bytes(bytes: usize) -> QuantResult<Self> {
        let permits =
            bytes
                .checked_add(MIB_BYTES - 1)
                .ok_or_else(|| InfraError::ComputeExecution {
                    detail: format!("offline memory reservation {bytes} bytes overflowed"),
                })?
                / MIB_BYTES;
        let permits_mib = u32::try_from(permits).map_err(|error| InfraError::ComputeExecution {
            detail: format!("offline memory reservation does not fit permit width: {error}"),
        })?;
        Self::from_mib(permits_mib.max(1))
    }

    fn from_mib(permits_mib: u32) -> QuantResult<Self> {
        if permits_mib == 0 || permits_mib > OFFLINE_MEMORY_PERMITS {
            return Err(InfraError::ComputeExecution {
                detail: format!(
                    "offline memory reservation must be in 1..={OFFLINE_MEMORY_PERMITS} MiB, got {permits_mib} MiB"
                ),
            }
            .into());
        }
        Ok(Self { permits_mib })
    }
}

/// Join handle for a governed computation whose leases live until the work
/// actually stops, even when its caller is cancelled.
pub struct ComputeTask<T> {
    result: Receiver<QuantResult<T>>,
}

impl<T> ComputeTask<T> {
    pub async fn join(self) -> QuantResult<T> {
        self.result.await.map_err(|_| {
            QuantError::from(InfraError::ComputeExecution {
                detail: "governed compute worker exited without a result".to_owned(),
            })
        })?
    }
}

struct OfflineLeases {
    _job: OwnedSemaphorePermit,
    _cpu: OwnedSemaphorePermit,
    _memory: OwnedSemaphorePermit,
}

/// The only production owner of application Rayon pools.
pub struct ComputeExecutor {
    serving_pool: ThreadPool,
    offline_pool: ThreadPool,
    serving_cpu: Arc<Semaphore>,
    offline_cpu: Arc<Semaphore>,
    offline_jobs: Arc<Semaphore>,
    offline_memory: Arc<Semaphore>,
}

impl ComputeExecutor {
    pub fn new() -> QuantResult<Self> {
        let serving_pool = build_pool("quant-serving", SERVING_THREADS)?;
        let offline_pool = build_pool("quant-offline", OFFLINE_THREADS)?;
        Ok(Self {
            serving_pool,
            offline_pool,
            serving_cpu: Arc::new(Semaphore::new(SERVING_THREADS)),
            offline_cpu: Arc::new(Semaphore::new(OFFLINE_THREADS)),
            offline_jobs: Arc::new(Semaphore::new(1)),
            offline_memory: Arc::new(Semaphore::new(OFFLINE_MEMORY_MIB)),
        })
    }

    /// Run latency-sensitive pure CPU work on the fixed two-thread serving pool.
    pub async fn run_serving<T, F>(&self, work: F) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        let cpu = Arc::clone(&self.serving_cpu)
            .acquire_many_owned(SERVING_CPU_PERMITS)
            .await
            .map_err(|_| compute_closed("serving CPU"))?;
        let (sender, result) = oneshot::channel();
        self.serving_pool.spawn(move || {
            let _cpu = cpu;
            let _ = sender.send(catch_compute(work));
        });
        ComputeTask { result }.join().await
    }

    /// Run borrowed serving inputs on the governed pool. In the production
    /// multi-thread runtime, the Tokio worker only waits while Rayon owns the
    /// CPU work; current-thread runtimes are supported for deterministic tests.
    pub async fn run_serving_scoped<T, F>(&self, work: F) -> QuantResult<T>
    where
        T: Send,
        F: FnOnce() -> QuantResult<T> + Send,
    {
        let cpu = Arc::clone(&self.serving_cpu)
            .acquire_many_owned(SERVING_CPU_PERMITS)
            .await
            .map_err(|_| compute_closed("serving CPU"))?;
        let execute = || self.serving_pool.install(|| catch_compute(work));
        let result = match Handle::current().runtime_flavor() {
            RuntimeFlavor::MultiThread => tokio::task::block_in_place(execute),
            _ => execute(),
        };
        drop(cpu);
        result
    }

    /// Run one exclusive offline job and retain its CPU/memory leases until
    /// cooperative cancellation has actually reached a work boundary.
    pub async fn run_offline<T, F>(&self, memory: OfflineMemory, work: F) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        self.spawn_offline(memory, work).await?.join().await
    }

    pub async fn run_offline_cancellable<T, F>(
        &self,
        memory: OfflineMemory,
        cancel: &CancellationToken,
        work: F,
    ) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        self.spawn_offline_cancellable(memory, cancel, work)
            .await?
            .join()
            .await
    }

    /// Borrowed counterpart for offline kernels assembled from immutable
    /// request slices. The caller's Tokio worker waits; Rayon owns the CPU.
    pub async fn run_offline_scoped<T, F>(
        &self,
        memory: OfflineMemory,
        cancel: &CancellationToken,
        work: F,
    ) -> QuantResult<T>
    where
        T: Send,
        F: FnOnce() -> QuantResult<T> + Send,
    {
        let leases = self.acquire_offline(memory, Some(cancel)).await?;
        let execute = || self.offline_pool.install(|| catch_compute(work));
        let result = match Handle::current().runtime_flavor() {
            RuntimeFlavor::MultiThread => tokio::task::block_in_place(execute),
            _ => execute(),
        };
        drop(leases);
        result
    }

    /// Start streaming offline work after acquiring the same exclusive job,
    /// CPU, and memory leases used by [`Self::run_offline`].
    pub async fn spawn_offline<T, F>(
        &self,
        memory: OfflineMemory,
        work: F,
    ) -> QuantResult<ComputeTask<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        let leases = self.acquire_offline(memory, None).await?;
        Ok(self.spawn_offline_with_leases(leases, work))
    }

    pub async fn spawn_offline_cancellable<T, F>(
        &self,
        memory: OfflineMemory,
        cancel: &CancellationToken,
        work: F,
    ) -> QuantResult<ComputeTask<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        let leases = self.acquire_offline(memory, Some(cancel)).await?;
        Ok(self.spawn_offline_with_leases(leases, work))
    }

    fn spawn_offline_with_leases<T, F>(&self, leases: OfflineLeases, work: F) -> ComputeTask<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        let (sender, result) = oneshot::channel();
        self.offline_pool.spawn(move || {
            let _leases = leases;
            let _ = sender.send(catch_compute(work));
        });
        ComputeTask { result }
    }

    async fn acquire_offline(
        &self,
        memory: OfflineMemory,
        cancel: Option<&CancellationToken>,
    ) -> QuantResult<OfflineLeases> {
        let job = acquire(Arc::clone(&self.offline_jobs), 1, cancel, "offline job").await?;
        let cpu = acquire(
            Arc::clone(&self.offline_cpu),
            OFFLINE_CPU_PERMITS,
            cancel,
            "offline CPU",
        )
        .await?;
        let memory = acquire(
            Arc::clone(&self.offline_memory),
            memory.permits_mib,
            cancel,
            "offline memory",
        )
        .await?;
        Ok(OfflineLeases {
            _job: job,
            _cpu: cpu,
            _memory: memory,
        })
    }
}

fn build_pool(name: &'static str, threads: usize) -> QuantResult<ThreadPool> {
    ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(move |index| format!("{name}-{index}"))
        .build()
        .map_err(|error| {
            InfraError::Misconfigured {
                detail: format!("failed to build {name} Rayon pool: {error}"),
            }
            .into()
        })
}

async fn acquire(
    semaphore: Arc<Semaphore>,
    permits: u32,
    cancel: Option<&CancellationToken>,
    resource: &'static str,
) -> QuantResult<OwnedSemaphorePermit> {
    let acquire = semaphore.acquire_many_owned(permits);
    let permit = if let Some(cancel) = cancel {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return Err(ResearchError::Cancelled {
                    detail: format!("cancelled while waiting for governed {resource}"),
                }
                .into());
            }
            permit = acquire => permit,
        }
    } else {
        acquire.await
    };
    permit.map_err(|_| compute_closed(resource).into())
}

fn catch_compute<T, F>(work: F) -> QuantResult<T>
where
    F: FnOnce() -> QuantResult<T>,
{
    panic::catch_unwind(AssertUnwindSafe(work)).map_err(|payload| {
        QuantError::from(InfraError::ComputeExecution {
            detail: format!(
                "governed compute panicked: {}",
                panic_detail(payload.as_ref())
            ),
        })
    })?
}

fn panic_detail(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

fn compute_closed(resource: &str) -> InfraError {
    InfraError::ComputeExecution {
        detail: format!("governed {resource} semaphore closed"),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };

    use quant_pivot_error::{QuantResult, infra::InfraError};
    use tokio_util::sync::CancellationToken;

    use super::{ComputeExecutor, OFFLINE_MEMORY_BYTES, OfflineMemory};

    #[test]
    fn memory_reservation_rejects_more_than_the_process_budget() {
        let error = OfflineMemory::try_bytes(OFFLINE_MEMORY_BYTES + 1)
            .expect_err("reservation must fail closed");
        assert!(error.to_string().contains("offline memory reservation"));
    }

    #[tokio::test]
    async fn cancelled_waiter_never_starts_a_second_offline_job() -> QuantResult<()> {
        let executor = ComputeExecutor::new()?;
        let first = executor
            .spawn_offline(OfflineMemory::try_gib(1)?, || {
                thread::sleep(Duration::from_millis(50));
                Ok(())
            })
            .await?;
        let started = Arc::new(AtomicUsize::new(0));
        let cancel = CancellationToken::new();
        cancel.cancel();
        let observed = Arc::clone(&started);
        let result = executor
            .run_offline_cancellable(OfflineMemory::try_gib(1)?, &cancel, move || {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await;
        assert!(result.is_err());
        assert_eq!(started.load(Ordering::SeqCst), 0);
        first.join().await?;
        Ok(())
    }

    #[tokio::test]
    async fn worker_panic_is_a_typed_error() -> QuantResult<()> {
        let executor = ComputeExecutor::new()?;
        let result = executor
            .run_serving(|| -> QuantResult<()> { panic!("boom") })
            .await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn dropped_join_keeps_offline_leases_until_worker_completion() -> QuantResult<()> {
        let executor = Arc::new(ComputeExecutor::new()?);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let first = executor
            .spawn_offline(OfflineMemory::try_gib(10)?, move || {
                started_tx
                    .send(())
                    .map_err(|error| InfraError::ComputeExecution {
                        detail: format!("signal offline start: {error}"),
                    })?;
                release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .map_err(|error| InfraError::ComputeExecution {
                        detail: format!("wait for offline release: {error}"),
                    })?;
                Ok(())
            })
            .await?;
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .map_err(|error| InfraError::ComputeExecution {
                detail: format!("wait for offline start: {error}"),
            })?;
        drop(first);

        let second_started = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&second_started);
        let second_executor = Arc::clone(&executor);
        let second = tokio::spawn(async move {
            second_executor
                .run_offline(OfflineMemory::try_gib(1)?, move || {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(second_started.load(Ordering::SeqCst), 0);

        release_tx
            .send(())
            .map_err(|error| InfraError::ComputeExecution {
                detail: format!("release offline worker: {error}"),
            })?;
        second
            .await
            .map_err(|error| InfraError::ComputeExecution {
                detail: format!("join second offline task: {error}"),
            })??;
        assert_eq!(second_started.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn serving_pool_remains_available_during_worst_offline_reservation() -> QuantResult<()> {
        let executor = ComputeExecutor::new()?;
        let (release_tx, release_rx) = mpsc::channel();
        let offline = executor
            .spawn_offline(OfflineMemory::try_gib(10)?, move || {
                release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .map_err(|error| InfraError::ComputeExecution {
                        detail: format!("wait for offline release: {error}"),
                    })?;
                Ok(())
            })
            .await?;

        let thread_name = tokio::time::timeout(
            Duration::from_millis(100),
            executor.run_serving(|| {
                Ok(thread::current()
                    .name()
                    .map(str::to_owned)
                    .unwrap_or_default())
            }),
        )
        .await
        .map_err(|_| InfraError::ComputeExecution {
            detail: "serving work was starved by an offline reservation".to_owned(),
        })??;
        assert!(thread_name.starts_with("quant-serving-"));

        release_tx
            .send(())
            .map_err(|error| InfraError::ComputeExecution {
                detail: format!("release offline worker: {error}"),
            })?;
        offline.join().await?;
        Ok(())
    }
}
