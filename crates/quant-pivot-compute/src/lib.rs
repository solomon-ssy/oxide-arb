//! Process-wide CPU, job, and offline-memory governance.

use std::{
    any::Any,
    panic::{self, AssertUnwindSafe},
    sync::Arc,
};

use quant_pivot_allocator as _;
use quant_pivot_error::{QuantError, QuantResult, infra::InfraError, research::ResearchError};
use rayon::{ThreadPool, ThreadPoolBuilder};
use tokio::sync::{
    OwnedSemaphorePermit, Semaphore, TryAcquireError,
    oneshot::{self, Receiver},
};
use tokio_util::sync::CancellationToken;

pub const SERVING_THREADS: usize = 2;
pub const OFFLINE_THREADS: usize = 2;
pub const SECURITY_THREADS: usize = 1;
pub const OFFLINE_MEMORY_BYTES: usize = 10 * 1024 * 1024 * 1024;

const SERVING_CPU_PERMITS: u32 = 2;
const OFFLINE_CPU_PERMITS: u32 = 2;
const SECURITY_CPU_PERMITS: u32 = 1;
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

/// Executor-owned offline-memory reservation that remains held across async
/// artifact I/O and persistence boundaries.
///
/// The permit is released automatically on drop. A lease may only be used by
/// the [`ComputeExecutor`] that created it; cross-executor use fails closed.
pub struct OfflineMemoryLease {
    reservation: Arc<OfflineMemoryReservation>,
}

struct OfflineMemoryReservation {
    owner: Arc<Semaphore>,
    _permit: OwnedSemaphorePermit,
    reserved_mib: u32,
}

impl OfflineMemoryLease {
    /// Logical bytes reserved from the process-wide offline-memory budget.
    #[must_use]
    pub fn reserved_bytes(&self) -> usize {
        self.reservation.reserved_mib as usize * MIB_BYTES
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

/// Exclusive offline admission covering memory, one job, and the fixed CPU pool.
///
/// Dropping an unused lease releases every permit. Once submitted through
/// [`ComputeExecutor::run_admitted`], the actual Rayon work owns the permits
/// until its closure and cleanup finish, independently of caller cancellation.
#[derive(Debug)]
pub struct OfflineComputeLease {
    job: OwnedSemaphorePermit,
    cpu: OwnedSemaphorePermit,
    memory: OwnedSemaphorePermit,
}

struct OfflineCpuLeases {
    _job: OwnedSemaphorePermit,
    _cpu: OwnedSemaphorePermit,
}

/// The only production owner of application Rayon pools.
pub struct ComputeExecutor {
    serving_pool: ThreadPool,
    offline_pool: ThreadPool,
    security_pool: ThreadPool,
    serving_cpu: Arc<Semaphore>,
    offline_cpu: Arc<Semaphore>,
    security_cpu: Arc<Semaphore>,
    offline_jobs: Arc<Semaphore>,
    offline_memory: Arc<Semaphore>,
}

impl ComputeExecutor {
    pub fn new() -> QuantResult<Self> {
        let serving_pool = build_pool("quant-serving", SERVING_THREADS)?;
        let offline_pool = build_pool("quant-offline", OFFLINE_THREADS)?;
        let security_pool = build_pool("quant-security", SECURITY_THREADS)?;
        Ok(Self {
            serving_pool,
            offline_pool,
            security_pool,
            serving_cpu: Arc::new(Semaphore::new(SERVING_THREADS)),
            offline_cpu: Arc::new(Semaphore::new(OFFLINE_THREADS)),
            security_cpu: Arc::new(Semaphore::new(SECURITY_THREADS)),
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

    /// Run credential hashing or verification on the isolated single-thread
    /// security pool. Authentication never occupies an Actix/Tokio worker and
    /// cannot queue behind report-serving or offline research computations.
    pub async fn run_security<T, F>(&self, work: F) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        let cpu = Arc::clone(&self.security_cpu)
            .acquire_many_owned(SECURITY_CPU_PERMITS)
            .await
            .map_err(|_| compute_closed("security CPU"))?;
        let (sender, result) = oneshot::channel();
        self.security_pool.spawn(move || {
            let _cpu = cpu;
            let _ = sender.send(catch_compute(work));
        });
        ComputeTask { result }.join().await
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

    /// Attempt complete offline admission without entering any semaphore queue.
    ///
    /// Capacity pressure returns `None`. Acquisitions use the same memory ->
    /// job -> CPU order as every blocking path, and a partial acquisition is
    /// released before returning. Closed resources remain typed failures.
    pub fn try_acquire_offline(
        &self,
        memory: OfflineMemory,
    ) -> QuantResult<Option<OfflineComputeLease>> {
        for (semaphore, resource) in [
            (&self.offline_memory, "offline memory"),
            (&self.offline_jobs, "offline job"),
            (&self.offline_cpu, "offline CPU"),
        ] {
            if semaphore.is_closed() {
                return Err(compute_closed(resource).into());
            }
        }
        let Some(memory) = try_acquire(
            Arc::clone(&self.offline_memory),
            memory.permits_mib,
            "offline memory",
        )?
        else {
            return Ok(None);
        };
        let Some(job) = try_acquire(Arc::clone(&self.offline_jobs), 1, "offline job")? else {
            return Ok(None);
        };
        let Some(cpu) = try_acquire(
            Arc::clone(&self.offline_cpu),
            OFFLINE_CPU_PERMITS,
            "offline CPU",
        )?
        else {
            return Ok(None);
        };
        Ok(Some(OfflineComputeLease { job, cpu, memory }))
    }

    /// Submit an already admitted pure CPU closure to this executor's offline pool.
    ///
    /// No further resource wait occurs. A foreign lease is rejected before
    /// spawning; after spawning, cancellation drops only the result receiver.
    pub async fn run_admitted<T, F>(&self, lease: OfflineComputeLease, work: F) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        if !Arc::ptr_eq(&self.offline_memory, lease.memory.semaphore())
            || !Arc::ptr_eq(&self.offline_jobs, lease.job.semaphore())
            || !Arc::ptr_eq(&self.offline_cpu, lease.cpu.semaphore())
        {
            return Err(InfraError::ComputeExecution {
                detail: "offline compute lease belongs to another compute executor".to_owned(),
            }
            .into());
        }
        self.spawn_offline_with_leases(lease, work).join().await
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

    /// Reserve offline memory independently of CPU execution so owned
    /// artifacts remain governed while crossing async I/O and database awaits.
    pub async fn acquire_offline_memory(
        &self,
        memory: OfflineMemory,
    ) -> QuantResult<OfflineMemoryLease> {
        self.acquire_memory(memory, None).await
    }

    /// Cancellation-aware counterpart to [`Self::acquire_offline_memory`].
    pub async fn acquire_offline_memory_cancellable(
        &self,
        memory: OfflineMemory,
        cancel: &CancellationToken,
    ) -> QuantResult<OfflineMemoryLease> {
        self.acquire_memory(memory, Some(cancel)).await
    }

    async fn acquire_memory(
        &self,
        memory: OfflineMemory,
        cancel: Option<&CancellationToken>,
    ) -> QuantResult<OfflineMemoryLease> {
        let permit = acquire(
            Arc::clone(&self.offline_memory),
            memory.permits_mib,
            cancel,
            "offline memory",
        )
        .await?;
        Ok(OfflineMemoryLease {
            reservation: Arc::new(OfflineMemoryReservation {
                owner: Arc::clone(&self.offline_memory),
                _permit: permit,
                reserved_mib: memory.permits_mib,
            }),
        })
    }

    /// Run exclusive offline CPU work under an existing memory reservation.
    /// The borrowed lease remains live until the computation has joined.
    pub async fn run_offline_with_lease<T, F>(
        &self,
        lease: &OfflineMemoryLease,
        work: F,
    ) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        self.run_with_lease(lease, None, work).await
    }

    /// Cancellation-aware counterpart to [`Self::run_offline_with_lease`].
    pub async fn run_leased_cancellable<T, F>(
        &self,
        lease: &OfflineMemoryLease,
        cancel: &CancellationToken,
        work: F,
    ) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        self.run_with_lease(lease, Some(cancel), work).await
    }

    async fn run_with_lease<T, F>(
        &self,
        lease: &OfflineMemoryLease,
        cancel: Option<&CancellationToken>,
        work: F,
    ) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        if !Arc::ptr_eq(&self.offline_memory, &lease.reservation.owner) {
            return Err(InfraError::ComputeExecution {
                detail: "offline memory lease belongs to another compute executor".to_owned(),
            }
            .into());
        }
        let leases = self.acquire_offline_cpu(cancel).await?;
        let reservation = Arc::clone(&lease.reservation);
        let (sender, result) = oneshot::channel();
        self.offline_pool.spawn(move || {
            let _leases = leases;
            let _reservation = reservation;
            let _ = sender.send(catch_compute(work));
        });
        ComputeTask { result }.join().await
    }

    /// Currently unreserved bytes in the fixed offline-memory semaphore.
    #[must_use]
    pub fn available_offline_memory_bytes(&self) -> usize {
        self.offline_memory.available_permits() * MIB_BYTES
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

    fn spawn_offline_with_leases<T, F>(
        &self,
        leases: OfflineComputeLease,
        work: F,
    ) -> ComputeTask<T>
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
    ) -> QuantResult<OfflineComputeLease> {
        // All offline paths acquire in memory -> job -> CPU order. Owned
        // memory leases use the same order before calling
        // `acquire_offline_cpu`, preventing cross-path lock inversion.
        let memory = acquire(
            Arc::clone(&self.offline_memory),
            memory.permits_mib,
            cancel,
            "offline memory",
        )
        .await?;
        let job = acquire(Arc::clone(&self.offline_jobs), 1, cancel, "offline job").await?;
        let cpu = acquire(
            Arc::clone(&self.offline_cpu),
            OFFLINE_CPU_PERMITS,
            cancel,
            "offline CPU",
        )
        .await?;
        Ok(OfflineComputeLease { job, cpu, memory })
    }

    async fn acquire_offline_cpu(
        &self,
        cancel: Option<&CancellationToken>,
    ) -> QuantResult<OfflineCpuLeases> {
        let job = acquire(Arc::clone(&self.offline_jobs), 1, cancel, "offline job").await?;
        let cpu = acquire(
            Arc::clone(&self.offline_cpu),
            OFFLINE_CPU_PERMITS,
            cancel,
            "offline CPU",
        )
        .await?;
        Ok(OfflineCpuLeases {
            _job: job,
            _cpu: cpu,
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

fn try_acquire(
    semaphore: Arc<Semaphore>,
    permits: u32,
    resource: &'static str,
) -> QuantResult<Option<OwnedSemaphorePermit>> {
    match semaphore.try_acquire_many_owned(permits) {
        Ok(permit) => Ok(Some(permit)),
        Err(TryAcquireError::NoPermits) => Ok(None),
        Err(TryAcquireError::Closed) => Err(compute_closed(resource).into()),
    }
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

    use quant_pivot_error::{QuantError, QuantResult, infra::InfraError};
    use tokio::{sync::oneshot, task, time::timeout};
    use tokio_util::sync::CancellationToken;

    use super::{ComputeExecutor, OFFLINE_MEMORY_BYTES, OFFLINE_THREADS, OfflineMemory};

    #[test]
    fn memory_reservation_rejects_budget() {
        let error = OfflineMemory::try_bytes(OFFLINE_MEMORY_BYTES + 1)
            .expect_err("reservation must fail closed");
        assert!(error.to_string().contains("offline memory reservation"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn offline_keeps_runtime_live() -> QuantResult<()> {
        let executor = Arc::new(ComputeExecutor::new()?);
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = Arc::clone(&executor);
        let computation = task::spawn(async move {
            worker
                .run_offline(OfflineMemory::try_gib(1)?, move || {
                    let thread_name =
                        thread::current().name().map(str::to_owned).ok_or_else(|| {
                            InfraError::ComputeExecution {
                                detail: "offline worker has no thread name".to_owned(),
                            }
                        })?;
                    let _ = started_tx.send(thread_name);
                    release_rx
                        .recv()
                        .map_err(|error| InfraError::ComputeExecution {
                            detail: format!("wait for offline liveness release: {error}"),
                        })?;
                    Ok(())
                })
                .await
        });
        let thread_name = started_rx.await.map_err(|_| InfraError::ComputeExecution {
            detail: "offline liveness worker exited before start".to_owned(),
        })?;
        assert!(thread_name.starts_with("quant-offline-"));

        let (heartbeat_tx, heartbeat_rx) = oneshot::channel();
        task::spawn(async move {
            task::yield_now().await;
            let _ = heartbeat_tx.send(());
        });
        timeout(Duration::from_millis(100), heartbeat_rx)
            .await
            .map_err(|_| InfraError::ComputeExecution {
                detail: "offline kernel blocked the single Tokio worker".to_owned(),
            })?
            .map_err(|_| InfraError::ComputeExecution {
                detail: "offline liveness heartbeat sender exited".to_owned(),
            })?;

        release_tx
            .send(())
            .map_err(|error| InfraError::ComputeExecution {
                detail: format!("release offline liveness worker: {error}"),
            })?;
        computation
            .await
            .map_err(|error| InfraError::ComputeExecution {
                detail: format!("join offline liveness caller: {error}"),
            })??;
        Ok(())
    }

    #[test]
    fn busy_admission_releases_partial() -> QuantResult<()> {
        let executor = ComputeExecutor::new()?;
        let memory = OfflineMemory::try_gib(4)?;
        let job = Arc::clone(&executor.offline_jobs)
            .try_acquire_owned()
            .expect("idle job permit");
        for _ in 0..3 {
            assert!(executor.try_acquire_offline(memory)?.is_none());
            assert_eq!(
                executor.available_offline_memory_bytes(),
                OFFLINE_MEMORY_BYTES
            );
            assert_eq!(executor.offline_cpu.available_permits(), OFFLINE_THREADS);
            assert_eq!(executor.offline_jobs.available_permits(), 0);
        }
        drop(job);
        let cpu = Arc::clone(&executor.offline_cpu)
            .try_acquire_owned()
            .expect("idle CPU permit");
        for _ in 0..3 {
            assert!(executor.try_acquire_offline(memory)?.is_none());
            assert_eq!(
                executor.available_offline_memory_bytes(),
                OFFLINE_MEMORY_BYTES
            );
            assert_eq!(executor.offline_jobs.available_permits(), 1);
            assert_eq!(
                executor.offline_cpu.available_permits(),
                OFFLINE_THREADS - 1
            );
        }
        drop(cpu);
        let lease = executor
            .try_acquire_offline(memory)?
            .expect("idle offline admission");
        assert_eq!(executor.offline_jobs.available_permits(), 0);
        assert_eq!(executor.offline_cpu.available_permits(), 0);
        drop(lease);
        assert_eq!(
            executor.available_offline_memory_bytes(),
            OFFLINE_MEMORY_BYTES
        );
        assert_eq!(executor.offline_jobs.available_permits(), 1);
        assert_eq!(executor.offline_cpu.available_permits(), OFFLINE_THREADS);
        Ok(())
    }

    #[tokio::test]
    async fn memory_pressure_returns_none() -> QuantResult<()> {
        let executor = ComputeExecutor::new()?;
        let existing = executor
            .acquire_offline_memory(OfflineMemory::try_gib(7)?)
            .await?;
        let remaining = OFFLINE_MEMORY_BYTES - existing.reserved_bytes();
        assert!(
            executor
                .try_acquire_offline(OfflineMemory::try_gib(4)?)?
                .is_none()
        );
        assert_eq!(executor.available_offline_memory_bytes(), remaining);
        assert_eq!(executor.offline_jobs.available_permits(), 1);
        assert_eq!(executor.offline_cpu.available_permits(), OFFLINE_THREADS);
        drop(existing);
        let lease = executor
            .try_acquire_offline(OfflineMemory::try_gib(4)?)?
            .expect("memory restored after the independent reservation");
        drop(lease);
        assert_eq!(
            executor.available_offline_memory_bytes(),
            OFFLINE_MEMORY_BYTES
        );
        Ok(())
    }

    #[test]
    fn closed_admission_is_error() -> QuantResult<()> {
        for resource in ["offline memory", "offline job", "offline CPU"] {
            let executor = ComputeExecutor::new()?;
            match resource {
                "offline memory" => executor.offline_memory.close(),
                "offline job" => executor.offline_jobs.close(),
                _ => executor.offline_cpu.close(),
            }
            let error = executor
                .try_acquire_offline(OfflineMemory::try_gib(1)?)
                .expect_err("closed resources are not retryable capacity pressure");
            assert!(
                matches!(error, QuantError::Infra(InfraError::ComputeExecution { detail })
                if detail == format!("governed {resource} semaphore closed"))
            );
            assert_eq!(
                executor.available_offline_memory_bytes(),
                OFFLINE_MEMORY_BYTES
            );
            assert_eq!(executor.offline_jobs.available_permits(), 1);
            assert_eq!(executor.offline_cpu.available_permits(), OFFLINE_THREADS);
        }
        Ok(())
    }

    #[tokio::test]
    async fn foreign_admission_is_rejected() -> QuantResult<()> {
        let owner = ComputeExecutor::new()?;
        let foreign = ComputeExecutor::new()?;
        let invoked = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&invoked);
        let result = {
            let lease = owner
                .try_acquire_offline(OfflineMemory::try_gib(10)?)?
                .expect("owner admission");
            foreign
                .run_admitted(lease, move || {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
        };
        assert!(
            matches!(result, Err(QuantError::Infra(InfraError::ComputeExecution { detail }))
            if detail.contains("another compute executor"))
        );
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
        assert_eq!(owner.available_offline_memory_bytes(), OFFLINE_MEMORY_BYTES);
        assert_eq!(owner.offline_jobs.available_permits(), 1);
        assert_eq!(owner.offline_cpu.available_permits(), OFFLINE_THREADS);
        assert_eq!(
            foreign.available_offline_memory_bytes(),
            OFFLINE_MEMORY_BYTES
        );
        Ok(())
    }

    #[test]
    fn unpolled_admission_releases_leases() -> QuantResult<()> {
        let executor = ComputeExecutor::new()?;
        let invoked = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&invoked);
        let future = executor.run_admitted(
            executor
                .try_acquire_offline(OfflineMemory::try_gib(10)?)?
                .expect("unpolled admission"),
            move || {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        );
        drop(future);
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
        assert_eq!(
            executor.available_offline_memory_bytes(),
            OFFLINE_MEMORY_BYTES
        );
        assert_eq!(executor.offline_jobs.available_permits(), 1);
        assert_eq!(executor.offline_cpu.available_permits(), OFFLINE_THREADS);
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_admission_keeps_leases() -> QuantResult<()> {
        let executor = Arc::new(ComputeExecutor::new()?);
        let lease = executor
            .try_acquire_offline(OfflineMemory::try_gib(10)?)?
            .expect("initial admission");
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = Arc::clone(&executor);
        let caller = tokio::spawn(async move {
            worker
                .run_admitted(lease, move || {
                    let _ = started_tx.send(());
                    release_rx
                        .recv_timeout(Duration::from_secs(2))
                        .map_err(|error| InfraError::ComputeExecution {
                            detail: format!("wait for admitted worker release: {error}"),
                        })?;
                    Ok(())
                })
                .await
        });
        started_rx.await.map_err(|_| InfraError::ComputeExecution {
            detail: "admitted worker exited before its start signal".to_owned(),
        })?;
        caller.abort();
        assert!(caller.await.is_err());
        assert_eq!(executor.available_offline_memory_bytes(), 0);
        assert_eq!(executor.offline_jobs.available_permits(), 0);
        assert_eq!(executor.offline_cpu.available_permits(), 0);
        assert!(
            executor
                .try_acquire_offline(OfflineMemory::try_gib(1)?)?
                .is_none()
        );
        release_tx
            .send(())
            .map_err(|error| InfraError::ComputeExecution {
                detail: format!("release admitted worker: {error}"),
            })?;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                {
                    let admission = executor.try_acquire_offline(OfflineMemory::try_gib(10)?)?;
                    if let Some(lease) = admission {
                        drop(lease);
                        return Ok::<(), QuantError>(());
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| InfraError::ComputeExecution {
            detail: "admitted worker did not release all permits after completion".to_owned(),
        })??;
        assert_eq!(
            executor.available_offline_memory_bytes(),
            OFFLINE_MEMORY_BYTES
        );
        assert_eq!(executor.offline_jobs.available_permits(), 1);
        assert_eq!(executor.offline_cpu.available_permits(), OFFLINE_THREADS);
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_waiter_never_job() -> QuantResult<()> {
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
    async fn worker_panic_typed_error() -> QuantResult<()> {
        let executor = ComputeExecutor::new()?;
        let result = executor
            .run_serving(|| -> QuantResult<()> { panic!("boom") })
            .await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn dropped_join_keeps_completion() -> QuantResult<()> {
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
    async fn aborted_caller_keeps_reservation() -> QuantResult<()> {
        let executor = Arc::new(ComputeExecutor::new()?);
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = Arc::clone(&executor);
        let caller = tokio::spawn(async move {
            let lease = worker
                .acquire_offline_memory(OfflineMemory::try_gib(10)?)
                .await?;
            assert_eq!(lease.reserved_bytes(), OFFLINE_MEMORY_BYTES);
            let result = worker
                .run_offline_with_lease(&lease, move || {
                    let _ = started_tx.send(());
                    release_rx
                        .recv_timeout(Duration::from_secs(2))
                        .map_err(|error| InfraError::ComputeExecution {
                            detail: format!("wait for leased worker release: {error}"),
                        })?;
                    Ok(())
                })
                .await;
            drop(lease);
            result
        });
        started_rx.await.map_err(|_| InfraError::ComputeExecution {
            detail: "leased worker exited before its start signal".to_owned(),
        })?;
        caller.abort();
        assert!(caller.await.is_err());
        assert_eq!(executor.available_offline_memory_bytes(), 0);

        release_tx
            .send(())
            .map_err(|error| InfraError::ComputeExecution {
                detail: format!("release leased worker: {error}"),
            })?;
        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.available_offline_memory_bytes() != OFFLINE_MEMORY_BYTES {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| InfraError::ComputeExecution {
            detail: "leased worker did not release its memory reservation".to_owned(),
        })?;
        Ok(())
    }

    #[tokio::test]
    async fn serving_pool_remains_reservation() -> QuantResult<()> {
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

    #[tokio::test]
    async fn security_pool_stays_isolated() -> QuantResult<()> {
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
            executor.run_security(|| {
                Ok(thread::current()
                    .name()
                    .map(str::to_owned)
                    .unwrap_or_default())
            }),
        )
        .await
        .map_err(|_| InfraError::ComputeExecution {
            detail: "security work was starved by an offline reservation".to_owned(),
        })??;
        assert!(thread_name.starts_with("quant-security-"));

        release_tx
            .send(())
            .map_err(|error| InfraError::ComputeExecution {
                detail: format!("release offline worker: {error}"),
            })?;
        offline.join().await?;
        Ok(())
    }
}
