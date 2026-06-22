//! Central task registry with staged graceful shutdown.
//!
//! Every long-lived background task is registered with a [`TaskId`].
//! At shutdown the registry cancels and drains tasks in deterministic
//! [`ShutdownStage`] order so producers stop before consumers flush.

use super::{
    lifecycle::{force_exit_on_second_signal, shutdown_signal},
    task_id::TaskId,
};
use crate::observability::metrics_hub::MetricsHub;
use quant_pivot_models::enums::quant::QuantRuntimeMode;
use std::{
    mem::take,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use strum::IntoStaticStr;
use tokio::{task::AbortHandle, time::MissedTickBehavior};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info, warn};

// ── Shutdown stages ─────────────────────────────────────────────────────────

/// Ordered drain stages executed sequentially at shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IntoStaticStr)]
#[repr(u8)]
#[strum(serialize_all = "snake_case")]
pub enum ShutdownStage {
    WsIngress = 0,
    CacheWorkers = 1,
    Reconciliation = 2,
    HealthMonitor = 3,
    Detection = 4,
    Execution = 5,
    Audit = 6,
    Analytics = 7,
    Persistence = 8,
    DbClose = 9,
}

impl ShutdownStage {
    pub const COUNT: usize = 10;

    pub const ALL: [Self; Self::COUNT] = [
        Self::WsIngress,
        Self::CacheWorkers,
        Self::Reconciliation,
        Self::HealthMonitor,
        Self::Detection,
        Self::Execution,
        Self::Audit,
        Self::Analytics,
        Self::Persistence,
        Self::DbClose,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    #[must_use]
    pub const fn index(self) -> usize {
        self as usize
    }
}

// ── Task kinds ──────────────────────────────────────────────────────────────

/// Classification of every registered background task.
///
/// The mapping from [`TaskKind`] to [`ShutdownStage`] is fixed at compile
/// time so shutdown ordering is self-documenting and unambiguous.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum TaskKind {
    /// HTTP/WebSocket server + broadcaster — outward-facing request ingress,
    /// drained first so the system stops accepting requests before detection.
    ApiIngress,
    WsIngress,
    CatalogSync,
    CacheWorker,
    BookReconciliation,
    LedgerReconciliation,
    HealthMonitor,
    Detection,
    Execution,
    ExecutionHeartbeat,
    Audit,
    AnalyticsWriter,
    PositionPersistence,
    ReportScheduler,
}

impl TaskKind {
    #[must_use]
    pub const fn shutdown_stage(self) -> ShutdownStage {
        match self {
            Self::ApiIngress | Self::WsIngress | Self::CatalogSync => ShutdownStage::WsIngress,
            Self::CacheWorker => ShutdownStage::CacheWorkers,
            Self::BookReconciliation | Self::LedgerReconciliation => ShutdownStage::Reconciliation,
            Self::HealthMonitor => ShutdownStage::HealthMonitor,
            Self::Detection => ShutdownStage::Detection,
            Self::Execution | Self::ExecutionHeartbeat | Self::ReportScheduler => {
                ShutdownStage::Execution
            }
            Self::Audit => ShutdownStage::Audit,
            Self::AnalyticsWriter => ShutdownStage::Analytics,
            Self::PositionPersistence => ShutdownStage::Persistence,
        }
    }

    /// Prometheus `kind` label (`snake_case`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

// ── Shutdown budget ─────────────────────────────────────────────────────────

/// Per-stage drain budget.
#[derive(Debug, Clone)]
pub struct ShutdownBudget {
    budgets: [Duration; ShutdownStage::COUNT],
}

impl ShutdownBudget {
    #[must_use]
    pub const fn new(budgets: [Duration; ShutdownStage::COUNT]) -> Self {
        Self { budgets }
    }

    /// Per-stage drain budget derived from the runtime execution mode.
    #[must_use]
    pub const fn for_quant_mode(mode: QuantRuntimeMode) -> Self {
        match mode {
            QuantRuntimeMode::ReportOnly => Self::dry_run(),
            QuantRuntimeMode::SemiAuto | QuantRuntimeMode::AutoExecution => Self::live(),
        }
    }

    #[must_use]
    pub const fn stage(&self, stage: ShutdownStage) -> Duration {
        self.budgets[stage.index()]
    }

    #[must_use]
    pub fn total(&self) -> Duration {
        self.budgets.iter().copied().sum()
    }

    #[must_use]
    pub const fn dry_run() -> Self {
        Self::from_secs([3, 4, 3, 2, 3, 8, 3, 4, 3, 1])
    }

    #[must_use]
    pub const fn paper() -> Self {
        Self::from_secs([3, 6, 3, 2, 3, 15, 5, 5, 5, 3])
    }

    #[must_use]
    pub const fn live() -> Self {
        Self::from_secs([5, 10, 5, 2, 5, 20, 5, 5, 5, 3])
    }

    #[must_use]
    pub const fn default_dry_run() -> Self {
        Self::dry_run()
    }

    #[must_use]
    const fn from_secs(secs: [u64; ShutdownStage::COUNT]) -> Self {
        let mut budgets = [Duration::from_secs(0); ShutdownStage::COUNT];
        let mut i = 0;
        while i < ShutdownStage::COUNT {
            budgets[i] = Duration::from_secs(secs[i]);
            i += 1;
        }
        Self { budgets }
    }
}

impl Default for ShutdownBudget {
    fn default() -> Self {
        Self::dry_run()
    }
}

// ── Internal tracking ───────────────────────────────────────────────────────

struct StageBucket {
    tracker: TaskTracker,
    aborts: Vec<AbortHandle>,
    exited: Arc<AtomicUsize>,
    panicked: Arc<AtomicUsize>,
}

impl Default for StageBucket {
    fn default() -> Self {
        Self {
            tracker: TaskTracker::new(),
            aborts: Vec::new(),
            exited: Arc::new(AtomicUsize::new(0)),
            panicked: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[derive(Clone, Default)]
pub struct DrainTelemetry {
    pub metrics: Option<Arc<MetricsHub>>,
}

impl DrainTelemetry {
    #[must_use]
    pub const fn with_metrics(metrics: Arc<MetricsHub>) -> Self {
        Self {
            metrics: Some(metrics),
        }
    }
}

type PendingSpawnFn =
    Box<dyn FnOnce(CancellationToken) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

pub struct PendingTask {
    pub id: TaskId,
    pub future_factory: PendingSpawnFn,
}

#[derive(Clone, Default)]
pub struct PendingTaskQueue {
    inner: Arc<std::sync::Mutex<Vec<PendingTask>>>,
}

impl PendingTaskQueue {
    pub fn push<F, Fut>(&self, id: TaskId, factory: F)
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let factory: PendingSpawnFn = Box::new(move |tok| {
            Box::pin(factory(tok)) as std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
        });
        if let Ok(mut guard) = self.inner.lock() {
            guard.push(PendingTask {
                id,
                future_factory: factory,
            });
        }
    }

    pub fn drain(&self) -> Vec<PendingTask> {
        self.inner
            .lock()
            .map(|mut g| take(&mut *g))
            .unwrap_or_default()
    }
}

// ── Task registry ─────────────────────────────────────────────────────────────

pub struct TaskRegistry {
    root: CancellationToken,
    stage_tokens: [CancellationToken; ShutdownStage::COUNT],
    stages: [StageBucket; ShutdownStage::COUNT],
    telemetry: DrainTelemetry,
}

impl TaskRegistry {
    #[must_use]
    pub fn new(root_shutdown: CancellationToken) -> Self {
        let stage_tokens = core::array::from_fn(|_| root_shutdown.child_token());
        Self {
            root: root_shutdown,
            stage_tokens,
            stages: core::array::from_fn(|_| StageBucket::default()),
            telemetry: DrainTelemetry::default(),
        }
    }

    #[must_use]
    pub fn with_telemetry(mut self, telemetry: DrainTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    #[must_use]
    pub fn root_token(&self) -> CancellationToken {
        self.root.clone()
    }

    #[must_use]
    pub fn stage_token(&self, id: TaskId) -> CancellationToken {
        self.stage_tokens[id.kind().shutdown_stage().index()].clone()
    }

    pub fn spawn<F, Fut>(&mut self, id: TaskId, f: F) -> &mut Self
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let stage = id.kind().shutdown_stage();
        let stage_idx = stage.index();
        let token = self.stage_tokens[stage_idx].clone();
        let bucket = &mut self.stages[stage_idx];

        let name = id.display_name();
        let kind = id.kind().as_str();
        let exited = Arc::clone(&bucket.exited);
        let panicked = Arc::clone(&bucket.panicked);

        let inner = tokio::spawn(f(token));
        bucket.aborts.push(inner.abort_handle());

        bucket.tracker.spawn(async move {
            match inner.await {
                Ok(()) => {
                    exited.fetch_add(1, Ordering::Relaxed);
                    info!(stage = stage.as_str(), task = %name, kind, "Task exited cleanly");
                }
                Err(e) if e.is_cancelled() => {
                    exited.fetch_add(1, Ordering::Relaxed);
                    info!(stage = stage.as_str(), task = %name, kind, "Task cancelled");
                }
                Err(e) => {
                    panicked.fetch_add(1, Ordering::Relaxed);
                    error!(
                        stage = stage.as_str(),
                        task = %name,
                        kind,
                        error = %e,
                        "Task panicked during drain"
                    );
                }
            }
        });

        self
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.stages.iter().map(|bucket| bucket.tracker.len()).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.stages.iter().all(|bucket| bucket.tracker.is_empty())
    }

    pub async fn drain(mut self, budget: ShutdownBudget) {
        let total_tasks = self.len();
        info!(
            total_tasks,
            total_budget_secs = budget.total().as_secs(),
            "Staged drain starting"
        );

        for stage in ShutdownStage::ALL {
            let idx = stage.index();
            self.stage_tokens[idx].cancel();
            let bucket = take(&mut self.stages[idx]);
            drain_stage(
                stage,
                bucket,
                budget.stage(stage),
                self.telemetry.metrics.as_deref(),
            )
            .await;
            self.stages[idx] = StageBucket::default();
        }

        info!("Staged drain complete");
    }
}

async fn drain_stage(
    stage: ShutdownStage,
    bucket: StageBucket,
    budget: Duration,
    metrics: Option<&MetricsHub>,
) {
    if bucket.tracker.is_empty() {
        if let Some(m) = metrics {
            m.set_shutdown_stage_remaining(stage.as_str(), 0);
        }
        return;
    }

    let count = bucket.tracker.len();
    info!(
        stage = stage.as_str(),
        tasks = count,
        budget_secs = budget.as_secs(),
        "Stage drain started"
    );

    bucket.tracker.close();
    let start = Instant::now();
    let wait = bucket.tracker.wait();
    tokio::pin!(wait);

    let deadline = tokio::time::sleep(budget);
    tokio::pin!(deadline);

    let mut heartbeat = tokio::time::interval(Duration::from_secs(2));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    heartbeat.tick().await;

    loop {
        tokio::select! {
            biased;
            () = &mut deadline => {
                let abandoned = bucket.tracker.len();
                let exited = bucket.exited.load(Ordering::Relaxed);
                let panicked = bucket.panicked.load(Ordering::Relaxed);
                warn!(
                    stage = stage.as_str(),
                    budget_secs = budget.as_secs(),
                    abandoned_tasks = abandoned,
                    exited_tasks = exited,
                    panicked_tasks = panicked,
                    "Stage drain timeout — aborting remaining tasks (STATE-AT-RISK)"
                );
                if let Some(m) = metrics {
                    m.record_shutdown_timeout(stage.as_str(), abandoned);
                }
                for abort in &bucket.aborts {
                    abort.abort();
                }
                let _ = tokio::time::timeout(Duration::from_millis(100), &mut wait).await;
                break;
            }
            () = &mut wait => break,
            _ = heartbeat.tick() => {
                let remaining = bucket.tracker.len();
                let exited = bucket.exited.load(Ordering::Relaxed);
                let panicked = bucket.panicked.load(Ordering::Relaxed);
                info!(
                    stage = stage.as_str(),
                    remaining,
                    exited,
                    panicked,
                    elapsed_ms = start.elapsed().as_millis(),
                    budget_secs = budget.as_secs(),
                    "Stage drain progress"
                );
                if let Some(m) = metrics {
                    m.set_shutdown_stage_remaining(stage.as_str(), remaining);
                }
            }
        }
    }

    let exited = bucket.exited.load(Ordering::Relaxed);
    let panicked = bucket.panicked.load(Ordering::Relaxed);
    info!(
        stage = stage.as_str(),
        elapsed_ms = start.elapsed().as_millis(),
        tasks = count,
        exited,
        panicked,
        "Stage drain finished"
    );
    if let Some(m) = metrics {
        m.set_shutdown_stage_remaining(stage.as_str(), 0);
    }
}

// ── App runner ────────────────────────────────────────────────────────────────

pub struct AppRunner {
    shutdown: CancellationToken,
    registry: TaskRegistry,
    budget: ShutdownBudget,
    pending_tasks: PendingTaskQueue,
}

impl AppRunner {
    #[must_use]
    pub fn new(shutdown: CancellationToken) -> Self {
        Self::for_quant_mode(shutdown, QuantRuntimeMode::ReportOnly)
    }

    #[must_use]
    pub fn for_quant_mode(shutdown: CancellationToken, mode: QuantRuntimeMode) -> Self {
        Self {
            shutdown: shutdown.clone(),
            registry: TaskRegistry::new(shutdown),
            budget: ShutdownBudget::for_quant_mode(mode),
            pending_tasks: PendingTaskQueue::default(),
        }
    }

    #[must_use]
    pub const fn with_shutdown_budget(mut self, budget: ShutdownBudget) -> Self {
        self.budget = budget;
        self
    }

    #[must_use]
    pub fn with_drain_telemetry(mut self, telemetry: DrainTelemetry) -> Self {
        self.registry = self.registry.with_telemetry(telemetry);
        self
    }

    #[must_use]
    pub const fn pending_tasks(&self) -> &PendingTaskQueue {
        &self.pending_tasks
    }

    pub const fn registry_mut(&mut self) -> &mut TaskRegistry {
        &mut self.registry
    }

    pub fn spawn<F, Fut>(&mut self, id: TaskId, f: F) -> &mut Self
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.registry.spawn(id, f);
        self
    }

    fn absorb_pending_tasks(&mut self) {
        let pending = self.pending_tasks.drain();
        if pending.is_empty() {
            return;
        }
        tracing::info!(
            count = pending.len(),
            "Registering subsystem-queued pending tasks"
        );
        for p in pending {
            let PendingTask { id, future_factory } = p;
            self.registry.spawn(id, move |token| async move {
                future_factory(token).await;
            });
        }
    }

    pub fn absorb_pending_queue(&mut self, queue: &PendingTaskQueue) {
        for PendingTask { id, future_factory } in queue.drain() {
            self.spawn(id, move |tok| async move { future_factory(tok).await });
        }
    }

    pub fn registry_len(&self) -> usize {
        self.registry.len()
    }

    pub async fn run(mut self) -> Result<(), quant_pivot_error::QuantError> {
        self.absorb_pending_tasks();

        let root = self.shutdown.clone();
        tokio::spawn(shutdown_signal(root.clone()));

        info!(
            tasks = self.registry.len(),
            total_budget_secs = self.budget.total().as_secs(),
            "quant-pivot is running — press Ctrl+C to stop",
        );

        root.cancelled().await;
        info!("Shutdown signal received — draining tasks");

        let force_exit_guard = tokio::spawn(force_exit_on_second_signal());
        self.registry.drain(self.budget).await;
        force_exit_guard.abort();

        info!("Shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn tick_budget() -> ShutdownBudget {
        let mut b = [Duration::from_secs(0); ShutdownStage::COUNT];
        for slot in &mut b {
            *slot = Duration::from_millis(500);
        }
        ShutdownBudget::new(b)
    }

    #[test]
    fn strum_labels_match_prometheus_names() {
        assert_eq!(ShutdownStage::Execution.as_str(), "execution");
        assert_eq!(TaskKind::AnalyticsWriter.as_str(), "analytics_writer");
    }

    #[tokio::test]
    async fn stage_order_is_respected() {
        let root = CancellationToken::new();
        let mut registry = TaskRegistry::new(root.clone());

        let order = Arc::new(tokio::sync::Mutex::new(Vec::<&'static str>::new()));
        let order_ws = Arc::clone(&order);
        let order_cache = Arc::clone(&order);
        let order_persist = Arc::clone(&order);

        registry.spawn(TaskId::DataPipeline, move |token| async move {
            token.cancelled().await;
            order_ws.lock().await.push("ws");
        });
        registry.spawn(TaskId::Coalescer, move |token| async move {
            token.cancelled().await;
            order_cache.lock().await.push("cache");
        });
        registry.spawn(TaskId::RiskStatePersist, move |token| async move {
            token.cancelled().await;
            order_persist.lock().await.push("persist");
        });

        root.cancel();
        registry.drain(tick_budget()).await;

        assert_eq!(*order.lock().await, vec!["ws", "cache", "persist"]);
    }

    #[tokio::test]
    async fn task_id_resolves_kind_and_name() {
        assert_eq!(TaskId::RiskAuditBatch.kind(), TaskKind::Audit);
        assert_eq!(TaskId::RiskAuditBatch.static_name(), "risk-audit-batch");
        assert_eq!(
            TaskId::ExecutionRunner { shard: 2 }.display_name(),
            "execution-runner-2"
        );
    }

    #[tokio::test]
    async fn shutdown_budget_scales_with_execution_mode() {
        let report_only = ShutdownBudget::for_quant_mode(QuantRuntimeMode::ReportOnly);
        let auto = ShutdownBudget::for_quant_mode(QuantRuntimeMode::AutoExecution);
        assert_eq!(
            report_only.stage(ShutdownStage::Execution),
            Duration::from_secs(8)
        );
        assert_eq!(
            auto.stage(ShutdownStage::Execution),
            Duration::from_secs(20)
        );
    }

    #[tokio::test]
    async fn timeout_aborts_stubborn_tasks() {
        let root = CancellationToken::new();
        let mut registry = TaskRegistry::new(root.clone());

        let stubborn_ran = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&stubborn_ran);
        registry.spawn(TaskId::DataPipeline, move |_token| async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            observed.store(true, Ordering::Relaxed);
        });

        let mut budget = [Duration::from_secs(0); ShutdownStage::COUNT];
        budget[ShutdownStage::WsIngress.index()] = Duration::from_millis(50);
        let budget = ShutdownBudget::new(budget);

        root.cancel();
        registry.drain(budget).await;

        assert!(!stubborn_ran.load(Ordering::Relaxed));
    }
}
