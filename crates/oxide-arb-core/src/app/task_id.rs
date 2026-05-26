//! Strongly-typed identifiers for every long-lived background task.
//!
//! [`TaskId`] is the single registration key: it resolves shutdown
//! [`TaskKind`](super::task_registry::TaskKind), log labels, and Prometheus
//! dimensions without a parallel string-constant module.

use super::task_registry::TaskKind;

/// Canonical identifier for a registered background task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskId {
    // ── Ingress ───────────────────────────────────────────────────────
    DataPipeline,

    // ── Catalog ───────────────────────────────────────────────────────
    GammaSync,
    CalibrationUpdater,

    // ── Cache / book workers ──────────────────────────────────────────
    Coalescer,

    // ── Reconciliation ────────────────────────────────────────────────
    LedgerReconcile,
    PotentialLossEscalation,

    // ── Health ────────────────────────────────────────────────────────
    HealthChecker,
    RiskMetricsRefresh,

    // ── Detection ─────────────────────────────────────────────────────
    Scanner,
    Funnel,

    // ── Execution ─────────────────────────────────────────────────────
    ExecutionRunner { shard: u8 },
    ExecutionOutcomeDrain,
    ExecutionHeartbeat,

    // ── Risk / periodic ───────────────────────────────────────────────
    RiskTick,
    ExposureGc,
    WalletBalanceRefresh,

    // ── Audit / analytics / persistence writers ───────────────────────
    RiskAuditBatch,
    OutboxFlusher,
    LifecycleWriter,
    TradeWriter,
    RiskStatePersist,
    RiskStateDebouncer,

    // ── Ops ───────────────────────────────────────────────────────────
    MetricsServer,
    ReportGenerator,
    Web,
}

impl TaskId {
    /// Shutdown taxonomy for this task.
    #[must_use]
    pub const fn kind(self) -> TaskKind {
        match self {
            Self::DataPipeline => TaskKind::WsIngress,
            Self::GammaSync | Self::CalibrationUpdater => TaskKind::CatalogSync,
            Self::Coalescer => TaskKind::CacheWorker,
            Self::LedgerReconcile | Self::PotentialLossEscalation => TaskKind::LedgerReconciliation,
            Self::HealthChecker | Self::RiskMetricsRefresh => TaskKind::HealthMonitor,
            Self::Scanner | Self::Funnel => TaskKind::Detection,
            Self::ExecutionRunner { .. } | Self::ExecutionOutcomeDrain => TaskKind::Execution,
            Self::ExecutionHeartbeat => TaskKind::ExecutionHeartbeat,
            Self::WalletBalanceRefresh => TaskKind::BalanceMonitor,
            Self::RiskTick | Self::ExposureGc | Self::ReportGenerator => TaskKind::ReportScheduler,
            Self::MetricsServer | Self::Web => TaskKind::Web,
            Self::RiskAuditBatch => TaskKind::Audit,
            Self::OutboxFlusher | Self::LifecycleWriter => TaskKind::OutboxFlusher,
            Self::TradeWriter => TaskKind::TickRecorder,
            Self::RiskStatePersist | Self::RiskStateDebouncer => TaskKind::PositionPersistence,
        }
    }

    /// Human-readable kebab-case name for structured logs.
    #[must_use]
    pub fn display_name(self) -> String {
        match self {
            Self::ExecutionRunner { shard } => format!("execution-runner-{shard}"),
            other => other.static_name().to_owned(),
        }
    }

    /// Static name for singleton tasks (no shard suffix).
    #[must_use]
    pub const fn static_name(self) -> &'static str {
        match self {
            Self::DataPipeline => "data-pipeline",
            Self::GammaSync => "gamma-sync",
            Self::CalibrationUpdater => "calibration-updater",
            Self::Coalescer => "coalescer",
            Self::LedgerReconcile => "ledger-reconcile",
            Self::PotentialLossEscalation => "potential-loss-escalation",
            Self::HealthChecker => "health-checker",
            Self::RiskMetricsRefresh => "risk-metrics-refresh",
            Self::Scanner => "scanner",
            Self::Funnel => "funnel",
            Self::ExecutionRunner { .. } => "execution-runner",
            Self::ExecutionOutcomeDrain => "execution-outcome-drain",
            Self::ExecutionHeartbeat => "execution-heartbeat",
            Self::RiskTick => "risk-tick",
            Self::ExposureGc => "exposure-gc",
            Self::WalletBalanceRefresh => "wallet-balance-refresh",
            Self::RiskAuditBatch => "risk-audit-batch",
            Self::OutboxFlusher => "outbox-flusher",
            Self::LifecycleWriter => "lifecycle-writer",
            Self::TradeWriter => "trade-writer",
            Self::RiskStatePersist => "risk-state-persist",
            Self::RiskStateDebouncer => "risk-state-debouncer",
            Self::MetricsServer => "metrics-server",
            Self::ReportGenerator => "report-generator",
            Self::Web => "web",
        }
    }
}
