//! Strongly-typed identifiers for every long-lived background task.
//!
//! [`TaskId`] is the single registration key: it resolves shutdown
//! [`TaskKind`](super::task_registry::TaskKind), log labels, and Prometheus
//! dimensions without a parallel string-constant module.

use super::task_registry::TaskKind;
use strum::IntoStaticStr;

/// Canonical identifier for a registered background task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IntoStaticStr)]
#[strum(serialize_all = "kebab-case")]
pub enum TaskId {
    // ── Ingress ───────────────────────────────────────────────────────
    DataPipeline,

    // ── Catalog ───────────────────────────────────────────────────────
    GammaSync,
    CalibrationUpdater,

    // ── Cache / book workers ──────────────────────────────────────────
    Coalescer,

    // ── Reconciliation ────────────────────────────────────────────────
    PotentialLossEscalation,
    LedgerReconciliation,
    MarketSettlement,
    MarketSettlementRetry,

    // ── Health ────────────────────────────────────────────────────────
    HealthChecker,
    RiskMetricsRefresh,

    // ── Detection ─────────────────────────────────────────────────────
    Scanner,
    Funnel,

    // ── Execution ─────────────────────────────────────────────────────
    #[strum(disabled)]
    ExecutionRunner {
        shard: u8,
    },
    PostTradeRelay,
    ExecutionHeartbeat,

    // ── Risk / periodic ───────────────────────────────────────────────
    RiskTick,
    ExposureGc,

    // ── Audit / analytics / persistence writers ───────────────────────
    RiskAuditBatch,
    ExecutionAuditWriter,
    DetectionWriter,
    TickEventsWriter,
    BookL2Writer,
    BookSnapshotWriter,
    BookSnapshotPublisher,
    RiskStatePersist,
    RiskStateDebouncer,

    // ── Ops ───────────────────────────────────────────────────────────
    ReportGenerator,
}

impl TaskId {
    /// Shutdown taxonomy for this task.
    #[must_use]
    pub const fn kind(self) -> TaskKind {
        match self {
            Self::DataPipeline => TaskKind::WsIngress,
            Self::GammaSync | Self::CalibrationUpdater => TaskKind::CatalogSync,
            Self::Coalescer => TaskKind::CacheWorker,
            Self::PotentialLossEscalation
            | Self::LedgerReconciliation
            | Self::MarketSettlement
            | Self::MarketSettlementRetry => TaskKind::LedgerReconciliation,
            Self::HealthChecker | Self::RiskMetricsRefresh => TaskKind::HealthMonitor,
            Self::Scanner | Self::Funnel => TaskKind::Detection,
            Self::ExecutionRunner { .. } | Self::PostTradeRelay => TaskKind::Execution,
            Self::ExecutionHeartbeat => TaskKind::ExecutionHeartbeat,
            Self::RiskTick | Self::ExposureGc | Self::ReportGenerator => TaskKind::ReportScheduler,
            Self::RiskAuditBatch => TaskKind::Audit,
            Self::ExecutionAuditWriter
            | Self::DetectionWriter
            | Self::TickEventsWriter
            | Self::BookL2Writer
            | Self::BookSnapshotWriter
            | Self::BookSnapshotPublisher => TaskKind::AnalyticsWriter,
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
    pub fn static_name(self) -> &'static str {
        match self {
            Self::ExecutionRunner { .. } => "execution-runner",
            other => other.into(),
        }
    }
}
