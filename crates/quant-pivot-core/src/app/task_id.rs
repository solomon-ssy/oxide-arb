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
    // ── API / WebSocket ingress ───────────────────────────────────────
    /// HTTP + WebSocket server. Drained first (stage 0) so the system stops
    /// accepting outward-facing requests before detection/execution wind down.
    WebServer,
    /// Fans `CoreEvent`s out to subscribed WebSocket sessions.
    WsBroadcaster,
    /// Periodic + nudged `SystemStatusChanged` pushes for dashboard clients.
    SystemStatusBroadcaster,
    /// Coalesces per-market order-book changes into throttled `MarketBookUpdate`
    /// events for watching WebSocket sessions (off the hot path).
    BookUpdateCoalescer,

    // ── Ingress ───────────────────────────────────────────────────────
    DataPipeline,
    /// Periodically ingests Polygon `OrderFilled` logs into `quant_trade_tape`.
    TradeTapeWorker,
    /// Periodically ingests external domain observations into `quant_domain_observation`.
    DomainIngestWorker,
    /// Dynamically ingests source-native Crypto/Weather live events from active linkages.
    DomainLiveIngestWorker,
    /// Delivers typed derived-domain events from the `PostgreSQL` outbox to `ClickHouse`.
    DomainEventOutboxWorker,
    /// Seals old `ClickHouse` partitions to Parquet before explicit deletion.
    ArchivePartitionWorker,

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
    DataQualityRefresh,

    // ── Operation log writer (web audit pipeline) ─────────────────────
    OperationLogWriter,

    // ── Execution ─────────────────────────────────────────────────────
    /// Auto-execution worker: pulls `ApprovedByPolicy` intents and submits them.
    ExecutionDispatcher,
    /// Evaluates durable recommendation condition instances in all runtime modes.
    EntryConditionWorker,
    /// Delivers committed condition evaluation traces from Postgres to `ClickHouse`.
    EntryConditionEvaluationOutboxWorker,
    /// Self-heals the execution breaker (`Degraded → Healthy` after cooldown).
    ExecutionBreakerTick,
    ReconciliationWorker,
    /// Scans open position lots and evaluates the exit priority ladder (05.6).
    ExitMonitor,
    /// Redeems resolved standard binary CTF positions and closes settlement lots.
    SettlementRedeemWorker,
    /// Writes final recommendation-attribution rows after execution reaches truth.
    AttributionWorker,
    /// Best-effort analytics mirror for final attribution events (05.7).
    AttributionEventsWriter,
    /// Best-effort analytics mirror for execution-order lifecycle events.
    ExecutionEventsWriter,
    /// Best-effort analytics mirror for capital-allocation ledger events.
    CapitalAllocationEventsWriter,
    /// Best-effort analytics mirror for position-lot ledger events.
    PositionEventsWriter,
    /// Best-effort analytics mirror for exit-signal evaluation audit events (06.1).
    ExitSignalEvaluationEventsWriter,

    // ── Risk / periodic ───────────────────────────────────────────────
    RiskTick,
    ExposureGc,

    // ── Audit / analytics / persistence writers ───────────────────────
    RiskAuditBatch,
    DetectionWriter,
    TickEventsWriter,
    BookL2ReplayWriter,
    BookSnapshotWriter,
    BookMicrostructure1sWriter,
    MarketResolutionWriter,
    FactorEventsWriter,
    SignalCandidateEventsWriter,
    RecommendationEventsWriter,
    BookSnapshotPublisher,
    RiskStatePersist,
    RiskStateDebouncer,

    // ── Ops ───────────────────────────────────────────────────────────
    ReportGenerator,
    /// Rolls reports up to `Expired` once all their recommendations are terminal.
    ReportExpireSweep,
    /// Best-effort strategy-capital equity history snapshots between reports.
    EquitySnapshotWorker,
    /// Expires recommendations past their data-driven `valid_until` and cascades
    /// their reserved capital.
    RecommendationExpireSweep,
    /// Precise per-recommendation TTL wake (`DelayQueue`); `RecommendationExpireSweep`
    /// is its backstop.
    RecommendationDeadlineScheduler,
    /// Expires order intents past their `expires_at` and releases their capital.
    IntentExpireSweep,
    /// Precise per-intent TTL wake (`DelayQueue`); `IntentExpireSweep` is its backstop.
    IntentDeadlineScheduler,

    // ── Research (async long-task engine) ─────────────────────────────
    /// Leases + executes durable research jobs (dataset build / model train /
    /// backtest) off the HTTP hot path, with crash recovery.
    ResearchJobWorker,
    /// Idempotently enqueues the frozen daily 24-hour full parity replay.
    FeatureParityScheduler,
}

impl TaskId {
    /// Shutdown taxonomy for this task.
    #[must_use]
    pub const fn kind(self) -> TaskKind {
        match self {
            Self::WebServer
            | Self::WsBroadcaster
            | Self::BookUpdateCoalescer
            | Self::SystemStatusBroadcaster => TaskKind::ApiIngress,
            Self::DataPipeline
            | Self::TradeTapeWorker
            | Self::DomainIngestWorker
            | Self::DomainLiveIngestWorker => TaskKind::WsIngress,
            Self::GammaSync | Self::CalibrationUpdater => TaskKind::CatalogSync,
            Self::Coalescer => TaskKind::CacheWorker,
            Self::PotentialLossEscalation
            | Self::LedgerReconciliation
            | Self::MarketSettlement
            | Self::MarketSettlementRetry => TaskKind::LedgerReconciliation,
            Self::HealthChecker | Self::RiskMetricsRefresh | Self::DataQualityRefresh => {
                TaskKind::HealthMonitor
            }
            Self::ExecutionDispatcher
            | Self::EntryConditionWorker
            | Self::ExecutionBreakerTick
            | Self::ReconciliationWorker
            | Self::ExitMonitor
            | Self::SettlementRedeemWorker
            | Self::AttributionWorker => TaskKind::Execution,
            Self::RiskTick
            | Self::ExposureGc
            | Self::ReportGenerator
            | Self::ReportExpireSweep
            | Self::EquitySnapshotWorker
            | Self::RecommendationExpireSweep
            | Self::RecommendationDeadlineScheduler
            | Self::IntentExpireSweep
            | Self::IntentDeadlineScheduler => TaskKind::ReportScheduler,
            Self::RiskAuditBatch | Self::OperationLogWriter => TaskKind::Audit,
            Self::DetectionWriter
            | Self::ArchivePartitionWorker
            | Self::DomainEventOutboxWorker
            | Self::EntryConditionEvaluationOutboxWorker
            | Self::TickEventsWriter
            | Self::BookL2ReplayWriter
            | Self::BookSnapshotWriter
            | Self::BookMicrostructure1sWriter
            | Self::MarketResolutionWriter
            | Self::FactorEventsWriter
            | Self::SignalCandidateEventsWriter
            | Self::RecommendationEventsWriter
            | Self::AttributionEventsWriter
            | Self::ExecutionEventsWriter
            | Self::ExitSignalEvaluationEventsWriter
            | Self::CapitalAllocationEventsWriter
            | Self::PositionEventsWriter
            | Self::BookSnapshotPublisher => TaskKind::AnalyticsWriter,
            Self::RiskStatePersist | Self::RiskStateDebouncer => TaskKind::PositionPersistence,
            Self::ResearchJobWorker | Self::FeatureParityScheduler => TaskKind::ResearchJob,
        }
    }

    /// Human-readable kebab-case name for structured logs.
    #[must_use]
    pub fn display_name(self) -> String {
        self.static_name().to_owned()
    }

    /// Static name for singleton tasks (no shard suffix).
    #[must_use]
    pub fn static_name(self) -> &'static str {
        self.into()
    }
}
