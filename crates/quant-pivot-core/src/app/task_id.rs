//! Strongly-typed identifiers for every long-lived background task.
//!
//! [`TaskId`] is the single registration key: it resolves shutdown
//! [`TaskKind`], log labels, and Prometheus
//! dimensions without a parallel string-constant module.

use strum::IntoStaticStr;

use super::task_registry::TaskKind;

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
    /// Claims and publishes durable feedback revisions to `research.feedback`.
    FeedbackOutboxWorker,
    /// Single writer for WebSocket sessions and reverse subscription indexes.
    SessionHub,
    /// Periodic + nudged `SystemStatusChanged` pushes for dashboard clients.
    SystemStatusBroadcaster,
    /// Converges the atomic runtime-control snapshot across application instances.
    RuntimeControlSync,
    /// Coalesces per-market order-book changes into throttled `MarketBookUpdate`
    /// events for watching WebSocket sessions (off the hot path).
    BookUpdateCoalescer,

    // ── Ingress ───────────────────────────────────────────────────────
    DataPipeline,
    /// Periodically ingests Polygon `OrderFilled` logs into `quant_trade_tape`.
    TradeTapeWorker,
    /// Reconciles Market WS prints with finalized on-chain fills one-to-one.
    TradeTapeReconciliationWorker,
    /// Reconciles the capability registry into the expected-source ledger.
    DomainSourceSupervisor,
    /// Ingests Binance kline archives and incremental close observations.
    CryptoKlineIngestWorker,
    /// Ingests source-native Binance/Chainlink Crypto reports.
    CryptoLiveIngestWorker,
    /// Ingests public Polymarket RTDS Binance and Chainlink price topics.
    CryptoRtdsIngestWorker,
    /// Ingests Weather observations, forecasts and calibration history.
    WeatherIngestWorker,
    /// Ingests public precipitation, AQI, tornado, cyclone, climate, sea-ice,
    /// and wind facts through source-native durable cursors.
    WeatherPublicIngestWorker,
    /// Recovers historical GEFS cycles through independent durable cursors.
    WeatherBackfillWorker,
    /// Delivers typed derived-domain events from the `PostgreSQL` outbox to `ClickHouse`.
    DomainEventOutboxWorker,
    /// Delivers and verifies report facts before reports become actionable.
    ReportFactDeliveryWorker,

    // ── Catalog ───────────────────────────────────────────────────────
    GammaSync,
    CatalogLinkageResolver,
    ClobMarketInfoSync,
    CalibrationUpdater,

    // ── Cache / book workers ──────────────────────────────────────────
    Coalescer,

    // ── Reconciliation ────────────────────────────────────────────────
    PotentialLossEscalation,
    LedgerReconciliation,

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
    /// Seals resolution and execution outcome truth in all runtime modes.
    OutcomeReconciliationWorker,
    /// Discovers resolved account-scoped cases from durable `PostgreSQL` truth.
    SettlementDiscovery,
    /// Mints signer-free current-deployment and full-inventory readiness.
    SettlementPreflight,
    /// Sole redeem prepare/dispatch/recovery worker.
    SettlementExecution,
    /// Reconciles current-deployment redemptions initiated outside this process.
    SettlementExternalObservation,
    /// Executes permission-authorized operator approval/revocation commands.
    SettlementGovernedAction,
    /// Scans open position lots and evaluates the exit priority ladder.
    ExitMonitor,
    /// Best-effort analytics mirror for execution-order lifecycle events.
    ExecutionEventsWriter,
    /// Best-effort analytics mirror for capital-allocation ledger events.
    CapitalAllocationEventsWriter,
    /// Best-effort analytics mirror for position-lot ledger events.
    PositionEventsWriter,
    /// Best-effort analytics mirror for exit-signal evaluation audit events.
    ExitSignalEvaluationEventsWriter,

    // ── Risk / periodic ───────────────────────────────────────────────
    RiskTick,
    ExposureGc,

    // ── Audit / analytics / persistence writers ───────────────────────
    RiskAuditBatch,
    DetectionWriter,
    BookStreamSessionWriter,
    BookL2LedgerWriter,
    BookMicrostructure1sWriter,
    FactorEventsWriter,
    SignalCandidateEventsWriter,
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
    /// Advances durable feedback cycles from `ResearchJob` ledger truth.
    FeedbackCoordinator,
    /// Captures signed `ClickHouse` retention and `ReportOnly` latency evidence.
    ResearchReadinessEvidenceWorker,
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
            | Self::FeedbackOutboxWorker
            | Self::SessionHub
            | Self::BookUpdateCoalescer
            | Self::SystemStatusBroadcaster => TaskKind::ApiIngress,
            Self::DataPipeline
            | Self::TradeTapeWorker
            | Self::DomainSourceSupervisor
            | Self::CryptoKlineIngestWorker
            | Self::CryptoLiveIngestWorker
            | Self::CryptoRtdsIngestWorker
            | Self::WeatherIngestWorker
            | Self::WeatherPublicIngestWorker
            | Self::WeatherBackfillWorker => TaskKind::WsIngress,
            Self::GammaSync
            | Self::CatalogLinkageResolver
            | Self::ClobMarketInfoSync
            | Self::CalibrationUpdater => TaskKind::CatalogSync,
            Self::Coalescer => TaskKind::CacheWorker,
            Self::TradeTapeReconciliationWorker => TaskKind::BookReconciliation,
            Self::PotentialLossEscalation | Self::LedgerReconciliation => {
                TaskKind::LedgerReconciliation
            }
            Self::HealthChecker
            | Self::RiskMetricsRefresh
            | Self::DataQualityRefresh
            | Self::RuntimeControlSync => TaskKind::HealthMonitor,
            Self::ExecutionDispatcher
            | Self::EntryConditionWorker
            | Self::ExecutionBreakerTick
            | Self::ReconciliationWorker
            | Self::OutcomeReconciliationWorker
            | Self::SettlementDiscovery
            | Self::SettlementPreflight
            | Self::SettlementExecution
            | Self::SettlementExternalObservation
            | Self::SettlementGovernedAction
            | Self::ExitMonitor => TaskKind::Execution,
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
            | Self::DomainEventOutboxWorker
            | Self::ReportFactDeliveryWorker
            | Self::EntryConditionEvaluationOutboxWorker
            | Self::BookStreamSessionWriter
            | Self::BookL2LedgerWriter
            | Self::BookMicrostructure1sWriter
            | Self::FactorEventsWriter
            | Self::SignalCandidateEventsWriter
            | Self::ExecutionEventsWriter
            | Self::ExitSignalEvaluationEventsWriter
            | Self::CapitalAllocationEventsWriter
            | Self::PositionEventsWriter
            | Self::BookSnapshotPublisher => TaskKind::AnalyticsWriter,
            Self::RiskStatePersist | Self::RiskStateDebouncer => TaskKind::PositionPersistence,
            Self::ResearchJobWorker
            | Self::FeedbackCoordinator
            | Self::ResearchReadinessEvidenceWorker
            | Self::FeatureParityScheduler => TaskKind::ResearchJob,
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
