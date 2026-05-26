//! Application context — system composition root and lifecycle manager.
//!
//! `AppContext` owns all subsystem instances and orchestrates startup,
//! run, and graceful shutdown. The struct is decomposed into four bundles
//! to avoid a 40+ field god struct.

pub mod lifecycle;
pub mod task_id;
pub mod task_registry;

use std::sync::Arc;

use flume::Receiver;
use oxide_arb_algorithm::calibration::{CalibrationUpdater, ResolutionCalibrator};
use oxide_arb_api::clob::ClobClient;
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_models::config::Settings;
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_repository::postgres::PgRiskAuditRepository;
use oxide_arb_risk::audit::RiskAuditEvent;
use oxide_arb_risk::engine::RiskEngine;
use oxide_arb_storage::cache::TieredCache;
use oxide_arb_storage::clickhouse::ClickHousePool;
use oxide_arb_storage::postgres::PostgresPool;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::app::task_id::TaskId;
use crate::app::task_registry::PendingTaskQueue;
use crate::bridge::CoreOpportunityPipeline;
use crate::bridge::potential_loss_store::CorePotentialLossStore;
use crate::bridge::risk_metrics::CoreRiskMetrics;
use crate::bridge::trading_gate::TradingGate;
use crate::detection::coalescer::Coalescer;
use crate::detection::funnel::Funnel;
use crate::detection::scanner::Scanner;
use crate::execution::execution_pipeline::{ExecutionPipeline, PostTradeJob};
use crate::execution::fsm::ExecutionFSM;
use crate::execution::heartbeat::HeartbeatTask;
use crate::execution::market_inflight::MarketInFlightRegistry;
use crate::execution::runner::ExecutionRunner;
use crate::exposure::in_memory::InMemoryExposureReservation;
use crate::infra::risk_decision_audit_buffer::RiskDecisionAuditBuffer;
use crate::infra::risk_decision_audit_drain::spawn_risk_decision_audit_drain;
use crate::observability::alert_dispatcher::AlertDispatcher;
use crate::observability::metrics_hub::MetricsHub;
use crate::pipeline::book_store::BookStore;
use crate::pipeline::data_pipeline::DataPipeline;
use crate::pipeline::market_cache::MarketCache;
use crate::pipeline::market_registry::MarketRegistry;
use crate::service::risk_metrics::RiskMetricsState;

/// Infrastructure subsystem: storage, metrics, alerts.
pub struct InfraBundle {
    pub pg: Arc<PostgresPool>,
    pub ch: Arc<ClickHousePool>,
    pub cache: Arc<TieredCache>,
    pub metrics: Arc<MetricsHub>,
    pub alerts: Arc<AlertDispatcher>,
    pub risk_decision_audit: Arc<RiskDecisionAuditBuffer>,
    pub risk_decision_audit_rx: Mutex<Option<Receiver<RiskAuditEvent>>>,
}

/// Data pipeline subsystem: WS event loop, order books, market metadata.
pub struct DataBundle {
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub data_pipeline: Arc<DataPipeline>,
}

/// Risk management subsystem.
pub struct RiskBundle {
    pub engine: Arc<RiskEngine>,
    pub metrics: Arc<CoreRiskMetrics>,
    pub metrics_state: Arc<RiskMetricsState>,
    pub exposure: Arc<InMemoryExposureReservation>,
    pub potential_loss_store: Arc<CorePotentialLossStore>,
}

/// Execution subsystem wired after opportunity detection.
pub struct ExecutionBundle {
    pub pipeline: Arc<ExecutionPipeline>,
    pub market_inflight: Arc<MarketInFlightRegistry>,
    pub trading_gate: TradingGate,
    post_trade_rx: Mutex<Option<Receiver<PostTradeJob>>>,
}

impl ExecutionBundle {
    pub const fn new(
        pipeline: Arc<ExecutionPipeline>,
        market_inflight: Arc<MarketInFlightRegistry>,
        risk_engine: Arc<RiskEngine>,
        fsm: Arc<ExecutionFSM>,
        post_trade_rx: Receiver<PostTradeJob>,
    ) -> Self {
        Self {
            pipeline,
            market_inflight,
            trading_gate: TradingGate::new(risk_engine, fsm),
            post_trade_rx: Mutex::new(Some(post_trade_rx)),
        }
    }
}

/// Trading subsystem: detection, execution, algorithm.
pub struct TradingBundle {
    pub opportunity_pipeline: Arc<CoreOpportunityPipeline>,
    pub calibrator: Arc<ResolutionCalibrator>,
    pub calibration_updater: Arc<CalibrationUpdater>,
    pub scanner: Arc<Scanner>,
    pub coalescer: Arc<Coalescer>,
    pub funnel: Arc<Funnel>,
    pub fsm: Arc<ExecutionFSM>,
    pub execution: Option<ExecutionBundle>,
    pub clob_client: Arc<ClobClient>,
    pub ws_manager: Arc<ClobWsManager>,
}

/// System composition root — owns all subsystems.
///
/// Decomposed into four bundles (`InfraBundle`, `DataBundle`,
/// `RiskBundle`, `TradingBundle`) to avoid a 40+ flat-field struct.
pub struct AppContext {
    pub config: Arc<Settings>,
    pub infra: InfraBundle,
    pub data: DataBundle,
    pub risk: RiskBundle,
    pub trading: TradingBundle,
    pub shutdown: CancellationToken,
    pub pending_tasks: PendingTaskQueue,
}

impl AppContext {
    /// Queue the background task that drains pre-trade audit events to Postgres.
    ///
    /// Consumes `infra.risk_decision_audit_rx` — call at most once before `AppRunner::run`.
    pub fn queue_risk_decision_audit_drain(&self, repo: Arc<PgRiskAuditRepository>) {
        let Some(rx) = self.infra.risk_decision_audit_rx.lock().take() else {
            tracing::warn!("risk decision audit drain already registered or rx unavailable");
            return;
        };

        self.pending_tasks
            .push(TaskId::RiskAuditBatch, move |shutdown| async move {
                if let Err(error) = spawn_risk_decision_audit_drain(rx, repo, shutdown).await {
                    tracing::error!(%error, "risk decision audit drain exited with error");
                }
            });
    }

    /// Queue post-trade accounting drain (consumes `trading.execution.post_trade_rx` once).
    pub fn queue_execution_outcome_drain(&self) {
        let Some(exec) = &self.trading.execution else {
            tracing::warn!("execution outcome drain skipped — execution bundle not configured");
            return;
        };
        let Some(rx) = exec.post_trade_rx.lock().take() else {
            tracing::warn!("execution outcome drain already registered or rx unavailable");
            return;
        };

        let risk_engine = Arc::clone(&self.risk.engine);
        let risk_metrics = Arc::clone(&self.risk.metrics);
        let fsm = Arc::clone(&self.trading.fsm);

        self.pending_tasks
            .push(TaskId::ExecutionOutcomeDrain, move |shutdown| async move {
                if let Err(error) = ExecutionPipeline::spawn_outcome_drain(
                    rx,
                    risk_engine,
                    risk_metrics,
                    fsm,
                    shutdown,
                )
                .await
                {
                    tracing::error!(%error, "execution outcome drain exited with error");
                }
            });
    }

    /// Queue venue heartbeat probe (Live mode only).
    pub fn queue_execution_heartbeat(&self, interval_secs: u64, execution_mode: ExecutionMode) {
        let clob_client = Arc::clone(&self.trading.clob_client);
        let risk_engine = Arc::clone(&self.risk.engine);
        let fsm = Arc::clone(&self.trading.fsm);

        self.pending_tasks
            .push(TaskId::ExecutionHeartbeat, move |shutdown| async move {
                let task = HeartbeatTask::new(
                    clob_client,
                    risk_engine,
                    fsm,
                    interval_secs,
                    shutdown,
                    execution_mode,
                );
                if let Err(error) = task.run().await {
                    tracing::error!(%error, "execution heartbeat exited with error");
                }
            });
    }

    /// Queue one registered task per execution shard.
    pub fn queue_execution_runners(&self, runners: Vec<ExecutionRunner>) {
        if runners.is_empty() {
            tracing::warn!("no execution runners to queue");
            return;
        }

        for (index, runner) in runners.into_iter().enumerate() {
            let shard = u8::try_from(index).unwrap_or(u8::MAX);
            self.pending_tasks.push(
                TaskId::ExecutionRunner { shard },
                move |_token| async move {
                    if let Err(error) = runner.run().await {
                        tracing::error!(shard, %error, "execution runner exited with error");
                    }
                },
            );
        }
    }
}

pub use task_registry::AppRunner;
